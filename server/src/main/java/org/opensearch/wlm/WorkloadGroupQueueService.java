/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.util.concurrent.KeyedLock;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.stats.WorkloadGroupState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Coordinator-local owner of the per-workload-group request queues. When {@link WorkloadGroupService} would reject a
 * search at a throttle limit, it parks the request here instead (if queueing is enabled and the group's queue has
 * room). A parked request holds no thread — only its {@link ActionListener} and open connection — and is admitted
 * later when a permit frees:
 * <ul>
 *   <li><b>node tier</b> — a completing request's {@code close()} calls {@link #drainNode} for the freed bucket
 *       (immediate, local);</li>
 *   <li><b>cluster tier</b> — a bucket owner pushes a grant carrying a reserved lease, handled via
 *       {@link #admitWithPermit} (see {@link WorkloadGroupSharedThrottleService});</li>
 *   <li><b>backstop</b> — {@link #sweep} (run on the {@link WorkloadGroupService} scheduled loop) evicts timed-out and
 *       cancelled requests and re-attempts admission for the rest, covering a lost grant or a ring remap.</li>
 * </ul>
 * Concurrency: each bucket's deque is guarded by a per-bucket lock ({@link KeyedLock}); the shared depth budget is an
 * atomic inside {@link WorkloadGroupQueue}. A parked listener is never completed while a bucket lock is held — every
 * admission dispatches the listener completion onto an executor to avoid running the downstream search inline on the
 * releasing/grant/sweep thread (and to avoid deep recursion when many drain at once).
 */
@ExperimentalApi
public class WorkloadGroupQueueService {

    private final ThreadPool threadPool;
    private final WorkloadGroupsStateAccessor stateAccessor;
    // One queue per group id. Created lazily on first enqueue for a group; the value captures the group's size at
    // creation. A group whose queue size changes keeps its old capacity until its queue drains and is recreated — an
    // accepted simplification (queue size is rarely retuned live).
    private final Map<String, WorkloadGroupQueue> queuesByGroup = new ConcurrentHashMap<>();
    private final KeyedLock<String> bucketLocks = new KeyedLock<>();

    public WorkloadGroupQueueService(ThreadPool threadPool, WorkloadGroupsStateAccessor stateAccessor) {
        this.threadPool = threadPool;
        this.stateAccessor = stateAccessor;
    }

    /**
     * Parks a throttle-denied request. Returns {@code false} if queueing is disabled ({@code size <= 0}) or the group's
     * queue is full — the caller then rejects with a 429. On success the request holds no thread; it is admitted later
     * by a drain, or failed by the sweep on timeout, or evicted+failed on task cancellation.
     *
     * @param groupId       the workload group id (queue identity + stat key)
     * @param bucketKey     the throttle bucket the request is waiting on
     * @param task          the search task (observed for cancellation)
     * @param size          the group's configured {@code queue.size}
     * @param timeoutNanos  the group's configured {@code queue.timeout} in nanos ({@code 0} = no timeout)
     * @param listener      the parked admission listener (already context-preserving)
     * @return {@code true} if enqueued, {@code false} if rejected (disabled/full)
     */
    public boolean tryEnqueue(
        String groupId,
        String bucketKey,
        WorkloadGroupTask task,
        String principal,
        int size,
        long timeoutNanos,
        ActionListener<Releasable> listener
    ) {
        if (size <= 0) {
            return false; // queueing disabled: not a queue rejection, the caller rejects with the normal throttle 429
        }
        WorkloadGroupQueue queue = queuesByGroup.computeIfAbsent(groupId, k -> new WorkloadGroupQueue(size));
        long deadlineNanos = timeoutNanos > 0 ? threadPool.relativeTimeInNanos() + timeoutNanos : 0L;
        WorkloadGroupQueue.QueuedRequest req = new WorkloadGroupQueue.QueuedRequest(listener, bucketKey, task, principal, deadlineNanos);

        final boolean enqueued;
        try (Releasable ignored = bucketLocks.acquire(lockKey(groupId, bucketKey))) {
            enqueued = queue.offer(req);
        }
        if (enqueued == false) {
            incrementQueueRejection(groupId); // queue full
            return false;
        }
        incrementQueued(groupId);
        // Register the cancellation callback AFTER enqueue so it can reference the enqueued request. addOnCancelledCallback
        // runs the callback immediately if the task is already cancelled, closing the race where cancellation lands
        // between enqueue and registration.
        req.cancellationHandle = task.addOnCancelledCallback(() -> evictCancelled(groupId, req));
        return true;
    }

    /**
     * Node-tier drain: a node permit for {@code bucketKey} just freed on this coordinator. Admit the oldest waiter for
     * that bucket if a node permit can be re-acquired. Admits at most one (one freed permit == one slot). The
     * {@code nodeAcquire} function re-acquires a node-tier permit for the bucket (or returns {@code null} if a racing
     * arrival took the freed slot). Guarded so it is only called when the queue is non-empty (see caller).
     */
    public void drainNode(String groupId, String bucketKey, Function<String, Releasable> nodeAcquire) {
        WorkloadGroupQueue queue = queuesByGroup.get(groupId);
        if (queue == null) {
            return;
        }
        WorkloadGroupQueue.QueuedRequest req;
        Releasable permit;
        try (Releasable ignored = bucketLocks.acquire(lockKey(groupId, bucketKey))) {
            // Peek-then-acquire-then-remove under the lock: only remove a waiter once we hold a permit for it, so a
            // failed re-acquire never drops a request from the queue.
            if (hasWaiter(queue, bucketKey) == false) {
                return;
            }
            permit = nodeAcquire.apply(bucketKey);
            if (permit == null) {
                return; // slot taken by a racing arrival; leave the waiter queued
            }
            req = queue.pollOldest(bucketKey);
            if (req == null) {
                // No waiter after all (shouldn't happen under the lock, but be safe): release the permit we took.
                permit.close();
                return;
            }
        }
        admit(req, permit);
    }

    /**
     * Cluster-tier drain ({@link WorkloadGroupSharedThrottleService.GrantConsumer}): the bucket owner pushed a grant
     * carrying a reserved shared permit. Hand it to the oldest waiter for {@code bucketKey}. If there is no drainable
     * waiter, returns {@code false} so the shared service returns the reserved permit to the next waiter.
     *
     * @return {@code true} if a waiter was admitted with the permit, {@code false} if there was none (caller releases)
     */
    public boolean admitWithPermit(String bucketKey, Releasable permit) {
        String groupId = groupIdOf(bucketKey);
        WorkloadGroupQueue queue = queuesByGroup.get(groupId);
        if (queue == null) {
            return false;
        }
        WorkloadGroupQueue.QueuedRequest req;
        try (Releasable ignored = bucketLocks.acquire(lockKey(groupId, bucketKey))) {
            req = queue.pollOldest(bucketKey);
        }
        if (req == null) {
            return false;
        }
        admit(req, permit);
        return true;
    }

    // The group id is the bucketKey prefix before the first ':' ("<groupId>:group" or "<groupId>:<attr>:<value>", see
    // WorkloadGroupService.buildBucketKey). Group ids are base64 UUIDs with no ':', so the first ':' is unambiguous.
    private static String groupIdOf(String bucketKey) {
        int idx = bucketKey.indexOf(':');
        return idx < 0 ? bucketKey : bucketKey.substring(0, idx);
    }

    /**
     * Backstop sweep, run on the {@link WorkloadGroupService} scheduled loop. Evicts timed-out (→ 429) and cancelled
     * requests, and re-attempts admission for still-parked requests via {@code reattempt} (which runs full throttle
     * admission asynchronously and completes/leaves the listener). This is the correctness net behind node-completion
     * and owner-push drains — it recovers a queued request stranded by a lost grant or a ring remap.
     *
     * @param reattempt re-runs admission for a still-parked request (typically {@link WorkloadGroupService#reattemptAdmission})
     */
    public void sweep(Reattempt reattempt) {
        long now = threadPool.relativeTimeInNanos();
        for (Map.Entry<String, WorkloadGroupQueue> entry : queuesByGroup.entrySet()) {
            String groupId = entry.getKey();
            WorkloadGroupQueue queue = entry.getValue();
            // Snapshot bucket keys so we don't iterate a map being mutated by concurrent drains.
            for (String bucketKey : new ArrayList<>(queue.bucketKeys())) {
                List<WorkloadGroupQueue.QueuedRequest> expired = new ArrayList<>();
                List<WorkloadGroupQueue.QueuedRequest> retry = new ArrayList<>();
                // Pull the whole bucket under the lock, classifying each entry, and DO NOT re-enqueue survivors here:
                // a survivor is handed to reattempt(), which re-runs admission and re-parks it via the normal enqueue
                // path if it is still denied. Re-offering here as well would double-enqueue it. Everything the sweep
                // removes is therefore acted on (failed or re-attempted) after the lock is released.
                try (Releasable ignored = bucketLocks.acquire(lockKey(groupId, bucketKey))) {
                    WorkloadGroupQueue.QueuedRequest req;
                    while ((req = queue.pollOldest(bucketKey)) != null) {
                        if (req.task().isCancelled() || req.isExpired(now)) {
                            expired.add(req);
                        } else {
                            retry.add(req);
                        }
                    }
                }
                for (WorkloadGroupQueue.QueuedRequest req : expired) {
                    failTimedOutOrCancelled(groupId, req);
                }
                // Re-attempt in original (oldest-first) order. Each re-attempt either admits, re-parks (fresh enqueue),
                // or 429s — so a survivor is never left both dequeued and un-acted-on.
                for (WorkloadGroupQueue.QueuedRequest req : retry) {
                    req.releaseCancellationHandle(); // it will re-register a fresh callback if re-parked
                    reattempt.reattempt(groupId, req);
                }
            }
        }
    }

    /** Total parked requests across all groups on this coordinator. Cheap; used for the node-drain fast-out. */
    public int totalDepth() {
        int total = 0;
        for (WorkloadGroupQueue queue : queuesByGroup.values()) {
            total += queue.currentDepth();
        }
        return total;
    }

    /** Current parked depth for a group (0 if none). Package-private for stats. */
    public int currentDepth(String groupId) {
        WorkloadGroupQueue queue = queuesByGroup.get(groupId);
        return queue == null ? 0 : queue.currentDepth();
    }

    /** Peak parked depth for a group since its queue was created (0 if none). Package-private for stats. */
    public long peakDepth(String groupId) {
        WorkloadGroupQueue queue = queuesByGroup.get(groupId);
        return queue == null ? 0L : queue.peakDepth();
    }

    // --- internals ---

    // Completes a parked listener with an acquired permit, off the caller's thread. Deregisters the cancellation
    // callback first (the request is leaving the queue). If the task was cancelled in the meantime, release the permit
    // and fail rather than starting a doomed search.
    private void admit(WorkloadGroupQueue.QueuedRequest req, Releasable permit) {
        req.releaseCancellationHandle();
        if (req.task().isCancelled()) {
            permit.close();
            req.listener().onFailure(new TaskCancelledException("task cancelled while queued"));
            return;
        }
        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
            if (req.task().isCancelled()) {
                permit.close();
                req.listener().onFailure(new TaskCancelledException("task cancelled while queued"));
                return;
            }
            req.listener().onResponse(permit);
        });
    }

    // Cancellation callback: remove the entry (if still queued) and fail it. Runs on whatever thread cancelled the task.
    private void evictCancelled(String groupId, WorkloadGroupQueue.QueuedRequest req) {
        WorkloadGroupQueue queue = queuesByGroup.get(groupId);
        if (queue == null) {
            return;
        }
        final boolean removed;
        try (Releasable ignored = bucketLocks.acquire(lockKey(groupId, req.bucketKey()))) {
            removed = queue.remove(req);
        }
        if (removed) {
            req.listener().onFailure(new TaskCancelledException("task cancelled while queued"));
        }
    }

    // Fail a request the sweep evicted (timed out or cancelled). The entry is already removed from the queue.
    private void failTimedOutOrCancelled(String groupId, WorkloadGroupQueue.QueuedRequest req) {
        req.releaseCancellationHandle();
        if (req.task().isCancelled()) {
            req.listener().onFailure(new TaskCancelledException("task cancelled while queued"));
        } else {
            incrementQueueTimeout(groupId);
            req.listener()
                .onFailure(
                    new OpenSearchRejectedExecutionException("Request timed out in workload group queue after waiting for a permit.")
                );
        }
    }

    private static boolean hasWaiter(WorkloadGroupQueue queue, String bucketKey) {
        return queue.bucketKeys().contains(bucketKey);
    }

    private static String lockKey(String groupId, String bucketKey) {
        return groupId + ' ' + bucketKey;
    }

    private void incrementQueued(String groupId) {
        WorkloadGroupState state = stateAccessor.getWorkloadGroupStateMap().get(groupId);
        if (state != null) {
            state.totalQueued.inc();
        }
    }

    private void incrementQueueTimeout(String groupId) {
        WorkloadGroupState state = stateAccessor.getWorkloadGroupStateMap().get(groupId);
        if (state != null) {
            state.totalQueueTimeouts.inc();
        }
    }

    private void incrementQueueRejection(String groupId) {
        WorkloadGroupState state = stateAccessor.getWorkloadGroupStateMap().get(groupId);
        if (state != null) {
            state.totalQueueRejections.inc();
        }
    }

    /**
     * Re-attempts full throttle admission for a still-parked request. Implemented by {@link WorkloadGroupService}; the
     * queue service holds no throttle logic.
     */
    @ExperimentalApi
    @FunctionalInterface
    public interface Reattempt {
        void reattempt(String groupId, WorkloadGroupQueue.QueuedRequest req);
    }
}
