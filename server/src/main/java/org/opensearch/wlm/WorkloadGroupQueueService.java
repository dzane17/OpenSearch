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
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.stats.WorkloadGroupState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
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
 *   <li><b>backstop</b> — {@link #sweep} (run on the {@link WorkloadGroupService} scheduled loop) evicts cancelled
 *       requests and re-attempts admission for the rest, covering a lost grant or a ring remap.</li>
 * </ul>
 * Concurrency: each bucket's request set is guarded by a per-bucket lock ({@link KeyedLock}); the group-total depth
 * counter is an atomic inside {@link WorkloadGroupQueue}. A parked listener is never completed while a bucket lock is held — every
 * admission dispatches the listener completion onto an executor to avoid running the downstream search inline on the
 * releasing/grant/sweep thread (and to avoid deep recursion when many drain at once).
 */
@ExperimentalApi
public class WorkloadGroupQueueService {

    private final ThreadPool threadPool;
    private final WorkloadGroupsStateAccessor stateAccessor;
    // One queue per group id, created lazily on first enqueue. The queue holds only the parked requests + depth; the
    // per-bucket capacity (queue.size_per_bucket) is NOT baked into it — it is passed in per enqueue from live config,
    // so a dynamic queue.size_per_bucket change takes effect immediately
    // (WorkloadGroupQueue.offer(req, sizePerBucket)). The per-group ceiling is the fixed MAX_GROUP_QUEUE_DEPTH.
    private final Map<String, WorkloadGroupQueue> queuesByGroup = new ConcurrentHashMap<>();
    private final KeyedLock<String> bucketLocks = new KeyedLock<>();

    public WorkloadGroupQueueService(ThreadPool threadPool, WorkloadGroupsStateAccessor stateAccessor) {
        this.threadPool = threadPool;
        this.stateAccessor = stateAccessor;
    }

    /**
     * Parks a throttle-denied request. Returns {@code false} if queueing is disabled ({@code sizePerBucket <= 0}), the
     * request's own bucket is full, or the group's fixed total ceiling
     * ({@link WorkloadGroupQueueSettings#MAX_GROUP_QUEUE_DEPTH}) is reached — the caller then rejects with a 429. On
     * success the request holds no thread; it is admitted later by a drain, or evicted+failed on task cancellation
     * (client disconnect / {@code cancel_after_time_interval}).
     * <p>
     * There is no queue timeout: a parked request has no wall-clock deadline. Legitimate queue wait is unbounded (it
     * grows with backlog depth over throughput), so a fixed cap would eventually cancel healthy, still-connected
     * requests under a large slow burst. Clients bound their own wait via task cancellation instead.
     *
     * @param groupId       the workload group id (queue identity + stat key)
     * @param bucketKey     the throttle bucket the request is waiting on
     * @param task          the search task (observed for cancellation)
     * @param sizePerBucket the group's configured {@code queue.size_per_bucket}
     * @param listener      the parked admission listener (already context-preserving)
     * @return {@code true} if enqueued, {@code false} if rejected (disabled / bucket full / group full)
     */
    public boolean tryEnqueue(
        String groupId,
        String bucketKey,
        WorkloadGroupTask task,
        int sizePerBucket,
        ActionListener<Releasable> listener
    ) {
        if (sizePerBucket <= 0) {
            return false; // queueing disabled: not a queue rejection, the caller rejects with the normal throttle 429
        }
        // Capture the enqueue instant once for the queue-wait metric. The request is never re-parked (the sweep
        // re-attempts in place), so this instant is stable for the request's whole queue lifetime and waitNanos at
        // admit reflects the true total time parked.
        final long enqueueNanos = threadPool.relativeTimeInNanos();
        WorkloadGroupQueue.QueuedRequest req = new WorkloadGroupQueue.QueuedRequest(listener, bucketKey, task, enqueueNanos);
        if (enqueue(groupId, req, sizePerBucket) == false) {
            incrementQueueRejection(groupId); // bucket or group queue full
            return false;
        }
        incrementQueued(groupId);
        return true;
    }

    // Parks a request (fresh or re-parked) and registers its cancellation callback. Returns false if the queue is full.
    // Cancellation is registered AFTER a successful offer (it references the enqueued request) but STILL UNDER the
    // bucket lock: a concurrent drain that admits this request must take the same lock, so installing the handle inside
    // the lock guarantees a racing admit cannot poll the request before its cancellation handle exists (which would
    // leave the callback registered on an already-admitted task forever). addOnCancelledCallback runs the callback
    // immediately if the task is already cancelled, closing the race where cancellation lands during enqueue.
    private boolean enqueue(String groupId, WorkloadGroupQueue.QueuedRequest req, int sizePerBucket) {
        WorkloadGroupQueue queue = queuesByGroup.computeIfAbsent(groupId, k -> new WorkloadGroupQueue());
        try (Releasable ignored = bucketLocks.acquire(lockKey(groupId, req.bucketKey()))) {
            if (queue.offer(req, sizePerBucket) == false) {
                return false;
            }
            req.cancellationHandle = req.task().addOnCancelledCallback(() -> evictCancelled(groupId, req));
            return true;
        }
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

    /** Group ids that currently hold a queue object. Small (groups are few); used to find backlogs needing a release. */
    public Set<String> queuedGroupIds() {
        return queuesByGroup.keySet();
    }

    /**
     * Releases a group's ENTIRE backlog across all of its buckets, untracked. See
     * {@link #admitAllUntracked(String, String)} for why a no-op permit is the right thing to hand out here.
     *
     * @return the number of requests admitted
     */
    public int admitAllUntracked(String groupId) {
        WorkloadGroupQueue queue = queuesByGroup.get(groupId);
        if (queue == null) {
            return 0;
        }
        int released = 0;
        // Snapshot the bucket keys: admitAllUntracked mutates the bucket map (an emptied bucket is pruned).
        for (String bucketKey : new ArrayList<>(queue.bucketKeys())) {
            released += admitAllUntracked(groupId, bucketKey);
        }
        return released;
    }

    /**
     * Admits <em>every</em> request parked for {@code bucketKey} with an untracked (no-op) permit, i.e. releases the
     * backlog without holding any throttle slot. Called only when the group has no throttle limit left to enforce, so
     * there is nothing for these requests to wait for: continuing to hold them would be an indefinite stall, since a
     * parked request has no deadline and an unthrottled group produces no permit completions to drive a drain.
     * <p>
     * A no-op permit matches the established fail-open semantics ({@code admitWithPermit(bucketKey, () -> {})}): the
     * request runs untracked and its {@code close()} does nothing. Requests are collected under the bucket lock but
     * completed by {@link #admit} outside it, which also re-checks cancellation per request.
     *
     * @return the number of requests admitted
     */
    private int admitAllUntracked(String groupId, String bucketKey) {
        WorkloadGroupQueue queue = queuesByGroup.get(groupId);
        if (queue == null) {
            return 0;
        }
        final List<WorkloadGroupQueue.QueuedRequest> drained = new ArrayList<>();
        try (Releasable ignored = bucketLocks.acquire(lockKey(groupId, bucketKey))) {
            WorkloadGroupQueue.QueuedRequest req;
            while ((req = queue.pollOldest(bucketKey)) != null) {
                drained.add(req);
            }
        }
        for (WorkloadGroupQueue.QueuedRequest req : drained) {
            admit(req, () -> {});
        }
        return drained.size();
    }

    // The group id is the bucketKey prefix before the first ':' ("<groupId>:group" or "<groupId>:<attr>:<value>", see
    // WorkloadGroupService.buildBucketKey). Group ids are base64 UUIDs with no ':', so the first ':' is unambiguous.
    private static String groupIdOf(String bucketKey) {
        int idx = bucketKey.indexOf(':');
        return idx < 0 ? bucketKey : bucketKey.substring(0, idx);
    }

    /**
     * Backstop sweep, run on the {@link WorkloadGroupService} scheduled loop. Two jobs, both done <em>in place</em> so
     * a still-waiting request keeps its original identity:
     * <ol>
     *   <li><b>Cancelled-entry cleanup</b> — remove and fail (with {@link TaskCancelledException}) every parked request
     *       whose task was cancelled. This is defense-in-depth for the per-request cancellation callback, which normally
     *       evicts a cancelled entry immediately; the sweep catches any it missed. There is NO time-based eviction: a
     *       still-live parked request is never removed regardless of how long it has waited (queue wait is unbounded by
     *       design; a client bounds its own wait via {@code cancel_after_time_interval} / disconnect).</li>
     *   <li><b>Node-tier backstop drain</b> — for each bucket that still has a waiter, try {@code drain} (a node-tier
     *       local re-acquire) once, admitting the head if a node permit is free. This recovers a request that the
     *       node-completion chain missed. It does NOT re-contact the shared owner: the owner recovers its own lost
     *       grants (grant-failure reclaim) and crashed reservations (lease TTL), and re-registering a shared waiter
     *       every tick is exactly what caused unbounded owner-side waiter-count growth.</li>
     * </ol>
     *
     * @param drain admits the oldest waiter for a (groupId, bucketKey) against a freshly re-acquired node permit if one
     *              is available; a no-op if the bucket is empty or no permit is free (typically
     *              {@link WorkloadGroupService#sweepDrainNode})
     */
    public void sweep(SweepDrain drain) {
        for (Map.Entry<String, WorkloadGroupQueue> entry : queuesByGroup.entrySet()) {
            String groupId = entry.getKey();
            WorkloadGroupQueue queue = entry.getValue();
            // Snapshot bucket keys so we don't iterate a map being mutated by concurrent drains.
            for (String bucketKey : new ArrayList<>(queue.bucketKeys())) {
                List<WorkloadGroupQueue.QueuedRequest> evicted;
                try (Releasable ignored = bucketLocks.acquire(lockKey(groupId, bucketKey))) {
                    evicted = queue.evictCancelled(bucketKey);
                }
                for (WorkloadGroupQueue.QueuedRequest req : evicted) {
                    failCancelled(req);
                }
                // Node-tier backstop: try to admit the head if a local permit is free. drainNode holds the bucket lock
                // internally and admits at most one; safe to call even if the bucket is now empty.
                drain.drain(groupId, bucketKey);
            }
        }
        // Note: an emptied group queue's map entry is intentionally NOT pruned here. A concurrent tryEnqueue could
        // offer to the same WorkloadGroupQueue between an "is it empty" check and its removal, which would orphan that
        // freshly-parked request (present in the queue object but no longer reachable from queuesByGroup). The leak is
        // one small empty object per group ever used — negligible (groups are few and long-lived) — and not worth a
        // race to reclaim. A parked request whose task is cancelled (e.g. for a DELETED group, whose in-flight searches
        // are cancelled) is still failed here by the cancelled-entry cleanup above.
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
    // callback first (the request is leaving the queue).
    //
    // The ENTIRE body — including the cancelled-task branch that closes the permit — runs on the GENERIC executor, never
    // inline on the caller's (draining/completion) thread. This is load-bearing for recursion safety: for a node-tier
    // permit, permit.close() is the wrapNodePermit wrapper whose close() re-enters drainNode -> admit. Running close()
    // on the caller thread would let a run of consecutively-cancelled waiters recurse close -> drainNode -> admit ->
    // close ... one synchronous frame per waiter (StackOverflow under a cancel storm). Dispatching first means each such
    // hop is a fresh executor task, so the chain unwinds across tasks rather than down one stack.
    private void admit(WorkloadGroupQueue.QueuedRequest req, Releasable permit) {
        req.releaseCancellationHandle();
        // Record the queue wait at the admission instant (this thread), before the executor hop, so the metric is the
        // true time parked and not inflated by dispatch latency. Only requests that actually parked reach admit(), so
        // this counts wait among queued requests. Recorded even for a task cancelled-while-queued: it still waited.
        recordQueueWait(groupIdOf(req.bucketKey()), TimeUnit.NANOSECONDS.toMillis(req.waitNanos(threadPool.relativeTimeInNanos())));
        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
            if (req.task().isCancelled()) {
                permit.close();
                req.listener().onFailure(new TaskCancelledException("task cancelled while queued"));
                return;
            }
            req.listener().onResponse(permit);
        });
    }

    // Cancellation callback: remove the entry (if still queued) and fail it. Runs on whatever thread cancelled the task
    // — including, for a task already cancelled at enqueue time, inline under the enqueue bucket lock (KeyedLock is
    // reentrant, so re-acquiring below does not deadlock). The failure is dispatched on the GENERIC executor rather
    // than completed inline so the listener is never completed while a bucket lock is held (consistent with admit()).
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
            threadPool.executor(ThreadPool.Names.GENERIC)
                .execute(() -> req.listener().onFailure(new TaskCancelledException("task cancelled while queued")));
        }
    }

    // Fail a cancelled request the sweep evicted. The entry is already removed from the queue.
    private void failCancelled(WorkloadGroupQueue.QueuedRequest req) {
        req.releaseCancellationHandle();
        req.listener().onFailure(new TaskCancelledException("task cancelled while queued"));
    }

    private static boolean hasWaiter(WorkloadGroupQueue queue, String bucketKey) {
        return queue.bucketKeys().contains(bucketKey);
    }

    private static String lockKey(String groupId, String bucketKey) {
        return groupId + '\0' + bucketKey;
    }

    private void incrementQueued(String groupId) {
        WorkloadGroupState state = stateAccessor.getWorkloadGroupStateMap().get(groupId);
        if (state != null) {
            state.totalQueued.inc();
        }
    }

    private void recordQueueWait(String groupId, long waitMillis) {
        WorkloadGroupState state = stateAccessor.getWorkloadGroupStateMap().get(groupId);
        if (state != null) {
            state.recordQueueWaitMillis(waitMillis);
        }
    }

    private void incrementQueueRejection(String groupId) {
        WorkloadGroupState state = stateAccessor.getWorkloadGroupStateMap().get(groupId);
        if (state != null) {
            state.totalQueueRejections.inc();
        }
    }

    /**
     * Node-tier backstop drain for the sweep: admit the oldest waiter for {@code (groupId, bucketKey)} against a
     * freshly re-acquired node permit, if one is free; otherwise a no-op. Implemented by {@link WorkloadGroupService}
     * (which owns the node permit tracker) so the queue service holds no throttle logic.
     */
    @ExperimentalApi
    @FunctionalInterface
    public interface SweepDrain {
        void drain(String groupId, String bucketKey);
    }
}
