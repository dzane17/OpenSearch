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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Coordinator-local owner of the per-workload-group retained-request queues. A node-tier denial is parked directly as
 * WAITING. A shared-tier overflow is registered as PENDING_ACQUIRE before the owner RPC, then atomically transitions to
 * WAITING only if the owner denies it. A retained request holds no thread — only its {@link ActionListener}, task, and
 * open connection — and is admitted later when a permit becomes available:
 * <ul>
 *   <li><b>node tier</b> — a completing request's {@code close()} calls {@link #drainNode} for the freed bucket
 *       (immediate, local);</li>
 *   <li><b>cluster tier</b> — a bucket owner pushes a grant carrying a reserved lease, handled via
 *       {@link #admitWithPermit} (see {@link WorkloadGroupSharedThrottleService});</li>
 *   <li><b>backstop</b> — {@link #sweep} periodically re-attempts node admission for retained buckets in case an
 *       exceptional race or failure interrupted the primary handoff path.</li>
 * </ul>
 * Concurrency: each bucket's request set is guarded by a per-bucket lock ({@link KeyedLock}); group totals are atomics
 * inside {@link WorkloadGroupQueue}. A retained listener is never completed while a bucket lock is held — every
 * admission or rejection dispatches listener completion onto an executor.
 */
@ExperimentalApi
public class WorkloadGroupQueueService {

    private final ThreadPool threadPool;
    private final WorkloadGroupsStateAccessor stateAccessor;
    // One queue per group id, created lazily on first retained request. queue.size_per_bucket is passed only when an
    // entry becomes WAITING; PENDING_ACQUIRE entries consume the fixed group ceiling but no configured bucket slot.
    private final Map<String, WorkloadGroupQueue> queuesByGroup = new ConcurrentHashMap<>();
    private final KeyedLock<String> bucketLocks = new KeyedLock<>();

    @ExperimentalApi
    public enum PendingAcquireTransition {
        WAITING,
        REQUEST_GONE,
        QUEUE_FULL
    }

    public WorkloadGroupQueueService(ThreadPool threadPool, WorkloadGroupsStateAccessor stateAccessor) {
        this.threadPool = threadPool;
        this.stateAccessor = stateAccessor;
    }

    /**
     * Parks a request that is already known to be waiting (the node-tier denial path). Returns {@code false} if
     * queueing is disabled, the bucket's WAITING capacity is full, or the group's combined
     * PENDING_ACQUIRE + WAITING ceiling is reached.
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
     * @see WorkloadGroupState#totalQueued why parking does not itself count towards {@code total_queued}
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
        final long waitStartNanos = threadPool.relativeTimeInNanos();
        WorkloadGroupQueue.QueuedRequest req = new WorkloadGroupQueue.QueuedRequest(listener, bucketKey, task, waitStartNanos);
        if (enqueueWaiting(groupId, req, sizePerBucket) == false) {
            incrementQueueRejection(groupId); // bucket or group queue full
            return false;
        }
        // Count the completed wait on admission, not here, so total_queued and the wait sum describe the same requests.
        return true;
    }

    /**
     * Registers a shared-tier request as PENDING_ACQUIRE before contacting the permit owner. Pending entries do not
     * consume {@code queue.size_per_bucket}; they are bounded only by the fixed per-group retained-request ceiling.
     *
     * @return the exact entry token, or {@code null} if the per-group retained-request ceiling is full
     */
    public WorkloadGroupQueue.QueuedRequest tryRegisterPendingAcquire(
        String groupId,
        String bucketKey,
        WorkloadGroupTask task,
        ActionListener<Releasable> listener
    ) {
        WorkloadGroupQueue.QueuedRequest req = WorkloadGroupQueue.QueuedRequest.pendingAcquire(listener, bucketKey, task);
        WorkloadGroupQueue queue = queuesByGroup.computeIfAbsent(groupId, k -> new WorkloadGroupQueue());
        try (Releasable ignored = bucketLocks.acquire(lockKey(groupId, bucketKey))) {
            if (queue.offerPendingAcquire(req) == false) {
                incrementQueueRejection(groupId);
                return null;
            }
            registerCancellation(groupId, req);
            return req;
        }
    }

    // Parks a known waiter and registers its cancellation callback. Returns false if either waiting limit is full.
    // Cancellation is registered AFTER a successful offer (it references the enqueued request) but STILL UNDER the
    // bucket lock: a concurrent drain that admits this request must take the same lock, so installing the handle inside
    // the lock guarantees a racing admit cannot poll the request before its cancellation handle exists (which would
    // leave the callback registered on an already-admitted task forever). addOnCancelledCallback runs the callback
    // immediately if the task is already cancelled, closing the race where cancellation lands during enqueue.
    private boolean enqueueWaiting(String groupId, WorkloadGroupQueue.QueuedRequest req, int sizePerBucket) {
        WorkloadGroupQueue queue = queuesByGroup.computeIfAbsent(groupId, k -> new WorkloadGroupQueue());
        try (Releasable ignored = bucketLocks.acquire(lockKey(groupId, req.bucketKey()))) {
            if (queue.offer(req, sizePerBucket) == false) {
                return false;
            }
            registerCancellation(groupId, req);
            return true;
        }
    }

    private void registerCancellation(String groupId, WorkloadGroupQueue.QueuedRequest req) {
        req.cancellationHandle = req.task().addOnCancelledCallback(() -> evictCancelled(groupId, req));
    }

    /**
     * Handles the owner denial for the exact provisional entry. The transition reserves one configured WAITING slot.
     * If that bucket is full, this exact request is removed and failed; existing waiters are untouched.
     */
    public PendingAcquireTransition transitionPendingAcquireToWaiting(
        WorkloadGroupQueue.QueuedRequest req,
        int sizePerBucket,
        Exception queueFullFailure
    ) {
        final String groupId = groupIdOf(req.bucketKey());
        WorkloadGroupQueue queue = queuesByGroup.get(groupId);
        if (queue == null) {
            return PendingAcquireTransition.REQUEST_GONE;
        }
        final WorkloadGroupQueue.WaitingTransition transition;
        try (Releasable ignored = bucketLocks.acquire(lockKey(groupId, req.bucketKey()))) {
            transition = queue.transitionToWaiting(req, sizePerBucket, threadPool.relativeTimeInNanos());
        }
        if (transition == WorkloadGroupQueue.WaitingTransition.BUCKET_FULL) {
            incrementQueueRejection(groupId);
            fail(req, queueFullFailure);
            return PendingAcquireTransition.QUEUE_FULL;
        }
        if (transition == WorkloadGroupQueue.WaitingTransition.REQUEST_GONE) {
            return PendingAcquireTransition.REQUEST_GONE;
        }
        return PendingAcquireTransition.WAITING;
    }

    /**
     * Applies a granted or fail-open acquire result to its exact provisional entry. A delayed result is a no-op once
     * cancellation or an early pushed grant has already removed that entry.
     */
    public boolean admitPendingAcquire(WorkloadGroupQueue.QueuedRequest req, Releasable permit) {
        final String groupId = groupIdOf(req.bucketKey());
        WorkloadGroupQueue queue = queuesByGroup.get(groupId);
        if (queue == null) {
            return false;
        }
        final boolean removed;
        try (Releasable ignored = bucketLocks.acquire(lockKey(groupId, req.bucketKey()))) {
            removed = queue.remove(req);
        }
        if (removed == false) {
            return false;
        }
        admit(req, permit, req.isWaiting());
        return true;
    }

    /**
     * Applies a denied-without-registration result to its exact provisional entry. A stale response cannot reject a
     * newer request in the same bucket.
     */
    public boolean rejectPendingAcquire(WorkloadGroupQueue.QueuedRequest req, Exception failure) {
        final String groupId = groupIdOf(req.bucketKey());
        WorkloadGroupQueue queue = queuesByGroup.get(groupId);
        if (queue == null) {
            return false;
        }
        final boolean removed;
        try (Releasable ignored = bucketLocks.acquire(lockKey(groupId, req.bucketKey()))) {
            removed = queue.remove(req);
        }
        if (removed) {
            fail(req, failure);
        }
        return removed;
    }

    /**
     * Atomically hands an already-held node permit to the oldest retained request for {@code bucketKey}. The caller
     * keeps the underlying tracker slot occupied while this method holds the bucket lock, so a new arrival cannot take
     * the returned request's slot between release and queue admission.
     * <p>
     * The supplied permit must represent the same still-held tracker slot and must repeat this handoff decision when
     * closed. If the bucket is empty, this method returns {@code false} and the caller releases the underlying slot.
     */
    public boolean handoffNodePermit(String groupId, String bucketKey, Releasable permit) {
        WorkloadGroupQueue queue = queuesByGroup.get(groupId);
        if (queue == null) {
            return false;
        }
        final WorkloadGroupQueue.QueuedRequest req;
        try (Releasable ignored = bucketLocks.acquire(lockKey(groupId, bucketKey))) {
            req = queue.pollOldest(bucketKey);
        }
        if (req == null) {
            return false;
        }
        admit(req, permit, req.isWaiting());
        return true;
    }

    /**
     * Node-tier recovery drain: try to acquire one currently-free permit for the oldest retained request. Normal request
     * completion uses {@link #handoffNodePermit} and therefore has no release/reacquire race; this method remains for the
     * slower periodic backstop and the limit-decrease path, where an existing over-limit slot must first be released.
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
            if (hasRetainedRequest(queue, bucketKey) == false) {
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
        admit(req, permit, req.isWaiting());
    }

    /**
     * Cluster-tier drain ({@link WorkloadGroupSharedThrottleService.GrantConsumer}): the bucket owner pushed a grant
     * carrying a reserved shared permit. Hand it to the oldest retained request for {@code bucketKey}. This includes
     * PENDING_ACQUIRE so a grant that races ahead of the original denial response always finds the request already
     * registered locally. If there is no request, returns {@code false} so the shared service returns the permit.
     *
     * @return {@code true} if a waiter was admitted with the permit, {@code false} if there was none (caller releases)
     */
    public boolean admitWithPermit(String bucketKey, Releasable permit) {
        return admitOldest(bucketKey, permit);
    }

    private boolean admitOldest(String bucketKey, Releasable permit) {
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
        admit(req, permit, req.isWaiting());
        return true;
    }

    /** Group ids that currently hold a queue object. Small (groups are few); used to find backlogs needing a release. */
    public Set<String> queuedGroupIds() {
        return queuesByGroup.keySet();
    }

    /**
     * Snapshot of every bucket on this coordinator that currently retains at least one PENDING_ACQUIRE or WAITING
     * request. Used only after an owner-ring change: the shared throttle service compares each bucket's previous and
     * current owner and re-registers demand that moved.
     */
    public Set<String> retainedBucketKeys() {
        Set<String> bucketKeys = new HashSet<>();
        for (WorkloadGroupQueue queue : queuesByGroup.values()) {
            bucketKeys.addAll(queue.bucketKeys());
        }
        return bucketKeys;
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
     * backlog without holding any throttle slot. Called when a live policy change invalidates the queue's old admission
     * contract (for example a tier/attribute switch or a move to monitor/disabled operation), so re-running old queued
     * requests through permit acquisition could strand them or apply a policy they were not queued under.
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
            admit(req, () -> {}, req.isWaiting());
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
     * Periodic node-tier recovery sweep. For every retained bucket, try one local permit acquisition and admit the
     * oldest request if capacity is free. This is only a backstop: normal node completion transfers its still-held slot
     * directly with {@link #handoffNodePermit}, and shared-tier recovery belongs to the shared owner.
     * <p>
     * Cancellation is deliberately not scanned here. Every retained request registers a task-cancellation callback
     * under the same bucket lock used by admission, and {@link #admit} rechecks cancellation after dequeueing. Those two
     * paths cover cancellation without an O(total queue depth) scan on every sweep.
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
                // drainNode holds the bucket lock internally and admits at most one; safe if the bucket emptied after
                // this snapshot.
                drain.drain(groupId, bucketKey);
            }
        }
        // Note: an emptied group queue's map entry is intentionally NOT pruned here. A concurrent tryEnqueue could
        // offer to the same WorkloadGroupQueue between an "is it empty" check and its removal, which would orphan that
        // freshly-parked request (present in the queue object but no longer reachable from queuesByGroup). The leak is
        // one small empty object per group ever used — negligible (groups are few and long-lived) — and not worth a
        // race to reclaim. A parked request whose task is cancelled (including a deleted group's tasks) is removed by
        // its per-request cancellation callback, independently of this recovery sweep.
    }

    /** Total retained PENDING_ACQUIRE + WAITING requests across all groups on this coordinator. */
    public int totalDepth() {
        int total = 0;
        for (WorkloadGroupQueue queue : queuesByGroup.values()) {
            total += queue.currentDepth();
        }
        return total;
    }

    /** Current WAITING depth for a group (0 if none). Pending owner RPCs are deliberately excluded from queue stats. */
    public int currentDepth(String groupId) {
        WorkloadGroupQueue queue = queuesByGroup.get(groupId);
        return queue == null ? 0 : queue.currentWaitingDepth();
    }

    /** Current retained PENDING_ACQUIRE + WAITING depth for a group (0 if none). */
    public int retainedDepth(String groupId) {
        WorkloadGroupQueue queue = queuesByGroup.get(groupId);
        return queue == null ? 0 : queue.currentDepth();
    }

    /** Peak WAITING depth for a group since its queue was created (0 if none). */
    public long peakDepth(String groupId) {
        WorkloadGroupQueue queue = queuesByGroup.get(groupId);
        return queue == null ? 0L : queue.peakDepth();
    }

    // --- internals ---

    // Completes a retained listener with an acquired permit, off the caller's thread. Deregisters the cancellation
    // callback first (the request is leaving the queue).
    //
    // The ENTIRE body — including the cancelled-task branch that closes the permit — runs on the GENERIC executor, never
    // inline on the caller's (draining/completion) thread. This is load-bearing for recursion safety: for a node-tier
    // permit, permit.close() is the wrapNodePermit wrapper whose close() re-enters drainNode -> admit. Running close()
    // on the caller thread would let a run of consecutively-cancelled waiters recurse close -> drainNode -> admit ->
    // close ... one synchronous frame per waiter (StackOverflow under a cancel storm). Dispatching first means each such
    // hop is a fresh executor task, so the chain unwinds across tasks rather than down one stack.
    private void admit(WorkloadGroupQueue.QueuedRequest req, Releasable permit, boolean recordWait) {
        req.releaseCancellationHandle();
        // Record the queue wait at the admission instant (this thread), before the executor hop, so the metric is the
        // true time parked and not inflated by dispatch latency. Recorded even for a task cancelled-while-queued: it
        // still waited. PENDING_ACQUIRE admissions skip this so owner round-trip latency is not reported as queue wait.
        if (recordWait) {
            recordQueueWait(groupIdOf(req.bucketKey()), TimeUnit.NANOSECONDS.toMillis(req.waitNanos(threadPool.relativeTimeInNanos())));
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

    private void fail(WorkloadGroupQueue.QueuedRequest req, Exception failure) {
        req.releaseCancellationHandle();
        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> req.listener().onFailure(failure));
    }

    private static boolean hasRetainedRequest(WorkloadGroupQueue queue, String bucketKey) {
        return queue.bucketKeys().contains(bucketKey);
    }

    private static String lockKey(String groupId, String bucketKey) {
        return groupId + '\0' + bucketKey;
    }

    // Counts one finished queue wait: total_queued plus the wait sum/max, all inside recordQueueWaitMillis so they cannot
    // diverge (see WorkloadGroupState#recordQueueWaitMillis). Called only for admissions the request did not supply itself.
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
