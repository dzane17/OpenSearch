/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.action.support.ContextPreservingActionListener;
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
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Coordinator-local owner of the per-workload-group retained-request queues. A node-tier denial is parked directly as
 * WAITING. A shared-tier overflow is registered as PENDING_ACQUIRE before the owner RPC. A direct grant admits that exact
 * entry; a denial, registered or not, or an acquire the shared tier could not answer attempts to transition it to WAITING,
 * while a full waiting bucket or a search-pool rejection removes it so the caller can reject it. A retained request holds
 * no thread — only its {@link ActionListener}, task, and open connection — and is admitted later when a permit becomes
 * available:
 * <ul>
 *   <li><b>node tier</b> — a completing request first hands its still-held permit directly to the oldest retained
 *       request; {@link #drainNode} is the fallback/recovery path after a slot is released;</li>
 *   <li><b>cluster tier</b> — a bucket owner pushes a grant carrying a reserved permit, handled via
 *       {@link #admitWithPermit} (see {@link WorkloadGroupSharedThrottleService});</li>
 *   <li><b>node-tier backstop</b> — {@link #sweep} periodically re-attempts node-tier admission for retained buckets,
 *       oldest first while node permits are free, in case an exceptional race interrupted the primary handoff or a live
 *       {@code node_limit} increase added slots no completion will hand over. It also releases, untracked, the retained
 *       requests of a group that was deleted or has no throttle tier left, counting a {@code shared_limit} no node can
 *       own as none. Shared-tier recovery is owner-push and is not done here. On the same pass,
 *       {@link WorkloadGroupService#doRun} re-registers this coordinator as a waiter for each bucket in
 *       {@link #bucketKeysWithWaiting} whose group still has a shared tier (see
 *       {@link WorkloadGroupSharedThrottleService#reconcileRetainedWaiters}); if that cannot be confirmed, those WAITING
 *       requests stay parked and the next pass tries again. With no eligible owner there is nothing to register with and
 *       nothing is rejected: the bucket gets the node-tier pass above at once.</li>
 * </ul>
 * Concurrency: each bucket's request set is guarded by a per-bucket lock ({@link KeyedLock}); group totals are atomics
 * inside {@link WorkloadGroupQueue}. A retained listener is never completed while a bucket lock is held. Actual WAITING
 * admissions and cancellation failures dispatch listener completion onto an executor; a direct PENDING_ACQUIRE grant
 * completes inline after releasing the lock, matching the queue-disabled shared path. A request claimed back by
 * {@link #claimPendingAcquire} or a full {@link #transitionPendingAcquireToWaiting} is completed by the caller.
 * <p>
 * Every admission against a permit marks the task as throttle-counted (see {@link WorkloadGroupTask#setThrottleCounted}),
 * so a nested search inherits the charge. A policy-change drain ({@link #admitAllUntracked}) holds no permit and leaves
 * the flag unset, so nested work is charged under whatever policy now applies.
 * <p>
 * Queue stats: a request is counted in {@code total_queued} when it enters WAITING, under its bucket lock, and its wait
 * is recorded once in {@code total_queue_wait_millis} when it leaves the queue, for admission or on cancellation (see
 * {@link WorkloadGroupState#totalQueued}).
 */
@ExperimentalApi
public class WorkloadGroupQueueService {

    private final ThreadPool threadPool;
    private final WorkloadGroupsStateAccessor stateAccessor;
    // One queue per group id, created lazily on first retained request. queue.size_per_bucket is passed only when an
    // entry becomes WAITING; PENDING_ACQUIRE entries consume the fixed group ceiling but no configured bucket slot.
    private final Map<String, WorkloadGroupQueue> queuesByGroup = new ConcurrentHashMap<>();
    // Retained requests across every group's queue, kept by the queues themselves (see hasRetainedRequests).
    private final AtomicInteger retainedRequests = new AtomicInteger(0);
    // Keyed by bucket key, which embeds its group id (see WorkloadGroupService#groupIdOf), so it is unique across groups.
    private final KeyedLock<String> bucketLocks = new KeyedLock<>();

    public WorkloadGroupQueueService(ThreadPool threadPool, WorkloadGroupsStateAccessor stateAccessor) {
        this.threadPool = threadPool;
        this.stateAccessor = stateAccessor;
    }

    /**
     * Parks a request that is already known to be waiting (the node-tier denial path). Returns {@code false} if
     * queueing is disabled, the bucket's WAITING capacity is full, or the group's combined
     * PENDING_ACQUIRE + WAITING ceiling is reached. A parked request is counted in {@code total_queued} at once, under
     * its bucket lock; its wait is recorded when it leaves the queue (see {@link #admit} and {@link #evictCancelled}).
     * <p>
     * There is no queue timeout (see {@link WorkloadGroupQueueSettings}): a parked request is bounded only by task
     * cancellation.
     *
     * @param groupId       the workload group id (queue identity + stat key)
     * @param bucketKey     the throttle bucket the request is waiting on
     * @param task          the search task (observed for cancellation)
     * @param sizePerBucket the group's configured {@code queue.size_per_bucket}
     * @param listener      the parked admission listener; wrapped here to restore the caller's thread context
     * @return {@code true} if enqueued, {@code false} if rejected (disabled / bucket full / group full)
     * @see WorkloadGroupState#totalQueued why a request counts towards {@code total_queued} when it starts waiting
     */
    boolean tryEnqueue(String groupId, String bucketKey, WorkloadGroupTask task, int sizePerBucket, ActionListener<Releasable> listener) {
        if (sizePerBucket <= 0) {
            return false; // queueing disabled: not a queue rejection, the caller rejects with the normal throttle 429
        }
        final long waitStartNanos = threadPool.preciseRelativeTimeInNanos();
        WorkloadGroupQueue.QueuedRequest req = new WorkloadGroupQueue.QueuedRequest(
            preserveContext(listener),
            bucketKey,
            task,
            waitStartNanos
        );
        if (enqueueWaiting(groupId, req, sizePerBucket) == false) {
            recordQueueRejection(groupId); // bucket or group queue full: a queue rejection, never counted in total_queued
            return false;
        }
        return true;
    }

    /**
     * Registers a shared-tier request as PENDING_ACQUIRE before contacting the permit owner. Pending entries do not
     * consume {@code queue.size_per_bucket}; they are bounded only by the fixed per-group retained-request ceiling.
     * A full ceiling is not counted as a queue rejection here: the caller still asks the owner without retaining the
     * request, and counts one via {@link #recordQueueRejection} only if that acquire is then refused (denied, or not
     * answered by the shared tier).
     *
     * @return the exact entry token, or {@code null} if the per-group retained-request ceiling is full
     */
    WorkloadGroupQueue.QueuedRequest tryRegisterPendingAcquire(
        String groupId,
        String bucketKey,
        WorkloadGroupTask task,
        ActionListener<Releasable> listener
    ) {
        WorkloadGroupQueue.QueuedRequest req = WorkloadGroupQueue.QueuedRequest.pendingAcquire(preserveContext(listener), bucketKey, task);
        WorkloadGroupQueue queue = queueFor(groupId);
        try (Releasable ignored = bucketLocks.acquire(bucketKey)) {
            if (queue.offerPendingAcquire(req) == false) {
                return null;
            }
            registerCancellation(groupId, req);
            return req;
        }
    }

    // Parks a known waiter, counts it in total_queued, and registers its cancellation callback. Returns false if either
    // waiting limit is full. Cancellation is registered AFTER a successful offer (it references the enqueued request) but
    // STILL UNDER the bucket lock: a concurrent drain that admits this request must take the same lock, so installing the
    // handle inside the lock guarantees a racing admit cannot poll the request before its cancellation handle exists
    // (which would leave the callback registered on an already-admitted task forever). addOnCancelledCallback runs the
    // callback immediately if the task is already cancelled, closing the race where cancellation lands during enqueue.
    // The entry is counted before that registration, so even that inline eviction records its exit after its entry.
    private boolean enqueueWaiting(String groupId, WorkloadGroupQueue.QueuedRequest req, int sizePerBucket) {
        WorkloadGroupQueue queue = queueFor(groupId);
        try (Releasable ignored = bucketLocks.acquire(req.bucketKey())) {
            if (queue.offer(req, sizePerBucket) == false) {
                return false;
            }
            recordQueueEntry(groupId, req.bucketKey());
            registerCancellation(groupId, req);
            return true;
        }
    }

    private WorkloadGroupQueue queueFor(String groupId) {
        return queuesByGroup.computeIfAbsent(groupId, k -> new WorkloadGroupQueue(retainedRequests));
    }

    // A retained listener is completed later on whichever thread frees its slot: another request's completion (node
    // handoff), the transport thread of a pushed grant, or a sweep/drain worker. Capture the submitting request's thread
    // context now so the search resumes as its own caller (security transients, headers, task tracking), never as the
    // thread that happened to admit it.
    private ActionListener<Releasable> preserveContext(ActionListener<Releasable> listener) {
        return ContextPreservingActionListener.wrapPreservingContext(listener, threadPool.getThreadContext());
    }

    private void registerCancellation(String groupId, WorkloadGroupQueue.QueuedRequest req) {
        req.cancellationHandle = req.task().addOnCancelledCallback(() -> evictCancelled(groupId, req));
    }

    /**
     * Handles an owner denial, registered or not, or an acquire the shared tier could not answer, for the exact provisional
     * entry. The transition reserves one configured WAITING slot, starts the request's wait clock, and counts it in
     * {@code total_queued}, under the bucket lock.
     * If that bucket is full, this exact request is removed (counted as a queue rejection, not in {@code total_queued}) and
     * {@link WorkloadGroupQueue.WaitingTransition#BUCKET_FULL} is returned: the caller then owns the request's listener and
     * must complete it. Existing waiters are untouched.
     */
    WorkloadGroupQueue.WaitingTransition transitionPendingAcquireToWaiting(WorkloadGroupQueue.QueuedRequest req, int sizePerBucket) {
        final String groupId = WorkloadGroupService.groupIdOf(req.bucketKey());
        WorkloadGroupQueue queue = queuesByGroup.get(groupId);
        if (queue == null) {
            return WorkloadGroupQueue.WaitingTransition.REQUEST_GONE;
        }
        final WorkloadGroupQueue.WaitingTransition transition;
        try (Releasable ignored = bucketLocks.acquire(req.bucketKey())) {
            // A request already WAITING also reports WAITING; only the PENDING_ACQUIRE -> WAITING move counts an entry.
            final boolean wasWaiting = req.isWaiting();
            transition = queue.transitionToWaiting(req, sizePerBucket, threadPool.preciseRelativeTimeInNanos());
            if (transition == WorkloadGroupQueue.WaitingTransition.WAITING && wasWaiting == false) {
                recordQueueEntry(groupId, req.bucketKey());
            }
        }
        if (transition == WorkloadGroupQueue.WaitingTransition.BUCKET_FULL) {
            recordQueueRejection(groupId);
            req.releaseCancellationHandle();
        }
        return transition;
    }

    /**
     * Applies a granted acquire result to its exact provisional entry. A delayed result is a no-op once cancellation or
     * an early pushed grant has already removed that entry; the caller then closes the unused permit.
     */
    boolean admitPendingAcquire(WorkloadGroupQueue.QueuedRequest req, Releasable permit) {
        final String groupId = WorkloadGroupService.groupIdOf(req.bucketKey());
        WorkloadGroupQueue queue = queuesByGroup.get(groupId);
        if (queue == null) {
            return false;
        }
        final boolean removed;
        final boolean wasWaiting;
        try (Releasable ignored = bucketLocks.acquire(req.bucketKey())) {
            wasWaiting = req.isWaiting();
            removed = queue.remove(req);
        }
        if (removed == false) {
            return false;
        }
        if (wasWaiting) {
            admit(req, permit, true, true);
            return true;
        }
        req.releaseCancellationHandle();
        if (req.task().isCancelled()) {
            permit.close();
            req.listener().onFailure(new TaskCancelledException("task cancelled while queued"));
        } else {
            req.task().setThrottleCounted(true);
            req.listener().onResponse(permit);
        }
        return true;
    }

    /**
     * Takes back the exact provisional entry for its one terminal non-grant outcome: a search-pool rejection of a grant's
     * hand-off. Returns {@code true} if this call removed it, in which case the caller owns the request's listener and must
     * complete it (with a 429). A stale outcome returns {@code false} once cancellation or an early pushed grant has removed
     * the entry, so it can never reject a newer request in the bucket.
     */
    boolean claimPendingAcquire(WorkloadGroupQueue.QueuedRequest req) {
        final String groupId = WorkloadGroupService.groupIdOf(req.bucketKey());
        WorkloadGroupQueue queue = queuesByGroup.get(groupId);
        if (queue == null) {
            return false;
        }
        final boolean removed;
        try (Releasable ignored = bucketLocks.acquire(req.bucketKey())) {
            removed = queue.remove(req);
        }
        if (removed) {
            req.releaseCancellationHandle();
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
    boolean handoffNodePermit(String groupId, String bucketKey, Releasable permit) {
        WorkloadGroupQueue queue = queuesByGroup.get(groupId);
        if (queue == null) {
            return false;
        }
        final WorkloadGroupQueue.QueuedRequest req;
        try (Releasable ignored = bucketLocks.acquire(bucketKey)) {
            req = queue.pollOldest(bucketKey);
        }
        if (req == null) {
            return false;
        }
        admit(req, permit, req.isWaiting(), true);
        return true;
    }

    /**
     * Node-tier recovery drain: try to acquire one currently-free permit for the oldest retained request. Normal request
     * completion uses {@link #handoffNodePermit} and therefore has no release/reacquire race; this method remains for the
     * slower periodic backstop, the limit-decrease and limit-increase paths, and the retry after a request parks while a
     * slot is freed, where an existing over-limit slot must first be released.
     *
     * @return {@code true} if it admitted the oldest retained request against a newly acquired permit
     */
    boolean drainNode(String groupId, String bucketKey, Function<String, Releasable> nodeAcquire) {
        WorkloadGroupQueue queue = queuesByGroup.get(groupId);
        if (queue == null) {
            return false;
        }
        WorkloadGroupQueue.QueuedRequest req;
        Releasable permit;
        try (Releasable ignored = bucketLocks.acquire(bucketKey)) {
            // Peek-then-acquire-then-remove under the lock: only remove a retained request once we hold a permit for it, so a
            // failed re-acquire never drops a request from the queue.
            if (hasRetainedRequest(queue, bucketKey) == false) {
                return false;
            }
            permit = nodeAcquire.apply(bucketKey);
            if (permit == null) {
                return false; // slot taken by a racing arrival; leave the retained request queued
            }
            req = queue.pollOldest(bucketKey);
            if (req == null) {
                // No retained request after all (shouldn't happen under the lock, but be safe): release the permit we took.
                permit.close();
                return false;
            }
        }
        admit(req, permit, req.isWaiting(), true);
        return true;
    }

    /**
     * Cluster-tier drain ({@link WorkloadGroupSharedThrottleService.GrantConsumer}): the bucket owner pushed a grant
     * carrying a reserved shared permit. Hand it to the oldest retained request for {@code bucketKey}. This includes
     * PENDING_ACQUIRE so a grant that races ahead of the original denial response always finds the request already
     * registered locally.
     *
     * @return the admitted request's task id, which the owner records on the reserved permit so that the permit's expiry
     *         check follows that task's liveness; empty if there was no retained request (the permit is unused, and the
     *         shared service returns it)
     */
    OptionalLong admitWithPermit(String bucketKey, Releasable permit) {
        WorkloadGroupQueue queue = queuesByGroup.get(WorkloadGroupService.groupIdOf(bucketKey));
        if (queue == null) {
            return OptionalLong.empty();
        }
        WorkloadGroupQueue.QueuedRequest req;
        try (Releasable ignored = bucketLocks.acquire(bucketKey)) {
            req = queue.pollOldest(bucketKey);
        }
        if (req == null) {
            return OptionalLong.empty();
        }
        admit(req, permit, req.isWaiting(), true);
        return OptionalLong.of(req.task().getId());
    }

    /** Snapshot of the bucket keys on this coordinator that currently hold at least one WAITING request. */
    Set<String> bucketKeysWithWaiting() {
        Set<String> bucketKeys = new HashSet<>();
        for (WorkloadGroupQueue queue : queuesByGroup.values()) {
            for (String bucketKey : new ArrayList<>(queue.bucketKeys())) {
                try (Releasable ignored = bucketLocks.acquire(bucketKey)) {
                    if (queue.hasWaiting(bucketKey)) {
                        bucketKeys.add(bucketKey);
                    }
                }
            }
        }
        return bucketKeys;
    }

    /** Counts one queue rejection: a request that could not be retained (bucket or group full) and was then rejected. */
    void recordQueueRejection(String groupId) {
        WorkloadGroupState state = stateAccessor.getWorkloadGroupStateMap().get(groupId);
        if (state != null) {
            state.totalQueueRejections.inc();
        }
    }

    /** Group ids that currently hold a queue object. Small (groups are few); used to find backlogs needing a release. */
    Set<String> queuedGroupIds() {
        return queuesByGroup.keySet();
    }

    /**
     * Whether this coordinator retains any PENDING_ACQUIRE or WAITING request, in any group. One atomic read and no
     * allocation, for the service loop to ask on every run while WLM is not enabled, when the queue is normally empty
     * (see {@code WorkloadGroupService#doRun}). A racing enqueue or admission can change it at once.
     */
    boolean hasRetainedRequests() {
        return retainedRequests.get() > 0;
    }

    /**
     * Whether the task {@code taskId} has a request WAITING in this coordinator's queue. Lock-free, one lookup per group, so
     * a racing admission or eviction can change it at once. A WAITING request holds no shared permit, so the shared tier
     * reports its task inactive to an owner's expiry check (see {@code WorkloadGroupSharedThrottleService#checkTasks}).
     */
    boolean isWaiting(long taskId) {
        for (WorkloadGroupQueue queue : queuesByGroup.values()) {
            if (queue.hasWaitingTask(taskId)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Snapshot of every bucket on this coordinator that currently retains at least one PENDING_ACQUIRE or WAITING
     * request. Used only after an owner-ring change: the shared throttle service compares each bucket's previous and
     * current owner and re-registers demand that moved.
     */
    Set<String> retainedBucketKeys() {
        Set<String> bucketKeys = new HashSet<>();
        for (WorkloadGroupQueue queue : queuesByGroup.values()) {
            bucketKeys.addAll(queue.bucketKeys());
        }
        return bucketKeys;
    }

    /**
     * Best-effort admission of a group's retained backlog across all bucket keys observed by this call, untracked. See
     * {@link #admitAllUntracked(String, String)} for why a no-op permit is the right thing to hand out here. Concurrent
     * arrivals may race this drain and obey either the old or new policy.
     *
     * @return the number of requests admitted
     */
    int admitAllUntracked(String groupId) {
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
     * Drains the retained requests ({@code PENDING_ACQUIRE} or {@code WAITING}) present for {@code bucketKey} while its
     * lock is held, using an untracked (no-op) permit. Called when a live policy change invalidates the queue's old
     * admission contract (for example a tier/attribute switch or a move to monitor/disabled operation), so re-running old
     * requests through permit acquisition could strand them or apply a policy they were not retained under.
     * <p>
     * The request runs untracked: its no-op permit's {@code close()} does nothing and the task is not marked
     * throttle-counted, because it no longer holds any throttle slot. Requests are collected under the bucket lock but
     * completed by {@link #admit} outside it, which also re-checks cancellation per request.
     *
     * Also used for a group that was deleted or has no throttle tier left, counting a {@code shared_limit} no node can own
     * as none (see {@link WorkloadGroupService#sweepDrainNode}): a request that parked just after the one-shot live-config
     * drain, or a shared-only bucket's request after its owners all left, has no permit that could admit it. Likewise,
     * while WLM is not enabled, for a request that parked just after the mode-change drain (see
     * {@code WorkloadGroupService#doRun}).
     *
     * @return the number of requests admitted
     */
    int admitAllUntracked(String groupId, String bucketKey) {
        WorkloadGroupQueue queue = queuesByGroup.get(groupId);
        if (queue == null) {
            return 0;
        }
        final List<WorkloadGroupQueue.QueuedRequest> drained = new ArrayList<>();
        try (Releasable ignored = bucketLocks.acquire(bucketKey)) {
            WorkloadGroupQueue.QueuedRequest req;
            while ((req = queue.pollOldest(bucketKey)) != null) {
                drained.add(req);
            }
        }
        for (WorkloadGroupQueue.QueuedRequest req : drained) {
            admit(req, () -> {}, req.isWaiting(), false);
        }
        return drained.size();
    }

    /**
     * Periodic node-tier recovery sweep. For every retained bucket, admit retained requests, oldest first, while node
     * permits are free (or release them untracked if the group was deleted or has no throttle tier left). This is only a
     * backstop: normal node completion transfers its still-held slot directly with {@link #handoffNodePermit}, and
     * shared-tier recovery is owner-push, re-confirmed by WorkloadGroupService's waiter re-registration, not by this
     * method.
     * <p>
     * Cancellation is deliberately not scanned here. Every retained request registers a task-cancellation callback
     * under the same bucket lock used by admission, and {@link #admit} rechecks cancellation after dequeueing. Those two
     * paths cover cancellation without an O(total queue depth) scan on every sweep.
     *
     * @param drain admits retained requests for a (groupId, bucketKey), oldest first, against freshly re-acquired node
     *              permits while they are available; a no-op if the bucket is empty or no permit is free (typically
     *              {@link WorkloadGroupService#sweepDrainNode})
     */
    void sweep(SweepDrain drain) {
        for (Map.Entry<String, WorkloadGroupQueue> entry : queuesByGroup.entrySet()) {
            String groupId = entry.getKey();
            WorkloadGroupQueue queue = entry.getValue();
            // Snapshot bucket keys so we don't iterate a map being mutated by concurrent drains.
            for (String bucketKey : new ArrayList<>(queue.bucketKeys())) {
                // Each drainNode call holds the bucket lock internally and admits at most one; the SweepDrain repeats it
                // while permits are free. Safe if the bucket emptied after this snapshot.
                drain.drain(groupId, bucketKey);
            }
        }
        // Note: an emptied group queue's map entry is intentionally NOT pruned here. A concurrent tryEnqueue or
        // tryRegisterPendingAcquire could add to the same WorkloadGroupQueue between an "is it empty" check and its
        // removal, which would orphan that retained request (present in the queue object but no longer reachable from
        // queuesByGroup). The leak is
        // one small empty object per group ever used — negligible (groups are few and long-lived) — and not worth a
        // race to reclaim. A retained request whose task is cancelled (including a deleted group's tasks) is removed by
        // its per-request cancellation callback, independently of this recovery sweep.
    }

    /** Current WAITING depth for a group (0 if none). Pending owner RPCs are deliberately excluded from queue stats. */
    int waitingDepth(String groupId) {
        WorkloadGroupQueue queue = queuesByGroup.get(groupId);
        return queue == null ? 0 : queue.waitingDepth();
    }

    /** Current retained PENDING_ACQUIRE + WAITING depth for a group (0 if none). */
    int retainedDepth(String groupId) {
        WorkloadGroupQueue queue = queuesByGroup.get(groupId);
        return queue == null ? 0 : queue.retainedDepth();
    }

    /**
     * Whether {@code bucketKey} currently retains a PENDING_ACQUIRE or WAITING request. Lock-free, so a racing enqueue or
     * admission can change it at once. It is a cheap pre-check for per-bucket queue work, which re-checks under the bucket
     * lock.
     */
    boolean hasRetained(String groupId, String bucketKey) {
        WorkloadGroupQueue queue = queuesByGroup.get(groupId);
        return queue != null && hasRetainedRequest(queue, bucketKey);
    }

    /** Peak WAITING depth for a group since its queue was created (0 if none). */
    long peakWaitingDepth(String groupId) {
        WorkloadGroupQueue queue = queuesByGroup.get(groupId);
        return queue == null ? 0L : queue.peakWaitingDepth();
    }

    // --- internals ---

    // Completes a retained listener with an acquired permit, off the caller's thread. Deregisters the cancellation
    // callback first (the request is leaving the queue). A tracked admission (the permit holds a real throttle slot)
    // marks the task as throttle-counted; an untracked policy-change drain does not.
    //
    // Listener completion — including the cancelled-task branch that closes the permit — runs on the GENERIC executor,
    // never inline on the caller's (draining/completion) thread. Cancellation-handle removal and wait-stat recording
    // happen before dispatch. This is load-bearing for recursion safety: closing a node-tier wrapper can synchronously
    // hand its still-held slot to another retained request (or fall back to drainNode). Running close() on the caller
    // thread would let consecutively-cancelled entries recurse close -> handoff/admit -> close, one frame per entry
    // (StackOverflow under a cancellation storm). Dispatching listener completion first makes each hop a fresh task.
    // The hop also keeps the releasing request's response from waiting on the next request's pre-fan-out, and keeps a
    // pushed grant's search off the transport thread.
    //
    // Intentional: GENERIC, not SEARCH (where a direct remote grant resumes). The admitted request's coordinator
    // pre-fan-out (pipeline resolution, rewrite, shard resolution, initial shard sends) runs here; its shard phases still
    // fork to SEARCH. GENERIC never rejects, so a request already taken off the queue is never dropped or turned into a
    // late 429 after waiting, whereas a search-pool rejection would close the permit inline and cascade through the
    // bucket's queue. Tracked admissions are already bounded by their permits. (TransportMultiSearchAction forks its next
    // sub-search to GENERIC for the same recursion reason.) Accepted: GENERIC is not resource-tracked, so this pre-fan-out
    // is not charged to the task (as on the inline network-thread path), and a policy-change drain submits one task per
    // retained request (at most MAX_GROUP_QUEUE_DEPTH per group), briefly scaling GENERIC up.
    private void admit(WorkloadGroupQueue.QueuedRequest req, Releasable permit, boolean recordWait, boolean tracked) {
        req.releaseCancellationHandle();
        // Record the queue wait when WAITING ends (selection/dequeue), before the executor hop, so dispatch latency is not
        // included; total_queued already counted the request when it entered WAITING. This covers every admission path,
        // including a policy-change drain and a node hand-off. A cancellation that wins the final check below is recorded
        // here, once: the entry has already left the queue, so evictCancelled removes nothing and records nothing. A
        // WAITING entry evicted before dequeue has its wait recorded by evictCancelled instead. PENDING_ACQUIRE skips
        // this so owner round-trip latency is not queue latency.
        if (recordWait) {
            final long waitNanos = req.waitNanos(threadPool.preciseRelativeTimeInNanos());
            recordQueueWait(WorkloadGroupService.groupIdOf(req.bucketKey()), TimeUnit.NANOSECONDS.toMillis(waitNanos));
        }
        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
            if (req.task().isCancelled()) {
                permit.close();
                req.listener().onFailure(new TaskCancelledException("task cancelled while queued"));
                return;
            }
            if (tracked) {
                req.task().setThrottleCounted(true);
            }
            req.listener().onResponse(permit);
        });
    }

    // Cancellation callback: remove the entry (if still queued) and fail it. Runs on whatever thread cancelled the task
    // — including, for a task already cancelled at enqueue time, inline under the enqueue bucket lock (KeyedLock is
    // reentrant, so re-acquiring below does not deadlock). The failure is dispatched on the GENERIC executor rather
    // than completed inline so the listener is never completed while a bucket lock is held (consistent with admit()).
    //
    // A WAITING entry removed here ends its wait, so its wait is recorded (total_queued counted it at entry), before the
    // failure is dispatched. The state is read under the lock: a racing denial could otherwise move a PENDING_ACQUIRE
    // entry to WAITING between the read and the removal. A cancelled PENDING_ACQUIRE never waited and records nothing.
    // Nor does a call that finds the entry already removed by another path (removed == false): admit records the wait of
    // a WAITING entry it dequeues, and every other path removes only a PENDING_ACQUIRE entry, which never waited.
    private void evictCancelled(String groupId, WorkloadGroupQueue.QueuedRequest req) {
        WorkloadGroupQueue queue = queuesByGroup.get(groupId);
        if (queue == null) {
            return;
        }
        final boolean removed;
        final boolean wasWaiting;
        try (Releasable ignored = bucketLocks.acquire(req.bucketKey())) {
            wasWaiting = req.isWaiting();
            removed = queue.remove(req);
        }
        if (removed == false) {
            return;
        }
        if (wasWaiting) {
            recordQueueWait(groupId, TimeUnit.NANOSECONDS.toMillis(req.waitNanos(threadPool.preciseRelativeTimeInNanos())));
        }
        threadPool.executor(ThreadPool.Names.GENERIC)
            .execute(() -> req.listener().onFailure(new TaskCancelledException("task cancelled while queued")));
    }

    private static boolean hasRetainedRequest(WorkloadGroupQueue queue, String bucketKey) {
        return queue.bucketKeys().contains(bucketKey);
    }

    // Counts one request entering WAITING in total_queued. Called under the request's bucket lock, right after the entry
    // became WAITING: every exit (admission or eviction) takes that lock to remove the entry, so its wait can never be
    // recorded before this count (enqueueWaiting counts before the one exit that can run inside its reentrant hold of the
    // lock, the inline eviction of an already-cancelled task).
    private void recordQueueEntry(String groupId, String bucketKey) {
        assert bucketLocks.isHeldByCurrentThread(bucketKey) : "total_queued counted outside the lock of bucket [" + bucketKey + "]";
        WorkloadGroupState state = stateAccessor.getWorkloadGroupStateMap().get(groupId);
        if (state != null) {
            state.recordQueueEntry();
        }
    }

    // Records one ended WAITING interval in the wait sum (see WorkloadGroupState#recordQueueWaitMillis). Called
    // exactly once per interval, after its WAITING entry left the queue: on dequeue for admission (admit) or on eviction
    // by task cancellation (evictCancelled), whichever removed it.
    private void recordQueueWait(String groupId, long waitMillis) {
        WorkloadGroupState state = stateAccessor.getWorkloadGroupStateMap().get(groupId);
        if (state != null) {
            state.recordQueueWaitMillis(waitMillis);
        }
    }

    /**
     * Node-tier backstop drain for the sweep: admit retained requests for {@code (groupId, bucketKey)}, oldest first,
     * against freshly re-acquired node permits while they are free; otherwise a no-op. It also releases, untracked, the
     * retained requests of a group that was deleted or has no throttle tier left. Implemented by
     * {@link WorkloadGroupService} (which owns the node permit tracker) so the queue service holds no throttle logic.
     */
    @FunctionalInterface
    interface SweepDrain {
        void drain(String groupId, String bucketKey);
    }
}
