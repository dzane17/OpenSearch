/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.common.lease.Releasable;
import org.opensearch.core.action.ActionListener;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A bounded, per-coordinator retained-request queue for a single workload group. A node-tier denial enters directly as
 * {@link RequestState#WAITING}. A shared-tier overflow is first registered as
 * {@link RequestState#PENDING_ACQUIRE} before contacting the owner. A direct grant admits that exact entry; a denial,
 * registered or not, or an acquire the shared tier could not answer attempts to transition it to {@code WAITING}, while a
 * full waiting bucket or a search-pool rejection rejects it.
 * <p>
 * The queue is partitioned into per-bucket insertion order ({@code byBucket}) so a permit freed for one bucket wakes a
 * request retained on that same bucket, and a heavily queued bucket cannot head-of-line-block another. Pushed grants
 * and node-tier drains take the oldest retained request; the exact result of a request's own shared acquire may remove
 * that provisional request from the middle. Capacity has two distinct meanings:
 * <ul>
 *   <li>{@code queue.size_per_bucket} bounds only {@code WAITING} entries in one bucket;</li>
 *   <li>{@link WorkloadGroupQueueSettings#MAX_GROUP_QUEUE_DEPTH} bounds
 *       {@code PENDING_ACQUIRE + WAITING} across the whole group.</li>
 * </ul>
 * A retained request holds no thread — only its {@link ActionListener}, task, and open client connection.
 * <p>
 * Thread-safety: the group counters are atomics; each bucket's request set and waiting count are guarded by the caller
 * holding this group's per-bucket lock (a {@code KeyedLock} in {@link WorkloadGroupQueueService}). Callers must never
 * invoke a retained listener while holding that lock.
 * <p>
 * Each bucket's container is a {@link LinkedHashSet}: insertion order gives pushed-grant and node-drain paths a FIFO
 * head, while membership-based removal is O(1) average. Exact acquire results and cancellation can remove a request
 * from the middle; an {@code ArrayDeque} would make that O(n), so a mass-cancellation storm on one bucket would be
 * O(n^2). {@link QueuedRequest} has no {@code equals}/{@code hashCode} override, so identity semantics hold and
 * distinct requests never collide.
 */
final class WorkloadGroupQueue {

    enum RequestState {
        PENDING_ACQUIRE,
        WAITING
    }

    enum WaitingTransition {
        WAITING,
        REQUEST_GONE,
        BUCKET_FULL
    }

    /**
     * A retained request: the held listener (context-preserving, wrapped by {@link WorkloadGroupQueueService} when the
     * request is retained, so it always completes in the submitting request's thread context), its bucket key, the owning
     * task, current state, and the handle that deregisters its task-cancellation callback once it leaves the queue.
     * <p>
     * A waiting request has no wall-clock deadline: it leaves the queue on admission (a pushed shared grant, a node permit,
     * or the release of a bucket no tier can admit any more), on a live configuration change that drains the queue, or on
     * task cancellation, which evicts the entry promptly. A parked search's task is cancelled when its client disconnects
     * or its {@code cancel_after_time_interval} (per request, or the cluster-level {@code search.cancel_after_time_interval})
     * fires; a parked scroll page's only through the tasks API. A shared-tier waiter registration that cannot be confirmed
     * never removes it. See {@link WorkloadGroupQueueSettings} for why there is no queue timeout. The queue service counts
     * a request in {@code total_queued} when it enters WAITING, and records its wait once when it leaves, whichever of
     * these ends it.
     */
    static final class QueuedRequest {
        final ActionListener<Releasable> listener;
        final String bucketKey;
        final WorkloadGroupTask task;
        volatile RequestState state;
        // Precise relative-clock instant (nanos) at which the request entered WAITING; the 200ms cached clock would quantize
        // sub-interval waits to 0 or about 200ms. A provisional PENDING_ACQUIRE has no queue-wait start: owner round-trip
        // time is not denied-backlog latency.
        volatile long waitStartNanos;
        // Set after construction (the callback needs the enqueued reference); deregisters the cancellation callback.
        volatile Releasable cancellationHandle;

        QueuedRequest(ActionListener<Releasable> listener, String bucketKey, WorkloadGroupTask task, long waitStartNanos) {
            this(listener, bucketKey, task, RequestState.WAITING, waitStartNanos);
        }

        private QueuedRequest(
            ActionListener<Releasable> listener,
            String bucketKey,
            WorkloadGroupTask task,
            RequestState state,
            long waitStartNanos
        ) {
            this.listener = listener;
            this.bucketKey = bucketKey;
            this.task = task;
            this.state = state;
            this.waitStartNanos = waitStartNanos;
        }

        static QueuedRequest pendingAcquire(ActionListener<Releasable> listener, String bucketKey, WorkloadGroupTask task) {
            return new QueuedRequest(listener, bucketKey, task, RequestState.PENDING_ACQUIRE, 0L);
        }

        ActionListener<Releasable> listener() {
            return listener;
        }

        String bucketKey() {
            return bucketKey;
        }

        WorkloadGroupTask task() {
            return task;
        }

        boolean isWaiting() {
            return state == RequestState.WAITING;
        }

        void transitionToWaiting(long nowNanos) {
            state = RequestState.WAITING;
            waitStartNanos = nowNanos;
        }

        /** Time spent in WAITING so far, in nanos, as of {@code nowNanos} (relative clock). Never negative. */
        long waitNanos(long nowNanos) {
            if (isWaiting() == false) {
                return 0L;
            }
            long w = nowNanos - waitStartNanos;
            return w < 0 ? 0 : w;
        }

        void releaseCancellationHandle() {
            Releasable handle = cancellationHandle;
            if (handle != null) {
                handle.close();
            }
        }
    }

    private static class BucketQueue {
        final LinkedHashSet<QueuedRequest> requests = new LinkedHashSet<>();
        int waitingDepth;
    }

    private final Map<String, BucketQueue> byBucket = new ConcurrentHashMap<>();
    // The group's WAITING requests by task id, so whether a task is waiting is one lookup, not a scan of every bucket (see
    // WorkloadGroupQueueService#isWaiting). A request joins when it enters WAITING and leaves when it leaves the queue.
    // Concurrent: the group's buckets are guarded by separate locks.
    private final Map<Long, QueuedRequest> waitingByTaskId = new ConcurrentHashMap<>();
    private final AtomicInteger retainedDepth = new AtomicInteger(0);
    // Retained requests across every group queue of one queue service, so it can tell it retains none without iterating
    // its groups (see WorkloadGroupQueueService#hasRetainedRequests). A queue built on its own counts only itself.
    private final AtomicInteger serviceRetainedDepth;
    private final AtomicInteger waitingDepth = new AtomicInteger(0);
    private final AtomicLong peakWaitingDepth = new AtomicLong(0);

    WorkloadGroupQueue() {
        this(new AtomicInteger(0));
    }

    WorkloadGroupQueue(AtomicInteger serviceRetainedDepth) {
        this.serviceRetainedDepth = serviceRetainedDepth;
    }

    /**
     * Attempts to insert a request that is already known to be waiting (the node-tier path). Must be called while
     * holding the per-bucket lock for {@code req.bucketKey}. Enforces two limits and returns {@code false} if either is
     * hit:
     * <ol>
     *   <li><b>per-bucket</b> — the request's own bucket already holds {@code sizePerBucket} waiting requests
     *       ({@code sizePerBucket <= 0} means queueing is disabled). This is the user-facing {@code queue.size_per_bucket}
     *       knob, giving cross-principal fairness.</li>
     *   <li><b>per-group retained total</b> — the group already holds
     *       {@link WorkloadGroupQueueSettings#MAX_GROUP_QUEUE_DEPTH} pending plus waiting requests.</li>
     * </ol>
     * {@code sizePerBucket} is passed per call (not stored) so a live {@code queue.size_per_bucket} change takes effect
     * immediately: a decrease stops admitting to a bucket once it is at/above the new cap (existing WAITING requests are
     * not evicted); an increase widens capacity at once.
     * <p>
     * The per-bucket depth is read (not created) before reserving the shared group counter, so a group-ceiling rejection
     * never leaves an empty bucket set behind — preserving the invariant that a present bucket key has a retained
     * request.
     * <p>
     * A {@code true} result is the request's entry into WAITING, which the caller counts in {@code total_queued} while
     * still holding the bucket lock; a {@code false} result is never counted there.
     *
     * @param req           the WAITING request to retain
     * @param sizePerBucket the group's <em>current</em> {@code queue.size_per_bucket}
     * @return {@code true} if the request was enqueued, {@code false} if rejected (disabled / bucket full / group full)
     */
    boolean offer(QueuedRequest req, int sizePerBucket) {
        if (sizePerBucket <= 0) {
            return false; // queueing disabled
        }
        // Per-bucket cap. Read under this bucket's lock (held by the caller), so the bucket set is stable for this key.
        BucketQueue existing = byBucket.get(req.bucketKey);
        if (existing != null && existing.waitingDepth >= sizePerBucket) {
            return false; // this bucket's queue is full
        }
        if (reserveRetainedSlot() == false) {
            return false;
        }
        BucketQueue bucket = byBucket.computeIfAbsent(req.bucketKey, k -> new BucketQueue());
        bucket.requests.add(req);
        bucket.waitingDepth++;
        recordWaitingAdded(req);
        return true;
    }

    /**
     * Registers a provisional shared-tier acquire. It consumes only the fixed per-group retained-request budget; the
     * user-facing per-bucket waiting budget is reserved later by {@link #transitionToWaiting} if a refusal (an owner
     * denial, or an acquire the shared tier could not answer) parks the request. Must be called while holding the
     * per-bucket lock for {@code req.bucketKey}.
     */
    boolean offerPendingAcquire(QueuedRequest req) {
        if (reserveRetainedSlot() == false) {
            return false;
        }
        byBucket.computeIfAbsent(req.bucketKey, k -> new BucketQueue()).requests.add(req);
        return true;
    }

    /**
     * Converts the exact provisional request to WAITING after an owner denial, or an acquire the shared tier could not
     * answer. If the bucket's configured waiting capacity is already full, removes this exact request while retaining all
     * existing waiters. Must be called while holding the per-bucket lock for {@code req.bucketKey}.
     * <p>
     * A request that is already WAITING also yields {@link WaitingTransition#WAITING}, without a second transition, so a
     * caller counting entries in {@code total_queued} counts only a request it saw in {@code PENDING_ACQUIRE} before
     * this call, under the same lock. {@link WaitingTransition#BUCKET_FULL} is a queue rejection, never an entry.
     */
    WaitingTransition transitionToWaiting(QueuedRequest req, int sizePerBucket, long nowNanos) {
        BucketQueue bucket = byBucket.get(req.bucketKey);
        if (bucket == null || bucket.requests.contains(req) == false) {
            return WaitingTransition.REQUEST_GONE;
        }
        if (req.isWaiting()) {
            return WaitingTransition.WAITING;
        }
        if (sizePerBucket <= 0 || bucket.waitingDepth >= sizePerBucket) {
            removePresent(bucket, req);
            pruneIfEmpty(req.bucketKey, bucket);
            return WaitingTransition.BUCKET_FULL;
        }
        req.transitionToWaiting(nowNanos);
        bucket.waitingDepth++;
        recordWaitingAdded(req);
        return WaitingTransition.WAITING;
    }

    /**
     * Removes and returns the oldest retained request for {@code bucketKey}, or {@code null} if none. Must be called
     * while holding the per-bucket lock. Updates both retained and waiting counts and prunes an empty bucket.
     */
    QueuedRequest pollOldest(String bucketKey) {
        BucketQueue bucket = byBucket.get(bucketKey);
        if (bucket == null) {
            return null;
        }
        Iterator<QueuedRequest> it = bucket.requests.iterator();
        QueuedRequest req = null;
        if (it.hasNext()) {
            req = it.next(); // head = oldest (insertion order)
            it.remove();
            recordRemoved(bucket, req);
        }
        pruneIfEmpty(bucketKey, bucket);
        return req;
    }

    /**
     * Removes a specific retained request from its bucket (used for an exact acquire result or cancellation/eviction).
     * Must be called while holding the per-bucket lock. Returns {@code true} if it was present and removed; retained
     * depth is always decremented, and WAITING depth is decremented when applicable.
     * O(1) average — this is the hot path under a cancellation storm, so the bucket is a {@link LinkedHashSet} rather
     * than a deque (whose {@code remove(Object)} would be O(n), making a mass cancel on one bucket O(n^2)).
     */
    boolean remove(QueuedRequest req) {
        BucketQueue bucket = byBucket.get(req.bucketKey);
        if (bucket == null) {
            return false;
        }
        boolean removed = bucket.requests.remove(req);
        if (removed) {
            recordRemoved(bucket, req);
        }
        pruneIfEmpty(req.bucketKey, bucket);
        return removed;
    }

    /** Whether {@code bucketKey} currently holds at least one WAITING request. Must be called holding the per-bucket lock. */
    boolean hasWaiting(String bucketKey) {
        BucketQueue bucket = byBucket.get(bucketKey);
        return bucket != null && bucket.waitingDepth > 0;
    }

    /** Live concurrent-map view of bucket keys that currently have at least one retained request. */
    Set<String> bucketKeys() {
        return byBucket.keySet();
    }

    /** Whether the task {@code taskId} has a WAITING request in this group. Lock-free, so it can change at once. */
    boolean hasWaitingTask(long taskId) {
        return waitingByTaskId.containsKey(taskId);
    }

    int retainedDepth() {
        return retainedDepth.get();
    }

    int waitingDepth() {
        return waitingDepth.get();
    }

    long peakWaitingDepth() {
        return peakWaitingDepth.get();
    }

    private boolean reserveRetainedSlot() {
        int updated = retainedDepth.incrementAndGet();
        if (updated > WorkloadGroupQueueSettings.MAX_GROUP_QUEUE_DEPTH) {
            retainedDepth.decrementAndGet();
            return false;
        }
        serviceRetainedDepth.incrementAndGet();
        return true;
    }

    private void recordWaitingAdded(QueuedRequest req) {
        waitingByTaskId.put(req.task.getId(), req);
        int updated = waitingDepth.incrementAndGet();
        peakWaitingDepth.accumulateAndGet(updated, Math::max);
    }

    private void removePresent(BucketQueue bucket, QueuedRequest req) {
        if (bucket.requests.remove(req)) {
            recordRemoved(bucket, req);
        }
    }

    private void recordRemoved(BucketQueue bucket, QueuedRequest req) {
        retainedDepth.decrementAndGet();
        serviceRetainedDepth.decrementAndGet();
        if (req.isWaiting()) {
            bucket.waitingDepth--;
            waitingDepth.decrementAndGet();
            waitingByTaskId.remove(req.task.getId(), req);
        }
    }

    private void pruneIfEmpty(String bucketKey, BucketQueue bucket) {
        if (bucket.requests.isEmpty()) {
            byBucket.remove(bucketKey, bucket);
        }
    }
}
