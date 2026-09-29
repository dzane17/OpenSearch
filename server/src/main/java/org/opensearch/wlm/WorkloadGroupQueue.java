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
 * {@link RequestState#PENDING_ACQUIRE} before contacting the owner. A direct grant or fail-open result admits that
 * exact entry; a registered denial attempts to transition it to {@code WAITING}, while an unregistered denial or full
 * waiting bucket rejects it.
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
@ExperimentalApi
public class WorkloadGroupQueue {

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
     * A retained request: the held listener (already context-preserving, wrapped upstream), its bucket key, the owning
     * task, current state, and the handle that deregisters its task-cancellation callback once it leaves the queue.
     * <p>
     * A waiting request has no wall-clock deadline. There is deliberately no queue timeout: legitimate queue wait is
     * unbounded (it grows with backlog depth over throughput), so any fixed cap would eventually cancel healthy,
     * still-connected requests under a large slow burst. A client bounds its own wait with
     * {@code cancel_after_time_interval} (or a disconnect); either cancels the task, which evicts the entry promptly.
     */
    @ExperimentalApi
    public static class QueuedRequest {
        final ActionListener<Releasable> listener;
        final String bucketKey;
        final WorkloadGroupTask task;
        volatile RequestState state;
        // Relative-clock instant (nanos) at which the request entered WAITING. A provisional PENDING_ACQUIRE has no
        // queue-wait start: owner round-trip time is not denied-backlog latency.
        volatile long waitStartNanos;
        // Set after construction (the callback needs the enqueued reference); deregisters the cancellation callback.
        volatile Releasable cancellationHandle;

        public QueuedRequest(ActionListener<Releasable> listener, String bucketKey, WorkloadGroupTask task, long waitStartNanos) {
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

        public ActionListener<Releasable> listener() {
            return listener;
        }

        public String bucketKey() {
            return bucketKey;
        }

        public WorkloadGroupTask task() {
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
    private final AtomicInteger retainedDepth = new AtomicInteger(0);
    private final AtomicInteger waitingDepth = new AtomicInteger(0);
    private final AtomicLong peakWaitingDepth = new AtomicLong(0);

    public WorkloadGroupQueue() {}

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
        recordWaitingAdded();
        return true;
    }

    /**
     * Registers a provisional shared-tier acquire. It consumes only the fixed per-group retained-request budget; the
     * user-facing per-bucket waiting budget is reserved later by {@link #transitionToWaiting} if a registered owner
     * denial is accepted. Must be called while holding the per-bucket lock for {@code req.bucketKey}.
     */
    boolean offerPendingAcquire(QueuedRequest req) {
        if (reserveRetainedSlot() == false) {
            return false;
        }
        byBucket.computeIfAbsent(req.bucketKey, k -> new BucketQueue()).requests.add(req);
        return true;
    }

    /**
     * Converts the exact provisional request to WAITING after an owner denial. If the bucket's configured waiting
     * capacity is already full, removes this exact request while retaining all existing waiters.
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
        recordWaitingAdded();
        return WaitingTransition.WAITING;
    }

    /**
     * Returns the oldest retained request for {@code bucketKey} without removing it, or {@code null} if none. Must be
     * called while holding the per-bucket lock.
     */
    QueuedRequest peekOldest(String bucketKey) {
        BucketQueue bucket = byBucket.get(bucketKey);
        if (bucket == null) {
            return null;
        }
        Iterator<QueuedRequest> it = bucket.requests.iterator();
        return it.hasNext() ? it.next() : null; // head = oldest (insertion order)
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

    /** Live concurrent-map view of bucket keys that currently have at least one retained request. */
    Set<String> bucketKeys() {
        return byBucket.keySet();
    }

    int currentDepth() {
        return retainedDepth.get();
    }

    int currentWaitingDepth() {
        return waitingDepth.get();
    }

    long peakDepth() {
        return peakWaitingDepth.get();
    }

    private boolean reserveRetainedSlot() {
        int updated = retainedDepth.incrementAndGet();
        if (updated > WorkloadGroupQueueSettings.MAX_GROUP_QUEUE_DEPTH) {
            retainedDepth.decrementAndGet();
            return false;
        }
        return true;
    }

    private void recordWaitingAdded() {
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
        if (req.isWaiting()) {
            bucket.waitingDepth--;
            waitingDepth.decrementAndGet();
        }
    }

    private void pruneIfEmpty(String bucketKey, BucketQueue bucket) {
        if (bucket.requests.isEmpty()) {
            byBucket.remove(bucketKey, bucket);
        }
    }
}
