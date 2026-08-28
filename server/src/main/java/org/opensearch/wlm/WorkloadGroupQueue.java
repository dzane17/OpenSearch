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

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A bounded, per-coordinator request queue for a single workload group. When a search is denied by a throttle limit,
 * instead of an immediate 429 the coordinator parks it here and admits it once a permit frees (node-tier completion or
 * a cluster-tier owner grant).
 * <p>
 * The queue is partitioned into a per-bucket FIFO ({@code byBucket}) so a permit freed for one bucket wakes a request
 * waiting on that same bucket, and a heavily-queued bucket cannot head-of-line-block another. Capacity is bounded on
 * two axes (see {@link #offer}): a per-bucket cap ({@code queue.size_per_bucket}, for cross-principal fairness) and a
 * fixed per-group total ({@link WorkloadGroupQueueSettings#MAX_GROUP_QUEUE_DEPTH}, a footprint backstop). A parked
 * request holds no thread — only its {@link ActionListener} and open client connection — so the queue's footprint
 * (heap + sockets) is what those two limits bound; {@code depth} tracks the group total for the fixed ceiling.
 * <p>
 * Thread-safety: {@code depth} is a shared {@link AtomicInteger}; each bucket's {@link LinkedHashSet} is guarded by the
 * caller holding this group's per-bucket lock (a {@code KeyedLock} in {@link WorkloadGroupQueueService}). Callers must
 * never invoke a parked listener while holding that lock.
 * <p>
 * Each bucket's container is a {@link LinkedHashSet}: insertion order gives the per-bucket FIFO (head = oldest), while
 * membership-based removal is O(1) average. This matters because a request can be removed from the middle on
 * cancellation ({@link #remove}); an {@code ArrayDeque} would make that O(n), so a mass-cancellation storm on one
 * bucket (each cancel firing an independent remove) would be O(n^2). {@link QueuedRequest} has no {@code equals}/
 * {@code hashCode} override, so identity semantics hold and distinct requests never collide.
 */
@ExperimentalApi
public class WorkloadGroupQueue {

    /**
     * A parked request: the held listener (already context-preserving, wrapped upstream), its bucket key, the owning
     * task, and the handle that deregisters its task-cancellation callback once it is admitted or evicted.
     * <p>
     * A parked request has no wall-clock deadline. There is deliberately no queue timeout: legitimate queue wait is
     * unbounded (it grows with backlog depth over throughput), so any fixed cap would eventually cancel healthy,
     * still-connected requests under a large slow burst. A client bounds its own wait with
     * {@code cancel_after_time_interval} (or a disconnect); either cancels the task, which evicts the entry promptly.
     */
    @ExperimentalApi
    public static class QueuedRequest {
        final ActionListener<Releasable> listener;
        final String bucketKey;
        final WorkloadGroupTask task;
        // Relative-clock instant (nanos) the request was parked, for observability (queue-wait metric). Stable across
        // the request's queue lifetime — the request is never re-parked, so this is the true total-wait basis.
        final long enqueueNanos;
        // Set after construction (the callback needs the enqueued reference); deregisters the cancellation callback.
        volatile Releasable cancellationHandle;

        public QueuedRequest(ActionListener<Releasable> listener, String bucketKey, WorkloadGroupTask task, long enqueueNanos) {
            this.listener = listener;
            this.bucketKey = bucketKey;
            this.task = task;
            this.enqueueNanos = enqueueNanos;
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

        /** Time parked so far, in nanos, as of {@code nowNanos} (relative clock). Never negative. */
        long waitNanos(long nowNanos) {
            long w = nowNanos - enqueueNanos;
            return w < 0 ? 0 : w;
        }

        void releaseCancellationHandle() {
            Releasable handle = cancellationHandle;
            if (handle != null) {
                handle.close();
            }
        }
    }

    private final Map<String, LinkedHashSet<QueuedRequest>> byBucket = new ConcurrentHashMap<>();
    private final AtomicInteger depth = new AtomicInteger(0);
    private final AtomicLong peak = new AtomicLong(0);

    public WorkloadGroupQueue() {}

    /**
     * Attempts to park a request. Must be called while holding the per-bucket lock for {@code req.bucketKey}. Enforces
     * TWO limits and returns {@code false} (the caller then rejects with a 429) if either is hit:
     * <ol>
     *   <li><b>per-bucket</b> — the request's own bucket already holds {@code sizePerBucket} parked requests
     *       ({@code sizePerBucket <= 0} means queueing is disabled). This is the user-facing {@code queue.size_per_bucket}
     *       knob, giving cross-principal fairness <em>while the group total is below the ceiling</em>: one bucket cannot
     *       consume another's per-bucket capacity. Note the fairness is bounded, not absolute — once the group total
     *       reaches the ceiling below, admission is first-come-first-served across buckets, so a high-cardinality flood
     *       can still crowd out a well-behaved bucket that has not yet filled its own allowance.</li>
     *   <li><b>per-group total</b> — the group already holds {@link WorkloadGroupQueueSettings#MAX_GROUP_QUEUE_DEPTH}
     *       parked requests across all buckets on this coordinator. A fixed, non-configurable footprint backstop against
     *       attacker-controlled bucket cardinality (username/role buckets are derived from the request principal).</li>
     * </ol>
     * {@code sizePerBucket} is passed per call (not stored) so a live {@code queue.size_per_bucket} change takes effect
     * immediately: a decrease stops admitting to a bucket once it is at/above the new cap (already-parked requests are
     * not evicted); an increase widens capacity at once.
     * <p>
     * The per-bucket depth is read (not created) before reserving the shared group counter, so a group-ceiling rejection
     * never leaves an empty bucket set behind — preserving the invariant that a present bucket key has a live waiter.
     *
     * @param req           the request to park
     * @param sizePerBucket the group's <em>current</em> {@code queue.size_per_bucket}
     * @return {@code true} if the request was enqueued, {@code false} if rejected (disabled / bucket full / group full)
     */
    boolean offer(QueuedRequest req, int sizePerBucket) {
        if (sizePerBucket <= 0) {
            return false; // queueing disabled
        }
        // Per-bucket cap. Read under this bucket's lock (held by the caller), so the bucket set is stable for this key.
        LinkedHashSet<QueuedRequest> existing = byBucket.get(req.bucketKey);
        if (existing != null && existing.size() >= sizePerBucket) {
            return false; // this bucket's queue is full
        }
        // Fixed per-group backstop on the TOTAL across all buckets. Reserve first; only touch the bucket set once the
        // slot is secured, so the set and the depth counter never disagree (and no empty set is left on rejection).
        int updated = depth.incrementAndGet();
        if (updated > WorkloadGroupQueueSettings.MAX_GROUP_QUEUE_DEPTH) {
            depth.decrementAndGet();
            return false; // group-wide ceiling hit
        }
        peak.accumulateAndGet(updated, Math::max);
        byBucket.computeIfAbsent(req.bucketKey, k -> new LinkedHashSet<>()).add(req);
        return true;
    }

    /**
     * Returns the oldest parked request for {@code bucketKey} without removing it, or {@code null} if none. Must be
     * called while holding the per-bucket lock.
     */
    QueuedRequest peekOldest(String bucketKey) {
        LinkedHashSet<QueuedRequest> bucket = byBucket.get(bucketKey);
        if (bucket == null) {
            return null;
        }
        java.util.Iterator<QueuedRequest> it = bucket.iterator();
        return it.hasNext() ? it.next() : null; // head = oldest (insertion order)
    }

    /**
     * Removes and returns the oldest parked request for {@code bucketKey}, or {@code null} if none. Must be called
     * while holding the per-bucket lock. Decrements the shared depth and prunes the bucket entry when it empties.
     */
    QueuedRequest pollOldest(String bucketKey) {
        LinkedHashSet<QueuedRequest> bucket = byBucket.get(bucketKey);
        if (bucket == null) {
            return null;
        }
        java.util.Iterator<QueuedRequest> it = bucket.iterator();
        QueuedRequest req = null;
        if (it.hasNext()) {
            req = it.next(); // head = oldest (insertion order)
            it.remove();
            depth.decrementAndGet();
        }
        if (bucket.isEmpty()) {
            byBucket.remove(bucketKey);
        }
        return req;
    }

    /**
     * Removes and returns every parked request in {@code bucketKey} whose task is cancelled, leaving all survivors in
     * place. Must be called while holding the per-bucket lock. Depth is decremented for each removed request.
     * <p>
     * This is the backstop sweep's cleanup: a defense-in-depth complement to the per-request cancellation callback
     * (which normally evicts a cancelled entry immediately), catching any cancelled task the callback missed. There is
     * no time-based eviction — a still-live parked request is never removed here regardless of how long it has waited;
     * a corrupt/dead but uncancelled entry self-heals when it drains to the head and fails on execution.
     */
    java.util.List<QueuedRequest> evictCancelled(String bucketKey) {
        LinkedHashSet<QueuedRequest> bucket = byBucket.get(bucketKey);
        if (bucket == null) {
            return java.util.Collections.emptyList();
        }
        java.util.List<QueuedRequest> evicted = new java.util.ArrayList<>();
        for (java.util.Iterator<QueuedRequest> it = bucket.iterator(); it.hasNext();) {
            QueuedRequest req = it.next();
            if (req.task().isCancelled()) {
                it.remove();
                depth.decrementAndGet();
                evicted.add(req);
            }
        }
        if (bucket.isEmpty()) {
            byBucket.remove(bucketKey);
        }
        return evicted;
    }

    /**
     * Removes a specific parked request from its bucket (used on cancellation/eviction). Must be called while holding
     * the per-bucket lock. Returns {@code true} if it was present and removed (in which case depth is decremented).
     * O(1) average — this is the hot path under a cancellation storm, so the bucket is a {@link LinkedHashSet} rather
     * than a deque (whose {@code remove(Object)} would be O(n), making a mass cancel on one bucket O(n^2)).
     */
    boolean remove(QueuedRequest req) {
        LinkedHashSet<QueuedRequest> bucket = byBucket.get(req.bucketKey);
        if (bucket == null) {
            return false;
        }
        boolean removed = bucket.remove(req);
        if (removed) {
            depth.decrementAndGet();
        }
        if (bucket.isEmpty()) {
            byBucket.remove(req.bucketKey);
        }
        return removed;
    }

    /** Snapshot of the bucket keys that currently have at least one parked request. */
    java.util.Set<String> bucketKeys() {
        return byBucket.keySet();
    }

    int currentDepth() {
        return depth.get();
    }

    long peakDepth() {
        return peak.get();
    }
}
