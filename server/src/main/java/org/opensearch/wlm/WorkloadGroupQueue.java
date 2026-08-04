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

import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A bounded, per-coordinator request queue for a single workload group. When a search is denied by a throttle limit,
 * instead of an immediate 429 the coordinator parks it here and admits it once a permit frees (node-tier completion or
 * a cluster-tier owner grant).
 * <p>
 * There is one shared capacity budget ({@code size}) and one depth measurement per group — this is "one queue per
 * group". Within that budget the queue is partitioned into a per-bucket FIFO ({@code byBucket}) so a permit freed for
 * one bucket wakes a request waiting on that same bucket, and a heavily-queued bucket cannot head-of-line-block
 * another. A parked request holds no thread — only its {@link ActionListener} and open client connection — so the
 * queue's footprint (heap + sockets) is what {@code size} bounds.
 * <p>
 * Thread-safety: {@code depth} is a shared {@link AtomicInteger}; each bucket's {@link ArrayDeque} is guarded by the
 * caller holding this group's per-bucket lock (a {@code KeyedLock} in {@link WorkloadGroupQueueService}). Callers must
 * never invoke a parked listener while holding that lock.
 */
@ExperimentalApi
public class WorkloadGroupQueue {

    /**
     * A parked request: the held listener (already context-preserving, wrapped upstream), its bucket key, the owning
     * task, the wall-clock deadline after which it is timed out, and the handle that deregisters its task-cancellation
     * callback once it is admitted or evicted.
     */
    @ExperimentalApi
    public static class QueuedRequest {
        final ActionListener<Releasable> listener;
        final String bucketKey;
        final WorkloadGroupTask task;
        // Captured principal header, so a backstop-sweep re-attempt (which runs off the request thread, without the
        // request's thread context) can re-resolve the same throttle bucket. Null when not principal-keyed.
        final String principal;
        final long deadlineNanos;
        // Set after construction (the callback needs the enqueued reference); deregisters the cancellation callback.
        volatile Releasable cancellationHandle;

        public QueuedRequest(
            ActionListener<Releasable> listener,
            String bucketKey,
            WorkloadGroupTask task,
            String principal,
            long deadlineNanos
        ) {
            this.listener = listener;
            this.bucketKey = bucketKey;
            this.task = task;
            this.principal = principal;
            this.deadlineNanos = deadlineNanos;
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

        public String principal() {
            return principal;
        }

        boolean isExpired(long nowNanos) {
            return deadlineNanos > 0 && nowNanos - deadlineNanos >= 0;
        }

        void releaseCancellationHandle() {
            Releasable handle = cancellationHandle;
            if (handle != null) {
                handle.close();
            }
        }
    }

    private final int size;
    private final Map<String, ArrayDeque<QueuedRequest>> byBucket = new ConcurrentHashMap<>();
    private final AtomicInteger depth = new AtomicInteger(0);
    private final AtomicLong peak = new AtomicLong(0);

    public WorkloadGroupQueue(int size) {
        this.size = size;
    }

    /**
     * Attempts to park a request under the group's shared {@code size} budget. Must be called while holding the
     * per-bucket lock for {@code req.bucketKey}. Returns {@code false} without enqueuing if queueing is disabled
     * ({@code size <= 0}) or the queue is full — the caller then rejects with a 429.
     *
     * @return {@code true} if the request was enqueued, {@code false} if it was rejected (disabled/full)
     */
    boolean offer(QueuedRequest req) {
        if (size <= 0) {
            return false;
        }
        // Reserve a slot against the shared budget first; only touch the bucket deque once a slot is secured, so the
        // deque and the depth counter never disagree.
        int updated = depth.incrementAndGet();
        if (updated > size) {
            depth.decrementAndGet();
            return false;
        }
        peak.accumulateAndGet(updated, Math::max);
        byBucket.computeIfAbsent(req.bucketKey, k -> new ArrayDeque<>()).addLast(req);
        return true;
    }

    /**
     * Removes and returns the oldest parked request for {@code bucketKey}, or {@code null} if none. Must be called
     * while holding the per-bucket lock. Decrements the shared depth and prunes the bucket entry when it empties.
     */
    QueuedRequest pollOldest(String bucketKey) {
        ArrayDeque<QueuedRequest> deque = byBucket.get(bucketKey);
        if (deque == null) {
            return null;
        }
        QueuedRequest req = deque.pollFirst();
        if (req != null) {
            depth.decrementAndGet();
        }
        if (deque.isEmpty()) {
            byBucket.remove(bucketKey);
        }
        return req;
    }

    /**
     * Removes a specific parked request from its bucket (used on cancellation/eviction). Must be called while holding
     * the per-bucket lock. Returns {@code true} if it was present and removed (in which case depth is decremented).
     */
    boolean remove(QueuedRequest req) {
        ArrayDeque<QueuedRequest> deque = byBucket.get(req.bucketKey);
        if (deque == null) {
            return false;
        }
        boolean removed = deque.remove(req);
        if (removed) {
            depth.decrementAndGet();
        }
        if (deque.isEmpty()) {
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

    int maxSize() {
        return size;
    }
}
