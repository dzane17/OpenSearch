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
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracks the number of in-flight requests per throttle bucket on a single node and enforces a per-node cap.
 * <p>
 * A bucket is identified by an opaque key (see {@code WorkloadGroupService} for how the key is built from a
 * workload group and its throttle attribute). A counter exists only while a bucket has at least one in-flight
 * request: it is created on first acquire and removed when it drains back to zero, so memory scales with the
 * number of concurrently active buckets rather than the total population of users/roles.
 * <p>
 * This tier is fully local — no cross-node coordination — mirroring the acquire/rollback + {@link Releasable}
 * release shape of {@link org.opensearch.index.IndexingPressure}.
 */
@ExperimentalApi
public class WorkloadGroupThrottleTracker {

    private final Map<String, AtomicInteger> inFlightByBucket = new ConcurrentHashMap<>();

    /**
     * Attempts to admit one request into the given bucket under the per-node limit.
     *
     * @param bucketKey the throttle bucket identifier
     * @param nodeLimit the maximum concurrent in-flight requests this node may admit for the bucket
     * @return a {@link Releasable} that decrements the bucket's in-flight count exactly once when closed
     * @throws OpenSearchRejectedExecutionException (HTTP 429) if the bucket is already at the limit
     */
    public Releasable acquire(String bucketKey, int nodeLimit) {
        // Create-and-increment atomically with respect to the decrement-and-remove in release(), so a concurrent
        // release draining a bucket to 0 can never orphan the counter this acquire is about to use.
        AtomicInteger counter = inFlightByBucket.compute(bucketKey, (k, existing) -> {
            AtomicInteger c = existing != null ? existing : new AtomicInteger(0);
            c.incrementAndGet();
            return c;
        });
        if (counter.get() > nodeLimit) {
            // Over the cap: roll back this increment and reject. (Reading get() after compute may over-reject under
            // concurrent acquires on the same bucket, but never admits over the limit — the safe direction.)
            release(bucketKey, counter);
            throw new OpenSearchRejectedExecutionException(
                "Node throttle limit reached for bucket [" + bucketKey + "]: " + nodeLimit + " in-flight requests"
            );
        }
        return releaseOnce(bucketKey, counter);
    }

    /**
     * Current in-flight count for a bucket, or 0 if the bucket has no active requests. Package-private for tests.
     */
    int inFlight(String bucketKey) {
        AtomicInteger counter = inFlightByBucket.get(bucketKey);
        return counter == null ? 0 : counter.get();
    }

    // Wraps release in a one-shot guard so a double close (e.g. onRequestEnd and onRequestFailure) decrements once.
    private Releasable releaseOnce(String bucketKey, AtomicInteger counter) {
        AtomicBoolean released = new AtomicBoolean(false);
        return () -> {
            if (released.compareAndSet(false, true)) {
                release(bucketKey, counter);
            }
        };
    }

    // Decrements the bucket and removes the map entry once it drains to 0. The compute() makes the
    // decrement-and-maybe-remove atomic per key, so a concurrent acquire can't be orphaned by a remove.
    private void release(String bucketKey, AtomicInteger counter) {
        counter.decrementAndGet();
        inFlightByBucket.compute(bucketKey, (k, existing) -> (existing != null && existing.get() <= 0) ? null : existing);
    }
}
