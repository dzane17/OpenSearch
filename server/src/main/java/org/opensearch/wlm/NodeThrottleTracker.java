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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Node-local tier of workload-group request throttling: tracks in-flight requests per throttle bucket on a single
 * node and enforces the bucket's {@code node_limit}. Its cluster-wide sibling is {@link SharedThrottleTracker}, and
 * {@code WorkloadGroupService.acquireThrottlePermit} composes the two (local first, then shared on overflow).
 * <p>
 * A bucket is identified by an opaque key (see {@code WorkloadGroupService} for how the key is built from a
 * workload group and its throttle attribute). A counter exists only while a bucket has at least one in-flight
 * request: it is created on first acquire and removed when it drains back to zero, so memory scales with the
 * number of concurrently active buckets rather than the total population of users/roles.
 * <p>
 * This tier is fully local — no cross-node coordination — mirroring the acquire/rollback + {@link Releasable}
 * release shape of {@link org.opensearch.index.IndexingPressure}. Because acquire and release happen in the same
 * process, a permit is a self-releasing {@link Releasable} and needs no permit id or TTL (contrast
 * {@link SharedThrottleTracker}, whose distributed acquire/release requires both). When a two-tier throttle is
 * configured, an exhausted local tier falls through to the cluster-level {@link SharedThrottleTracker} rather than
 * rejecting outright; see {@link #tryAcquire}, which returns {@code null} on exhaustion instead of throwing.
 */
@ExperimentalApi
public class NodeThrottleTracker {

    private final Map<String, AtomicInteger> inFlightByBucket = new ConcurrentHashMap<>();

    /**
     * Attempts to admit one request into the given bucket under the per-node limit, returning {@code null} instead
     * of throwing when the bucket is already at the limit. This is the fall-through-friendly form: a caller that
     * also has a cluster-level shared pool can try the shared pool on a {@code null} result rather than rejecting.
     *
     * @param bucketKey the throttle bucket identifier
     * @param nodeLimit the maximum concurrent in-flight requests this node may admit for the bucket
     * @return a {@link Releasable} that decrements the bucket's in-flight count exactly once when closed, or
     *         {@code null} if the per-node limit is already reached (nothing was acquired, nothing to release)
     */
    public Releasable tryAcquire(String bucketKey, int nodeLimit) {
        // Create-and-increment atomically with respect to the decrement-and-remove in release(), so a concurrent
        // release draining a bucket to 0 can never orphan the counter this acquire is about to use. Capture THIS
        // caller's post-increment value from inside compute() and decide on it — reading counter.get() afterwards
        // would observe other concurrent acquirers' increments too, causing spurious over-declines (all racers seeing
        // an inflated count and rolling back, admitting zero when a slot was free).
        final int[] countAfterIncrement = new int[1];
        AtomicInteger counter = inFlightByBucket.compute(bucketKey, (k, existing) -> {
            AtomicInteger c = existing != null ? existing : new AtomicInteger(0);
            countAfterIncrement[0] = c.incrementAndGet();
            return c;
        });
        if (countAfterIncrement[0] > nodeLimit) {
            // Over the cap: roll back this increment and decline. Never admits over the limit.
            release(bucketKey, counter);
            return null;
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
