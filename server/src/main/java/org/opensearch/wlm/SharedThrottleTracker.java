/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * Cluster-level tier of workload-group request throttling: the authoritative, cluster-wide in-flight counter for a
 * throttle bucket, enforcing the bucket's {@code shared_limit}. Its node-local sibling is {@link NodeThrottleTracker},
 * and {@code WorkloadGroupService.acquireThrottlePermit} composes the two (local first, then shared on overflow). On the node that
 * owns a bucket (chosen by {@link ThrottleOwnerSelector}), this holds one live-lease set per active bucket.
 * <p>
 * Unlike the node tier, acquire and release happen on different nodes (a coordinator acquires from the owner, then
 * releases via a separate RPC), so each permit is a lease with an id and a TTL rather than a self-releasing
 * {@link org.opensearch.common.lease.Releasable}.
 * <p>
 * Correctness under concurrency and node churn:
 * <ul>
 *   <li><b>Atomic admission.</b> The check-count-then-add step runs inside {@link ConcurrentHashMap#compute},
 *       which holds the per-key bin lock, so many coordinators racing on the same bucket can never push the live
 *       count over the limit.</li>
 *   <li><b>Count = size of the live-lease map</b>, never a bare integer. This makes {@link #release} and
 *       {@link #sweepExpired} idempotent: removing by lease id is a no-op if the lease is already gone (already
 *       swept, or the bucket was remapped to a different owner), so a sweep racing a late release cannot
 *       double-decrement, and a stray release after ring remap cannot corrupt a live count.</li>
 *   <li><b>TTL leases.</b> Every lease carries an expiry. A coordinator that crashes (or whose release RPC is
 *       lost) leaves a lease that {@link #sweepExpired} reclaims after the TTL, so a bucket cannot get wedged at
 *       its limit forever. A rejoining owner starts empty — no phantom leases.</li>
 * </ul>
 * A bucket entry exists only while it has at least one live lease, so memory scales with concurrently-active
 * buckets, not the total user/role population.
 */
@ExperimentalApi
public class SharedThrottleTracker {

    private final Map<String, Map<String, Long>> leasesByBucket = new ConcurrentHashMap<>();
    private final LongSupplier nanoTimeSupplier;

    public SharedThrottleTracker() {
        this(System::nanoTime);
    }

    public SharedThrottleTracker(LongSupplier nanoTimeSupplier) {
        this.nanoTimeSupplier = nanoTimeSupplier;
    }

    /**
     * Attempts to admit one request into a bucket under its cluster-wide shared limit.
     *
     * @param bucketKey   the throttle bucket identifier
     * @param sharedLimit the maximum concurrent in-flight requests allowed across the cluster for this bucket
     * @param leaseId     a globally-unique lease id minted by the requesting coordinator
     * @param ttlNanos    how long the lease may live before {@link #sweepExpired} may reclaim it
     * @return {@code true} if the lease was granted (live count was below the limit), {@code false} if the bucket
     *         is already at the limit
     */
    public boolean tryAcquire(String bucketKey, int sharedLimit, String leaseId, long ttlNanos) {
        final long expiresAt = nanoTimeSupplier.getAsLong() + ttlNanos;
        final boolean[] granted = new boolean[1];
        leasesByBucket.compute(bucketKey, (k, leases) -> {
            if (leases == null) {
                leases = new ConcurrentHashMap<>();
            }
            // Only prune when the bucket looks full. An expired lease can change the outcome only when we would
            // otherwise reject; below the limit there is a free slot regardless, so we skip the O(size) scan and keep
            // the common under-limit acquire O(1). A saturated bucket still self-heals its stuck leases right here, at
            // the one moment it matters. (Leases in an idle bucket are reclaimed by the periodic sweep instead.)
            if (leases.size() >= sharedLimit) {
                pruneExpired(leases);
            }
            if (leases.size() < sharedLimit) {
                leases.put(leaseId, expiresAt);
                granted[0] = true;
            }
            // Never leave an empty map behind (keeps the live set == active buckets).
            return leases.isEmpty() ? null : leases;
        });
        return granted[0];
    }

    /**
     * Releases a previously-granted lease. Removing by lease id makes this idempotent and safe: an unknown id
     * (already swept, double release, or a lease that belonged to a previous owner of a since-remapped bucket) is
     * a no-op and can never decrement a live lease that belongs to some other request.
     *
     * @param bucketKey the throttle bucket identifier
     * @param leaseId   the lease id returned to the coordinator at acquire time
     */
    public void release(String bucketKey, String leaseId) {
        leasesByBucket.computeIfPresent(bucketKey, (k, leases) -> {
            leases.remove(leaseId);
            return leases.isEmpty() ? null : leases;
        });
    }

    /**
     * Reclaims all expired leases across every bucket. Intended to be called periodically by the owning service.
     * Safe to run concurrently with {@link #tryAcquire}/{@link #release} thanks to per-key {@code compute}.
     */
    public void sweepExpired() {
        for (String bucketKey : leasesByBucket.keySet()) {
            leasesByBucket.computeIfPresent(bucketKey, (k, leases) -> {
                pruneExpired(leases);
                return leases.isEmpty() ? null : leases;
            });
        }
    }

    // Current live (non-expired-at-read-time) count for a bucket. Package-private for tests.
    int inFlight(String bucketKey) {
        Map<String, Long> leases = leasesByBucket.get(bucketKey);
        return leases == null ? 0 : leases.size();
    }

    // Number of buckets currently holding at least one lease. Package-private for tests.
    int activeBuckets() {
        return leasesByBucket.size();
    }

    private void pruneExpired(Map<String, Long> leases) {
        final long now = nanoTimeSupplier.getAsLong();
        for (Iterator<Map.Entry<String, Long>> it = leases.entrySet().iterator(); it.hasNext();) {
            if (it.next().getValue() <= now) {
                it.remove();
            }
        }
    }
}
