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
import java.util.concurrent.atomic.LongAdder;
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
 * <p>
 * Prune cost: reclaiming expired leases is an O(size) scan of the bucket's lease map. To keep a saturated hot bucket
 * from paying that scan on every denied acquire, each bucket caches the earliest lease expiry ({@code minExpiry}); the
 * scan is skipped whenever the bucket is full but nothing can have expired yet ({@code minExpiry > now}), which is the
 * common case under sustained load (fresh leases, long TTL). The scan still runs promptly the moment a lease actually
 * expires, so reclamation is never delayed.
 */
@ExperimentalApi
public class SharedThrottleTracker {

    /**
     * Per-bucket state: the live leases plus the earliest expiry among them. Both fields are only read and written
     * inside a {@code leasesByBucket.compute(...)} block, i.e. under the outer map's per-key bin lock, so the plain
     * {@code long} needs no additional synchronization. {@code minExpiry} is a lower bound on the true earliest expiry:
     * it is tightened on insert and recomputed exactly during a prune, but deliberately left stale (small) on
     * {@link #release} — removing a lease can only raise the true minimum, so a stale-small value can at most trigger
     * one extra (harmless) scan, never cause a full bucket to skip a scan that would have freed a slot.
     */
    private static final class Bucket {
        final Map<String, Long> leases = new ConcurrentHashMap<>();
        long minExpiry = Long.MAX_VALUE;
    }

    private final Map<String, Bucket> leasesByBucket = new ConcurrentHashMap<>();
    private final LongSupplier nanoTimeSupplier;

    // Number of times an O(size) expiry scan actually ran (as opposed to being skipped by the minExpiry guard).
    // Package-private diagnostic, used by tests to assert the scan-skipping optimization holds.
    private final LongAdder pruneScans = new LongAdder();

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
        final long now = nanoTimeSupplier.getAsLong();
        final long expiresAt = now + ttlNanos;
        final boolean[] granted = new boolean[1];
        leasesByBucket.compute(bucketKey, (k, bucket) -> {
            if (bucket == null) {
                bucket = new Bucket();
            }
            // Only scan when the bucket looks full AND some lease could actually have expired. Below the limit there
            // is a free slot regardless; and while minExpiry > now every lease is still live, so the O(size) scan
            // could not free anything. Skipping it keeps the common saturated-but-fresh acquire O(1) while still
            // self-healing stuck leases the instant one expires.
            if (bucket.leases.size() >= sharedLimit && bucket.minExpiry <= now) {
                pruneExpired(bucket, now);
            }
            if (bucket.leases.size() < sharedLimit) {
                bucket.leases.put(leaseId, expiresAt);
                if (expiresAt < bucket.minExpiry) {
                    bucket.minExpiry = expiresAt;
                }
                granted[0] = true;
            }
            // Never leave an empty bucket behind (keeps the live set == active buckets).
            return bucket.leases.isEmpty() ? null : bucket;
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
        leasesByBucket.computeIfPresent(bucketKey, (k, bucket) -> {
            bucket.leases.remove(leaseId);
            // minExpiry is intentionally left unchanged: removing a lease can only raise the true earliest expiry, so
            // the cached value stays a valid (possibly loose) lower bound. Recomputing here would add an O(size) scan
            // to the release hot path for no correctness benefit.
            return bucket.leases.isEmpty() ? null : bucket;
        });
    }

    /**
     * Reclaims all expired leases across every bucket. Intended to be called periodically by the owning service.
     * Safe to run concurrently with {@link #tryAcquire}/{@link #release} thanks to per-key {@code compute}.
     */
    public void sweepExpired() {
        final long now = nanoTimeSupplier.getAsLong();
        for (String bucketKey : leasesByBucket.keySet()) {
            leasesByBucket.computeIfPresent(bucketKey, (k, bucket) -> {
                pruneExpired(bucket, now);
                return bucket.leases.isEmpty() ? null : bucket;
            });
        }
    }

    // Current live (non-expired-at-read-time) count for a bucket. Package-private for tests.
    int inFlight(String bucketKey) {
        Bucket bucket = leasesByBucket.get(bucketKey);
        return bucket == null ? 0 : bucket.leases.size();
    }

    // Number of buckets currently holding at least one lease. Package-private for tests.
    int activeBuckets() {
        return leasesByBucket.size();
    }

    // Number of O(size) expiry scans performed so far. Package-private for tests to verify scan-skipping.
    long pruneScanCount() {
        return pruneScans.sum();
    }

    // Removes every expired lease and recomputes the bucket's earliest surviving expiry. Runs the O(size) scan, so it
    // is only reached when a scan is actually warranted (see the guard in tryAcquire) or from the periodic sweep.
    private void pruneExpired(Bucket bucket, long now) {
        pruneScans.increment();
        long min = Long.MAX_VALUE;
        for (Iterator<Map.Entry<String, Long>> it = bucket.leases.entrySet().iterator(); it.hasNext();) {
            final long expiresAt = it.next().getValue();
            if (expiresAt <= now) {
                it.remove();
            } else if (expiresAt < min) {
                min = expiresAt;
            }
        }
        bucket.minExpiry = min;
    }
}
