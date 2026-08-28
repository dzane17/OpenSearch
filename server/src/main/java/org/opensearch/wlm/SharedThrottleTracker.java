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
 * owns a bucket (chosen by {@link ThrottleOwnerSelector}), this holds one live-permit set per active bucket.
 * <p>
 * Unlike the node tier, acquire and release happen on different nodes (a coordinator acquires from the owner, then
 * releases via a separate RPC), so each permit is a permit with an id and a TTL rather than a self-releasing
 * {@link org.opensearch.common.lease.Releasable}.
 * <p>
 * Correctness under concurrency and node churn:
 * <ul>
 *   <li><b>Atomic admission.</b> The check-count-then-add step runs inside {@link ConcurrentHashMap#compute},
 *       which holds the per-key bin lock, so many coordinators racing on the same bucket can never push the live
 *       count over the limit.</li>
 *   <li><b>Count = size of the live-permit map</b>, never a bare integer. This makes {@link #release} and
 *       {@link #sweepExpired} idempotent: removing by permit id is a no-op if the permit is already gone (already
 *       swept, or the bucket was remapped to a different owner), so a sweep racing a late release cannot
 *       double-decrement, and a stray release after ring remap cannot corrupt a live count.</li>
 *   <li><b>TTL permits.</b> Every permit carries an expiry. A coordinator that crashes (or whose release RPC is
 *       lost) leaves a permit that {@link #sweepExpired} reclaims after the TTL, so a bucket cannot get wedged at
 *       its limit forever. A rejoining owner starts empty — no phantom permits.</li>
 * </ul>
 * A bucket entry exists only while it has at least one live permit, so memory scales with concurrently-active
 * buckets, not the total user/role population.
 * <p>
 * Prune cost: reclaiming expired permits is an O(size) scan of the bucket's permit map. To keep a saturated hot bucket
 * from paying that scan on every denied acquire, each bucket caches the earliest permit expiry ({@code minExpiry}); the
 * scan is skipped whenever the bucket is full but nothing can have expired yet ({@code minExpiry > now}), which is the
 * common case under sustained load (fresh permits, long TTL). The scan still runs promptly the moment a permit actually
 * expires, so reclamation is never delayed.
 */
@ExperimentalApi
public class SharedThrottleTracker {

    /**
     * Per-bucket state: the live permits plus the earliest expiry among them. Both fields are only read and written
     * inside a {@code permitsByBucket.compute(...)} block, i.e. under the outer map's per-key bin lock, so the plain
     * {@code long} needs no additional synchronization. {@code minExpiry} is a lower bound on the true earliest expiry:
     * it is tightened on insert and recomputed exactly during a prune, but deliberately left stale (small) on
     * {@link #release} — removing a permit can only raise the true minimum, so a stale-small value can at most trigger
     * one extra (harmless) scan, never cause a full bucket to skip a scan that would have freed a slot.
     */
    private static final class Bucket {
        final Map<String, Long> permits = new ConcurrentHashMap<>();
        long minExpiry = Long.MAX_VALUE;
    }

    private final Map<String, Bucket> permitsByBucket = new ConcurrentHashMap<>();
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
     * @param permitId     a globally-unique permit id minted by the requesting coordinator
     * @param ttlNanos    how long the permit may live before {@link #sweepExpired} may reclaim it
     * @return {@code true} if the permit was granted (live count was below the limit), {@code false} if the bucket
     *         is already at the limit
     */
    public boolean tryAcquire(String bucketKey, int sharedLimit, String permitId, long ttlNanos) {
        final long now = nanoTimeSupplier.getAsLong();
        final long expiresAt = now + ttlNanos;
        final boolean[] granted = new boolean[1];
        permitsByBucket.compute(bucketKey, (k, bucket) -> {
            if (bucket == null) {
                bucket = new Bucket();
            }
            // Only scan when the bucket looks full AND some permit could actually have expired. Below the limit there
            // is a free slot regardless; and while minExpiry > now every permit is still live, so the O(size) scan
            // could not free anything. Skipping it keeps the common saturated-but-fresh acquire O(1) while still
            // self-healing stuck permits the instant one expires.
            if (bucket.permits.size() >= sharedLimit && bucket.minExpiry <= now) {
                pruneExpired(bucket, now);
            }
            if (bucket.permits.size() < sharedLimit) {
                bucket.permits.put(permitId, expiresAt);
                if (expiresAt < bucket.minExpiry) {
                    bucket.minExpiry = expiresAt;
                }
                granted[0] = true;
            }
            // Never leave an empty bucket behind (keeps the live set == active buckets).
            return bucket.permits.isEmpty() ? null : bucket;
        });
        return granted[0];
    }

    /**
     * Releases a previously-granted permit. Removing by permit id makes this idempotent and safe: an unknown id
     * (already swept, double release, or a permit that belonged to a previous owner of a since-remapped bucket) is
     * a no-op and can never decrement a live permit that belongs to some other request.
     *
     * @param bucketKey the throttle bucket identifier
     * @param permitId   the permit id returned to the coordinator at acquire time
     */
    public void release(String bucketKey, String permitId) {
        permitsByBucket.computeIfPresent(bucketKey, (k, bucket) -> {
            bucket.permits.remove(permitId);
            // minExpiry is intentionally left unchanged: removing a permit can only raise the true earliest expiry, so
            // the cached value stays a valid (possibly loose) lower bound. Recomputing here would add an O(size) scan
            // to the release hot path for no correctness benefit.
            return bucket.permits.isEmpty() ? null : bucket;
        });
    }

    /**
     * Reclaims all expired permits across every bucket. Intended to be called periodically by the owning service.
     * Safe to run concurrently with {@link #tryAcquire}/{@link #release} thanks to per-key {@code compute}.
     */
    public void sweepExpired() {
        final long now = nanoTimeSupplier.getAsLong();
        for (String bucketKey : permitsByBucket.keySet()) {
            permitsByBucket.computeIfPresent(bucketKey, (k, bucket) -> {
                pruneExpired(bucket, now);
                return bucket.permits.isEmpty() ? null : bucket;
            });
        }
    }

    // Current live (non-expired-at-read-time) count for a bucket. Package-private for tests.
    int inFlight(String bucketKey) {
        Bucket bucket = permitsByBucket.get(bucketKey);
        return bucket == null ? 0 : bucket.permits.size();
    }

    // Number of buckets currently holding at least one permit. Package-private for tests.
    int activeBuckets() {
        return permitsByBucket.size();
    }

    // Number of O(size) expiry scans performed so far. Package-private for tests to verify scan-skipping.
    long pruneScanCount() {
        return pruneScans.sum();
    }

    // Removes every expired permit and recomputes the bucket's earliest surviving expiry. Runs the O(size) scan, so it
    // is only reached when a scan is actually warranted (see the guard in tryAcquire) or from the periodic sweep.
    private void pruneExpired(Bucket bucket, long now) {
        pruneScans.increment();
        long min = Long.MAX_VALUE;
        for (Iterator<Map.Entry<String, Long>> it = bucket.permits.entrySet().iterator(); it.hasNext();) {
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
