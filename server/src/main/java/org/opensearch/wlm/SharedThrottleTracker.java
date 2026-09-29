/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

/**
 * Cluster-level tier of workload-group request throttling: the authoritative, cluster-wide in-flight counter for a
 * throttle bucket, enforcing the bucket's {@code shared_limit}. Its node-local sibling is {@link NodeThrottleTracker},
 * and {@code WorkloadGroupService.acquireThrottlePermit} composes the two (local first, then shared on overflow). On the node that
 * owns a bucket (chosen by {@link ThrottleOwnerSelector}), this holds one recorded-permit set per active or recently
 * active bucket.
 * <p>
 * Unlike the node tier, acquire and release happen on different nodes (a coordinator acquires from the owner, then
 * releases via a separate RPC), so each permit is a permit with an id and a TTL rather than a self-releasing
 * {@link org.opensearch.common.lease.Releasable}.
 * <p>
 * Correctness under concurrency and node churn:
 * <ul>
 *   <li><b>Atomic admission.</b> The check-count-then-add step runs inside {@link ConcurrentHashMap#compute},
 *       which holds the per-key bin lock, so many coordinators racing on the same bucket can never push the recorded
 *       count over the limit.</li>
 *   <li><b>Count = size of the recorded-permit map</b>, never a bare integer. This makes {@link #release} and
 *       {@link #sweepExpired} idempotent: removing by permit id is a no-op if the permit is absent (already released or
 *       swept, or the release reached a different owner after remapping), so a sweep racing a late release cannot
 *       double-decrement and a stray release after ring remap cannot corrupt the current owner's count.</li>
 *   <li><b>TTL permits.</b> Every permit carries an expiry. A coordinator that crashes (or whose release RPC is
 *       lost) leaves a permit that {@link #sweepExpired} reclaims after the TTL, so a bucket cannot get wedged at
 *       its limit forever. A different newly-selected owner process starts empty; records on a former owner remain until
 *       release or TTL. If the same live process later regains that bucket, an old record may count until it expires.</li>
 * </ul>
 * A bucket entry exists only while it has at least one recorded permit. An expired record can remain until a saturated
 * acquire or the periodic sweep prunes it, so memory scales with active or recently-active buckets rather than the total
 * user/role population.
 * <p>
 * Prune cost: reclaiming expired permits is an O(size) scan of the bucket's permit map. To keep a saturated hot bucket
 * from paying that scan on every denied acquire, each bucket caches the earliest permit expiry ({@code minExpiry}); the
 * scan is skipped whenever the bucket is full but nothing can have expired yet ({@code minExpiry > now}), which is the
 * common case under sustained load (fresh permits, long TTL). The first saturated acquire at or after the cached expiry
 * prunes the bucket; without another acquire, reclamation waits for the periodic sweep.
 */
@ExperimentalApi
public class SharedThrottleTracker {

    /**
     * Per-bucket state: the recorded permits plus the earliest expiry among them. Both fields are only read and written
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
     * @return {@code true} if the permit was granted (the recorded count was below the limit after any eligible expiry
     *         pruning), {@code false} if the bucket is already at the limit
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
            // self-healing stuck permits on the first saturated acquire at or after an expiry.
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
            // Never leave an empty bucket behind (keeps the map limited to buckets with recorded accounting).
            return bucket.permits.isEmpty() ? null : bucket;
        });
        return granted[0];
    }

    /**
     * Releases a previously-granted permit. Removing by permit id makes this idempotent and safe: an unknown id
     * (already swept, double release, or a release that reached a different owner after remapping) is a no-op and can
     * never remove a recorded permit that belongs to some other request.
     *
     * @param bucketKey the throttle bucket identifier
     * @param permitId   the permit id returned to the coordinator at acquire time
     * @return {@code true} if a recorded permit was actually removed, i.e. a slot genuinely just freed. This is the
     *         authoritative answer to "did capacity become available?", and the owner is the only node that can give it:
     *         a coordinator sending a speculative release (e.g. after a lost acquire reply) cannot know whether its
     *         permit was ever recorded. Callers drive owner-push on {@code true} only, so a no-op release never produces
     *         a phantom grant and — the case a coordinator-supplied hint used to get wrong — a release that DID free a
     *         slot always drives one.
     */
    public boolean release(String bucketKey, String permitId) {
        final boolean[] removed = new boolean[1];
        permitsByBucket.computeIfPresent(bucketKey, (k, bucket) -> {
            removed[0] = bucket.permits.remove(permitId) != null;
            // minExpiry is intentionally left unchanged: removing a permit can only raise the true earliest expiry, so
            // the cached value stays a valid (possibly loose) lower bound. Recomputing here would add an O(size) scan
            // to the release hot path for no correctness benefit.
            return bucket.permits.isEmpty() ? null : bucket;
        });
        return removed[0];
    }

    /**
     * Reclaims all expired permits across every bucket. Intended to be called periodically by the owning service.
     * Safe to run concurrently with {@link #tryAcquire}/{@link #release} thanks to per-key {@code compute}.
     *
     * @return the bucket keys for which at least one permit was reclaimed.
     */
    public List<String> sweepExpired() {
        return new ArrayList<>(sweepExpiredCounts().keySet());
    }

    /**
     * Reclaims all expired permits like {@link #sweepExpired()}, but preserves the exact number reclaimed per bucket so
     * the owning service can offer every slot created by one sweep instead of collapsing several expiries to one wake-up.
     *
     * @return each bucket that gained free capacity mapped to its number of reclaimed permits
     */
    Map<String, Integer> sweepExpiredCounts() {
        final long now = nanoTimeSupplier.getAsLong();
        final Map<String, Integer> freedByBucket = new HashMap<>();
        for (String bucketKey : permitsByBucket.keySet()) {
            permitsByBucket.computeIfPresent(bucketKey, (k, bucket) -> {
                final int before = bucket.permits.size();
                pruneExpired(bucket, now);
                final int reclaimed = before - bucket.permits.size();
                if (reclaimed > 0) {
                    // Preserve the count, not just the bucket identity. One sweep can reclaim many crashed holders, and
                    // there is no release RPC per expired permit to generate the missing wake-ups.
                    freedByBucket.put(bucketKey, reclaimed);
                }
                return bucket.permits.isEmpty() ? null : bucket;
            });
        }
        return freedByBucket;
    }

    // Current recorded count for a bucket. This can include expired permits until an acquire or sweep prunes them.
    // Package-private for tests.
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
