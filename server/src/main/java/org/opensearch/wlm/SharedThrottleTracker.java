/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.common.lease.Releasable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

/**
 * Owner-side in-flight counter for {@code shared_limit} buckets, the cluster-wide sibling of
 * {@link WorkloadGroupThrottleTracker}. Acquire and release arrive as separate RPCs from a coordinator, so each grant is
 * a permit with an id and a TTL rather than a self-releasing {@link Releasable}.
 * <ul>
 *   <li><b>Atomic admission:</b> check-then-add runs inside {@link ConcurrentHashMap#compute}, so racing coordinators
 *       can never push a bucket over its limit.</li>
 *   <li><b>Count = size of the permit map</b>, so release and sweep are idempotent by permit id: a stale or repeated
 *       release can never decrement another request's permit.</li>
 *   <li><b>Departed coordinators:</b> each permit records its coordinator's ephemeral id, and {@link #releaseAllFrom}
 *       drops a departed coordinator's permits instead of leaving them until the TTL.</li>
 *   <li><b>TTL:</b> a permit whose release never arrives expires, so a bucket cannot stay wedged at its limit.</li>
 * </ul>
 * A bucket entry exists only while it holds a permit, so memory scales with concurrently active buckets.
 * <p>
 * Expired permits are pruned by an O(size) scan, which a saturated bucket skips while its cached earliest expiry
 * ({@code minExpiry}) is still in the future, keeping denied acquires O(1) under sustained load.
 */
final class SharedThrottleTracker {

    /**
     * Per-bucket state, only accessed under the outer map's per-key lock. {@code minExpiry} is a lower bound on the
     * earliest expiry: tightened on insert, recomputed on prune, and left stale on release, which can only cost one
     * extra scan, never skip a scan that would free a slot.
     */
    private static final class Bucket {
        final Map<String, Permit> permits = new ConcurrentHashMap<>();
        long minExpiry = Long.MAX_VALUE;
    }

    /** One live permit: its expiry and the ephemeral id of the coordinator holding it ({@code ""} if unknown). */
    private record Permit(long expiresAt, String coordinatorId) {
    }

    /** Coordinator id of a permit that must never be purged by {@link #releaseAllFrom} (the TTL still applies). */
    static final String UNKNOWN_COORDINATOR = "";

    private final Map<String, Bucket> permitsByBucket = new ConcurrentHashMap<>();
    private final LongSupplier nanoTimeSupplier;

    // Expiry scans actually run; read by tests.
    private final LongAdder pruneScans = new LongAdder();

    SharedThrottleTracker() {
        this(System::nanoTime);
    }

    // Injectable clock for tests.
    SharedThrottleTracker(LongSupplier nanoTimeSupplier) {
        this.nanoTimeSupplier = nanoTimeSupplier;
    }

    /**
     * Admits one request into {@code bucketKey} if fewer than {@code sharedLimit} permits are live, recording the permit
     * against {@code coordinatorId} (an ephemeral node id, or {@link #UNKNOWN_COORDINATOR}) for {@link #releaseAllFrom}.
     *
     * @return {@code true} if granted, {@code false} if the bucket is at its limit
     */
    boolean tryAcquire(String bucketKey, int sharedLimit, String permitId, long ttlNanos, String coordinatorId) {
        final long now = nanoTimeSupplier.getAsLong();
        final long expiresAt = now + ttlNanos;
        final Permit permit = new Permit(expiresAt, coordinatorId);
        final boolean[] granted = new boolean[1];
        permitsByBucket.compute(bucketKey, (k, bucket) -> {
            if (bucket == null) {
                bucket = new Bucket();
            }
            // Scan only when the bucket is full and some permit could have expired.
            if (bucket.permits.size() >= sharedLimit && bucket.minExpiry <= now) {
                pruneExpired(bucket, now);
            }
            if (bucket.permits.size() < sharedLimit) {
                bucket.permits.put(permitId, permit);
                if (expiresAt < bucket.minExpiry) {
                    bucket.minExpiry = expiresAt;
                }
                granted[0] = true;
            }
            return bucket.permits.isEmpty() ? null : bucket;
        });
        return granted[0];
    }

    /**
     * Releases a permit by id; a no-op for an unknown or already-released id.
     *
     * @return {@code true} if a recorded permit was removed, i.e. a slot just freed. Only the owner can know this, so
     *         owner-push is driven on {@code true} only and a no-op release never produces a phantom grant.
     */
    boolean release(String bucketKey, String permitId) {
        final boolean[] removed = new boolean[1];
        permitsByBucket.computeIfPresent(bucketKey, (k, bucket) -> {
            removed[0] = bucket.permits.remove(permitId) != null;
            // minExpiry intentionally left as is (see Bucket).
            return bucket.permits.isEmpty() ? null : bucket;
        });
        return removed[0];
    }

    /**
     * Releases every permit held by one of {@code coordinatorIds} (ephemeral node ids of coordinators that left the
     * cluster), across all buckets. Permits recorded with {@link #UNKNOWN_COORDINATOR} are never matched. An O(total
     * permits) scan, so only call it on node removal.
     *
     * @return each bucket that lost permits mapped to the number released (empty if none)
     */
    Map<String, Integer> releaseAllFrom(Set<String> coordinatorIds) {
        final Map<String, Integer> releasedByBucket = new HashMap<>();
        if (coordinatorIds.isEmpty()) {
            return releasedByBucket;
        }
        for (String bucketKey : permitsByBucket.keySet()) {
            permitsByBucket.computeIfPresent(bucketKey, (k, bucket) -> {
                int released = 0;
                for (Iterator<Permit> it = bucket.permits.values().iterator(); it.hasNext();) {
                    final String coordinatorId = it.next().coordinatorId();
                    if (UNKNOWN_COORDINATOR.equals(coordinatorId) == false && coordinatorIds.contains(coordinatorId)) {
                        it.remove();
                        released++;
                    }
                }
                if (released > 0) {
                    releasedByBucket.put(bucketKey, released);
                }
                return bucket.permits.isEmpty() ? null : bucket;
            });
        }
        return releasedByBucket;
    }

    /**
     * Reclaims expired permits across all buckets; safe to run concurrently with acquire and release.
     *
     * @return the bucket keys for which at least one permit was reclaimed
     */
    List<String> sweepExpired() {
        return new ArrayList<>(sweepExpiredCounts().keySet());
    }

    /**
     * Like {@link #sweepExpired()}, but keeps the number reclaimed per bucket so the owner can offer every freed slot
     * (expired permits get no release RPC to wake a waiter).
     *
     * @return each bucket that gained free capacity mapped to its number of reclaimed permits
     */
    Map<String, Integer> sweepExpiredCounts() {
        final long now = nanoTimeSupplier.getAsLong();
        final Map<String, Integer> freedByBucket = new HashMap<>();
        for (String bucketKey : permitsByBucket.keySet()) {
            permitsByBucket.computeIfPresent(bucketKey, (k, bucket) -> {
                if (bucket.minExpiry <= now) {
                    final int before = bucket.permits.size();
                    pruneExpired(bucket, now);
                    final int reclaimed = before - bucket.permits.size();
                    if (reclaimed > 0) {
                        freedByBucket.put(bucketKey, reclaimed);
                    }
                }
                return bucket.permits.isEmpty() ? null : bucket;
            });
        }
        return freedByBucket;
    }

    // Raw number of permit records for a bucket, not filtered by expiry.
    int inFlight(String bucketKey) {
        Bucket bucket = permitsByBucket.get(bucketKey);
        return bucket == null ? 0 : bucket.permits.size();
    }

    // Visible for tests.
    int activeBuckets() {
        return permitsByBucket.size();
    }

    // Visible for tests.
    long pruneScanCount() {
        return pruneScans.sum();
    }

    // Removes expired permits and recomputes the bucket's earliest surviving expiry.
    private void pruneExpired(Bucket bucket, long now) {
        pruneScans.increment();
        long min = Long.MAX_VALUE;
        for (Iterator<Permit> it = bucket.permits.values().iterator(); it.hasNext();) {
            final long expiresAt = it.next().expiresAt();
            if (expiresAt <= now) {
                it.remove();
            } else if (expiresAt < min) {
                min = expiresAt;
            }
        }
        bucket.minExpiry = min;
    }
}
