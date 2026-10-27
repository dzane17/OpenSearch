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
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
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
 *   <li><b>TTL:</b> expired permits remain counted while their coordinator tasks are checked. Active tasks renew their
 *       permits; completed tasks, and tasks still waiting in their coordinator's queue, free their slots.</li>
 * </ul>
 * A bucket entry exists only while it holds a permit, so memory scales with concurrently active buckets.
 * <p>
 * Expired permits are found by an O(size) scan, which a saturated bucket skips while its cached earliest expiry
 * ({@code minExpiry}) is still in the future. Pending checks and short extensions after failed checks also skip scans,
 * keeping denied acquires O(1) under sustained load.
 */
final class SharedThrottleTracker {

    /**
     * Per-bucket state, only accessed under the outer map's per-key lock. {@code minExpiry} is a lower bound on the
     * earliest expiry: tightened on insert, check completion and re-key, recomputed on scan, and left stale on release,
     * which can only cost one extra scan, never skip a scan that would free a slot.
     */
    private static final class Bucket {
        final Map<String, Permit> permits = new ConcurrentHashMap<>();
        long minExpiry = Long.MAX_VALUE;
    }

    /** A retained permit; {@code expiresAt == Long.MAX_VALUE} means a task check is already pending. */
    private record Permit(long expiresAt, long ttlNanos, String coordinatorId, long coordinatorTaskId) {
    }

    /** An expired permit snapshot. Its identity fences late replies against release, purge, and subsequent checks. */
    static final class ExpiredPermit {
        final String bucketKey;
        final String permitId;
        private final Permit expected;

        private ExpiredPermit(String bucketKey, String permitId, Permit expected) {
            this.bucketKey = bucketKey;
            this.permitId = permitId;
            this.expected = expected;
        }

        String coordinatorId() {
            return expected.coordinatorId();
        }

        long coordinatorTaskId() {
            return expected.coordinatorTaskId();
        }
    }

    private static final class AcquireResult {
        boolean granted;
        List<ExpiredPermit> expired;
    }

    private final Map<String, Bucket> permitsByBucket = new ConcurrentHashMap<>();
    private final LongSupplier nanoTimeSupplier;
    private final Consumer<List<ExpiredPermit>> expiredPermitConsumer;

    // Expiry scans actually run; read by tests.
    private final LongAdder pruneScans = new LongAdder();

    SharedThrottleTracker() {
        this(System::nanoTime);
    }

    // Injectable clock for tests.
    SharedThrottleTracker(LongSupplier nanoTimeSupplier) {
        this(nanoTimeSupplier, expired -> {});
    }

    SharedThrottleTracker(LongSupplier nanoTimeSupplier, Consumer<List<ExpiredPermit>> expiredPermitConsumer) {
        this.nanoTimeSupplier = nanoTimeSupplier;
        this.expiredPermitConsumer = expiredPermitConsumer;
    }

    /**
     * Admits one request into its bucket if fewer than the request's {@code sharedLimit} permits are live. On a grant the
     * owner-side {@link Permit} is created from the request's permit id, TTL, coordinator (an ephemeral node id, for
     * {@link #releaseAllFrom}) and coordinator task id (for expiry checks).
     *
     * @return {@code true} if granted, {@code false} if the bucket is at its limit
     */
    boolean tryAcquire(WorkloadGroupSharedThrottleService.AcquirePermitRequest request) {
        final String bucketKey = request.bucketKey;
        final int sharedLimit = request.sharedLimit;
        final String permitId = request.permitId;
        final long ttlNanos = request.ttlNanos;
        final String coordinatorId = request.coordinatorId;
        final long coordinatorTaskId = request.coordinatorTaskId;
        final long now = nanoTimeSupplier.getAsLong();
        final long expiresAt = now + ttlNanos;
        final Permit permit = new Permit(expiresAt, ttlNanos, coordinatorId, coordinatorTaskId);
        final AcquireResult result = new AcquireResult();
        permitsByBucket.compute(bucketKey, (k, bucket) -> {
            if (bucket == null) {
                bucket = new Bucket();
            }
            // Pending checks stay counted, and their Long.MAX_VALUE check time prevents repeated scans.
            if (bucket.permits.size() >= sharedLimit && bucket.minExpiry <= now) {
                result.expired = collectExpired(bucketKey, bucket, now);
            }
            if (bucket.permits.size() < sharedLimit) {
                bucket.permits.put(permitId, permit);
                if (expiresAt < bucket.minExpiry) {
                    bucket.minExpiry = expiresAt;
                }
                result.granted = true;
            }
            return bucket.permits.isEmpty() ? null : bucket;
        });
        if (result.expired != null && result.expired.isEmpty() == false) {
            expiredPermitConsumer.accept(result.expired);
        }
        return result.granted;
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
     * cluster), across all buckets. An O(total permits) scan, so only call it on node removal.
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
                    if (coordinatorIds.contains(it.next().coordinatorId())) {
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
     * Collects due task checks across all buckets; safe to run concurrently with acquire and release. An expired permit
     * stays counted while its check is pending, so a sweep never frees a slot by itself: a check that reports the task
     * inactive (finished, or waiting in its coordinator's queue) frees it in {@link #completeCheck}.
     */
    void sweepExpired() {
        final long now = nanoTimeSupplier.getAsLong();
        final List<ExpiredPermit> expired = new ArrayList<>();
        for (String bucketKey : permitsByBucket.keySet()) {
            permitsByBucket.computeIfPresent(bucketKey, (k, bucket) -> {
                if (bucket.minExpiry <= now) {
                    expired.addAll(collectExpired(bucketKey, bucket, now));
                }
                return bucket.permits.isEmpty() ? null : bucket;
            });
        }
        if (expired.isEmpty() == false) {
            expiredPermitConsumer.accept(expired);
        }
    }

    /**
     * Applies a task check only while its exact pending permit record remains present.
     *
     * @return {@code true} if the check reported the task inactive and removed the permit, i.e. a slot just freed
     */
    boolean completeCheck(ExpiredPermit expired, boolean active) {
        if (active) {
            renew(expired, expired.expected.ttlNanos());
            return false;
        }
        final boolean[] removed = new boolean[1];
        permitsByBucket.computeIfPresent(expired.bucketKey, (k, bucket) -> {
            if (bucket.permits.get(expired.permitId) == expired.expected) {
                bucket.permits.remove(expired.permitId);
                removed[0] = true;
            }
            return bucket.permits.isEmpty() ? null : bucket;
        });
        return removed[0];
    }

    /** A failed check receives a short extension; the next active reply restores the original TTL. */
    void renewForRetry(ExpiredPermit expired, long retryTtlNanos) {
        renew(expired, retryTtlNanos);
    }

    /**
     * Re-keys a permit from {@code placeholderTaskId} to {@code coordinatorTaskId}: an owner-push reservation learns which
     * queued request its grant admitted only once the grant is consumed (from the GRANT's reply, or in place for a local
     * waiter). Applies only while the permit is still recorded under the placeholder, so a release or purge that overtook
     * the re-key is never undone. The re-keyed permit is a new record with a fresh TTL: a check already pending on the
     * placeholder is fenced by the identity checks of {@link #completeCheck} and {@link #renewForRetry}, and the permit's
     * next check asks about {@code coordinatorTaskId}.
     *
     * @return {@code true} if this call re-keyed the permit from {@code placeholderTaskId}; {@code false} if the permit is
     *         no longer recorded (released, purged, or freed by a check) or no longer names the placeholder
     */
    boolean rekeyTask(String bucketKey, String permitId, long placeholderTaskId, long coordinatorTaskId) {
        final long now = nanoTimeSupplier.getAsLong();
        final boolean[] rekeyed = new boolean[1];
        permitsByBucket.computeIfPresent(bucketKey, (k, bucket) -> {
            final Permit current = bucket.permits.get(permitId);
            if (current != null && current.coordinatorTaskId() == placeholderTaskId) {
                final long expiresAt = now + current.ttlNanos();
                bucket.permits.put(permitId, new Permit(expiresAt, current.ttlNanos(), current.coordinatorId(), coordinatorTaskId));
                bucket.minExpiry = Math.min(bucket.minExpiry, expiresAt);
                rekeyed[0] = true;
            }
            return bucket.permits.isEmpty() ? null : bucket;
        });
        return rekeyed[0];
    }

    private void renew(ExpiredPermit expired, long extensionNanos) {
        final long expiresAt = nanoTimeSupplier.getAsLong() + extensionNanos;
        permitsByBucket.computeIfPresent(expired.bucketKey, (k, bucket) -> {
            if (bucket.permits.get(expired.permitId) == expired.expected) {
                final Permit previous = expired.expected;
                bucket.permits.put(
                    expired.permitId,
                    new Permit(expiresAt, previous.ttlNanos(), previous.coordinatorId(), previous.coordinatorTaskId())
                );
                bucket.minExpiry = Math.min(bucket.minExpiry, expiresAt);
            }
            return bucket.permits.isEmpty() ? null : bucket;
        });
    }

    // Raw number of permit records for a bucket, not filtered by expiry.
    int inFlight(String bucketKey) {
        Bucket bucket = permitsByBucket.get(bucketKey);
        return bucket == null ? 0 : bucket.permits.size();
    }

    // Visible for tests: permits whose task check is pending (expiresAt == Long.MAX_VALUE, see Permit).
    int pendingChecks() {
        int pending = 0;
        for (Bucket bucket : permitsByBucket.values()) {
            for (Permit permit : bucket.permits.values()) {
                if (permit.expiresAt() == Long.MAX_VALUE) {
                    pending++;
                }
            }
        }
        return pending;
    }

    // Visible for tests.
    int activeBuckets() {
        return permitsByBucket.size();
    }

    // Visible for tests.
    long pruneScanCount() {
        return pruneScans.sum();
    }

    // Runs under the bucket lock; callbacks run after compute returns, so local checks cannot re-enter it.
    private List<ExpiredPermit> collectExpired(String bucketKey, Bucket bucket, long now) {
        pruneScans.increment();
        final List<ExpiredPermit> expired = new ArrayList<>();
        long min = Long.MAX_VALUE;
        for (Iterator<Entry<String, Permit>> it = bucket.permits.entrySet().iterator(); it.hasNext();) {
            final Entry<String, Permit> entry = it.next();
            Permit permit = entry.getValue();
            if (permit.expiresAt() <= now) {
                permit = new Permit(Long.MAX_VALUE, permit.ttlNanos(), permit.coordinatorId(), permit.coordinatorTaskId());
                entry.setValue(permit);
                expired.add(new ExpiredPermit(bucketKey, entry.getKey(), permit));
            }
            min = Math.min(min, permit.expiresAt());
        }
        bucket.minExpiry = min;
        return expired;
    }
}
