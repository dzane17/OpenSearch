/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SharedThrottleTrackerTests extends OpenSearchTestCase {

    private static final long TTL = TimeUnit.MINUTES.toNanos(5);

    public void testGrantsUpToLimitThenDenies() {
        SharedThrottleTracker tracker = new SharedThrottleTracker();
        assertTrue(tracker.tryAcquire("b", 2, "l1", TTL));
        assertTrue(tracker.tryAcquire("b", 2, "l2", TTL));
        assertFalse("third acquire must be denied at limit 2", tracker.tryAcquire("b", 2, "l3", TTL));
        assertEquals(2, tracker.inFlight("b"));
    }

    public void testReleaseFreesASlot() {
        SharedThrottleTracker tracker = new SharedThrottleTracker();
        assertTrue(tracker.tryAcquire("b", 1, "l1", TTL));
        assertFalse(tracker.tryAcquire("b", 1, "l2", TTL));
        tracker.release("b", "l1");
        assertEquals(0, tracker.inFlight("b"));
        assertTrue("slot freed after release", tracker.tryAcquire("b", 1, "l3", TTL));
    }

    public void testReleaseByUnknownLeaseIsNoOp() {
        SharedThrottleTracker tracker = new SharedThrottleTracker();
        assertTrue(tracker.tryAcquire("b", 2, "l1", TTL));
        // Unknown permit (already swept, double release, or belonged to a previous owner of a remapped bucket).
        tracker.release("b", "does-not-exist");
        tracker.release("unknown-bucket", "l1");
        assertEquals("a stray release must not decrement a live permit", 1, tracker.inFlight("b"));
    }

    public void testDoubleReleaseDecrementsOnce() {
        SharedThrottleTracker tracker = new SharedThrottleTracker();
        assertTrue(tracker.tryAcquire("b", 2, "l1", TTL));
        assertTrue(tracker.tryAcquire("b", 2, "l2", TTL));
        tracker.release("b", "l1");
        tracker.release("b", "l1"); // second release of the same permit id is a no-op
        assertEquals(1, tracker.inFlight("b"));
    }

    public void testDrainedBucketIsRemoved() {
        SharedThrottleTracker tracker = new SharedThrottleTracker();
        assertTrue(tracker.tryAcquire("b", 5, "l1", TTL));
        assertEquals(1, tracker.activeBuckets());
        tracker.release("b", "l1");
        assertEquals("bucket entry removed when it drains to zero", 0, tracker.activeBuckets());
    }

    public void testExpiredLeaseIsReclaimedBySweep() {
        AtomicLong clock = new AtomicLong(0);
        SharedThrottleTracker tracker = new SharedThrottleTracker(clock::get);
        assertTrue(tracker.tryAcquire("b", 1, "l1", 100));
        assertFalse(tracker.tryAcquire("b", 1, "l2", 100)); // at limit
        clock.set(101); // permit l1 has now expired
        tracker.sweepExpired();
        assertEquals(0, tracker.inFlight("b"));
        assertTrue("slot reclaimed after TTL sweep", tracker.tryAcquire("b", 1, "l3", 100));
    }

    public void testExpiredLeaseReclaimedLazilyOnAcquire() {
        AtomicLong clock = new AtomicLong(0);
        SharedThrottleTracker tracker = new SharedThrottleTracker(clock::get);
        assertTrue(tracker.tryAcquire("b", 1, "l1", 100));
        clock.set(101); // l1 expired; a fresh acquire should prune it and be granted without an explicit sweep
        assertTrue(tracker.tryAcquire("b", 1, "l2", 100));
        assertEquals(1, tracker.inFlight("b"));
    }

    public void testSaturatedBucketWithExpiredLeaseIsReclaimedOnAcquire() {
        // Prune now runs only when the bucket is at/over its limit; verify a fully-saturated bucket with one expired
        // permit still admits the next acquire (the case the "prune only when full" optimization must not break).
        AtomicLong clock = new AtomicLong(0);
        SharedThrottleTracker tracker = new SharedThrottleTracker(clock::get);
        assertTrue(tracker.tryAcquire("b", 2, "l1", 100)); // expires at 100
        assertTrue(tracker.tryAcquire("b", 2, "l2", 500)); // expires at 500
        assertFalse("bucket is full", tracker.tryAcquire("b", 2, "l3", 100));
        clock.set(200); // l1 expired, l2 still live
        // Bucket is at the limit -> acquire prunes l1 and grants; l2 remains.
        assertTrue(tracker.tryAcquire("b", 2, "l4", 500));
        assertEquals(2, tracker.inFlight("b"));
    }

    public void testSweepRacingLateReleaseDoesNotUnderCount() {
        AtomicLong clock = new AtomicLong(0);
        SharedThrottleTracker tracker = new SharedThrottleTracker(clock::get);
        assertTrue(tracker.tryAcquire("b", 5, "l1", 100));
        assertTrue(tracker.tryAcquire("b", 5, "l2", 100));
        clock.set(101); // both expired
        tracker.sweepExpired(); // removes l1 and l2
        // A late release for an already-swept permit must be a no-op, not push the count negative.
        tracker.release("b", "l1");
        assertEquals(0, tracker.inFlight("b"));
        // fresh permit unaffected
        clock.set(150);
        assertTrue(tracker.tryAcquire("b", 5, "l3", 100));
        assertEquals(1, tracker.inFlight("b"));
    }

    public void testConcurrentAcquireNeverExceedsLimit() throws Exception {
        final SharedThrottleTracker tracker = new SharedThrottleTracker();
        final int limit = 10;
        final int threads = 64;
        final CountDownLatch start = new CountDownLatch(1);
        final AtomicInteger granted = new AtomicInteger(0);
        Thread[] workers = new Thread[threads];
        for (int i = 0; i < threads; i++) {
            final int id = i;
            workers[i] = new Thread(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                if (tracker.tryAcquire("hot", limit, "permit-" + id, TTL)) {
                    granted.incrementAndGet();
                }
            });
            workers[i].start();
        }
        start.countDown();
        for (Thread w : workers) {
            w.join();
        }
        assertEquals("exactly limit grants under contention", limit, granted.get());
        assertEquals(limit, tracker.inFlight("hot"));
    }

    public void testBucketsAreIndependent() {
        SharedThrottleTracker tracker = new SharedThrottleTracker();
        assertTrue(tracker.tryAcquire("a", 1, "la", TTL));
        assertTrue("different bucket has its own budget", tracker.tryAcquire("b", 1, "lb", TTL));
        assertFalse(tracker.tryAcquire("a", 1, "la2", TTL));
        assertEquals(2, tracker.activeBuckets());
    }

    public void testSaturatedBucketWithFreshLeasesSkipsPruneScan() {
        // The minExpiry guard: while a full bucket's permits are all still live, repeated denied acquires must not
        // trigger the O(size) expiry scan at all.
        AtomicLong clock = new AtomicLong(0);
        SharedThrottleTracker tracker = new SharedThrottleTracker(clock::get);
        assertTrue(tracker.tryAcquire("b", 2, "l1", 100)); // expires at 100
        assertTrue(tracker.tryAcquire("b", 2, "l2", 100)); // expires at 100
        assertEquals("no scan needed to fill an empty bucket", 0, tracker.pruneScanCount());
        for (int i = 0; i < 50; i++) {
            assertFalse("full bucket denies while fresh", tracker.tryAcquire("b", 2, "denied-" + i, 100));
        }
        assertEquals("no expiry scan runs while every permit is still live", 0, tracker.pruneScanCount());
    }

    public void testSaturatedBucketScansOnceLeasesCanExpire() {
        // The flip side: the instant a permit could have expired (minExpiry <= now), a full-bucket acquire runs exactly
        // one scan, reclaims, and admits.
        AtomicLong clock = new AtomicLong(0);
        SharedThrottleTracker tracker = new SharedThrottleTracker(clock::get);
        assertTrue(tracker.tryAcquire("b", 2, "l1", 100)); // expires at 100
        assertTrue(tracker.tryAcquire("b", 2, "l2", 100)); // expires at 100
        assertFalse(tracker.tryAcquire("b", 2, "l3", 100)); // denied while fresh, no scan
        assertEquals(0, tracker.pruneScanCount());
        clock.set(100); // both permits now at/past expiry
        assertTrue("expired permits reclaimed and slot granted", tracker.tryAcquire("b", 2, "l4", 100));
        assertEquals("exactly one scan ran once expiry was possible", 1, tracker.pruneScanCount());
        assertEquals(1, tracker.inFlight("b"));
    }

    public void testReleaseLeavesMinExpiryStaleButReclamationStillCorrect() {
        // release() intentionally does not recompute minExpiry. Verify the resulting stale-small bound is safe: it may
        // cost at most one extra scan but never causes a full bucket to wrongly skip reclaiming an expired permit.
        AtomicLong clock = new AtomicLong(0);
        SharedThrottleTracker tracker = new SharedThrottleTracker(clock::get);
        assertTrue(tracker.tryAcquire("b", 2, "early", 100)); // expires at 100 -> minExpiry = 100
        assertTrue(tracker.tryAcquire("b", 2, "late", 1000)); // expires at 1000
        tracker.release("b", "early"); // removes the earliest permit; minExpiry stays a stale 100
        assertTrue(tracker.tryAcquire("b", 2, "l3", 1000)); // refill to the limit; "late" (1000) + "l3" (1000) live
        assertFalse(tracker.tryAcquire("b", 2, "l4", 1000)); // full again
        clock.set(500); // past the stale minExpiry (100) but before any real expiry (1000)
        // Guard fires on the stale bound and scans, but nothing is expired -> no reclaim, still denied. The scan is the
        // documented "one wasted scan", and correctness (no over-admission) holds.
        assertFalse("nothing truly expired -> still denied", tracker.tryAcquire("b", 2, "l5", 1000));
        assertEquals(2, tracker.inFlight("b"));
        clock.set(1000); // now the real permits expire
        assertTrue("real expiry is reclaimed on the next acquire", tracker.tryAcquire("b", 2, "l6", 1000));
        assertEquals(1, tracker.inFlight("b"));
    }

    public void testSweepRecomputesMinExpiryEnablingLaterSkip() {
        // After a periodic sweep prunes expired permits, the recomputed minExpiry must let a still-full bucket skip the
        // scan again while the survivors remain fresh.
        AtomicLong clock = new AtomicLong(0);
        SharedThrottleTracker tracker = new SharedThrottleTracker(clock::get);
        assertTrue(tracker.tryAcquire("b", 2, "l1", 100));  // expires at 100
        assertTrue(tracker.tryAcquire("b", 2, "l2", 1_000)); // expires at 1000
        clock.set(100); // l1 expired
        tracker.sweepExpired(); // prunes l1, recomputes minExpiry to l2's expiry (1000)
        assertEquals(1, tracker.pruneScanCount());
        assertTrue(tracker.tryAcquire("b", 2, "l3", 1_000)); // back to full; survivors expire at 1000
        for (int i = 0; i < 20; i++) {
            assertFalse(tracker.tryAcquire("b", 2, "denied-" + i, 1_000));
        }
        assertEquals("no further scans while survivors are fresh", 1, tracker.pruneScanCount());
    }

    public void testSweepExpiredReportsOnlyBucketsThatActuallyFreedCapacity() {
        // The owning service drives owner-push from this return value, so it must name exactly the buckets that gained
        // free capacity: a miss strands a waiting coordinator (an expiry has no release RPC to re-drive the bucket), and
        // a false positive would make the owner reserve-and-grant a slot that does not exist.
        AtomicLong now = new AtomicLong(0L);
        SharedThrottleTracker tracker = new SharedThrottleTracker(now::get);
        assertTrue(tracker.tryAcquire("expiring", 5, "lease-a", 1000L));
        assertTrue(tracker.tryAcquire("surviving", 5, "lease-b", 100_000L));

        // Nothing has expired yet.
        assertTrue("no expiry yet -> no bucket reported", tracker.sweepExpired().isEmpty());

        // Past only the first lease's TTL.
        now.set(1001L);
        List<String> freed = tracker.sweepExpired();
        assertEquals("exactly the bucket whose lease was reclaimed", List.of("expiring"), freed);
        assertEquals(0, tracker.inFlight("expiring"));
        assertEquals(1, tracker.inFlight("surviving"));

        // Idempotent: a second pass has nothing left to reclaim for that bucket.
        assertTrue("a repeat sweep must not re-report an already-reclaimed bucket", tracker.sweepExpired().isEmpty());
    }
}
