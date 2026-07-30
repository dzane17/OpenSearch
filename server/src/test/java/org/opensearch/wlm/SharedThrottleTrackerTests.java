/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.test.OpenSearchTestCase;

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
        // Unknown lease (already swept, double release, or belonged to a previous owner of a remapped bucket).
        tracker.release("b", "does-not-exist");
        tracker.release("unknown-bucket", "l1");
        assertEquals("a stray release must not decrement a live lease", 1, tracker.inFlight("b"));
    }

    public void testDoubleReleaseDecrementsOnce() {
        SharedThrottleTracker tracker = new SharedThrottleTracker();
        assertTrue(tracker.tryAcquire("b", 2, "l1", TTL));
        assertTrue(tracker.tryAcquire("b", 2, "l2", TTL));
        tracker.release("b", "l1");
        tracker.release("b", "l1"); // second release of the same lease id is a no-op
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
        clock.set(101); // lease l1 has now expired
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
        // lease still admits the next acquire (the case the "prune only when full" optimization must not break).
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
        // A late release for an already-swept lease must be a no-op, not push the count negative.
        tracker.release("b", "l1");
        assertEquals(0, tracker.inFlight("b"));
        // fresh lease unaffected
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
                if (tracker.tryAcquire("hot", limit, "lease-" + id, TTL)) {
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
}
