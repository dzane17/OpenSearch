/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.common.lease.Releasable;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class NodeThrottleTrackerTests extends OpenSearchTestCase {

    public void testAcquireUnderLimitSucceeds() {
        NodeThrottleTracker tracker = new NodeThrottleTracker();
        Releasable p1 = tracker.tryAcquire("bucket", 2);
        Releasable p2 = tracker.tryAcquire("bucket", 2);
        assertNotNull(p1);
        assertNotNull(p2);
        assertEquals(2, tracker.inFlight("bucket"));
        p1.close();
        p2.close();
    }

    public void testAcquireAtLimitReturnsNull() {
        NodeThrottleTracker tracker = new NodeThrottleTracker();
        assertNotNull(tracker.tryAcquire("bucket", 1));
        // at the limit -> tryAcquire returns null (production then falls through to the shared tier or rejects)
        assertNull(tracker.tryAcquire("bucket", 1));
        // declined acquire must not leave the count inflated
        assertEquals(1, tracker.inFlight("bucket"));
    }

    public void testReleaseFreesAPermit() {
        NodeThrottleTracker tracker = new NodeThrottleTracker();
        Releasable p = tracker.tryAcquire("bucket", 1);
        assertNotNull(p);
        // at the limit
        assertNull(tracker.tryAcquire("bucket", 1));
        p.close();
        // permit freed -> a fresh acquire now succeeds
        Releasable p2 = tracker.tryAcquire("bucket", 1);
        assertNotNull(p2);
        assertEquals(1, tracker.inFlight("bucket"));
        p2.close();
    }

    public void testDrainToZeroRemovesBucketThenReacquire() {
        NodeThrottleTracker tracker = new NodeThrottleTracker();
        Releasable p = tracker.tryAcquire("bucket", 5);
        assertNotNull(p);
        assertEquals(1, tracker.inFlight("bucket"));
        p.close();
        assertEquals(0, tracker.inFlight("bucket"));
        // re-acquiring after the bucket drained (and was removed) works and starts from 1
        Releasable p2 = tracker.tryAcquire("bucket", 5);
        assertNotNull(p2);
        assertEquals(1, tracker.inFlight("bucket"));
        p2.close();
    }

    public void testReleaseIsIdempotent() {
        NodeThrottleTracker tracker = new NodeThrottleTracker();
        Releasable p = tracker.tryAcquire("bucket", 5);
        assertNotNull(p);
        p.close();
        p.close(); // double close must not decrement twice
        assertEquals(0, tracker.inFlight("bucket"));
    }

    public void testBucketsAreIndependent() {
        NodeThrottleTracker tracker = new NodeThrottleTracker();
        assertNotNull(tracker.tryAcquire("a", 1));
        // "a" is full but "b" is a separate bucket
        assertNull(tracker.tryAcquire("a", 1));
        Releasable pb = tracker.tryAcquire("b", 1);
        assertNotNull(pb);
        assertEquals(1, tracker.inFlight("a"));
        assertEquals(1, tracker.inFlight("b"));
        pb.close();
    }

    public void testConcurrentAcquireAdmitsExactlyLimit() throws Exception {
        // Many threads race to acquire the same bucket at once. tryAcquire must admit exactly nodeLimit of them —
        // never fewer (the over-decline race where all racers observe an inflated shared count and roll back) and
        // never more (over-admit). Each caller decides on its own captured post-increment value.
        final NodeThrottleTracker tracker = new NodeThrottleTracker();
        final int limit = 10;
        final int threads = 64;
        final CountDownLatch start = new CountDownLatch(1);
        final AtomicInteger granted = new AtomicInteger(0);
        Thread[] workers = new Thread[threads];
        for (int i = 0; i < threads; i++) {
            workers[i] = new Thread(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                if (tracker.tryAcquire("hot", limit) != null) {
                    granted.incrementAndGet();
                }
            });
            workers[i].start();
        }
        start.countDown();
        for (Thread w : workers) {
            w.join(TimeUnit.SECONDS.toMillis(30));
        }
        assertEquals("must admit exactly the limit under contention", limit, granted.get());
        assertEquals(limit, tracker.inFlight("hot"));
    }
}
