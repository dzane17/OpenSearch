/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.common.lease.Releasable;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.test.OpenSearchTestCase;

public class WorkloadGroupThrottleTrackerTests extends OpenSearchTestCase {

    public void testAcquireUnderLimitSucceeds() {
        WorkloadGroupThrottleTracker tracker = new WorkloadGroupThrottleTracker();
        Releasable p1 = tracker.acquire("bucket", 2);
        Releasable p2 = tracker.acquire("bucket", 2);
        assertEquals(2, tracker.inFlight("bucket"));
        p1.close();
        p2.close();
    }

    public void testAcquireAtLimitRejects() {
        WorkloadGroupThrottleTracker tracker = new WorkloadGroupThrottleTracker();
        tracker.acquire("bucket", 1);
        OpenSearchRejectedExecutionException e = expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> tracker.acquire("bucket", 1)
        );
        assertTrue(e.getMessage().contains("throttle limit reached"));
        assertFalse(e.getMessage().contains("bucket"));
        // rejected acquire must not leave the count inflated
        assertEquals(1, tracker.inFlight("bucket"));
    }

    public void testReleaseFreesAPermit() {
        WorkloadGroupThrottleTracker tracker = new WorkloadGroupThrottleTracker();
        Releasable p = tracker.acquire("bucket", 1);
        // at the limit
        expectThrows(OpenSearchRejectedExecutionException.class, () -> tracker.acquire("bucket", 1));
        p.close();
        // permit freed -> a fresh acquire now succeeds
        Releasable p2 = tracker.acquire("bucket", 1);
        assertEquals(1, tracker.inFlight("bucket"));
        p2.close();
    }

    public void testDrainToZeroRemovesBucketThenReacquire() {
        WorkloadGroupThrottleTracker tracker = new WorkloadGroupThrottleTracker();
        Releasable p = tracker.acquire("bucket", 5);
        assertEquals(1, tracker.inFlight("bucket"));
        p.close();
        assertEquals(0, tracker.inFlight("bucket"));
        // re-acquiring after the bucket drained (and was removed) works and starts from 1
        Releasable p2 = tracker.acquire("bucket", 5);
        assertEquals(1, tracker.inFlight("bucket"));
        p2.close();
    }

    public void testReleaseIsIdempotent() {
        WorkloadGroupThrottleTracker tracker = new WorkloadGroupThrottleTracker();
        Releasable p = tracker.acquire("bucket", 5);
        p.close();
        p.close(); // double close must not decrement twice
        assertEquals(0, tracker.inFlight("bucket"));
    }

    public void testBucketsAreIndependent() {
        WorkloadGroupThrottleTracker tracker = new WorkloadGroupThrottleTracker();
        tracker.acquire("a", 1);
        // "a" is full but "b" is a separate bucket
        expectThrows(OpenSearchRejectedExecutionException.class, () -> tracker.acquire("a", 1));
        Releasable pb = tracker.acquire("b", 1);
        assertEquals(1, tracker.inFlight("a"));
        assertEquals(1, tracker.inFlight("b"));
        pb.close();
    }
}
