/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.stats;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.wlm.ResourceType;

import java.util.ArrayList;
import java.util.List;

public class WorkloadGroupStateTests extends OpenSearchTestCase {
    WorkloadGroupState workloadGroupState;

    public void testRandomWorkloadGroupsStateUpdates() {
        workloadGroupState = new WorkloadGroupState();
        List<Thread> updaterThreads = new ArrayList<>();

        for (int i = 0; i < 25; i++) {
            if (i % 5 == 0) {
                updaterThreads.add(new Thread(() -> { workloadGroupState.totalCompletions.inc(); }));
            } else if (i % 5 == 1) {
                updaterThreads.add(new Thread(() -> {
                    workloadGroupState.totalRejections.inc();
                    if (randomBoolean()) {
                        workloadGroupState.getResourceState().get(ResourceType.CPU).rejections.inc();
                    } else {
                        workloadGroupState.getResourceState().get(ResourceType.MEMORY).rejections.inc();
                    }
                }));
            } else if (i % 5 == 2) {
                updaterThreads.add(new Thread(() -> workloadGroupState.failures.inc()));
            } else if (i % 5 == 3) {
                updaterThreads.add(new Thread(() -> workloadGroupState.getResourceState().get(ResourceType.CPU).cancellations.inc()));
            } else {
                updaterThreads.add(new Thread(() -> workloadGroupState.getResourceState().get(ResourceType.MEMORY).cancellations.inc()));
            }

            if (i % 5 == 3 || i % 5 == 4) {
                updaterThreads.add(new Thread(() -> workloadGroupState.totalCancellations.inc()));
            }
        }

        // trigger the updates
        updaterThreads.forEach(Thread::start);
        // wait for updates to be finished
        updaterThreads.forEach(thread -> {
            try {
                thread.join();
            } catch (InterruptedException ignored) {

            }
        });

        assertEquals(5, workloadGroupState.getTotalCompletions());
        assertEquals(5, workloadGroupState.getTotalRejections());

        final long sumOfRejectionsDueToResourceTypes = workloadGroupState.getResourceState().get(ResourceType.CPU).rejections.count()
            + workloadGroupState.getResourceState().get(ResourceType.MEMORY).rejections.count();
        assertEquals(sumOfRejectionsDueToResourceTypes, workloadGroupState.getTotalRejections());

        assertEquals(5, workloadGroupState.getFailures());
        assertEquals(10, workloadGroupState.getTotalCancellations());
        assertEquals(5, workloadGroupState.getResourceState().get(ResourceType.CPU).cancellations.count());
        assertEquals(5, workloadGroupState.getResourceState().get(ResourceType.MEMORY).cancellations.count());
    }

    public void testRecordQueueWaitAggregates() {
        WorkloadGroupState state = new WorkloadGroupState();
        assertEquals(0, state.getTotalQueueWaitMillis());
        assertEquals(0, state.getMaxQueueWaitMillis());

        assertEquals(0, state.getTotalQueued());

        state.recordQueueWaitMillis(100);
        state.recordQueueWaitMillis(300);
        state.recordQueueWaitMillis(50);

        assertEquals("one queued request counted per recorded wait", 3, state.getTotalQueued());
        assertEquals(450, state.getTotalQueueWaitMillis()); // sum -> mean = 450/3 = 150
        assertEquals(300, state.getMaxQueueWaitMillis()); // high-water mark, does not decrease
    }

    public void testRecordQueueWaitClampsNegative() {
        WorkloadGroupState state = new WorkloadGroupState();
        state.recordQueueWaitMillis(-5); // clock skew guard
        assertEquals("a clamped sample is still one queued request", 1, state.getTotalQueued());
        assertEquals(0, state.getTotalQueueWaitMillis());
        assertEquals(0, state.getMaxQueueWaitMillis());
    }

    public void testTotalQueuedAndWaitSumCannotDiverge() {
        // The invariant the single-site design exists to guarantee: total_queued is written ONLY by
        // recordQueueWaitMillis, so the mean-wait denominator always matches the number of samples in the sum. No
        // interleaving of throttle denials, grants or drains can move one without the other.
        WorkloadGroupState state = new WorkloadGroupState();
        long samples = randomIntBetween(1, 50);
        long expectedSum = 0;
        for (int i = 0; i < samples; i++) {
            long wait = randomIntBetween(0, 1000);
            expectedSum += wait;
            state.recordQueueWaitMillis(wait);
        }
        assertEquals("denominator must equal the sample count", samples, state.getTotalQueued());
        assertEquals(expectedSum, state.getTotalQueueWaitMillis());
        assertTrue("a non-zero count means the mean is well-formed", state.getTotalQueued() > 0);
    }

}
