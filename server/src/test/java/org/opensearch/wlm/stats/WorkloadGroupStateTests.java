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

        assertEquals(0, state.getTotalQueued());

        state.recordQueueWaitMillis(100);
        state.recordQueueWaitMillis(300);
        state.recordQueueWaitMillis(50);

        assertEquals("an ended wait does not count its request: its entry into WAITING did", 0, state.getTotalQueued());
        assertEquals(450, state.getTotalQueueWaitMillis()); // sum
    }

    public void testRecordQueueEntryCountsWithoutRecordingAWait() {
        WorkloadGroupState state = new WorkloadGroupState();
        state.recordQueueEntry();
        state.recordQueueEntry();
        assertEquals("one per request entering WAITING", 2, state.getTotalQueued());
        assertEquals("a wait is recorded only when it ends", 0, state.getTotalQueueWaitMillis());
    }

    public void testRecordQueueWaitClampsNegative() {
        WorkloadGroupState state = new WorkloadGroupState();
        state.recordQueueWaitMillis(-5); // clock skew guard
        assertEquals(0, state.getTotalQueueWaitMillis());
        assertEquals("recording a wait never counts an entry", 0, state.getTotalQueued());
    }

    public void testEntriesAndEndedWaitsAreRecordedIndependently() {
        // total_queued counts each request when it enters WAITING, and the wait sum grows only when a wait ends.
        // With queued_current requests still waiting, the mean wait of the requests that left the queue is therefore
        // total_queue_wait_millis / (total_queued - queued_current).
        WorkloadGroupState state = new WorkloadGroupState();
        int entered = randomIntBetween(1, 50);
        int stillWaiting = randomIntBetween(0, entered - 1);
        for (int i = 0; i < entered; i++) {
            state.recordQueueEntry();
        }
        assertEquals(entered, state.getTotalQueued());
        assertEquals("no wait has ended yet", 0, state.getTotalQueueWaitMillis());

        long expectedSum = 0;
        for (int i = 0; i < entered - stillWaiting; i++) {
            long wait = randomIntBetween(0, 1000);
            expectedSum += wait;
            state.recordQueueWaitMillis(wait);
        }
        assertEquals("an ended wait does not count its request again", entered, state.getTotalQueued());
        assertEquals(expectedSum, state.getTotalQueueWaitMillis());
    }

}
