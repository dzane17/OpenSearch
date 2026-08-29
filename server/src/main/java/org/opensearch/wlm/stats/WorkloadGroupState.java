/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.stats;

import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.wlm.ResourceType;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class will keep the point in time view of the workload group stats
 */
public class WorkloadGroupState {
    /**
     * co-ordinator level completions at the workload group level, this is a cumulative counter since the Opensearch start time
     */
    public final CounterMetric totalCompletions = new CounterMetric();

    /**
     * rejections at the workload group level, this is a cumulative counter since the OpenSearch start time
     */
    public final CounterMetric totalRejections = new CounterMetric();

    /**
     * this will track the cumulative failures in a workload group
     */
    public final CounterMetric failures = new CounterMetric();

    /**
     * This will track total number of cancellations in the workload group due to all resource type breaches
     */
    public final CounterMetric totalCancellations = new CounterMetric();

    /**
     * This will track the cumulative requests throttled (rejected by the node-level in-flight throttle) in the workload group since the OpenSearch start time
     */
    public final CounterMetric totalThrottled = new CounterMetric();

    /**
     * Cumulative requests parked in the workload group's request queue (admitted into the queue after a throttle
     * denial), since the OpenSearch start time.
     */
    public final CounterMetric totalQueued = new CounterMetric();

    /**
     * Cumulative requests rejected because the workload group's request queue was full, since the OpenSearch start time.
     */
    public final CounterMetric totalQueueRejections = new CounterMetric();

    /**
     * Cumulative time (in millis) that admitted requests spent parked in the queue, summed across all requests that
     * were queued and then admitted. Paired with {@link #queueWaitCount} this yields the mean wait; use
     * {@link #maxQueueWaitMillis} for the tail. Recorded only for requests that actually parked (never-queued requests
     * do not contribute), so the mean reflects wait among queued requests, not all requests.
     */
    public final CounterMetric totalQueueWaitMillis = new CounterMetric();

    /**
     * Number of admitted requests that had been parked in the queue (the denominator for mean queue wait).
     */
    public final CounterMetric queueWaitCount = new CounterMetric();

    /**
     * High-water mark (in millis) of any single admitted request's queue wait, since the OpenSearch start time.
     */
    private final AtomicLong maxQueueWaitMillis = new AtomicLong(0);

    /**
     * This is used to store the resource type state both for CPU and MEMORY
     */
    private final Map<ResourceType, ResourceTypeState> resourceState;

    public WorkloadGroupState() {
        resourceState = new EnumMap<>(ResourceType.class);
        for (ResourceType resourceType : ResourceType.values()) {
            if (resourceType.hasStatsEnabled()) {
                resourceState.put(resourceType, new ResourceTypeState(resourceType));
            }
        }
    }

    /**
     *
     * @return co-ordinator completions in the workload group
     */
    public long getTotalCompletions() {
        return totalCompletions.count();
    }

    /**
     *
     * @return rejections in the workload group
     */
    public long getTotalRejections() {
        return totalRejections.count();
    }

    /**
     *
     * @return failures in the workload group
     */
    public long getFailures() {
        return failures.count();
    }

    public long getTotalCancellations() {
        return totalCancellations.count();
    }

    /**
     *
     * @return requests throttled in the workload group
     */
    public long getTotalThrottled() {
        return totalThrottled.count();
    }

    /**
     *
     * @return requests parked in the workload group's request queue
     */
    public long getTotalQueued() {
        return totalQueued.count();
    }

    /**
     *
     * @return requests rejected because the workload group's request queue was full
     */
    public long getTotalQueueRejections() {
        return totalQueueRejections.count();
    }

    /**
     * Records the queue wait of one request that was parked and then admitted: adds to the cumulative sum and count
     * and advances the high-water mark. Called once per admitted-from-queue request.
     *
     * @param waitMillis how long the request was parked, in millis (non-negative)
     */
    public void recordQueueWaitMillis(long waitMillis) {
        if (waitMillis < 0) {
            waitMillis = 0;
        }
        totalQueueWaitMillis.inc(waitMillis);
        queueWaitCount.inc();
        final long w = waitMillis;
        maxQueueWaitMillis.accumulateAndGet(w, Math::max);
    }

    /**
     * @return cumulative parked time (millis) summed over admitted-from-queue requests
     */
    public long getTotalQueueWaitMillis() {
        return totalQueueWaitMillis.count();
    }

    /**
     * @return number of admitted requests that had been parked (denominator for mean wait)
     */
    public long getQueueWaitCount() {
        return queueWaitCount.count();
    }

    /**
     * @return the longest single queue wait observed (millis)
     */
    public long getMaxQueueWaitMillis() {
        return maxQueueWaitMillis.get();
    }

    /**
     * getter for workload group resource state
     * @return the workload group resource state
     */
    public Map<ResourceType, ResourceTypeState> getResourceState() {
        return resourceState;
    }

    /**
     * This class holds the resource level stats for the workload group
     */
    public static class ResourceTypeState {
        public final ResourceType resourceType;
        public final CounterMetric cancellations = new CounterMetric();
        public final CounterMetric rejections = new CounterMetric();
        private double lastRecordedUsage = 0;

        public ResourceTypeState(ResourceType resourceType) {
            this.resourceType = resourceType;
        }

        public void setLastRecordedUsage(double recordedUsage) {
            lastRecordedUsage = recordedUsage;
        }

        public double getLastRecordedUsage() {
            return lastRecordedUsage;
        }
    }
}
