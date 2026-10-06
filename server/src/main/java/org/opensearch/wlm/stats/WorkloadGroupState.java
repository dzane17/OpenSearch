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
     * Cumulative requests rejected by an enforced throttle since the OpenSearch start time: they exceeded the group's
     * {@code node_limit} and/or {@code shared_limit}, or the shared limit could not be checked (cluster-wide throttle
     * unavailable), and could not be retained for later admission (queueing disabled or full, failed shared-waiter
     * registration, or an unavailable shared tier, which is never queued).
     */
    public final CounterMetric totalThrottled = new CounterMetric();

    /**
     * This will track the cumulative requests that would have been throttled in MONITOR mode but were admitted anyway, in the
     * workload group since the OpenSearch start time, for the same causes as {@link #totalThrottled} ({@code node_limit},
     * {@code shared_limit}, or the shared tier being unavailable). It gives operators a signal to size those limits before
     * switching a group to an enforcing mode; unlike {@link #totalThrottled}, no request was actually rejected.
     */
    public final CounterMetric totalWouldThrottle = new CounterMetric();

    /**
     * Cumulative requests that completed a {@code WAITING} interval in the workload group's request queue, since the
     * OpenSearch start time. Counted when an entry is selected and dequeued for admission, by
     * {@link #recordQueueWaitMillis}.
     * <p>
     * This deliberately does NOT count every request retained by the queue service. A shared-tier request first enters
     * {@code PENDING_ACQUIRE} before the owner's verdict, solely to close the grant-before-registration race. Only a
     * registered owner denial that fits the bucket cap transitions the exact entry to {@code WAITING}; direct grants,
     * unregistered denials, full-bucket denials, and an unavailable shared tier leave without a recorded wait.
     * <p>
     * Incremented by the same method as {@link #totalQueueWaitMillis}, so both counters describe the same conceptual
     * sample population. They are independent atomics, so a concurrent stats snapshot can transiently observe one update
     * before the other.
     * <p>
     * It excludes a request still in {@code WAITING} (see {@code queued_current}) and one cancelled/evicted before
     * dequeue. A cancellation that wins the final post-dequeue check is still counted because its queue wait ended.
     */
    public final CounterMetric totalQueued = new CounterMetric();

    /**
     * Cumulative requests rejected because the workload group's per-bucket waiting cap or fixed per-group retained
     * request cap was full, since the OpenSearch start time.
     */
    public final CounterMetric totalQueueRejections = new CounterMetric();

    /**
     * Cumulative time (in millis) that WAITING requests spent parked in the queue. Divide by
     * {@link #totalQueued} for the mean wait; use {@link #getMaxQueueWaitMillis} for the tail.
     * <p>
     * For a shared-tier request, the wait clock starts on the PENDING_ACQUIRE -> WAITING transition rather than when the
     * provisional entry is registered, so owner round-trip latency is never reported as queue latency. A node-tier
     * request enters WAITING directly and starts its clock when enqueued.
     */
    public final CounterMetric totalQueueWaitMillis = new CounterMetric();

    // High-water mark (in millis) of any completed WAITING interval, since the OpenSearch start time.
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
     * @return requests that would have been throttled in MONITOR mode in the workload group
     */
    public long getTotalWouldThrottle() {
        return totalWouldThrottle.count();
    }

    /**
     *
     * @return requests that completed a WAITING interval in the workload group's request queue
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
     * Records one completed {@code WAITING} interval: counts it in {@link #totalQueued}, adds its wait to the cumulative
     * sum, and advances the high-water mark. Called once when a WAITING entry is selected and dequeued for admission.
     * This is the single site that can observe "a request's queue wait ended", so making it the only writer of
     * {@link #totalQueued} keeps the count and the wait sum describing the same requests.
     *
     * @param waitMillis how long the request was parked, in millis (negative values are clamped to 0 for clock skew)
     */
    public void recordQueueWaitMillis(long waitMillis) {
        final long w = Math.max(0L, waitMillis);
        totalQueued.inc();
        totalQueueWaitMillis.inc(w);
        maxQueueWaitMillis.accumulateAndGet(w, Math::max);
    }

    /**
     * @return cumulative parked time (millis) summed over genuinely-waiting requests
     */
    public long getTotalQueueWaitMillis() {
        return totalQueueWaitMillis.count();
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
