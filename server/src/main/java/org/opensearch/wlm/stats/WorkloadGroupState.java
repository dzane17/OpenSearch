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
     * unavailable), and could not be retained for later admission because queueing is disabled or full. A request that
     * parks instead is not counted here, and once {@code WAITING} it is never rejected by a throttle.
     */
    public final CounterMetric totalThrottled = new CounterMetric();

    /**
     * This will track the cumulative requests that would have been throttled in MONITOR mode but were admitted anyway, in the
     * workload group since the OpenSearch start time ({@code node_limit}, {@code shared_limit}, or the shared tier being
     * unavailable). It gives operators a signal to size those limits before switching a group to an enforcing mode; unlike
     * {@link #totalThrottled}, no request was actually rejected.
     * <p>
     * Intentional: MONITOR ignores {@code queue.size_per_bucket}, so a dry run never parks a request (a shared-tier request
     * still waits for the owner's answer, up to the acquire timeout). Every breach is
     * admitted at once and counted here, including one an enforcing mode would have queued, and the queue stats do not
     * move. With queueing configured, read it as the requests an enforcing mode would queue or reject, not as
     * {@link #totalThrottled} alone. It sizes the limits, not the queue.
     */
    public final CounterMetric totalWouldThrottle = new CounterMetric();

    /**
     * Cumulative requests that entered the {@code WAITING} state of the workload group's request queue, since the
     * OpenSearch start time. Counted by {@link #recordQueueEntry} when the wait begins: a node-tier denial parked directly
     * as WAITING, or a shared-tier request moved from {@code PENDING_ACQUIRE} to WAITING by an owner denial (registered or
     * not) or by an acquire the shared tier could not answer. Each WAITING interval is counted once, at entry, however it
     * later ends (admission or cancellation).
     * <p>
     * This deliberately does NOT count every request retained by the queue service. A shared-tier request first enters
     * {@code PENDING_ACQUIRE} before the owner's verdict, solely to close the grant-before-registration race; one that never
     * becomes WAITING (a direct grant, a search-pool rejection, or a cancellation before its verdict) is never counted. A
     * refusal that finds the bucket full is not counted here either: it is a queue rejection ({@link #totalQueueRejections}).
     * <p>
     * It includes the requests still WAITING ({@code queued_current}), so {@code total_queued - queued_current} is the
     * number of waits that have ended, each of which recorded its wait in {@link #totalQueueWaitMillis}. The entry is
     * counted under the bucket lock that every exit must take to remove the request, so an exit is never recorded before
     * its entry. The counters are independent atomics, so a concurrent stats snapshot can transiently observe one update
     * before the other.
     */
    public final CounterMetric totalQueued = new CounterMetric();

    /**
     * Cumulative requests rejected because the workload group's per-bucket waiting cap or fixed per-group retained
     * request cap was full, since the OpenSearch start time. Each one is also counted in {@link #totalThrottled}.
     */
    public final CounterMetric totalQueueRejections = new CounterMetric();

    /**
     * Cumulative time (in millis) that WAITING requests spent parked in the queue, recorded by {@link #recordQueueWaitMillis}
     * when each wait ends: when the request leaves the queue for admission (including a policy-change drain) or is evicted
     * by task cancellation. The mean wait of requests that left the queue is
     * {@code total_queue_wait_millis / (total_queued - queued_current)}.
     * <p>
     * For a shared-tier request, the wait clock starts on the PENDING_ACQUIRE -> WAITING transition rather than when the
     * provisional entry is registered, so owner round-trip latency is never reported as queue latency. A node-tier
     * request enters WAITING directly and starts its clock when enqueued.
     */
    public final CounterMetric totalQueueWaitMillis = new CounterMetric();

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
     * @return requests that entered WAITING in the workload group's request queue, including those still waiting
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
     * Counts one request entering {@code WAITING} in {@link #totalQueued}. Called once per WAITING interval, when it
     * begins; {@link #recordQueueWaitMillis} records the same interval's wait when it ends.
     */
    public void recordQueueEntry() {
        totalQueued.inc();
    }

    /**
     * Records one ended {@code WAITING} interval: adds its wait to the cumulative sum.
     * Called once per interval, when the request leaves the queue: for admission, or on eviction by task cancellation.
     * It does not count the request; {@link #recordQueueEntry} did that when the wait began.
     *
     * @param waitMillis how long the request was parked, in millis (negative values are clamped to 0 for clock skew)
     */
    public void recordQueueWaitMillis(long waitMillis) {
        totalQueueWaitMillis.inc(Math.max(0L, waitMillis));
    }

    /**
     * @return cumulative parked time (millis) summed over the WAITING intervals that have ended, admitted or cancelled
     */
    public long getTotalQueueWaitMillis() {
        return totalQueueWaitMillis.count();
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
