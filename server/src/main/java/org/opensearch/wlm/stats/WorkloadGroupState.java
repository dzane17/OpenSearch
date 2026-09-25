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
 * Mutable node-local workload-group metrics. Most fields are cumulative counters since process start; resource usage is
 * the latest sampled value, and queue high-water marks are historical maxima. The stats API snapshots this state.
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
     * Cumulative requests rejected by an enforced node- or shared-tier throttle after they could not be retained for
     * later admission, including disabled/full queues and failed shared-waiter registration.
     */
    public final CounterMetric totalThrottled = new CounterMetric();

    /**
     * Cumulative requests that completed a {@code WAITING} interval in the workload group's request queue, since the
     * OpenSearch start time. Counted when an entry is selected and dequeued for admission, by
     * {@link #recordQueueWaitMillis}.
     * <p>
     * This deliberately does NOT count every request retained by the queue service. A shared-tier request first enters
     * {@code PENDING_ACQUIRE} before the owner's verdict, solely to close the grant-before-registration race. Only a
     * registered owner denial that fits the bucket cap transitions the exact entry to {@code WAITING}; direct grants,
     * fail-open outcomes, unregistered denials, and full-bucket denials leave without a recorded wait.
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
     * {@link #totalQueued} for the mean wait; use {@link #maxQueueWaitMillis} for the tail.
     * <p>
     * For a shared-tier request, the wait clock starts on the PENDING_ACQUIRE -> WAITING transition rather than when the
     * provisional entry is registered, so owner round-trip latency is never reported as queue latency. A node-tier
     * request enters WAITING directly and starts its clock when enqueued.
     * <p>
     * {@link #totalQueued} is incremented by the same call, so sum and count converge on the same set of requests. A
     * snapshot racing that call may briefly observe only one of the two independent atomic updates.
     */
    public final CounterMetric totalQueueWaitMillis = new CounterMetric();

    /**
     * High-water mark (in millis) of any completed {@code WAITING} interval, since the OpenSearch start time.
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
     * <p>
     * All three are updated by this one method ON PURPOSE. This is the single site that can observe "a request's queue
     * wait ended", so making it the only writer of {@link #totalQueued} keeps their eventual sample population aligned.
     * The fields are independent atomics rather than a transactional snapshot, so a concurrent stats read can briefly
     * straddle one update. Splitting the count out to the throttle-denial signal instead looks equivalent but is not —
     * see the note on {@link #totalQueued}.
     *
     * @param waitMillis how long the request was parked, in millis (negative values are clamped to 0 for clock skew)
     */
    public void recordQueueWaitMillis(long waitMillis) {
        if (waitMillis < 0) {
            waitMillis = 0;
        }
        totalQueued.inc();
        totalQueueWaitMillis.inc(waitMillis);
        final long w = waitMillis;
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
