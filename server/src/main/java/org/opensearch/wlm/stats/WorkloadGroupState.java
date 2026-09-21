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
     * Cumulative requests that <em>waited</em> in the workload group's request queue for capacity to free, since the
     * OpenSearch start time. Counted when such a request is admitted, by {@link #recordQueueWaitMillis}.
     * <p>
     * This deliberately does NOT count every request that entered the queue. Parking is enqueue-first — an internal
     * optimisation that parks a request before the owner's verdict is known, to close a registration race — so under an
     * active queue every shared-tier request passes through the queue, and most leave again immediately on the permit
     * their own acquire returned. Counting those would make this stat read as "all shared-tier traffic" rather than
     * "traffic the throttle actually delayed", so an admission the request supplied itself is excluded.
     * <p>
     * Incremented at the same site as {@link #totalQueueWaitMillis}, which is what makes it a sound denominator for mean
     * queue wait: one increment per recorded wait sample, so the two cannot drift apart. Counting instead from the
     * <em>denial</em> signal would be close but not exact — a granted acquire supplies zero drains when a concurrent
     * drain emptied the bucket first, which produces a wait sample with no matching denial (and, with a fresh counter, a
     * zero denominator).
     * <p>
     * Note FIFO means the request counted here is not necessarily the one whose own acquire was denied: a granted permit
     * drains the OLDEST parked request. The identity is permuted; what this counts is unambiguous either way — a request
     * that was admitted having genuinely waited for someone else's capacity.
     * <p>
     * Two things it therefore does not include, both by design: a request that is denied and still parked right now (it
     * has not finished waiting — see {@code queued_current}), and one cancelled or evicted before ever being admitted.
     */
    public final CounterMetric totalQueued = new CounterMetric();

    /**
     * Cumulative requests rejected because the workload group's request queue was full, since the OpenSearch start time.
     */
    public final CounterMetric totalQueueRejections = new CounterMetric();

    /**
     * Cumulative time (in millis) that genuinely-waiting requests spent parked in the queue. Divide by
     * {@link #totalQueued} for the mean wait; use {@link #maxQueueWaitMillis} for the tail.
     * <p>
     * Recorded only for admissions the request did not supply itself — an owner-push grant, a node-tier completion
     * drain, or the backstop sweep. An enqueue-first pass-through, admitted on the permit its own acquire returned, is
     * skipped: its "wait" is just the owner round-trip, and including it would drag the mean toward zero.
     * <p>
     * {@link #totalQueued} is incremented by the same call, so sum and count always describe exactly the same set of
     * requests and the mean is well-formed whenever the count is non-zero.
     */
    public final CounterMetric totalQueueWaitMillis = new CounterMetric();

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
     * @return requests that had to wait in the workload group's request queue for a permit to free
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
     * Records one request finishing a genuine wait in the queue: counts it in {@link #totalQueued}, adds its wait to the
     * cumulative sum, and advances the high-water mark. Called once per admission that the request did not supply itself.
     * <p>
     * All three move together here ON PURPOSE. This is the single site that can observe "a request waited and has now
     * been admitted", so making it the only writer of {@link #totalQueued} is what makes sum-over-count a sound mean:
     * there is no interleaving in which one advances without the other. Splitting the count out to the throttle-denial
     * signal instead looks equivalent but is not — see the note on {@link #totalQueued}.
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
