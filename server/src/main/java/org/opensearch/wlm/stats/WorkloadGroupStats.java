/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.stats;

import org.opensearch.Version;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.stats.WorkloadGroupState.ResourceTypeState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * {
 *     "workloadGroupID": {
 *          "completions": 1233234234,
 *          "rejections": 12,
 *          "failures": 97,
 *          "total_cancellations": 474,
 *          "CPU": { "current_usage": 49.6, "cancellation": 432, "rejections": 8 },
 *          "MEMORY": { "current_usage": 39.6, "cancellation": 42, "rejections": 4 }
 *     },
 *     ...
 *     ...
 * }
 */
public class WorkloadGroupStats implements ToXContentObject, Writeable {
    private final Map<String, WorkloadGroupStatsHolder> stats;

    public WorkloadGroupStats(Map<String, WorkloadGroupStatsHolder> stats) {
        this.stats = stats;
    }

    public WorkloadGroupStats(StreamInput in) throws IOException {
        stats = in.readMap(StreamInput::readString, WorkloadGroupStatsHolder::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(stats, StreamOutput::writeString, WorkloadGroupStatsHolder::writeTo);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("workload_groups");
        // to keep the toXContent consistent
        List<Map.Entry<String, WorkloadGroupStatsHolder>> entryList = new ArrayList<>(stats.entrySet());
        entryList.sort((k1, k2) -> k1.getKey().compareTo(k2.getKey()));

        for (Map.Entry<String, WorkloadGroupStatsHolder> workloadGroupStats : entryList) {
            builder.startObject(workloadGroupStats.getKey());
            workloadGroupStats.getValue().toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WorkloadGroupStats that = (WorkloadGroupStats) o;
        return Objects.equals(stats, that.stats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stats);
    }

    public Map<String, WorkloadGroupStatsHolder> getStats() {
        return stats;
    }

    /**
     * This is a stats holder object which will hold the data for a workload group at a point in time
     * the instance will only be created on demand through stats api
     */
    public static class WorkloadGroupStatsHolder implements ToXContentObject, Writeable {
        public static final String COMPLETIONS = "total_completions";
        public static final String REJECTIONS = "total_rejections";
        public static final String TOTAL_CANCELLATIONS = "total_cancellations";
        public static final String FAILURES = "failures";
        public static final String THROTTLED = "total_throttled";
        public static final String QUEUED = "total_queued";
        public static final String QUEUE_REJECTIONS = "total_queue_rejections";
        public static final String QUEUED_CURRENT = "queued_current";
        public static final String QUEUE_PEAK = "queue_peak";
        public static final String TOTAL_QUEUE_WAIT_MILLIS = "total_queue_wait_millis";
        public static final String QUEUE_WAIT_COUNT = "queue_wait_count";
        public static final String MAX_QUEUE_WAIT_MILLIS = "max_queue_wait_millis";
        private long completions;
        private long rejections;
        private long failures;
        private long cancellations;
        private long throttled;
        private long queued;
        private long queueRejections;
        private long queuedCurrent;
        private long queuePeak;
        // Cumulative parked time + count (mean = sum/count) and the single-request high-water mark, for queued-then-
        // admitted requests. Populated from WorkloadGroupState in from(...); 0 via the plain constructors.
        private long totalQueueWaitMillis;
        private long queueWaitCount;
        private long maxQueueWaitMillis;
        private Map<ResourceType, ResourceStats> resourceStats;

        // this is needed to support the factory method
        public WorkloadGroupStatsHolder() {}

        public WorkloadGroupStatsHolder(
            long completions,
            long rejections,
            long failures,
            long cancellations,
            long throttled,
            Map<ResourceType, ResourceStats> resourceStats
        ) {
            this(completions, rejections, failures, cancellations, throttled, 0, 0, 0, 0, resourceStats);
        }

        public WorkloadGroupStatsHolder(
            long completions,
            long rejections,
            long failures,
            long cancellations,
            long throttled,
            long queued,
            long queueRejections,
            long queuedCurrent,
            long queuePeak,
            Map<ResourceType, ResourceStats> resourceStats
        ) {
            this.completions = completions;
            this.rejections = rejections;
            this.failures = failures;
            this.cancellations = cancellations;
            this.throttled = throttled;
            this.queued = queued;
            this.queueRejections = queueRejections;
            this.queuedCurrent = queuedCurrent;
            this.queuePeak = queuePeak;
            this.resourceStats = resourceStats;
        }

        public WorkloadGroupStatsHolder(StreamInput in) throws IOException {
            this.completions = in.readVLong();
            this.rejections = in.readVLong();
            this.failures = in.readVLong();
            this.cancellations = in.readVLong();
            // total_throttled and the queue stats are version-gated so a pre-throttling node's stats stream stays readable.
            if (in.getVersion().onOrAfter(Version.V_3_7_0)) {
                this.throttled = in.readVLong();
                this.queued = in.readVLong();
                this.queueRejections = in.readVLong();
                this.queuedCurrent = in.readVLong();
                this.queuePeak = in.readVLong();
                this.totalQueueWaitMillis = in.readVLong();
                this.queueWaitCount = in.readVLong();
                this.maxQueueWaitMillis = in.readVLong();
            }
            this.resourceStats = in.readMap((i) -> ResourceType.fromName(i.readString()), ResourceStats::new);
        }

        public long getCompletions() {
            return completions;
        }

        public long getRejections() {
            return rejections;
        }

        public long getCancellations() {
            return cancellations;
        }

        public long getThrottled() {
            return throttled;
        }

        public long getQueued() {
            return queued;
        }

        public long getQueueRejections() {
            return queueRejections;
        }

        public long getQueuedCurrent() {
            return queuedCurrent;
        }

        public long getQueuePeak() {
            return queuePeak;
        }

        public long getTotalQueueWaitMillis() {
            return totalQueueWaitMillis;
        }

        public long getQueueWaitCount() {
            return queueWaitCount;
        }

        public long getMaxQueueWaitMillis() {
            return maxQueueWaitMillis;
        }

        public Map<ResourceType, ResourceStats> getResourceStats() {
            return resourceStats;
        }

        /**
         * static factory method to convert {@link WorkloadGroupState} into {@link WorkloadGroupStatsHolder}, with no
         * live queue depth (used where a queue service is not available, e.g. tests).
         * @param workloadGroupState which needs to be converted
         * @return WorkloadGroupStatsHolder object
         */
        public static WorkloadGroupStatsHolder from(WorkloadGroupState workloadGroupState) {
            return from(workloadGroupState, 0L, 0L);
        }

        /**
         * static factory method to convert {@link WorkloadGroupState} into {@link WorkloadGroupStatsHolder}, including
         * the point-in-time queue depth gauges (which live in the queue service, not the state).
         * @param workloadGroupState which needs to be converted
         * @param queuedCurrent current queued depth for this group
         * @param queuePeak peak queued depth for this group
         * @return WorkloadGroupStatsHolder object
         */
        public static WorkloadGroupStatsHolder from(WorkloadGroupState workloadGroupState, long queuedCurrent, long queuePeak) {
            final WorkloadGroupStatsHolder statsHolder = new WorkloadGroupStatsHolder();

            Map<ResourceType, ResourceStats> resourceStatsMap = new HashMap<>();

            for (Map.Entry<ResourceType, ResourceTypeState> resourceTypeStateEntry : workloadGroupState.getResourceState().entrySet()) {
                resourceStatsMap.put(resourceTypeStateEntry.getKey(), ResourceStats.from(resourceTypeStateEntry.getValue()));
            }

            statsHolder.completions = workloadGroupState.getTotalCompletions();
            statsHolder.rejections = workloadGroupState.getTotalRejections();
            statsHolder.failures = workloadGroupState.getFailures();
            statsHolder.cancellations = workloadGroupState.getTotalCancellations();
            statsHolder.throttled = workloadGroupState.getTotalThrottled();
            statsHolder.queued = workloadGroupState.getTotalQueued();
            statsHolder.queueRejections = workloadGroupState.getTotalQueueRejections();
            statsHolder.queuedCurrent = queuedCurrent;
            statsHolder.queuePeak = queuePeak;
            statsHolder.totalQueueWaitMillis = workloadGroupState.getTotalQueueWaitMillis();
            statsHolder.queueWaitCount = workloadGroupState.getQueueWaitCount();
            statsHolder.maxQueueWaitMillis = workloadGroupState.getMaxQueueWaitMillis();
            statsHolder.resourceStats = resourceStatsMap;
            return statsHolder;
        }

        /**
         * Writes the @param {statsHolder} to @param {out}
         * @param out StreamOutput
         * @param statsHolder WorkloadGroupStatsHolder
         * @throws IOException exception
         */
        public static void writeTo(StreamOutput out, WorkloadGroupStatsHolder statsHolder) throws IOException {
            out.writeVLong(statsHolder.completions);
            out.writeVLong(statsHolder.rejections);
            out.writeVLong(statsHolder.failures);
            out.writeVLong(statsHolder.cancellations);
            // version-gated to match the StreamInput ctor; read/write order must stay in sync.
            if (out.getVersion().onOrAfter(Version.V_3_7_0)) {
                out.writeVLong(statsHolder.throttled);
                out.writeVLong(statsHolder.queued);
                out.writeVLong(statsHolder.queueRejections);
                out.writeVLong(statsHolder.queuedCurrent);
                out.writeVLong(statsHolder.queuePeak);
                out.writeVLong(statsHolder.totalQueueWaitMillis);
                out.writeVLong(statsHolder.queueWaitCount);
                out.writeVLong(statsHolder.maxQueueWaitMillis);
            }
            out.writeMap(statsHolder.resourceStats, (o, val) -> o.writeString(val.getName()), ResourceStats::writeTo);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            WorkloadGroupStatsHolder.writeTo(out, this);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(COMPLETIONS, completions);
            // builder.field(SHARD_COMPLETIONS, shardCompletions);
            builder.field(REJECTIONS, rejections);
            // builder.field(FAILURES, failures);
            builder.field(TOTAL_CANCELLATIONS, cancellations);
            builder.field(THROTTLED, throttled);
            builder.field(QUEUED, queued);
            builder.field(QUEUE_REJECTIONS, queueRejections);
            builder.field(QUEUED_CURRENT, queuedCurrent);
            builder.field(QUEUE_PEAK, queuePeak);
            builder.field(TOTAL_QUEUE_WAIT_MILLIS, totalQueueWaitMillis);
            builder.field(QUEUE_WAIT_COUNT, queueWaitCount);
            builder.field(MAX_QUEUE_WAIT_MILLIS, maxQueueWaitMillis);

            for (ResourceType resourceType : ResourceType.getSortedValues()) {
                ResourceStats resourceStats1 = resourceStats.get(resourceType);
                if (resourceStats1 == null) continue;
                builder.startObject(resourceType.getName());
                resourceStats1.toXContent(builder, params);
                builder.endObject();
            }
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            WorkloadGroupStatsHolder that = (WorkloadGroupStatsHolder) o;
            return completions == that.completions
                && rejections == that.rejections
                && Objects.equals(resourceStats, that.resourceStats)
                && failures == that.failures
                && cancellations == that.cancellations
                && throttled == that.throttled
                && queued == that.queued
                && queueRejections == that.queueRejections
                && queuedCurrent == that.queuedCurrent
                && queuePeak == that.queuePeak
                && totalQueueWaitMillis == that.totalQueueWaitMillis
                && queueWaitCount == that.queueWaitCount
                && maxQueueWaitMillis == that.maxQueueWaitMillis;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                completions,
                rejections,
                cancellations,
                failures,
                throttled,
                queued,
                queueRejections,
                queuedCurrent,
                queuePeak,
                totalQueueWaitMillis,
                queueWaitCount,
                maxQueueWaitMillis,
                resourceStats
            );
        }
    }

    /**
     * point in time resource level stats holder
     */
    public static class ResourceStats implements ToXContentObject, Writeable {
        public static final String CURRENT_USAGE = "current_usage";
        public static final String CANCELLATIONS = "cancellations";
        public static final String REJECTIONS = "rejections";
        public static final double PRECISION = 1e-9;
        private final double currentUsage;
        private final long cancellations;
        private final long rejections;

        public ResourceStats(double currentUsage, long cancellations, long rejections) {
            this.currentUsage = currentUsage;
            this.cancellations = cancellations;
            this.rejections = rejections;
        }

        public ResourceStats(StreamInput in) throws IOException {
            this.currentUsage = in.readDouble();
            this.cancellations = in.readVLong();
            this.rejections = in.readVLong();
        }

        public double getCurrentUsage() {
            return currentUsage;
        }

        public long getCancellations() {
            return cancellations;
        }

        public long getRejections() {
            return rejections;
        }

        /**
         * static factory method to convert {@link ResourceTypeState} into {@link ResourceStats}
         * @param resourceTypeState which needs to be converted
         * @return WorkloadGroupStatsHolder object
         */
        public static ResourceStats from(ResourceTypeState resourceTypeState) {
            return new ResourceStats(
                resourceTypeState.getLastRecordedUsage(),
                resourceTypeState.cancellations.count(),
                resourceTypeState.rejections.count()
            );
        }

        /**
         * Writes the @param {stats} to @param {out}
         * @param out StreamOutput
         * @param stats WorkloadGroupStatsHolder
         * @throws IOException exception
         */
        public static void writeTo(StreamOutput out, ResourceStats stats) throws IOException {
            out.writeDouble(stats.currentUsage);
            out.writeVLong(stats.cancellations);
            out.writeVLong(stats.rejections);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            ResourceStats.writeTo(out, this);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(CURRENT_USAGE, currentUsage);
            builder.field(CANCELLATIONS, cancellations);
            builder.field(REJECTIONS, rejections);
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ResourceStats that = (ResourceStats) o;
            return (currentUsage - that.currentUsage) < PRECISION && cancellations == that.cancellations && rejections == that.rejections;
        }

        @Override
        public int hashCode() {
            return Objects.hash(currentUsage, cancellations, rejections);
        }
    }
}
