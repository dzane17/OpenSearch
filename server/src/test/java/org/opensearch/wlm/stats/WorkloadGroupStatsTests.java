/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.stats;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.wlm.ResourceType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class WorkloadGroupStatsTests extends AbstractWireSerializingTestCase<WorkloadGroupStats> {

    public void testToXContent() throws IOException {
        final Map<String, WorkloadGroupStats.WorkloadGroupStatsHolder> stats = new HashMap<>();
        final String workloadGroupId = "afakjklaj304041-afaka";
        stats.put(
            workloadGroupId,
            new WorkloadGroupStats.WorkloadGroupStatsHolder(
                123456789,
                13,
                2,
                0,
                5,
                Map.of(ResourceType.CPU, new WorkloadGroupStats.ResourceStats(0.3, 13, 2))
            )
        );
        XContentBuilder builder = JsonXContent.contentBuilder();
        WorkloadGroupStats workloadGroupStats = new WorkloadGroupStats(stats);
        builder.startObject();
        workloadGroupStats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertEquals(
            "{\"workload_groups\":{\"afakjklaj304041-afaka\":{\"total_completions\":123456789,\"total_rejections\":13,\"total_cancellations\":0,\"total_throttled\":5,\"total_queued\":0,\"total_queue_rejections\":0,\"total_queue_timeouts\":0,\"queued_current\":0,\"queue_peak\":0,\"cpu\":{\"current_usage\":0.3,\"cancellations\":13,\"rejections\":2}}}}",
            builder.toString()
        );
    }

    @Override
    protected Writeable.Reader<WorkloadGroupStats> instanceReader() {
        return WorkloadGroupStats::new;
    }

    @Override
    protected WorkloadGroupStats createTestInstance() {
        return new WorkloadGroupStats(Map.of(randomAlphaOfLength(10), randomStatsHolder()));
    }

    // Uses the full 11-arg constructor with a random value for EVERY field — including all five queue stats — so the
    // wire round-trip (testSerialization) and equals/hashCode (testEqualsAndHashcode) actually exercise the new fields.
    // A previous version used the 6-arg ctor, leaving the five queue fields hard-zeroed, so a write/read order swap
    // among them would have been invisible.
    private static WorkloadGroupStats.WorkloadGroupStatsHolder randomStatsHolder() {
        return new WorkloadGroupStats.WorkloadGroupStatsHolder(
            randomNonNegativeLong(), // completions
            randomNonNegativeLong(), // rejections
            randomNonNegativeLong(), // failures
            randomNonNegativeLong(), // cancellations
            randomNonNegativeLong(), // throttled
            randomNonNegativeLong(), // queued
            randomNonNegativeLong(), // queueRejections
            randomNonNegativeLong(), // queueTimeouts
            randomNonNegativeLong(), // queuedCurrent
            randomNonNegativeLong(), // queuePeak
            Map.of(
                ResourceType.CPU,
                new WorkloadGroupStats.ResourceStats(
                    randomDoubleBetween(0.0, 0.90, false),
                    randomNonNegativeLong(),
                    randomNonNegativeLong()
                )
            )
        );
    }

    @Override
    protected WorkloadGroupStats mutateInstance(WorkloadGroupStats instance) {
        // Flip exactly one queue field on one holder so the mutation-inequality check actually asserts that queue
        // fields participate in equals/hashCode and, via the round-trip, that their wire order is honored.
        Map<String, WorkloadGroupStats.WorkloadGroupStatsHolder> stats = new HashMap<>(instance.getStats());
        String key = stats.isEmpty() ? randomAlphaOfLength(10) : stats.keySet().iterator().next();
        WorkloadGroupStats.WorkloadGroupStatsHolder h = stats.get(key);
        long bump = h == null ? 1 : h.getQueuedCurrent() + 1;
        stats.put(
            key,
            new WorkloadGroupStats.WorkloadGroupStatsHolder(
                h == null ? 0 : h.getCompletions(),
                h == null ? 0 : h.getRejections(),
                0,
                h == null ? 0 : h.getCancellations(),
                h == null ? 0 : h.getThrottled(),
                h == null ? 0 : h.getQueued(),
                h == null ? 0 : h.getQueueRejections(),
                h == null ? 0 : h.getQueueTimeouts(),
                bump, // mutated field: queuedCurrent
                h == null ? 0 : h.getQueuePeak(),
                h == null ? Map.of() : h.getResourceStats()
            )
        );
        return new WorkloadGroupStats(stats);
    }
}
