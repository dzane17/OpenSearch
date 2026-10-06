/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.stats;

import org.opensearch.Version;
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
                8,
                Map.of(ResourceType.CPU, new WorkloadGroupStats.ResourceStats(0.3, 13, 2))
            )
        );
        XContentBuilder builder = JsonXContent.contentBuilder();
        WorkloadGroupStats workloadGroupStats = new WorkloadGroupStats(stats);
        builder.startObject();
        workloadGroupStats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertEquals(
            "{\"workload_groups\":{\"afakjklaj304041-afaka\":{\"total_completions\":123456789,\"total_rejections\":13,\"total_cancellations\":0,\"total_throttled\":5,\"total_would_throttle\":8,\"total_queued\":0,\"total_queue_rejections\":0,\"queued_current\":0,\"queue_peak\":0,\"total_queue_wait_millis\":0,\"max_queue_wait_millis\":0,\"cpu\":{\"current_usage\":0.3,\"cancellations\":13,\"rejections\":2}}}}",
            builder.toString()
        );
    }

    public void testThrottledIsVersionGatedAndKeepsOlderStreamsAligned() throws IOException {
        WorkloadGroupStats original = new WorkloadGroupStats(
            Map.of(
                "group-1",
                new WorkloadGroupStats.WorkloadGroupStatsHolder(
                    100,
                    13,
                    2,
                    7,
                    5,
                    9,
                    Map.of(ResourceType.CPU, new WorkloadGroupStats.ResourceStats(0.3, 11, 4))
                )
            )
        );

        // A 3.10 peer exchanges total_throttled and total_would_throttle, and everything after them on the wire stays aligned.
        WorkloadGroupStats.WorkloadGroupStatsHolder current = copyInstance(original, Version.V_3_10_0).getStats().get("group-1");
        assertEquals(5, current.getThrottled());
        assertEquals(9, current.getWouldThrottle());
        assertEquals(100, current.getCompletions());
        assertEquals(0.3, current.getResourceStats().get(ResourceType.CPU).getCurrentUsage(), 0.0);

        // A pre-throttling peer writes neither counter: they read as 0 and the following resourceStats map still decodes.
        WorkloadGroupStats.WorkloadGroupStatsHolder legacy = copyInstance(original, Version.V_3_9_0).getStats().get("group-1");
        assertEquals(0, legacy.getThrottled());
        assertEquals(0, legacy.getWouldThrottle());
        assertEquals(100, legacy.getCompletions());
        assertEquals(13, legacy.getRejections());
        assertEquals(7, legacy.getCancellations());
        assertEquals(0.3, legacy.getResourceStats().get(ResourceType.CPU).getCurrentUsage(), 0.0);
        assertEquals(11, legacy.getResourceStats().get(ResourceType.CPU).getCancellations());
        assertEquals(4, legacy.getResourceStats().get(ResourceType.CPU).getRejections());
    }

    // The randomized createTestInstance() covers every field through the full constructor; this drives the queue-wait
    // fields through the real production path (WorkloadGroupState -> from(...)) so a mapping bug among them is caught.
    public void testQueueWaitFieldsSurviveWireRoundTrip() throws IOException {
        WorkloadGroupState state = new WorkloadGroupState();
        state.recordQueueWaitMillis(100);
        state.recordQueueWaitMillis(300); // two samples => queued=2, sum=400, max=300 -> mean=200
        WorkloadGroupStats.WorkloadGroupStatsHolder holder = WorkloadGroupStats.WorkloadGroupStatsHolder.from(state, 7L, 9L);
        assertEquals(2L, holder.getQueued());
        assertEquals(400L, holder.getTotalQueueWaitMillis());
        assertEquals(300L, holder.getMaxQueueWaitMillis());

        WorkloadGroupStats original = new WorkloadGroupStats(Map.of("g", holder));
        WorkloadGroupStats roundTripped = copyWriteable(original, writableRegistry(), WorkloadGroupStats::new);
        WorkloadGroupStats.WorkloadGroupStatsHolder rt = roundTripped.getStats().get("g");
        assertEquals(2L, rt.getQueued());
        assertEquals(400L, rt.getTotalQueueWaitMillis());
        assertEquals(300L, rt.getMaxQueueWaitMillis());
        assertEquals(7L, rt.getQueuedCurrent());
        assertEquals(9L, rt.getQueuePeak());
        assertEquals(original, roundTripped);
    }

    public void testQueueStatsAreVersionGatedAndKeepOlderStreamsAligned() throws IOException {
        WorkloadGroupStats original = new WorkloadGroupStats(
            Map.of(
                "group-1",
                new WorkloadGroupStats.WorkloadGroupStatsHolder(
                    100,
                    13,
                    2,
                    7,
                    5,
                    9,
                    21,
                    22,
                    23,
                    24,
                    25,
                    26,
                    Map.of(ResourceType.CPU, new WorkloadGroupStats.ResourceStats(0.3, 11, 4))
                )
            )
        );

        WorkloadGroupStats.WorkloadGroupStatsHolder current = copyInstance(original, Version.V_3_10_0).getStats().get("group-1");
        assertEquals(original.getStats().get("group-1"), current);

        // A pre-queueing peer writes none of the queue counters: they read as 0 and the resourceStats map still decodes.
        WorkloadGroupStats.WorkloadGroupStatsHolder legacy = copyInstance(original, Version.V_3_9_0).getStats().get("group-1");
        assertEquals(0, legacy.getQueued());
        assertEquals(0, legacy.getQueueRejections());
        assertEquals(0, legacy.getQueuedCurrent());
        assertEquals(0, legacy.getQueuePeak());
        assertEquals(0, legacy.getTotalQueueWaitMillis());
        assertEquals(0, legacy.getMaxQueueWaitMillis());
        assertEquals(100, legacy.getCompletions());
        assertEquals(11, legacy.getResourceStats().get(ResourceType.CPU).getCancellations());
    }

    @Override
    protected Writeable.Reader<WorkloadGroupStats> instanceReader() {
        return WorkloadGroupStats::new;
    }

    @Override
    protected WorkloadGroupStats createTestInstance() {
        return new WorkloadGroupStats(Map.of(randomAlphaOfLength(10), randomStatsHolder()));
    }

    // A random value for EVERY field, so the wire round-trip (testSerialization) and equals/hashCode
    // (testEqualsAndHashcode) exercise the queue fields too; a write/read order swap among them would otherwise be invisible.
    private static WorkloadGroupStats.WorkloadGroupStatsHolder randomStatsHolder() {
        return new WorkloadGroupStats.WorkloadGroupStatsHolder(
            randomNonNegativeLong(), // completions
            randomNonNegativeLong(), // rejections
            randomNonNegativeLong(), // failures
            randomNonNegativeLong(), // cancellations
            randomNonNegativeLong(), // throttled
            randomNonNegativeLong(), // wouldThrottle
            randomNonNegativeLong(), // queued
            randomNonNegativeLong(), // queueRejections
            randomNonNegativeLong(), // queuedCurrent
            randomNonNegativeLong(), // queuePeak
            randomNonNegativeLong(), // totalQueueWaitMillis
            randomNonNegativeLong(), // maxQueueWaitMillis
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

    // Changes exactly one randomly chosen holder field, copying all others (failures included), so the mutation check
    // proves every field participates in equals/hashCode.
    @Override
    protected WorkloadGroupStats mutateInstance(WorkloadGroupStats instance) {
        Map<String, WorkloadGroupStats.WorkloadGroupStatsHolder> stats = new HashMap<>(instance.getStats());
        String key = stats.keySet().iterator().next();
        WorkloadGroupStats.WorkloadGroupStatsHolder h = stats.get(key);
        long[] fields = new long[] {
            h.getCompletions(),
            h.getRejections(),
            h.getFailures(),
            h.getCancellations(),
            h.getThrottled(),
            h.getWouldThrottle(),
            h.getQueued(),
            h.getQueueRejections(),
            h.getQueuedCurrent(),
            h.getQueuePeak(),
            h.getTotalQueueWaitMillis(),
            h.getMaxQueueWaitMillis() };
        int mutated = randomIntBetween(0, fields.length - 1);
        fields[mutated] = fields[mutated] == Long.MAX_VALUE ? 0 : fields[mutated] + 1;
        stats.put(
            key,
            new WorkloadGroupStats.WorkloadGroupStatsHolder(
                fields[0],
                fields[1],
                fields[2],
                fields[3],
                fields[4],
                fields[5],
                fields[6],
                fields[7],
                fields[8],
                fields[9],
                fields[10],
                fields[11],
                h.getResourceStats()
            )
        );
        return new WorkloadGroupStats(stats);
    }
}
