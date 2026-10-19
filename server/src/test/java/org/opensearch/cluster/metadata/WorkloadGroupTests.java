/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.common.UUIDs;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.AbstractSerializingTestCase;
import org.opensearch.wlm.MutableWorkloadGroupFragment;
import org.opensearch.wlm.MutableWorkloadGroupFragment.ResiliencyMode;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.WorkloadGroupQueueSettings;
import org.opensearch.wlm.WorkloadGroupThrottleSettings;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class WorkloadGroupTests extends AbstractSerializingTestCase<WorkloadGroup> {

    private static final List<ResiliencyMode> allowedModes = List.of(ResiliencyMode.SOFT, ResiliencyMode.ENFORCED, ResiliencyMode.MONITOR);
    public static final Settings TEST_WLM_SEARCH_SETTINGS = Settings.builder().put("search.default_search_timeout", "30s").build();

    static WorkloadGroup createRandomWorkloadGroup(String _id) {
        String name = randomAlphaOfLength(10);
        Map<ResourceType, Double> resourceLimit = new HashMap<>();
        resourceLimit.put(ResourceType.MEMORY, randomDoubleBetween(0.0, 0.80, false));
        // Generate a valid throttling config: either disabled (empty), or one or more attribute rules, each with at least
        // one positive limit (so every rule's effective ceiling is >= 1).
        Settings.Builder throttling = Settings.builder();
        boolean throttlingConfigured = false;
        if (randomBoolean()) {
            throttlingConfigured = true;
            for (String attribute : randomSubsetOf(randomIntBetween(1, 3), List.of("group", "username", "role"))) {
                if (randomBoolean()) {
                    throttling.put(attribute + ".node_limit", randomIntBetween(1, 100));
                    if (randomBoolean()) {
                        throttling.put(attribute + ".shared_limit", randomIntBetween(0, 100));
                    }
                } else {
                    throttling.put(attribute + ".shared_limit", randomIntBetween(1, 100));
                    if (randomBoolean()) {
                        throttling.put(attribute + ".node_limit", randomIntBetween(0, 100));
                    }
                }
            }
        }
        // This random factory generates active queueing configurations by pairing queue settings with a throttle.
        // Dedicated tests below cover the also-valid queue-without-throttle case, where the queue is inert.
        Settings.Builder queue = Settings.builder();
        if (throttlingConfigured && randomBoolean()) {
            queue.put("size_per_bucket", randomIntBetween(1, 1000));
        }
        return new WorkloadGroup(
            name,
            _id,
            new MutableWorkloadGroupFragment(randomMode(), resourceLimit, Settings.EMPTY, throttling.build(), queue.build()),
            Instant.now().getMillis()
        );
    }

    private static ResiliencyMode randomMode() {
        return allowedModes.get(randomIntBetween(0, allowedModes.size() - 1));
    }

    /**
     * Parses to a new instance using the provided {@link XContentParser}
     *
     * @param parser the XContentParser
     */
    @Override
    protected WorkloadGroup doParseInstance(XContentParser parser) throws IOException {
        return WorkloadGroup.fromXContent(parser);
    }

    /**
     * Returns a {@link Writeable.Reader} that can be used to de-serialize the instance
     */
    @Override
    protected Writeable.Reader<WorkloadGroup> instanceReader() {
        return WorkloadGroup::new;
    }

    /**
     * Creates a random test instance to use in the tests. This method will be
     * called multiple times during test execution and should return a different
     * random instance each time it is called.
     */
    @Override
    protected WorkloadGroup createTestInstance() {
        return createRandomWorkloadGroup("1232sfraeradf_");
    }

    public void testNullName() {
        assertThrows(
            NullPointerException.class,
            () -> new WorkloadGroup(
                null,
                "_id",
                new MutableWorkloadGroupFragment(randomMode(), Collections.emptyMap()),
                Instant.now().getMillis()
            )
        );
    }

    public void testNullId() {
        assertThrows(
            NullPointerException.class,
            () -> new WorkloadGroup(
                "Dummy",
                null,
                new MutableWorkloadGroupFragment(randomMode(), Collections.emptyMap()),
                Instant.now().getMillis()
            )
        );
    }

    public void testNullResourceLimits() {
        assertThrows(
            NullPointerException.class,
            () -> new WorkloadGroup("analytics", "_id", new MutableWorkloadGroupFragment(randomMode(), null), Instant.now().getMillis())
        );
    }

    public void testEmptyResourceLimits() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new WorkloadGroup(
                "analytics",
                "_id",
                new MutableWorkloadGroupFragment(randomMode(), Collections.emptyMap()),
                Instant.now().getMillis()
            )
        );
    }

    public void testIllegalWorkloadGroupMode() {
        assertThrows(
            NullPointerException.class,
            () -> new WorkloadGroup(
                "analytics",
                "_id",
                new MutableWorkloadGroupFragment(null, Map.of(ResourceType.MEMORY, 0.4)),
                Instant.now().getMillis()
            )
        );
    }

    public void testWorkloadGroupInitiation() {
        WorkloadGroup workloadGroup = new WorkloadGroup(
            "analytics",
            new MutableWorkloadGroupFragment(randomMode(), Map.of(ResourceType.MEMORY, 0.4), TEST_WLM_SEARCH_SETTINGS)
        );
        assertNotNull(workloadGroup.getName());
        assertNotNull(workloadGroup.get_id());
        assertNotNull(workloadGroup.getResourceLimits());
        assertFalse(workloadGroup.getResourceLimits().isEmpty());
        assertEquals(1, workloadGroup.getResourceLimits().size());
        assertTrue(allowedModes.contains(workloadGroup.getResiliencyMode()));
        assertTrue(workloadGroup.getUpdatedAtInMillis() != 0);
        assertNotNull(workloadGroup.getSettings());
        assertEquals(TEST_WLM_SEARCH_SETTINGS, workloadGroup.getSettings());
    }

    public void testIllegalWorkloadGroupName() {
        assertThrows(
            NullPointerException.class,
            () -> new WorkloadGroup("a".repeat(51), "_id", new MutableWorkloadGroupFragment(), Instant.now().getMillis())
        );
        assertThrows(
            NullPointerException.class,
            () -> new WorkloadGroup("", "_id", new MutableWorkloadGroupFragment(), Instant.now().getMillis())
        );

    }

    public void testInvalidResourceLimitWhenInvalidSystemResourceValueIsGiven() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new WorkloadGroup(
                "analytics",
                "_id",
                new MutableWorkloadGroupFragment(randomMode(), Map.of(ResourceType.MEMORY, randomDoubleBetween(1.1, 1.8, false))),
                Instant.now().getMillis()
            )
        );
    }

    public void testValidWorkloadGroup() {
        WorkloadGroup workloadGroup = new WorkloadGroup(
            "analytics",
            "_id",
            new MutableWorkloadGroupFragment(randomMode(), Map.of(ResourceType.MEMORY, randomDoubleBetween(0.01, 0.8, false))),
            Instant.ofEpochMilli(1717187289).getMillis()
        );

        assertNotNull(workloadGroup.getName());
        assertEquals("analytics", workloadGroup.getName());
        assertNotNull(workloadGroup.getResourceLimits());
        assertFalse(workloadGroup.getResourceLimits().isEmpty());
        assertEquals(1, workloadGroup.getResourceLimits().size());
        assertTrue(allowedModes.contains(workloadGroup.getResiliencyMode()));
        assertEquals(1717187289, workloadGroup.getUpdatedAtInMillis());
    }

    public void testUpdatedAtAllowsCurrentTimestamp() {
        long currentTimestamp = Instant.now().getMillis();
        WorkloadGroup workloadGroup = new WorkloadGroup(
            "analytics",
            "_id",
            new MutableWorkloadGroupFragment(randomMode(), Map.of(ResourceType.MEMORY, randomDoubleBetween(0.01, 0.8, false))),
            currentTimestamp
        );

        assertEquals(currentTimestamp, workloadGroup.getUpdatedAtInMillis());
    }

    public void testUpdatedAtAllowsJitterAroundNow() {
        long now = Instant.now().getMillis();
        long[] timestamps = new long[] { Math.max(0L, now - 20L), now + 20L };

        for (long timestamp : timestamps) {
            WorkloadGroup workloadGroup = new WorkloadGroup(
                "analytics",
                "_id",
                new MutableWorkloadGroupFragment(randomMode(), Map.of(ResourceType.MEMORY, randomDoubleBetween(0.01, 0.8, false))),
                timestamp
            );
            assertEquals(timestamp, workloadGroup.getUpdatedAtInMillis());
        }
    }

    public void testUpdatedAtRejectsNegativeTimestamp() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new WorkloadGroup(
                "analytics",
                "_id",
                new MutableWorkloadGroupFragment(randomMode(), Map.of(ResourceType.MEMORY, randomDoubleBetween(0.01, 0.8, false))),
                -1L
            )
        );
    }

    public void testToXContent() throws IOException {
        long currentTimeInMillis = Instant.now().getMillis();
        String workloadGroupId = UUIDs.randomBase64UUID();
        WorkloadGroup workloadGroup = new WorkloadGroup(
            "TestWorkloadGroup",
            workloadGroupId,
            new MutableWorkloadGroupFragment(
                ResiliencyMode.ENFORCED,
                Map.of(ResourceType.CPU, 0.30, ResourceType.MEMORY, 0.40),
                TEST_WLM_SEARCH_SETTINGS
            ),
            currentTimeInMillis
        );
        XContentBuilder builder = JsonXContent.contentBuilder();
        workloadGroup.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String expected = String.format(
            Locale.ROOT,
            "{\"_id\":\"%s\",\"name\":\"TestWorkloadGroup\",\"resiliency_mode\":\"enforced\","
                + "\"resource_limits\":{\"cpu\":0.3,\"memory\":0.4},"
                + "\"settings\":{\"search.default_search_timeout\":\"30s\"},"
                + "\"updated_at\":%d}",
            workloadGroupId,
            currentTimeInMillis
        );
        assertEquals(expected, builder.toString());
    }

    public void testLegacySearchSettingsFieldRejected() throws IOException {
        String json = "{\"_id\":\"test_id\",\"name\":\"test\",\"resiliency_mode\":\"enforced\","
            + "\"resource_limits\":{\"memory\":0.5},"
            + "\"search_settings\":{\"timeout\":\"30s\"},"
            + "\"updated_at\":1720047207}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> WorkloadGroup.fromXContent(parser));
        assertTrue(exception.getMessage().contains("search_settings"));
    }

    public void testUpdateWithEmptySettingsClearsExisting() {
        WorkloadGroup existing = new WorkloadGroup(
            "test",
            "test_id",
            new MutableWorkloadGroupFragment(
                ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.builder().put("search.default_search_timeout", "30s").build()
            ),
            System.currentTimeMillis()
        );

        // Empty settings should clear all search settings
        MutableWorkloadGroupFragment updateFragment = new MutableWorkloadGroupFragment(null, Map.of(), Settings.EMPTY);

        WorkloadGroup updated = WorkloadGroup.updateExistingWorkloadGroup(existing, updateFragment);
        // All settings should be cleared
        assertTrue(updated.getSettings().isEmpty());
    }

    public void testUpdateMergesSettings() {
        WorkloadGroup existing = new WorkloadGroup(
            "test",
            "test_id",
            new MutableWorkloadGroupFragment(
                ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.builder().put("search.default_search_timeout", "30s").put("search.max_concurrent_shard_requests", "5").build()
            ),
            System.currentTimeMillis()
        );

        // Update only timeout — max_concurrent_shard_requests should persist
        MutableWorkloadGroupFragment updateFragment = new MutableWorkloadGroupFragment(
            null,
            Map.of(),
            Settings.builder().put("search.default_search_timeout", "1m").build()
        );

        WorkloadGroup updated = WorkloadGroup.updateExistingWorkloadGroup(existing, updateFragment);
        assertEquals("1m", updated.getSettings().get("search.default_search_timeout"));
        assertEquals("5", updated.getSettings().get("search.max_concurrent_shard_requests"));
    }

    public void testUpdateWithNullValueRemovesSetting() {
        WorkloadGroup existing = new WorkloadGroup(
            "test",
            "test_id",
            new MutableWorkloadGroupFragment(
                ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.builder().put("search.default_search_timeout", "30s").put("search.max_concurrent_shard_requests", "5").build()
            ),
            System.currentTimeMillis()
        );

        // Send null for timeout — should remove it, keep max_concurrent
        MutableWorkloadGroupFragment updateFragment = new MutableWorkloadGroupFragment(
            null,
            Map.of(),
            Settings.builder().putNull("search.default_search_timeout").build()
        );

        WorkloadGroup updated = WorkloadGroup.updateExistingWorkloadGroup(existing, updateFragment);
        assertNull(updated.getSettings().get("search.default_search_timeout"));
        assertEquals("5", updated.getSettings().get("search.max_concurrent_shard_requests"));
    }

    public void testUpdateWithNullFragmentSettingsKeepsExisting() throws IOException {
        WorkloadGroup existing = new WorkloadGroup(
            "test",
            "test_id",
            new MutableWorkloadGroupFragment(
                ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.builder().put("search.default_search_timeout", "30s").build()
            ),
            System.currentTimeMillis()
        );

        // Parse an update request that doesn't include "settings" key
        String json = "{\"resiliency_mode\":\"soft\",\"resource_limits\":{\"memory\":0.6}}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        WorkloadGroup.Builder builder = WorkloadGroup.Builder.fromXContent(parser);
        MutableWorkloadGroupFragment updateFragment = builder.getMutableWorkloadGroupFragment();

        WorkloadGroup updated = WorkloadGroup.updateExistingWorkloadGroup(existing, updateFragment);
        // Settings should be preserved
        assertEquals("30s", updated.getSettings().get("search.default_search_timeout"));
        assertEquals(ResiliencyMode.SOFT, updated.getResiliencyMode());
    }

    public void testUpdateOverrideRequestValuesPersistsThroughMerge() {
        WorkloadGroup existing = new WorkloadGroup(
            "test",
            "test_id",
            new MutableWorkloadGroupFragment(
                ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.builder().put("search.default_search_timeout", "30s").put("override_request_values", "true").build()
            ),
            System.currentTimeMillis()
        );

        // Update only timeout — override_request_values should persist as "true"
        MutableWorkloadGroupFragment updateFragment = new MutableWorkloadGroupFragment(
            null,
            Map.of(),
            Settings.builder().put("search.default_search_timeout", "1m").build()
        );

        WorkloadGroup updated = WorkloadGroup.updateExistingWorkloadGroup(existing, updateFragment);
        assertEquals("1m", updated.getSettings().get("search.default_search_timeout"));
        assertEquals("true", updated.getSettings().get("override_request_values"));
    }

    public void testToXContentOmitsUnsetThrottling() throws IOException {
        WorkloadGroup workloadGroup = new WorkloadGroup(
            "test",
            "test_id",
            new MutableWorkloadGroupFragment(ResiliencyMode.ENFORCED, Map.of(ResourceType.MEMORY, 0.5), Settings.EMPTY),
            System.currentTimeMillis()
        );
        XContentBuilder builder = JsonXContent.contentBuilder();
        workloadGroup.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertFalse(builder.toString().contains("throttling"));
    }

    public void testToXContentEmitsThrottling() throws IOException {
        long currentTimeInMillis = Instant.now().getMillis();
        String workloadGroupId = UUIDs.randomBase64UUID();
        Settings throttling = Settings.builder()
            .put("username.node_limit", 10)
            .put("username.shared_limit", 20)
            .put("role.shared_limit", 50)
            .build();
        WorkloadGroup workloadGroup = new WorkloadGroup(
            "TestWorkloadGroup",
            workloadGroupId,
            new MutableWorkloadGroupFragment(ResiliencyMode.ENFORCED, Map.of(ResourceType.CPU, 0.30), Settings.EMPTY, throttling),
            currentTimeInMillis
        );
        XContentBuilder builder = JsonXContent.contentBuilder();
        workloadGroup.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String expected = String.format(
            Locale.ROOT,
            "{\"_id\":\"%s\",\"name\":\"TestWorkloadGroup\",\"resiliency_mode\":\"enforced\","
                + "\"resource_limits\":{\"cpu\":0.3},"
                + "\"settings\":{},"
                + "\"throttling\":{\"role\":{\"shared_limit\":50},\"username\":{\"node_limit\":10,\"shared_limit\":20}},"
                + "\"updated_at\":%d}",
            workloadGroupId,
            currentTimeInMillis
        );
        assertEquals(expected, builder.toString());
    }

    public void testThrottlingXContentRoundTrip() throws IOException {
        WorkloadGroup created = parseCreate(
            "{\"resiliency_mode\":\"enforced\",\"resource_limits\":{\"memory\":0.5},"
                + "\"throttling\":{\"username\":{\"node_limit\":5,\"shared_limit\":10},\"role\":{\"node_limit\":3},"
                + "\"group\":{\"shared_limit\":100}}}"
        );
        Settings throttling = created.getMutableWorkloadGroupFragment().getThrottling();
        assertEquals(
            Settings.builder()
                .put("username.node_limit", 5)
                .put("username.shared_limit", 10)
                .put("role.node_limit", 3)
                .put("group.shared_limit", 100)
                .build(),
            throttling
        );
        List<WorkloadGroupThrottleSettings.Rule> rules = WorkloadGroupThrottleSettings.rules(throttling);
        // Acquisition order: narrowest first.
        assertEquals(List.of("username", "role", "group"), rules.stream().map(r -> r.attribute().key()).toList());

        XContentBuilder builder = JsonXContent.contentBuilder();
        created.toXContent(builder, ToXContent.EMPTY_PARAMS);
        WorkloadGroup reparsed = WorkloadGroup.Builder.fromXContent(createParser(JsonXContent.jsonXContent, builder.toString())).build();
        assertEquals(throttling, reparsed.getMutableWorkloadGroupFragment().getThrottling());
    }

    public void testNegativeThrottleLimitRejected() {
        // -1 is the internal "unset" sentinel and, like any negative value, is not user-settable.
        for (int badLimit : new int[] { -1, -2 }) {
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> new MutableWorkloadGroupFragment(
                    ResiliencyMode.ENFORCED,
                    Map.of(ResourceType.MEMORY, 0.5),
                    Settings.EMPTY,
                    Settings.builder().put("username.node_limit", badLimit).build()
                )
            );
            assertTrue(exception.getMessage().contains("throttling.username.node_limit must be non-negative"));
        }
    }

    public void testThrottleLimitExceedingMaxRejected() {
        // Integer.MAX_VALUE + 1: a well-formed non-negative integer, but too large for the int-backed setting. The error
        // must call out the overflow rather than falsely claiming it is "not an integer".
        String tooLarge = Long.toString((long) Integer.MAX_VALUE + 1);
        for (String key : new String[] { "node_limit", "shared_limit" }) {
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> new MutableWorkloadGroupFragment(
                    ResiliencyMode.ENFORCED,
                    Map.of(ResourceType.MEMORY, 0.5),
                    Settings.EMPTY,
                    Settings.builder().put("username." + key, tooLarge).build()
                )
            );
            assertTrue(exception.getMessage().contains(key + " must not exceed " + Integer.MAX_VALUE));
            assertTrue(exception.getMessage().contains(tooLarge));
        }
    }

    public void testThrottleLimitAtMaxAccepted() {
        // Integer.MAX_VALUE is the largest limit the int-backed setting can hold and must be accepted.
        WorkloadGroup workloadGroup = new WorkloadGroup(
            "test",
            "test_id",
            new MutableWorkloadGroupFragment(
                ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.EMPTY,
                Settings.builder().put("username.node_limit", Integer.MAX_VALUE).build()
            ),
            System.currentTimeMillis()
        );
        Settings throttling = workloadGroup.getMutableWorkloadGroupFragment().getThrottling();
        assertEquals(Integer.MAX_VALUE, WorkloadGroupThrottleSettings.rule(throttling, "username").nodeLimit());
    }

    public void testNonNumericThrottleLimitRejected() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new MutableWorkloadGroupFragment(
                ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.EMPTY,
                Settings.builder().put("username.node_limit", "not_a_number").build()
            )
        );
        assertTrue(exception.getMessage().contains("throttling.username.node_limit must be an integer"));
    }

    public void testLargeMergedCeilingDoesNotOverflow() {
        // Both limits at Integer.MAX_VALUE: the merged ceiling must be summed as a long so it does not overflow to a
        // negative value and get misreported as an effective ceiling of 0.
        WorkloadGroup workloadGroup = new WorkloadGroup(
            "test",
            "test_id",
            new MutableWorkloadGroupFragment(
                ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.EMPTY,
                Settings.builder().put("username.node_limit", Integer.MAX_VALUE).put("username.shared_limit", Integer.MAX_VALUE).build()
            ),
            System.currentTimeMillis()
        );
        WorkloadGroupThrottleSettings.Rule rule = WorkloadGroupThrottleSettings.rule(
            workloadGroup.getMutableWorkloadGroupFragment().getThrottling(),
            "username"
        );
        assertEquals(Integer.MAX_VALUE, rule.nodeLimit());
        assertEquals(Integer.MAX_VALUE, rule.sharedLimit());
    }

    public void testInvalidThrottleAttributeRejected() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new MutableWorkloadGroupFragment(
                ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.EMPTY,
                Settings.builder().put("index.node_limit", 5).build()
            )
        );
        assertEquals(
            "throttling attribute [index] is not supported; supported attributes are [group, role, username]",
            exception.getMessage()
        );
    }

    public void testUnknownThrottleKeyRejected() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new MutableWorkloadGroupFragment(
                ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.EMPTY,
                Settings.builder().put("role.bogus_limit", 5).build()
            )
        );
        assertEquals("Unknown throttle setting: throttling.role.bogus_limit", exception.getMessage());
    }

    public void testLegacyFlatThrottlingRejected() {
        for (Settings legacy : List.of(
            Settings.builder().put("attribute", "username").put("node_limit", 5).build(),
            Settings.builder().put("shared_limit", 5).build()
        )) {
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> new MutableWorkloadGroupFragment(ResiliencyMode.ENFORCED, Map.of(ResourceType.MEMORY, 0.5), Settings.EMPTY, legacy)
            );
            assertTrue(exception.getMessage(), exception.getMessage().startsWith("throttling must be keyed by attribute"));
        }
    }

    public void testScalarAttributeValueRejected() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new MutableWorkloadGroupFragment(
                ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.EMPTY,
                Settings.builder().put("role", 5).build()
            )
        );
        assertTrue(exception.getMessage(), exception.getMessage().contains("throttling.role must be an object"));
    }

    public void testZeroEffectiveCeilingRejected() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new WorkloadGroup(
                "test",
                "test_id",
                new MutableWorkloadGroupFragment(
                    ResiliencyMode.ENFORCED,
                    Map.of(ResourceType.MEMORY, 0.5),
                    Settings.EMPTY,
                    // A valid username rule does not rescue a role rule that would reject everything.
                    Settings.builder().put("username.node_limit", 5).put("role.node_limit", 0).build()
                ),
                System.currentTimeMillis()
            )
        );
        assertTrue(exception.getMessage(), exception.getMessage().contains("Effective throttle ceiling for throttling.role is 0"));
        assertTrue(exception.getMessage(), exception.getMessage().contains("node_limit=0, shared_limit=unset"));
        assertFalse(exception.getMessage().contains("-1"));
    }

    public void testZeroLimitValidWhenOtherLimitPositive() {
        // node_limit=0 is fine as long as the effective ceiling is >= 1 (shared_limit carries it).
        WorkloadGroup workloadGroup = new WorkloadGroup(
            "test",
            "test_id",
            new MutableWorkloadGroupFragment(
                ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.EMPTY,
                Settings.builder().put("username.node_limit", 0).put("username.shared_limit", 10).build()
            ),
            System.currentTimeMillis()
        );
        WorkloadGroupThrottleSettings.Rule rule = WorkloadGroupThrottleSettings.rule(
            workloadGroup.getMutableWorkloadGroupFragment().getThrottling(),
            "username"
        );
        assertEquals(0, rule.nodeLimit());
        assertEquals(10, rule.sharedLimit());
    }

    public void testUpdateMergesThrottling() {
        WorkloadGroup existing = new WorkloadGroup(
            "test",
            "test_id",
            new MutableWorkloadGroupFragment(
                ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.EMPTY,
                Settings.builder().put("username.node_limit", 10).put("username.shared_limit", 20).build()
            ),
            System.currentTimeMillis()
        );

        // Update one field of the username rule and add a role rule; everything else persists.
        MutableWorkloadGroupFragment updateFragment = new MutableWorkloadGroupFragment(
            null,
            Map.of(),
            Settings.EMPTY,
            Settings.builder().put("username.node_limit", 50).put("role.shared_limit", 30).build()
        );

        WorkloadGroup updated = WorkloadGroup.updateExistingWorkloadGroup(existing, updateFragment);
        assertEquals(
            Settings.builder().put("username.node_limit", 50).put("username.shared_limit", 20).put("role.shared_limit", 30).build(),
            updated.getMutableWorkloadGroupFragment().getThrottling()
        );
    }

    public void testUpdateWithNullClearsThrottleKey() throws IOException {
        WorkloadGroup existing = new WorkloadGroup(
            "test",
            "test_id",
            new MutableWorkloadGroupFragment(
                ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.EMPTY,
                Settings.builder().put("username.node_limit", 10).put("username.shared_limit", 20).build()
            ),
            System.currentTimeMillis()
        );

        String json = "{\"resource_limits\":{\"memory\":0.5},\"throttling\":{\"username\":{\"node_limit\":null}}}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        MutableWorkloadGroupFragment updateFragment = WorkloadGroup.Builder.fromXContent(parser).getMutableWorkloadGroupFragment();

        WorkloadGroup updated = WorkloadGroup.updateExistingWorkloadGroup(existing, updateFragment);
        assertEquals(
            Settings.builder().put("username.shared_limit", 20).build(),
            updated.getMutableWorkloadGroupFragment().getThrottling()
        );
    }

    public void testUpdateWithNullAttributeRemovesRule() throws IOException {
        WorkloadGroup existing = new WorkloadGroup(
            "test",
            "test_id",
            new MutableWorkloadGroupFragment(
                ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.EMPTY,
                Settings.builder().put("username.node_limit", 10).put("role.node_limit", 5).put("role.shared_limit", 20).build()
            ),
            System.currentTimeMillis()
        );

        String json = "{\"resource_limits\":{\"memory\":0.5},\"throttling\":{\"role\":null}}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        MutableWorkloadGroupFragment updateFragment = WorkloadGroup.Builder.fromXContent(parser).getMutableWorkloadGroupFragment();

        WorkloadGroup updated = WorkloadGroup.updateExistingWorkloadGroup(existing, updateFragment);
        assertEquals(Settings.builder().put("username.node_limit", 10).build(), updated.getMutableWorkloadGroupFragment().getThrottling());
    }

    public void testUpdateClearingEveryFieldOfARuleRemovesIt() {
        WorkloadGroup existing = new WorkloadGroup(
            "test",
            "test_id",
            new MutableWorkloadGroupFragment(
                ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.EMPTY,
                Settings.builder().put("username.node_limit", 10).put("role.node_limit", 5).build()
            ),
            System.currentTimeMillis()
        );
        WorkloadGroup updated = WorkloadGroup.updateExistingWorkloadGroup(
            existing,
            new MutableWorkloadGroupFragment(null, Map.of(), Settings.EMPTY, Settings.builder().putNull("role.node_limit").build())
        );
        Settings throttling = updated.getMutableWorkloadGroupFragment().getThrottling();
        assertEquals(Settings.builder().put("username.node_limit", 10).build(), throttling);
        assertNull(WorkloadGroupThrottleSettings.rule(throttling, "role"));
    }

    public void testUpdateWithNullThrottlingObjectDisables() throws IOException {
        WorkloadGroup existing = new WorkloadGroup(
            "test",
            "test_id",
            new MutableWorkloadGroupFragment(
                ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.EMPTY,
                Settings.builder().put("username.node_limit", 10).put("role.node_limit", 10).build()
            ),
            System.currentTimeMillis()
        );

        // "throttling": null disables throttling entirely
        String json = "{\"resource_limits\":{\"memory\":0.5},\"throttling\":null}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        MutableWorkloadGroupFragment updateFragment = WorkloadGroup.Builder.fromXContent(parser).getMutableWorkloadGroupFragment();

        WorkloadGroup updated = WorkloadGroup.updateExistingWorkloadGroup(existing, updateFragment);
        assertTrue(updated.getMutableWorkloadGroupFragment().getThrottling().isEmpty());
    }

    public void testCreateDropsNullThrottleValues() throws IOException {
        // On create there is nothing to clear, so null-valued keys are dropped rather than persisted; an
        // all-null throttling object therefore collapses to empty (disabled) instead of hitting a ceiling error.
        WorkloadGroup partial = parseCreate(
            "{\"resiliency_mode\":\"enforced\",\"resource_limits\":{\"memory\":0.5},"
                + "\"throttling\":{\"username\":{\"node_limit\":10,\"shared_limit\":null},\"role\":null}}"
        );
        Settings throttling = partial.getMutableWorkloadGroupFragment().getThrottling();
        // raw check: null-valued keys were dropped, not persisted
        assertEquals(Settings.builder().put("username.node_limit", 10).build(), throttling);

        WorkloadGroup allNull = parseCreate(
            "{\"resiliency_mode\":\"enforced\",\"resource_limits\":{\"memory\":0.5},"
                + "\"throttling\":{\"username\":{\"node_limit\":null}}}"
        );
        assertTrue(allNull.getMutableWorkloadGroupFragment().getThrottling().isEmpty());
    }

    private WorkloadGroup throttledGroup(Settings throttling) {
        return new WorkloadGroup(
            "test",
            "test_id",
            new MutableWorkloadGroupFragment(ResiliencyMode.ENFORCED, Map.of(ResourceType.MEMORY, 0.5), Settings.EMPTY, throttling),
            System.currentTimeMillis()
        );
    }

    private MutableWorkloadGroupFragment parseUpdate(String json) throws IOException {
        return WorkloadGroup.Builder.fromXContent(createParser(JsonXContent.jsonXContent, json)).getMutableWorkloadGroupFragment();
    }

    public void testEmptyRuleObjectIsRejectedInsteadOfClearingEveryRule() {
        // Settings.fromXContent flattens {} to nothing, which an update would read as "clear all"; reject it instead.
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parseUpdate("{\"throttling\":{\"role\":{}}}"));
        assertTrue(e.getMessage(), e.getMessage().contains("throttling.role must not be empty; set it to null to remove the rule"));
        e = expectThrows(IllegalArgumentException.class, () -> parseUpdate("{\"throttling\":{}}"));
        assertTrue(e.getMessage(), e.getMessage().contains("throttling must not be empty; set it to null to clear every rule"));
        expectThrows(
            IllegalArgumentException.class,
            () -> parseCreate("{\"resiliency_mode\":\"enforced\",\"resource_limits\":{\"memory\":0.5},\"throttling\":{\"username\":{}}}")
        );
    }

    public void testUnknownAttributeIsRejectedOnCreateAndUpdate() {
        String throttling = "{\"username\":{\"node_limit\":5,\"shared_limit\":10},\"role\":{\"node_limit\":20,\"shared_limit\":50},"
            + "\"abc\":{\"node_limit\":100}}";
        IllegalArgumentException create = expectThrows(
            IllegalArgumentException.class,
            () -> parseCreate("{\"resiliency_mode\":\"enforced\",\"resource_limits\":{\"memory\":0.5},\"throttling\":" + throttling + "}")
        );
        assertEquals("throttling attribute [abc] is not supported; supported attributes are [group, role, username]", create.getMessage());
        IllegalArgumentException update = expectThrows(
            IllegalArgumentException.class,
            () -> parseUpdate("{\"throttling\":" + throttling + "}")
        );
        assertEquals(create.getMessage(), update.getMessage());
    }

    public void testDottedThrottleKeysAreRejected() {
        for (String json : List.of(
            "{\"throttling\":{\"username.node_limit\":5}}",
            "{\"throttling\":{\"username\":{\"node_limit.x\":5}}}"
        )) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parseUpdate(json));
            assertTrue(e.getMessage(), e.getMessage().contains("use nested objects"));
        }
    }

    public void testNonScalarLimitIsRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> parseUpdate("{\"throttling\":{\"username\":{\"node_limit\":[5]}}}")
        );
        assertEquals("throttling.username.node_limit must be an integer", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> parseUpdate("{\"throttling\":{\"username\":{\"burst\":{}}}}"));
        assertEquals("Unknown throttle setting: throttling.username.burst", e.getMessage());
    }

    public void testStoredLegacyFlatThrottlingIsTranslatedOnLoad() throws IOException {
        // Metadata persisted by the single-rule build of this feature still loads, and is enforced under the keyed form.
        String stored = "{\"_id\":\"test_id\",\"name\":\"test\",\"resiliency_mode\":\"enforced\","
            + "\"resource_limits\":{\"memory\":0.5},"
            + "\"throttling\":{\"attribute\":\"username\",\"node_limit\":5,\"shared_limit\":10},"
            + "\"updated_at\":1720047207}";
        WorkloadGroup group = WorkloadGroup.fromXContent(createParser(JsonXContent.jsonXContent, stored));
        assertEquals(
            Settings.builder().put("username.node_limit", 5).put("username.shared_limit", 10).build(),
            group.getMutableWorkloadGroupFragment().getThrottling()
        );

        // The same legacy bag arriving over the wire (cluster state from a node on the older build) is translated too.
        Settings legacy = Settings.builder().put("attribute", "role").put("shared_limit", 7).build();
        assertEquals(Settings.builder().put("role.shared_limit", 7).build(), WorkloadGroupThrottleSettings.normalizeStored(legacy));
        // A legacy group still loads (and its config is not rejected) on the stream path.
        WorkloadGroup fromStream = roundTrip(
            new WorkloadGroup(
                "test",
                "test_id",
                MutableWorkloadGroupFragment.ofStoredThrottling(
                    ResiliencyMode.ENFORCED,
                    Map.of(ResourceType.MEMORY, 0.5),
                    Settings.EMPTY,
                    legacy,
                    Settings.EMPTY
                ),
                1L
            )
        );
        assertEquals(Settings.builder().put("role.shared_limit", 7).build(), fromStream.getMutableWorkloadGroupFragment().getThrottling());
    }

    public void testStoredUnknownAttributeLoadsRoundTripsAndDoesNotBlockUpdates() throws IOException {
        // A rule for an attribute (or a field) this build doesn't know — e.g. written by a newer build — must load from
        // persisted metadata, survive being written back, and not block an unrelated update.
        String stored = "{\"_id\":\"test_id\",\"name\":\"test\",\"resiliency_mode\":\"enforced\","
            + "\"resource_limits\":{\"memory\":0.5},"
            + "\"throttling\":{\"tenant\":{\"node_limit\":3},\"username\":{\"node_limit\":5,\"burst\":2}},"
            + "\"updated_at\":1720047207}";
        WorkloadGroup group = WorkloadGroup.fromXContent(createParser(JsonXContent.jsonXContent, stored));
        Settings throttling = group.getMutableWorkloadGroupFragment().getThrottling();
        assertEquals("3", throttling.get("tenant.node_limit"));
        assertEquals("2", throttling.get("username.burst"));
        assertEquals(
            List.of("username"),
            group.getMutableWorkloadGroupFragment().getThrottleRules().stream().map(r -> r.attribute().key()).toList()
        );

        XContentBuilder builder = JsonXContent.contentBuilder();
        group.toXContent(builder, ToXContent.EMPTY_PARAMS);
        WorkloadGroup reloaded = WorkloadGroup.fromXContent(createParser(JsonXContent.jsonXContent, builder.toString()));
        assertEquals(throttling, reloaded.getMutableWorkloadGroupFragment().getThrottling());
        assertEquals(group, roundTrip(group));

        WorkloadGroup updated = WorkloadGroup.updateExistingWorkloadGroup(group, parseUpdate("{\"resiliency_mode\":\"soft\"}"));
        assertEquals(ResiliencyMode.SOFT, updated.getResiliencyMode());
        assertEquals(throttling, updated.getMutableWorkloadGroupFragment().getThrottling());
    }

    public void testStoredBareValueSurvivesPersistence() throws IOException {
        String stored = "{\"_id\":\"test_id\",\"name\":\"test\",\"resiliency_mode\":\"enforced\","
            + "\"resource_limits\":{\"memory\":0.5},\"throttling\":{\"mode\":\"x\",\"username\":{\"node_limit\":5}},"
            + "\"updated_at\":1720047207}";
        WorkloadGroup group = WorkloadGroup.fromXContent(createParser(JsonXContent.jsonXContent, stored));
        XContentBuilder builder = JsonXContent.contentBuilder();
        group.toXContent(builder, ToXContent.EMPTY_PARAMS);
        WorkloadGroup reloaded = WorkloadGroup.fromXContent(createParser(JsonXContent.jsonXContent, builder.toString()));
        assertEquals("x", reloaded.getMutableWorkloadGroupFragment().getThrottling().get("mode"));
        assertEquals(group.getMutableWorkloadGroupFragment().getThrottling(), reloaded.getMutableWorkloadGroupFragment().getThrottling());
    }

    public void testMultiRuleGroupWireRoundTrip() {
        WorkloadGroup group = throttledGroup(
            Settings.builder().put("username.node_limit", 5).put("username.shared_limit", 10).put("role.shared_limit", 50).build()
        );
        WorkloadGroup copy = roundTrip(group);
        assertEquals(group, copy);
        assertEquals(2, copy.getMutableWorkloadGroupFragment().getThrottleRules().size());
    }

    public void testUpdateFragmentRemoveMarkerSurvivesTheWire() throws IOException {
        // An update travels from the REST node to the cluster manager over the wire; its "remove this rule" marker must
        // survive read-path normalization.
        MutableWorkloadGroupFragment fragment = parseUpdate("{\"throttling\":{\"role\":null,\"username\":{\"node_limit\":2}}}");
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            fragment.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                MutableWorkloadGroupFragment read = new MutableWorkloadGroupFragment(in);
                assertEquals(fragment.getThrottling(), read.getThrottling());
                WorkloadGroup updated = WorkloadGroup.updateExistingWorkloadGroup(
                    throttledGroup(Settings.builder().put("role.node_limit", 1).put("username.node_limit", 1).build()),
                    read
                );
                assertEquals(
                    Settings.builder().put("username.node_limit", 2).build(),
                    updated.getMutableWorkloadGroupFragment().getThrottling()
                );
            }
        }
    }

    public void testUpdateWithoutThrottlingOverTheWireKeepsEveryRule() throws IOException {
        // An update that doesn't mention throttling is "keep existing", including after the REST node forwards it to the
        // cluster manager (absent must not deserialize as empty, which would mean "clear every rule").
        MutableWorkloadGroupFragment fragment = parseUpdate("{\"resiliency_mode\":\"soft\"}");
        assertNull(fragment.getThrottling());
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            fragment.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                MutableWorkloadGroupFragment read = new MutableWorkloadGroupFragment(in);
                Settings existing = Settings.builder().put("username.node_limit", 5).put("group.shared_limit", 100).build();
                WorkloadGroup updated = WorkloadGroup.updateExistingWorkloadGroup(throttledGroup(existing), read);
                assertEquals(existing, updated.getMutableWorkloadGroupFragment().getThrottling());
                assertEquals(ResiliencyMode.SOFT, updated.getResiliencyMode());
            }
        }
    }

    public void testUpdateValidatesThrottlingThatBypassedTheRestParser() {
        // An update fragment deserialized on the cluster manager has not been through the strict parser.
        MutableWorkloadGroupFragment unchecked = MutableWorkloadGroupFragment.ofStoredThrottling(
            null,
            Map.of(),
            Settings.EMPTY,
            Settings.builder().put("username.node_limit", "abc").build(),
            null
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> WorkloadGroup.updateExistingWorkloadGroup(
                throttledGroup(Settings.builder().put("group.node_limit", 1).build()),
                unchecked
            )
        );
    }

    private static WorkloadGroup roundTrip(WorkloadGroup group) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            group.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new WorkloadGroup(in);
            }
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private WorkloadGroup parseCreate(String json) throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        return WorkloadGroup.Builder.fromXContent(parser).name("test")._id("test_id").updatedAt(System.currentTimeMillis()).build();
    }

    public void testSettingsNullFromXContentClearsSettings() throws IOException {
        // Simulate parsing {"settings": null} via XContent
        String json = "{\"_id\":\"test_id\",\"name\":\"test\",\"resiliency_mode\":\"enforced\","
            + "\"resource_limits\":{\"memory\":0.5},"
            + "\"settings\":null,"
            + "\"updated_at\":1720047207}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        WorkloadGroup.Builder builder = WorkloadGroup.Builder.fromXContent(parser);
        MutableWorkloadGroupFragment fragment = builder.getMutableWorkloadGroupFragment();
        // Settings should be empty (cleared)
        assertTrue(fragment.getSettings().isEmpty());
    }

    public void testQueueWithoutThrottleLimitIsAccepted() {
        // A queue with no throttle limit is INERT, not invalid: queueing engages only on a throttle denial, so admission
        // fails open long before it could reach the queue. Accepting it keeps "disable the throttle" non-destructive (the
        // queue sizing survives, because validation runs against the merged config) and — more importantly — keeps this
        // constructor from throwing on the cluster-state READ path, where a throw is an unreadable-metadata failure rather
        // than a rejected API call.
        WorkloadGroup group = new WorkloadGroup(
            "test",
            "test_id",
            new MutableWorkloadGroupFragment(
                ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.EMPTY,
                Settings.EMPTY, // no throttling
                Settings.builder().put("size_per_bucket", 100).build()
            ),
            System.currentTimeMillis()
        );
        assertTrue("throttling must stay empty", group.getMutableWorkloadGroupFragment().getThrottling().isEmpty());
        assertEquals(
            "the queue setting must be preserved verbatim",
            100,
            WorkloadGroupQueueSettings.SIZE_PER_BUCKET.get(group.getMutableWorkloadGroupFragment().getQueue()).intValue()
        );
    }

    public void testUpdateCanClearThrottlingWhileKeepingTheQueue() {
        // The operator-facing consequence: an update that clears throttling.* while the group keeps its queue must be
        // ACCEPTED, so re-enabling the throttle later does not require retyping the queue sizing. Before the rule was
        // dropped this threw, because validation runs against the fully merged config.
        WorkloadGroup throttledWithQueue = new WorkloadGroup(
            "test",
            "test_id",
            new MutableWorkloadGroupFragment(
                ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.EMPTY,
                Settings.builder().put("username.node_limit", 5).build(),
                Settings.builder().put("size_per_bucket", 100).build()
            ),
            System.currentTimeMillis()
        );
        // A null attribute removes the only rule; the queue is restated because this constructor reads null as "clear".
        WorkloadGroup updated = WorkloadGroup.updateExistingWorkloadGroup(
            throttledWithQueue,
            new MutableWorkloadGroupFragment(
                ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.EMPTY,
                Settings.builder().putNull("username").build(),
                Settings.builder().put("size_per_bucket", 100).build()
            )
        );
        assertTrue("throttling must be cleared", updated.getMutableWorkloadGroupFragment().getThrottling().isEmpty());
        assertEquals(
            "clearing throttling alone must not force the queue to be cleared too",
            100,
            WorkloadGroupQueueSettings.SIZE_PER_BUCKET.get(updated.getMutableWorkloadGroupFragment().getQueue()).intValue()
        );
    }

    public void testQueueWithoutThrottleLimitSurvivesWireRoundTrip() {
        // The reason the check could not live in this constructor: it also guards the deserialization path. A group with a
        // queue but no throttle must round-trip through the wire, or a node receiving it could not apply cluster state.
        WorkloadGroup group = new WorkloadGroup(
            "test",
            "test_id",
            new MutableWorkloadGroupFragment(
                ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.EMPTY,
                Settings.EMPTY,
                Settings.builder().put("size_per_bucket", 100).build()
            ),
            System.currentTimeMillis()
        );
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            group.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                WorkloadGroup roundTripped = new WorkloadGroup(in);
                assertEquals(group, roundTripped);
            }
        } catch (IOException e) {
            throw new AssertionError("a queue-without-throttle group must deserialize, not throw", e);
        }
    }

    public void testQueueTimeoutIsRejectedAsUnknownSetting() {
        // queue.timeout was removed: a client bounds its wait via cancel_after_time_interval, and the queue has no
        // wall-clock deadline. A stale queue.timeout config must be rejected as an unknown key, not silently accepted.
        Settings throttling = Settings.builder().put("username.node_limit", 10).build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new WorkloadGroup(
                "test",
                "test_id",
                new MutableWorkloadGroupFragment(
                    ResiliencyMode.ENFORCED,
                    Map.of(ResourceType.MEMORY, 0.5),
                    Settings.EMPTY,
                    throttling,
                    Settings.builder().put("size_per_bucket", 10).put("timeout", "30s").build()
                ),
                System.currentTimeMillis()
            )
        );
        assertTrue(e.getMessage(), e.getMessage().contains("Unknown queue setting"));
    }

    public void testToXContentEmitsQueue() throws IOException {
        long currentTimeInMillis = Instant.now().getMillis();
        String workloadGroupId = UUIDs.randomBase64UUID();
        Settings throttling = Settings.builder().put("username.node_limit", 10).build();
        Settings queue = Settings.builder().put("size_per_bucket", 200).build();
        WorkloadGroup workloadGroup = new WorkloadGroup(
            "TestWorkloadGroup",
            workloadGroupId,
            new MutableWorkloadGroupFragment(ResiliencyMode.ENFORCED, Map.of(ResourceType.CPU, 0.30), Settings.EMPTY, throttling, queue),
            currentTimeInMillis
        );
        XContentBuilder builder = JsonXContent.contentBuilder();
        workloadGroup.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String expected = String.format(
            Locale.ROOT,
            "{\"_id\":\"%s\",\"name\":\"TestWorkloadGroup\",\"resiliency_mode\":\"enforced\","
                + "\"resource_limits\":{\"cpu\":0.3},"
                + "\"settings\":{},"
                + "\"throttling\":{\"username\":{\"node_limit\":10}},"
                + "\"queue\":{\"size_per_bucket\":200},"
                + "\"updated_at\":%d}",
            workloadGroupId,
            currentTimeInMillis
        );
        assertEquals(expected, builder.toString());
    }

    public void testToXContentOmitsUnsetQueue() throws IOException {
        Settings throttling = Settings.builder().put("username.node_limit", 10).build();
        WorkloadGroup workloadGroup = new WorkloadGroup(
            "test",
            "test_id",
            new MutableWorkloadGroupFragment(ResiliencyMode.ENFORCED, Map.of(ResourceType.MEMORY, 0.5), Settings.EMPTY, throttling),
            System.currentTimeMillis()
        );
        XContentBuilder builder = JsonXContent.contentBuilder();
        workloadGroup.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertFalse(builder.toString().contains("queue"));
    }

    public void testQueueNullFromXContentClearsQueue() throws IOException {
        String json = "{\"_id\":\"test_id\",\"name\":\"test\",\"resiliency_mode\":\"enforced\","
            + "\"resource_limits\":{\"memory\":0.5},"
            + "\"queue\":null,"
            + "\"updated_at\":1720047207}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        WorkloadGroup.Builder builder = WorkloadGroup.Builder.fromXContent(parser);
        MutableWorkloadGroupFragment fragment = builder.getMutableWorkloadGroupFragment();
        assertTrue(fragment.getQueue().isEmpty());
    }

    public void testArrayFieldIsRejected() {
        for (String field : List.of("throttling", "settings", "queue", "resource_limits")) {
            String json = "{\"" + field + "\":[{\"x\":1}]}";
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> WorkloadGroup.Builder.fromXContent(createParser(JsonXContent.jsonXContent, json))
            );
            assertEquals(field + " is not a valid array in WorkloadGroup", e.getMessage());
        }
    }

    public void testStoredArrayFieldIsSkippedOnLoad() throws IOException {
        String stored = "{\"_id\":\"test_id\",\"name\":\"test\",\"resiliency_mode\":\"enforced\","
            + "\"resource_limits\":{\"memory\":0.5},\"throttling\":[{\"username\":{\"node_limit\":5}}],"
            + "\"updated_at\":1720047207}";
        WorkloadGroup group = WorkloadGroup.fromXContent(createParser(JsonXContent.jsonXContent, stored));
        assertTrue(group.getMutableWorkloadGroupFragment().getThrottling().isEmpty());
        assertEquals("test", group.getName());
    }
}
