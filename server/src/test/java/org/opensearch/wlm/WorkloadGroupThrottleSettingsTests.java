/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class WorkloadGroupThrottleSettingsTests extends OpenSearchTestCase {

    public void testBucketKeysAndTheirRule() {
        String userKey = WorkloadGroupThrottleSettings.bucketKey("gid", ThrottleAttribute.USERNAME, "svc:etl");
        assertEquals("gid:username:svc:etl", userKey);
        // A value containing ':' must not confuse the attribute / rule lookup.
        assertEquals("username", WorkloadGroupThrottleSettings.attributeOf(userKey));
        assertEquals("gid:username", WorkloadGroupThrottleSettings.ruleKeyOf(userKey));

        String groupKey = WorkloadGroupThrottleSettings.bucketKey("gid", ThrottleAttribute.GROUP, null);
        assertEquals("gid:group", groupKey);
        assertEquals("group", WorkloadGroupThrottleSettings.attributeOf(groupKey));
        assertEquals("gid:group", WorkloadGroupThrottleSettings.ruleKeyOf(groupKey));
        assertEquals(WorkloadGroupThrottleSettings.ruleKey("gid", "group"), WorkloadGroupThrottleSettings.ruleKeyOf(groupKey));
    }

    public void testRuleLookupReadsEachRulesOwnLimits() {
        Settings throttling = Settings.builder()
            .put("username.node_limit", 5)
            .put("username.shared_limit", 10)
            .put("group.shared_limit", 100)
            .build();
        WorkloadGroupThrottleSettings.Rule username = WorkloadGroupThrottleSettings.rule(throttling, "username");
        assertEquals(5, username.nodeLimit());
        assertEquals(10, username.sharedLimit());
        WorkloadGroupThrottleSettings.Rule group = WorkloadGroupThrottleSettings.rule(throttling, "group");
        assertFalse(group.hasNodeTier());
        assertEquals(100, group.sharedLimit());
        assertNull("no role rule", WorkloadGroupThrottleSettings.rule(throttling, "role"));
    }

    public void testRulesSkipUnknownAttributesAndEmptyRules() {
        // A rule for an attribute this node doesn't know (e.g. stored by a newer build) is ignored rather than failing.
        Settings throttling = Settings.builder().put("index.node_limit", 3).put("role.shared_limit", 4).build();
        List<WorkloadGroupThrottleSettings.Rule> rules = WorkloadGroupThrottleSettings.rules(throttling);
        assertEquals(1, rules.size());
        assertEquals("role", rules.get(0).attribute().key());
        assertFalse(rules.get(0).hasNodeTier());
        assertTrue(rules.get(0).hasSharedTier());
        assertTrue(WorkloadGroupThrottleSettings.rules(Settings.EMPTY).isEmpty());
        assertTrue(WorkloadGroupThrottleSettings.rules(null).isEmpty());
    }

    public void testMergeRemovesWholeRuleOnNullAttribute() {
        Settings existing = Settings.builder().put("username.node_limit", 1).put("role.node_limit", 2).put("role.shared_limit", 3).build();
        Settings merged = WorkloadGroupThrottleSettings.merge(
            existing,
            Settings.builder().putNull("role").put("group.node_limit", 9).build()
        );
        assertEquals(Settings.builder().put("username.node_limit", 1).put("group.node_limit", 9).build(), merged);
        assertEquals(existing, WorkloadGroupThrottleSettings.merge(existing, null));
        assertTrue(WorkloadGroupThrottleSettings.merge(existing, Settings.EMPTY).isEmpty());
    }

    public void testPrincipalParsing() {
        String delimiter = WorkloadGroupTask.WORKLOAD_GROUP_PRINCIPAL_VALUE_DELIMITER;
        String principal = String.join(delimiter, "role|writer", "username|alice", "role|admin", "role|", "garbage", "|x");
        assertEquals("alice", ThrottleAttribute.USERNAME.resolve(principal));
        assertEquals("smallest role", "admin", ThrottleAttribute.ROLE.resolve(principal));
        assertEquals("first role, as on older builds", "writer", ThrottleAttribute.ROLE.resolve(principal, true));
        assertNull(ThrottleAttribute.ROLE.resolve("username|bob"));
        assertNull(ThrottleAttribute.USERNAME.resolve(null));
        assertNull(ThrottleAttribute.GROUP.resolve(principal));
        assertEquals("a|b", ThrottleAttribute.USERNAME.resolve("username|a|b"));
    }

    public void testAttributeKeysAreUsableAsConfigAndBucketKeys() {
        for (ThrottleAttribute attribute : ThrottleAttribute.values()) {
            assertTrue(attribute.key(), attribute.key().matches("[a-z][a-z_]*"));
            assertFalse("must not collide with a rule field", List.of("attribute", "node_limit", "shared_limit").contains(attribute.key()));
            assertSame(attribute, WorkloadGroupThrottleSettings.ATTRIBUTES.get(attribute.key()));
        }
    }

    public void testUnreadableStoredLimitMakesOnlyThatRuleInert() {
        Settings stored = Settings.builder().put("username.node_limit", "abc").put("group.node_limit", 3).build();
        List<WorkloadGroupThrottleSettings.Rule> rules = WorkloadGroupThrottleSettings.rules(stored);
        assertEquals(1, rules.size());
        assertEquals("group", rules.get(0).attribute().key());
        assertNull(WorkloadGroupThrottleSettings.rule(stored, "username"));
    }

    public void testNormalizeStoredKeepsAbsentAsAbsent() {
        assertNull(WorkloadGroupThrottleSettings.normalizeStored(null));
        assertEquals(Settings.EMPTY, WorkloadGroupThrottleSettings.normalizeStored(Settings.EMPTY));
    }

    private static DiscoveryNode node(String name, boolean keyedRules) {
        return new DiscoveryNode(
            name,
            name,
            buildNewFakeTransportAddress(),
            keyedRules ? Map.of(WorkloadGroupThrottleSettings.MULTI_ATTRIBUTE_THROTTLING_NODE_ATTRIBUTE, "true") : Map.of(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
    }

    private static DiscoveryNodes nodes(DiscoveryNode... nodes) {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        for (DiscoveryNode n : nodes) {
            builder.add(n);
        }
        return builder.build();
    }

    private static final Settings ONE_RULE = Settings.builder().put("username.node_limit", 5).put("username.shared_limit", 10).build();
    private static final Settings TWO_RULES = Settings.builder().put("username.node_limit", 5).put("group.shared_limit", 10).build();

    public void testSingleRuleHasAFlatFormThatReadsBackUnchanged() {
        Settings flat = WorkloadGroupThrottleSettings.toLegacyFlat(ONE_RULE);
        assertEquals(Settings.builder().put("attribute", "username").put("node_limit", 5).put("shared_limit", 10).build(), flat);
        assertEquals(ONE_RULE, WorkloadGroupThrottleSettings.normalizeStored(flat));

        Settings groupNodeOnly = Settings.builder().put("group.node_limit", 3).build();
        Settings groupFlat = WorkloadGroupThrottleSettings.toLegacyFlat(groupNodeOnly);
        assertEquals(Settings.builder().put("attribute", "group").put("node_limit", 3).build(), groupFlat);
        assertEquals(groupNodeOnly, WorkloadGroupThrottleSettings.normalizeStored(groupFlat));
    }

    public void testConfigsWithoutAFlatForm() {
        assertNull(WorkloadGroupThrottleSettings.toLegacyFlat(null));
        assertNull(WorkloadGroupThrottleSettings.toLegacyFlat(Settings.EMPTY));
        assertNull("several rules", WorkloadGroupThrottleSettings.toLegacyFlat(TWO_RULES));
        assertNull("unknown attribute", WorkloadGroupThrottleSettings.toLegacyFlat(Settings.builder().put("tenant.node_limit", 1).build()));
        assertNull("unknown field", WorkloadGroupThrottleSettings.toLegacyFlat(Settings.builder().put("username.burst", 1).build()));
        assertNull(
            "clear marker",
            WorkloadGroupThrottleSettings.toLegacyFlat(
                Settings.builder().putNull("username.node_limit").put("username.shared_limit", 1).build()
            )
        );
    }

    public void testSeveralRulesNeedEveryNodeToSupportThem() {
        DiscoveryNodes mixed = nodes(node("new-1", true), node("old-1", false));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> WorkloadGroupThrottleSettings.validateSupportedByCluster(TWO_RULES, mixed)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("[old-1]"));
        // One rule is always fine: it goes to older nodes in the flat form.
        WorkloadGroupThrottleSettings.validateSupportedByCluster(ONE_RULE, mixed);
        WorkloadGroupThrottleSettings.validateSupportedByCluster(TWO_RULES, nodes(node("new-1", true), node("new-2", true)));
        assertFalse(WorkloadGroupThrottleSettings.allSupportMultiAttributeThrottling(mixed));
    }

    public void testAOneRuleConfigWithAnExtraKeyStillNeedsTheKeyedForm() {
        // rules() counts one enforceable rule, but there is no flat form, so older nodes could not read it.
        Settings oneRulePlusUnknown = Settings.builder().put("username.node_limit", 5).put("tenant.node_limit", 3).build();
        assertTrue(WorkloadGroupThrottleSettings.requiresMultiAttributeSupport(oneRulePlusUnknown));
        assertFalse(WorkloadGroupThrottleSettings.requiresMultiAttributeSupport(ONE_RULE));
        assertFalse(WorkloadGroupThrottleSettings.requiresMultiAttributeSupport(Settings.EMPTY));
        expectThrows(
            IllegalArgumentException.class,
            () -> WorkloadGroupThrottleSettings.validateSupportedByCluster(oneRulePlusUnknown, nodes(node("old-1", false)))
        );
    }

    public void testDetectsAnOlderElectedClusterManager() {
        DiscoveryNode olderManager = node("old-cm", false);
        DiscoveryNode newerManager = node("new-cm", true);
        assertTrue(
            WorkloadGroupThrottleSettings.clusterManagerPredatesMultiAttributeThrottling(
                DiscoveryNodes.builder().add(olderManager).add(newerManager).clusterManagerNodeId(olderManager.getId()).build()
            )
        );
        assertFalse(
            WorkloadGroupThrottleSettings.clusterManagerPredatesMultiAttributeThrottling(
                DiscoveryNodes.builder().add(olderManager).add(newerManager).clusterManagerNodeId(newerManager.getId()).build()
            )
        );
    }

    private static Settings flat(String... kv) {
        Settings.Builder b = Settings.builder();
        for (int i = 0; i < kv.length; i += 2) {
            if (kv[i + 1] == null) {
                b.putNull(kv[i]);
            } else {
                b.put(kv[i], kv[i + 1]);
            }
        }
        return b.build();
    }

    public void testKeyedUpdatesAreSentFlatToAnOlderClusterManager() {
        Settings existing = Settings.builder().put("username.node_limit", 5).put("username.shared_limit", 10).build();
        // Change one limit: the whole rule is written explicitly.
        assertEquals(
            flat("attribute", "username", "node_limit", "7", "shared_limit", "10"),
            WorkloadGroupThrottleSettings.toFlatUpdate(Settings.builder().put("username.node_limit", 7).build(), existing)
        );
        // Clear a field.
        assertEquals(
            flat("attribute", "username", "node_limit", null, "shared_limit", "10"),
            WorkloadGroupThrottleSettings.toFlatUpdate(Settings.builder().putNull("username.node_limit").build(), existing)
        );
        // Remove the rule (what the older build would otherwise silently ignore): clear every flat key.
        assertEquals(
            flat("attribute", null, "node_limit", null, "shared_limit", null),
            WorkloadGroupThrottleSettings.toFlatUpdate(Settings.builder().putNull("username").build(), existing)
        );
        // Replace the rule with another attribute's, in one update.
        assertEquals(
            flat("attribute", "role", "node_limit", "3", "shared_limit", null),
            WorkloadGroupThrottleSettings.toFlatUpdate(Settings.builder().putNull("username").put("role.node_limit", 3).build(), existing)
        );
        // Clear all is sent as is; a change that changes nothing is dropped.
        assertEquals(Settings.EMPTY, WorkloadGroupThrottleSettings.toFlatUpdate(Settings.EMPTY, existing));
        assertNull(WorkloadGroupThrottleSettings.toFlatUpdate(Settings.builder().putNull("role").build(), existing));
        assertNull(WorkloadGroupThrottleSettings.toFlatUpdate(null, existing));
        // A second rule can't be expressed (and isn't allowed while older nodes are present).
        expectThrows(
            IllegalArgumentException.class,
            () -> WorkloadGroupThrottleSettings.toFlatUpdate(Settings.builder().put("group.node_limit", 1).build(), existing)
        );
        // A group without throttling gets its first rule.
        assertEquals(
            flat("attribute", "group", "node_limit", null, "shared_limit", "4"),
            WorkloadGroupThrottleSettings.toFlatUpdate(Settings.builder().put("group.shared_limit", 4).build(), Settings.EMPTY)
        );
    }

    public void testFlatUpdateLeavingAnAttributeWithoutLimitsIsRejectedLikeTheOlderBuild() {
        // Older build: clearing the only limit leaves {attribute} with a ceiling of 0, which it rejects.
        IllegalArgumentException cleared = expectThrows(
            IllegalArgumentException.class,
            () -> WorkloadGroupThrottleSettings.mergeUpdate(
                Settings.builder().put("username.node_limit", 5).build(),
                flat("node_limit", null)
            )
        );
        assertTrue(cleared.getMessage(), cleared.getMessage().contains("ceiling"));
        // And an attribute alone on a group without throttling.
        expectThrows(
            IllegalArgumentException.class,
            () -> WorkloadGroupThrottleSettings.mergeUpdate(Settings.EMPTY, flat("attribute", "role"))
        );
        // Clearing every flat key still clears throttling.
        assertEquals(
            Settings.EMPTY,
            WorkloadGroupThrottleSettings.mergeUpdate(
                Settings.builder().put("username.node_limit", 5).build(),
                flat("attribute", null, "node_limit", null, "shared_limit", null)
            )
        );
    }

    public void testVerifyUpdateAppliedCatchesASilentlyIgnoredChange() {
        Settings before = Settings.builder().put("username.node_limit", 5).put("group.node_limit", 9).build();
        Settings removeUsername = Settings.builder().putNull("username").build();
        // An older cluster manager acknowledged the removal but the rule is still there.
        expectThrows(OpenSearchStatusException.class, () -> WorkloadGroupThrottleSettings.verifyUpdateApplied(removeUsername, before));
        WorkloadGroupThrottleSettings.verifyUpdateApplied(removeUsername, Settings.builder().put("group.node_limit", 9).build());

        Settings clearField = Settings.builder().putNull("group.node_limit").build();
        expectThrows(OpenSearchStatusException.class, () -> WorkloadGroupThrottleSettings.verifyUpdateApplied(clearField, before));
        Settings setField = Settings.builder().put("group.node_limit", 3).build();
        expectThrows(OpenSearchStatusException.class, () -> WorkloadGroupThrottleSettings.verifyUpdateApplied(setField, before));
        WorkloadGroupThrottleSettings.verifyUpdateApplied(setField, Settings.builder().put("group.node_limit", 3).build());

        expectThrows(OpenSearchStatusException.class, () -> WorkloadGroupThrottleSettings.verifyUpdateApplied(Settings.EMPTY, before));
        WorkloadGroupThrottleSettings.verifyUpdateApplied(Settings.EMPTY, Settings.EMPTY);
        WorkloadGroupThrottleSettings.verifyUpdateApplied(null, before); // throttling untouched
    }
}
