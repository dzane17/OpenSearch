/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

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
}
