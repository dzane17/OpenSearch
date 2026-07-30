/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ThrottleOwnerSelectorTests extends OpenSearchTestCase {

    private DiscoveryNode dataNode(String id, Version version) {
        return new DiscoveryNode(
            "name_" + id,
            id,
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            version
        );
    }

    private DiscoveryNode managerOnlyNode(String id) {
        return new DiscoveryNode(
            "name_" + id,
            id,
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            Version.CURRENT
        );
    }

    private DiscoveryNodes nodesOf(DiscoveryNode... nodes) {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        for (DiscoveryNode node : nodes) {
            builder.add(node);
        }
        builder.localNodeId(nodes[0].getId());
        return builder.build();
    }

    public void testEmptyRingWhenNoDataNodes() {
        ThrottleOwnerSelector selector = ThrottleOwnerSelector.fromDiscoveryNodes(nodesOf(managerOnlyNode("m1")));
        assertTrue(selector.isEmpty());
        assertFalse("empty ring must yield no owner (fail open)", selector.ownerFor("group-a:group").isPresent());
    }

    public void testClusterManagerOnlyNodesExcluded() {
        ThrottleOwnerSelector selector = ThrottleOwnerSelector.fromDiscoveryNodes(
            nodesOf(dataNode("d1", Version.CURRENT), managerOnlyNode("m1"))
        );
        // Only the data node is eligible; the manager-only node must never be an owner.
        for (int i = 0; i < 200; i++) {
            assertEquals("d1", selector.ownerFor("bucket-" + i).orElseThrow().getId());
        }
    }

    public void testOldVersionNodesExcludedFromRing() {
        DiscoveryNode newNode = dataNode("new", Version.CURRENT);
        DiscoveryNode oldNode = dataNode("old", Version.V_3_6_0); // below MIN_OWNER_VERSION
        ThrottleOwnerSelector selector = ThrottleOwnerSelector.fromDiscoveryNodes(nodesOf(newNode, oldNode));
        assertEquals(1, selector.eligibleNodeSet().size());
        for (int i = 0; i < 200; i++) {
            assertEquals("only the upgraded node may own buckets", "new", selector.ownerFor("bucket-" + i).orElseThrow().getId());
        }
    }

    public void testDeterministicForSameNodeSet() {
        DiscoveryNodes nodes = nodesOf(dataNode("a", Version.CURRENT), dataNode("b", Version.CURRENT), dataNode("c", Version.CURRENT));
        ThrottleOwnerSelector s1 = ThrottleOwnerSelector.fromDiscoveryNodes(nodes);
        ThrottleOwnerSelector s2 = ThrottleOwnerSelector.fromDiscoveryNodes(nodes);
        for (int i = 0; i < 500; i++) {
            String key = "bucket-" + i;
            assertEquals(s1.ownerFor(key).orElseThrow().getId(), s2.ownerFor(key).orElseThrow().getId());
        }
    }

    public void testDistributionAcrossNodesIsReasonablyBalanced() {
        ThrottleOwnerSelector selector = ThrottleOwnerSelector.fromDiscoveryNodes(
            nodesOf(dataNode("a", Version.CURRENT), dataNode("b", Version.CURRENT), dataNode("c", Version.CURRENT))
        );
        Map<String, Integer> counts = new HashMap<>();
        int n = 3000;
        for (int i = 0; i < n; i++) {
            String owner = selector.ownerFor("bucket-" + i).orElseThrow().getId();
            counts.merge(owner, 1, Integer::sum);
        }
        // With 128 virtual nodes each, every physical node should get a non-trivial share (well above 10%).
        assertEquals(3, counts.size());
        for (int c : counts.values()) {
            assertTrue("each node should own a meaningful share, saw " + counts, c > n / 10);
        }
    }

    public void testRebalanceOnlyRemapsSmallFraction() {
        DiscoveryNode a = dataNode("a", Version.CURRENT);
        DiscoveryNode b = dataNode("b", Version.CURRENT);
        DiscoveryNode c = dataNode("c", Version.CURRENT);
        DiscoveryNode d = dataNode("d", Version.CURRENT);
        ThrottleOwnerSelector before = ThrottleOwnerSelector.fromDiscoveryNodes(nodesOf(a, b, c, d));
        ThrottleOwnerSelector after = ThrottleOwnerSelector.fromDiscoveryNodes(nodesOf(a, b, c)); // d leaves

        int n = 5000;
        int remapped = 0;
        for (int i = 0; i < n; i++) {
            String key = "bucket-" + i;
            String ownerBefore = before.ownerFor(key).orElseThrow().getId();
            String ownerAfter = after.ownerFor(key).orElseThrow().getId();
            if (ownerBefore.equals(ownerAfter) == false) {
                remapped++;
            }
        }
        // Removing 1 of 4 nodes should remap roughly 1/4 of keys. Allow generous slack, but it must be far from "all".
        double fraction = (double) remapped / n;
        assertTrue("remap fraction should be well under half, was " + fraction, fraction < 0.45);
        // Keys not owned by the departed node 'd' should mostly keep their owner.
        for (int i = 0; i < n; i++) {
            String key = "bucket-" + i;
            if (before.ownerFor(key).orElseThrow().getId().equals("d") == false) {
                assertEquals(
                    "survivor-owned keys must not move",
                    before.ownerFor(key).orElseThrow().getId(),
                    after.ownerFor(key).orElseThrow().getId()
                );
            }
        }
    }
}
