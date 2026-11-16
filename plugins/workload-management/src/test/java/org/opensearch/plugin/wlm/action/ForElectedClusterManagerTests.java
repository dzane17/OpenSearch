/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.wlm.MutableWorkloadGroupFragment;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.WorkloadGroupThrottleSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Translating a throttling update for a cluster manager on a build that predates multi-attribute throttling must change
 * only the throttling field: every other field (null meaning "keep") has to reach that cluster manager unchanged.
 */
public class ForElectedClusterManagerTests extends OpenSearchTestCase {

    private static final String GROUP = "g1";

    private ClusterState olderClusterManagerWithGroup() {
        DiscoveryNode olderManager = new DiscoveryNode(
            "old-cm",
            "old-cm",
            buildNewFakeTransportAddress(),
            Map.of(),
            Set.of(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
        WorkloadGroup group = new WorkloadGroup(
            GROUP,
            "_id_g1",
            new MutableWorkloadGroupFragment(
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.builder().put("search.default_search_timeout", "30s").build(),
                Settings.builder().put("username.node_limit", 5).put("username.shared_limit", 10).build(),
                Settings.builder().put("size_per_bucket", 8).build()
            ),
            1690934400000L
        );
        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(olderManager).localNodeId(olderManager.getId()).clusterManagerNodeId(olderManager.getId()))
            .metadata(Metadata.builder().put(group))
            .build();
    }

    private UpdateWorkloadGroupRequest update(String json) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            return UpdateWorkloadGroupRequest.fromXContent(parser, GROUP);
        }
    }

    public void testOnlyThrottlingIsTranslatedEverythingElseIsKept() throws IOException {
        MutableWorkloadGroupFragment sent = TransportUpdateWorkloadGroupAction.forElectedClusterManager(
            update("{\"throttling\":{\"username\":{\"node_limit\":7}}}"),
            olderClusterManagerWithGroup()
        ).getmMutableWorkloadGroupFragment();

        assertNull("search settings untouched by the update stay null (keep), not empty (clear)", sent.getSettings());
        assertNull("queue untouched by the update stays null (keep), not empty (clear)", sent.getQueue());
        assertNull(sent.getResiliencyMode());
        assertEquals(
            Settings.builder().put("attribute", "username").put("node_limit", "7").put("shared_limit", "10").build(),
            sent.getThrottling()
        );
    }

    public void testANoOpThrottlingChangeIsSentAsKeep() throws IOException {
        // Removing a rule the group doesn't have changes nothing; it must not turn into "clear every rule".
        MutableWorkloadGroupFragment sent = TransportUpdateWorkloadGroupAction.forElectedClusterManager(
            update("{\"throttling\":{\"role\":null}}"),
            olderClusterManagerWithGroup()
        ).getmMutableWorkloadGroupFragment();
        assertNull(sent.getThrottling());
        assertNull(sent.getSettings());
        assertNull(sent.getQueue());
    }

    public void testRequestIsSentAsIsToAnUpgradedClusterManager() throws IOException {
        DiscoveryNode newerManager = new DiscoveryNode(
            "new-cm",
            "new-cm",
            buildNewFakeTransportAddress(),
            Map.of(WorkloadGroupThrottleSettings.MULTI_ATTRIBUTE_THROTTLING_NODE_ATTRIBUTE, "true"),
            Set.of(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            Version.CURRENT
        );
        ClusterState state = ClusterState.builder(olderClusterManagerWithGroup())
            .nodes(DiscoveryNodes.builder().add(newerManager).localNodeId(newerManager.getId()).clusterManagerNodeId(newerManager.getId()))
            .build();
        UpdateWorkloadGroupRequest request = update("{\"throttling\":{\"role\":null}}");
        assertSame(request, TransportUpdateWorkloadGroupAction.forElectedClusterManager(request, state));
    }
}
