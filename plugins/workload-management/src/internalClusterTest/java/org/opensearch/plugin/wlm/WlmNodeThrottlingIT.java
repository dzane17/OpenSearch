/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm;

import org.apache.logging.log4j.LogManager;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.plugin.wlm.rule.WorkloadGroupFeatureType;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginsService;
import org.opensearch.rule.RuleAttribute;
import org.opensearch.rule.RuleFrameworkPlugin;
import org.opensearch.rule.RulePersistenceServiceRegistry;
import org.opensearch.rule.RuleRoutingServiceRegistry;
import org.opensearch.rule.action.CreateRuleAction;
import org.opensearch.rule.action.CreateRuleRequest;
import org.opensearch.rule.autotagging.AutoTaggingRegistry;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.rule.autotagging.Rule;
import org.opensearch.script.MockScriptPlugin;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.lookup.LeafFieldsLookup;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.wlm.MutableWorkloadGroupFragment;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.WorkloadGroupThrottleSettings;
import org.opensearch.wlm.WorkloadManagementSettings;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.opensearch.index.query.QueryBuilders.scriptQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThan;

/**
 * End-to-end integration test for per-node WLM request throttling ({@code node_limit}, {@code attribute=group}).
 * <p>
 * The scripted-block plugin holds a search in-flight (occupying a throttle permit) so that a second concurrent
 * search deterministically exceeds the node limit and must be rejected with a 429
 * ({@link OpenSearchRejectedExecutionException}). This exercises the real coordinator admission hook in
 * {@code TransportSearchAction} through auto-tagging, not a mocked service.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class WlmNodeThrottlingIT extends OpenSearchIntegTestCase {

    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private static final String PUT = "PUT";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(WlmAutoTaggingIT.TestWorkloadManagementPlugin.class);
        plugins.add(RuleFrameworkPlugin.class);
        plugins.add(ScriptedBlockPlugin.class);
        return plugins;
    }

    @Before
    public void registerFeatureTypeIfMissingOnAllNodes() {
        // AutoTaggingRegistry is a JVM-static singleton, but each test (Scope.TEST) restarts the cluster and rebuilds
        // the feature type — including its WorkloadGroupFeatureValueValidator, which is bound to that cluster's live
        // ClusterService. Always refresh the registry to the current cluster's feature type; otherwise a later test
        // would validate rules against a previous (dead) cluster's state and fail with "not a valid workload group id".
        AutoTaggingRegistry.featureTypesRegistryMap.remove(WorkloadGroupFeatureType.NAME);
        FeatureType featureType = WlmAutoTaggingIT.TestWorkloadManagementPlugin.featureType;
        AutoTaggingRegistry.registerFeatureType(featureType);

        for (String node : internalCluster().getNodeNames()) {
            RulePersistenceServiceRegistry persistenceRegistry = internalCluster().getInstance(RulePersistenceServiceRegistry.class, node);
            RuleRoutingServiceRegistry routingRegistry = internalCluster().getInstance(RuleRoutingServiceRegistry.class, node);
            try {
                routingRegistry.getRuleRoutingService(featureType);
            } catch (IllegalArgumentException ex) {
                persistenceRegistry.register(featureType, WlmAutoTaggingIT.TestWorkloadManagementPlugin.rulePersistenceService);
                routingRegistry.register(featureType, WlmAutoTaggingIT.TestWorkloadManagementPlugin.ruleRoutingService);
            }
        }
    }

    @After
    public void clearWlmModeSetting() throws Exception {
        Settings.Builder builder = Settings.builder().putNull(WorkloadManagementSettings.WLM_MODE_SETTING.getKey());
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(builder).get());
    }

    public void testSecondConcurrentSearchRejectedWhenNodeLimitReached() throws Exception {
        String workloadGroupId = "wlm_throttle_group";
        String ruleId = "wlm_throttle_rule";
        String indexName = "throttle_index";

        setWlmMode("enabled");

        // Workload group throttled to a single in-flight request per node.
        WorkloadGroup workloadGroup = createThrottledWorkloadGroup("throttle_test_group", workloadGroupId, 1);
        updateWorkloadGroupInClusterState(PUT, workloadGroup);

        FeatureType featureType = AutoTaggingRegistry.getFeatureType(WorkloadGroupFeatureType.NAME);
        createRule(ruleId, "throttle rule", indexName, featureType, workloadGroupId);

        indexDocument(indexName);

        // Rule propagation to the in-memory processing service is asynchronous. Wait until a
        // (non-blocking) search is actually tagged to the throttled group before exercising
        // the concurrency scenario, otherwise the requests are untagged and never throttled.
        assertBusy(() -> {
            int before = getCompletions(workloadGroupId);
            client().prepareSearch(indexName).setQuery(org.opensearch.index.query.QueryBuilders.matchAllQuery()).get();
            int after = getCompletions(workloadGroupId);
            assertTrue("Expected search to be tagged to the throttled workload group", after > before);
        }, 30, TimeUnit.SECONDS);

        List<ScriptedBlockPlugin> plugins = initBlockFactory();

        // First search: blocks in the query phase, holding the single permit.
        ActionFuture<org.opensearch.action.search.SearchResponse> blockedSearch = blockingSearch(indexName).execute();
        awaitForBlock(plugins);

        int throttledBefore = getThrottled(workloadGroupId);

        // Second search while the first is still in-flight: must be rejected (429).
        Throwable rejection = expectThrows(Throwable.class, () -> blockingSearch(indexName).get());
        assertTrue(
            "Expected an OpenSearchRejectedExecutionException in the cause chain but was: " + rejection,
            hasRejectedExecutionCause(rejection)
        );

        // The rejection must be counted in total_throttled.
        assertEquals("total_throttled should increment by exactly one", throttledBefore + 1, getThrottled(workloadGroupId));

        // The rejected request must NOT have entered the request-operations start path, so the in-flight search
        // gauge reflects only the one still-blocked search (not two). This guards against the gauge leak where a
        // throttle rejection increments 'current' via onRequestStart but never reaches onRequestEnd/onRequestFailure.
        assertEquals("in-flight search gauge must exclude the throttle-rejected request", 1L, currentInFlightSearches());

        // Release the block; the first search should complete successfully.
        disableBlocks(plugins);
        assertNotNull(blockedSearch.actionGet(TIMEOUT));

        // Once the blocked search finishes, the gauge must drain back to zero (no leaked in-flight count).
        assertBusy(() -> assertEquals("in-flight search gauge must drain to zero", 0L, currentInFlightSearches()), 30, TimeUnit.SECONDS);
    }

    public void testUsernameThrottlingKeepsPerUserBuckets() throws Exception {
        String workloadGroupId = "wlm_user_throttle_group";
        String ruleId = "wlm_user_throttle_rule";
        String indexName = "user_throttle_index";

        setWlmMode("enabled");

        // Group throttled per-username to a single in-flight request per node (attribute = username).
        WorkloadGroup workloadGroup = createThrottledWorkloadGroup("user_throttle_test_group", workloadGroupId, 1, "username");
        updateWorkloadGroupInClusterState(PUT, workloadGroup);

        FeatureType featureType = AutoTaggingRegistry.getFeatureType(WorkloadGroupFeatureType.NAME);
        // The rule's feature value (the workload group id) is validated against applied cluster state, which the group
        // update above populates asynchronously. Wait until the group is visible in cluster state before creating the
        // rule, otherwise rule creation races the update and fails validation.
        assertBusy(() -> {
            boolean present = client().admin()
                .cluster()
                .prepareState()
                .get()
                .getState()
                .metadata()
                .workloadGroups()
                .containsKey(workloadGroupId);
            assertTrue("workload group not yet applied in cluster state", present);
        }, 30, TimeUnit.SECONDS);
        createRule(ruleId, "user throttle rule", indexName, featureType, workloadGroupId);

        indexDocument(indexName);

        // Wait for rule propagation: a search tagged as alice must reach the group before the concurrency scenario.
        assertBusy(() -> {
            int before = getCompletions(workloadGroupId);
            searchAs("alice", indexName).setQuery(org.opensearch.index.query.QueryBuilders.matchAllQuery()).get();
            int after = getCompletions(workloadGroupId);
            assertTrue("Expected search to be tagged to the throttled workload group", after > before);
        }, 30, TimeUnit.SECONDS);

        List<ScriptedBlockPlugin> plugins = initBlockFactory();

        // alice's first search blocks in the query phase, holding her single per-user permit.
        ActionFuture<org.opensearch.action.search.SearchResponse> aliceBlocked = blockingSearchAs("alice", indexName).execute();
        awaitForBlock(plugins);

        int throttledBefore = getThrottled(workloadGroupId);

        // alice's second concurrent search hits her per-user node_limit -> 429.
        Throwable rejection = expectThrows(Throwable.class, () -> blockingSearchAs("alice", indexName).get());
        assertTrue(
            "Expected an OpenSearchRejectedExecutionException in the cause chain but was: " + rejection,
            hasRejectedExecutionCause(rejection)
        );
        assertEquals("total_throttled should increment by exactly one", throttledBefore + 1, getThrottled(workloadGroupId));

        // bob is a different principal -> a different bucket -> admitted even while alice is at her limit.
        // (bob's search also blocks; we just need it to get past admission, so run it async and then release.)
        ActionFuture<org.opensearch.action.search.SearchResponse> bobBlocked = blockingSearchAs("bob", indexName).execute();
        assertBusy(() -> {
            int blocked = 0;
            for (ScriptedBlockPlugin plugin : plugins) {
                blocked += plugin.hits.get();
            }
            assertThat("bob's search should have been admitted and reached the blocking script", blocked, greaterThan(1));
        }, 30, TimeUnit.SECONDS);
        // bob was admitted, so no additional throttle beyond alice's one rejection.
        assertEquals("bob must not be throttled by alice's bucket", throttledBefore + 1, getThrottled(workloadGroupId));

        // Release the blocks; both alice's and bob's blocked searches complete successfully.
        disableBlocks(plugins);
        assertNotNull(aliceBlocked.actionGet(TIMEOUT));
        assertNotNull(bobBlocked.actionGet(TIMEOUT));
    }

    // Helpers

    private static boolean hasRejectedExecutionCause(Throwable t) {
        for (Throwable cur = t; cur != null; cur = cur.getCause()) {
            if (cur instanceof OpenSearchRejectedExecutionException) {
                return true;
            }
            if (cur.getCause() == cur) {
                break;
            }
        }
        return false;
    }

    private int getCompletions(String groupId) throws Exception {
        org.opensearch.action.admin.cluster.wlm.WlmStatsRequest request = new org.opensearch.action.admin.cluster.wlm.WlmStatsRequest(
            null,
            new java.util.HashSet<>(Collections.singletonList(groupId)),
            null
        );
        org.opensearch.action.admin.cluster.wlm.WlmStatsResponse response = client().execute(
            org.opensearch.action.admin.cluster.wlm.WlmStatsAction.INSTANCE,
            request
        ).get();
        return extractStatField(response.toString(), groupId, "total_completions");
    }

    private int getThrottled(String groupId) throws Exception {
        org.opensearch.action.admin.cluster.wlm.WlmStatsRequest request = new org.opensearch.action.admin.cluster.wlm.WlmStatsRequest(
            null,
            new java.util.HashSet<>(Collections.singletonList(groupId)),
            null
        );
        org.opensearch.action.admin.cluster.wlm.WlmStatsResponse response = client().execute(
            org.opensearch.action.admin.cluster.wlm.WlmStatsAction.INSTANCE,
            request
        ).get();
        return extractStatField(response.toString(), groupId, "total_throttled");
    }

    /**
     * Sums the current in-flight search gauge ({@link org.opensearch.action.search.SearchRequestStats#getTookCurrent()})
     * across all data nodes. This is the counter incremented in {@code onRequestStart} and decremented in
     * {@code onRequestEnd}/{@code onRequestFailure}; a throttle rejection must never touch it.
     */
    private long currentInFlightSearches() {
        long total = 0;
        for (org.opensearch.action.search.SearchRequestStats stats : internalCluster().getDataNodeInstances(
            org.opensearch.action.search.SearchRequestStats.class
        )) {
            total += stats.getTookCurrent();
        }
        return total;
    }

    private int extractStatField(String responseBody, String workloadGroupId, String fieldName) {
        int total = 0;
        String groupKey = "\"" + workloadGroupId + "\"";
        String field = "\"" + fieldName + "\"";
        int index = 0;
        while ((index = responseBody.indexOf(groupKey, index)) != -1) {
            int groupStart = responseBody.indexOf("{", index);
            int fieldIndex = responseBody.indexOf(field, groupStart);
            if (fieldIndex == -1) break;
            int colonIndex = responseBody.indexOf(":", fieldIndex);
            int commaIndex = responseBody.indexOf(",", colonIndex);
            String numberStr = responseBody.substring(colonIndex + 1, commaIndex).trim();
            total += Integer.parseInt(numberStr);
            index = commaIndex;
        }
        return total;
    }

    private SearchRequestBuilder blockingSearch(String indexName) {
        return client().prepareSearch(indexName)
            .setQuery(scriptQuery(new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap())));
    }

    // Injects the resolved principal directly into the propagated thread-context header (WORKLOAD_GROUP_PRINCIPAL_HEADER).
    // In production the WLM auto-tagging filter sets this header from the security plugin's principal extractor; here we
    // stand in for that so the core username/role bucket-keying can be exercised without a real security plugin.
    private org.opensearch.transport.client.Client clientAs(String username) {
        return client().filterWithHeader(
            Map.of(org.opensearch.wlm.WorkloadGroupTask.WORKLOAD_GROUP_PRINCIPAL_HEADER, "username|" + username)
        );
    }

    private SearchRequestBuilder searchAs(String username, String indexName) {
        return clientAs(username).prepareSearch(indexName);
    }

    private SearchRequestBuilder blockingSearchAs(String username, String indexName) {
        return clientAs(username).prepareSearch(indexName)
            .setQuery(scriptQuery(new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap())));
    }

    private List<ScriptedBlockPlugin> initBlockFactory() {
        List<ScriptedBlockPlugin> plugins = new ArrayList<>();
        for (PluginsService pluginsService : internalCluster().getDataNodeInstances(PluginsService.class)) {
            plugins.addAll(pluginsService.filterPlugins(ScriptedBlockPlugin.class));
        }
        for (ScriptedBlockPlugin plugin : plugins) {
            plugin.reset();
            plugin.enableBlock();
        }
        return plugins;
    }

    private void awaitForBlock(List<ScriptedBlockPlugin> plugins) throws Exception {
        assertBusy(() -> {
            int blocked = 0;
            for (ScriptedBlockPlugin plugin : plugins) {
                blocked += plugin.hits.get();
            }
            assertThat(blocked, greaterThan(0));
        });
    }

    private void disableBlocks(List<ScriptedBlockPlugin> plugins) {
        for (ScriptedBlockPlugin plugin : plugins) {
            plugin.disableBlock();
        }
    }

    private void createRule(String ruleId, String ruleName, String indexPattern, FeatureType featureType, String workloadGroupId)
        throws Exception {
        Rule rule = new Rule(
            ruleId,
            ruleName,
            Map.of(RuleAttribute.INDEX_PATTERN, Set.of(indexPattern)),
            featureType,
            workloadGroupId,
            Instant.now().toString()
        );
        client().execute(CreateRuleAction.INSTANCE, new CreateRuleRequest(rule)).get();
    }

    private void setWlmMode(String mode) throws Exception {
        Settings.Builder settings = Settings.builder().put("wlm.workload_group.mode", mode);
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest().persistentSettings(settings);
        client().admin().cluster().updateSettings(request).get();
    }

    private WorkloadGroup createThrottledWorkloadGroup(String name, String id, int nodeLimit) {
        return createThrottledWorkloadGroup(name, id, nodeLimit, "group");
    }

    private WorkloadGroup createThrottledWorkloadGroup(String name, String id, int nodeLimit, String attribute) {
        Settings throttling = Settings.builder()
            .put(WorkloadGroupThrottleSettings.ATTRIBUTE.getKey(), attribute)
            .put(WorkloadGroupThrottleSettings.NODE_LIMIT.getKey(), nodeLimit)
            .build();
        return new WorkloadGroup(
            name,
            id,
            new MutableWorkloadGroupFragment(
                MutableWorkloadGroupFragment.ResiliencyMode.SOFT,
                Map.of(ResourceType.CPU, 0.9, ResourceType.MEMORY, 0.9),
                Settings.EMPTY,
                throttling
            ),
            Instant.now().getMillis()
        );
    }

    private void indexDocument(String indexName) {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
        );
        IndexResponse response = client().prepareIndex(indexName)
            .setId("1")
            .setSource(Map.of("field", "value"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        assertEquals(org.opensearch.action.DocWriteResponse.Result.CREATED, response.getResult());
    }

    private void updateWorkloadGroupInClusterState(String method, WorkloadGroup workloadGroup) throws InterruptedException {
        WlmAutoTaggingIT.ExceptionCatchingListener listener = new WlmAutoTaggingIT.ExceptionCatchingListener();
        client().execute(
            WlmAutoTaggingIT.TestClusterUpdateTransportAction.ACTION,
            new WlmAutoTaggingIT.TestClusterUpdateRequest(workloadGroup, method),
            listener
        );
        boolean completed = listener.getLatch().await(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
        assertTrue("cluster-state update did not complete in time", completed);
        if (listener.getException() != null) {
            throw new AssertionError("cluster-state update failed", listener.getException());
        }
    }

    /**
     * Test script plugin that blocks during the query phase until released, keeping a search in-flight.
     */
    public static class ScriptedBlockPlugin extends MockScriptPlugin {
        static final String SCRIPT_NAME = "search_block";

        private final AtomicInteger hits = new AtomicInteger();
        private final AtomicBoolean shouldBlock = new AtomicBoolean(true);

        public void reset() {
            hits.set(0);
        }

        public void disableBlock() {
            shouldBlock.set(false);
        }

        public void enableBlock() {
            shouldBlock.set(true);
        }

        @Override
        public Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap(SCRIPT_NAME, params -> {
                LeafFieldsLookup fieldsLookup = (LeafFieldsLookup) params.get("_fields");
                LogManager.getLogger(WlmNodeThrottlingIT.class).info("Blocking on the document {}", fieldsLookup.get("_id"));
                hits.incrementAndGet();
                try {
                    assertBusy(() -> assertFalse(shouldBlock.get()));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return true;
            });
        }
    }
}
