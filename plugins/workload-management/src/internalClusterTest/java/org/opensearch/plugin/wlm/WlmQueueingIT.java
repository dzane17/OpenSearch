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
import org.opensearch.action.admin.cluster.wlm.WlmStatsAction;
import org.opensearch.action.admin.cluster.wlm.WlmStatsRequest;
import org.opensearch.action.admin.cluster.wlm.WlmStatsResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
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
import org.opensearch.wlm.WorkloadGroupQueueSettings;
import org.opensearch.wlm.WorkloadGroupThrottleSettings;
import org.opensearch.wlm.WorkloadManagementSettings;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
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
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * End-to-end integration test for WLM request QUEUEING on top of node-level throttling. With {@code node_limit=1} and
 * {@code queue.size_per_bucket>0}, a second concurrent search that would be rejected by the throttle is instead PARKED in the
 * queue (holding no thread) and admitted once the first search completes and frees the permit — exercising the real
 * coordinator admission + node-completion drain path, not a mocked service.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class WlmQueueingIT extends OpenSearchIntegTestCase {

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

    public void testSecondConcurrentSearchQueuedThenAdmitted() throws Exception {
        String workloadGroupId = "wlm_queue_group";
        String ruleId = "wlm_queue_rule";
        String indexName = "queue_index";

        setWlmMode("enabled");

        // node_limit=1 with queueing enabled (size 5): the 2nd concurrent search parks instead of 429ing.
        WorkloadGroup workloadGroup = createQueueingWorkloadGroup("queue_test_group", workloadGroupId, 1, 5);
        updateWorkloadGroupInClusterState(PUT, workloadGroup);

        FeatureType featureType = AutoTaggingRegistry.getFeatureType(WorkloadGroupFeatureType.NAME);
        createRule(ruleId, "queue rule", indexName, featureType, workloadGroupId);

        indexDocument(indexName);

        // Wait for rule propagation: a (non-blocking) search must be tagged to the group before the scenario.
        assertBusy(() -> {
            int before = getStat(workloadGroupId, "total_completions");
            client().prepareSearch(indexName).setQuery(org.opensearch.index.query.QueryBuilders.matchAllQuery()).get();
            int after = getStat(workloadGroupId, "total_completions");
            assertTrue("Expected search to be tagged to the workload group", after > before);
        }, 30, TimeUnit.SECONDS);

        List<ScriptedBlockPlugin> plugins = initBlockFactory();

        // First search blocks in the query phase, holding the single permit.
        ActionFuture<SearchResponse> firstBlocked = blockingSearch(indexName).execute();
        awaitForBlock(plugins);

        int queuedBefore = getStat(workloadGroupId, "total_queued");
        int throttledBefore = getStat(workloadGroupId, "total_throttled");

        // Second search while the first is in-flight: it must be QUEUED (parked), not rejected. Run it async and assert
        // it gets parked (total_queued increments) and does NOT fail with a 429.
        ActionFuture<SearchResponse> secondQueued = blockingSearch(indexName).execute();
        assertBusy(() -> {
            assertEquals("second search should be parked in the queue", queuedBefore + 1, getStat(workloadGroupId, "total_queued"));
        }, 30, TimeUnit.SECONDS);
        // It was queued, not throttle-rejected.
        assertEquals("a queued request must not be counted as throttled", throttledBefore, getStat(workloadGroupId, "total_throttled"));
        // queued_current reflects the one parked request.
        assertEquals("one request should currently be queued", 1, getStat(workloadGroupId, "queued_current"));

        // Release the block. The first search completes, frees the permit, and the node-completion drain admits the
        // parked second search — which then also runs (and completes, since blocks are now disabled).
        disableBlocks(plugins);
        assertNotNull(firstBlocked.actionGet(TIMEOUT));
        assertNotNull("the queued search must be admitted and complete once a permit frees", secondQueued.actionGet(TIMEOUT));

        // The queue drains back to empty.
        assertBusy(() -> assertEquals("queue must drain to empty", 0, getStat(workloadGroupId, "queued_current")), 30, TimeUnit.SECONDS);

        // The admitted-from-queue request contributed to the queue-wait aggregate: exactly one recorded wait. The
        // magnitude is not asserted (the test releases the block as soon as the park is observed, so the actual wait
        // can be sub-millisecond; the arithmetic of sum/mean/max is covered deterministically in unit tests). Total and
        // max must be internally consistent: max <= total when there is a single sample, and both non-negative.
        int waitCount = getStat(workloadGroupId, "queue_wait_count");
        int totalWait = getStat(workloadGroupId, "total_queue_wait_millis");
        int maxWait = getStat(workloadGroupId, "max_queue_wait_millis");
        assertEquals("one admitted request should have a recorded queue wait", 1, waitCount);
        assertThat("total queue wait must be non-negative", totalWait, greaterThanOrEqualTo(0));
        assertThat("max queue wait must be non-negative", maxWait, greaterThanOrEqualTo(0));
        assertEquals("with a single sample, max equals total", totalWait, maxWait);
    }

    public void testQueueFullRejectsWith429() throws Exception {
        String workloadGroupId = "wlm_queuefull_group";
        String ruleId = "wlm_queuefull_rule";
        String indexName = "queuefull_index";

        setWlmMode("enabled");

        // node_limit=1, queue.size_per_bucket=1: 1 running + 1 queued is the ceiling; a 3rd concurrent search is rejected.
        WorkloadGroup workloadGroup = createQueueingWorkloadGroup("queuefull_test_group", workloadGroupId, 1, 1);
        updateWorkloadGroupInClusterState(PUT, workloadGroup);

        FeatureType featureType = AutoTaggingRegistry.getFeatureType(WorkloadGroupFeatureType.NAME);
        createRule(ruleId, "queuefull rule", indexName, featureType, workloadGroupId);

        indexDocument(indexName);

        assertBusy(() -> {
            int before = getStat(workloadGroupId, "total_completions");
            client().prepareSearch(indexName).setQuery(org.opensearch.index.query.QueryBuilders.matchAllQuery()).get();
            int after = getStat(workloadGroupId, "total_completions");
            assertTrue("Expected search to be tagged to the workload group", after > before);
        }, 30, TimeUnit.SECONDS);

        List<ScriptedBlockPlugin> plugins = initBlockFactory();

        // First search blocks, holding the permit.
        ActionFuture<SearchResponse> firstBlocked = blockingSearch(indexName).execute();
        awaitForBlock(plugins);

        // Second search parks (fills the single queue slot).
        ActionFuture<SearchResponse> secondQueued = blockingSearch(indexName).execute();
        assertBusy(
            () -> assertEquals("second search should be parked", 1, getStat(workloadGroupId, "queued_current")),
            30,
            TimeUnit.SECONDS
        );

        int queueRejectionsBefore = getStat(workloadGroupId, "total_queue_rejections");

        // Third concurrent search: permit taken, queue full -> 429.
        Throwable rejection = expectThrows(Throwable.class, () -> blockingSearch(indexName).get());
        assertTrue(
            "Expected an OpenSearchRejectedExecutionException in the cause chain but was: " + rejection,
            hasRejectedExecutionCause(rejection)
        );
        assertEquals(
            "queue-full rejection should increment total_queue_rejections",
            queueRejectionsBefore + 1,
            getStat(workloadGroupId, "total_queue_rejections")
        );

        // Release; the first and the queued second both complete.
        disableBlocks(plugins);
        assertNotNull(firstBlocked.actionGet(TIMEOUT));
        assertNotNull(secondQueued.actionGet(TIMEOUT));
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

    private int getStat(String groupId, String fieldName) throws Exception {
        WlmStatsRequest request = new WlmStatsRequest(null, new HashSet<>(Collections.singletonList(groupId)), null);
        WlmStatsResponse response = client().execute(WlmStatsAction.INSTANCE, request).get();
        return extractStatField(response.toString(), groupId, fieldName);
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
            int braceIndex = responseBody.indexOf("}", colonIndex);
            int end = (commaIndex == -1 || (braceIndex != -1 && braceIndex < commaIndex)) ? braceIndex : commaIndex;
            String numberStr = responseBody.substring(colonIndex + 1, end).trim();
            total += Integer.parseInt(numberStr);
            index = end;
        }
        return total;
    }

    private SearchRequestBuilder blockingSearch(String indexName) {
        return client().prepareSearch(indexName)
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

    private WorkloadGroup createQueueingWorkloadGroup(String name, String id, int nodeLimit, int queueSizePerBucket) {
        Settings throttling = Settings.builder()
            .put(WorkloadGroupThrottleSettings.ATTRIBUTE.getKey(), "group")
            .put(WorkloadGroupThrottleSettings.NODE_LIMIT.getKey(), nodeLimit)
            .build();
        Settings queue = Settings.builder().put(WorkloadGroupQueueSettings.SIZE_PER_BUCKET.getKey(), queueSizePerBucket).build();
        return new WorkloadGroup(
            name,
            id,
            new MutableWorkloadGroupFragment(
                MutableWorkloadGroupFragment.ResiliencyMode.SOFT,
                Map.of(ResourceType.CPU, 0.9, ResourceType.MEMORY, 0.9),
                Settings.EMPTY,
                throttling,
                queue
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
                LogManager.getLogger(WlmQueueingIT.class).info("Blocking on the document {}", fieldsLookup.get("_id"));
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
