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
import org.opensearch.wlm.stats.WlmStats;
import org.opensearch.wlm.stats.WorkloadGroupStats.WorkloadGroupStatsHolder;
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
import java.util.function.ToLongFunction;

import static org.opensearch.index.query.QueryBuilders.scriptQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
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
            long before = getCompletions(workloadGroupId);
            client().prepareSearch(indexName).setQuery(org.opensearch.index.query.QueryBuilders.matchAllQuery()).get();
            long after = getCompletions(workloadGroupId);
            assertTrue("Expected search to be tagged to the workload group", after > before);
        }, 30, TimeUnit.SECONDS);

        List<ScriptedBlockPlugin> plugins = initBlockFactory();

        // First search blocks in the query phase, holding the single permit.
        ActionFuture<SearchResponse> firstBlocked = blockingSearch(indexName).execute();
        awaitForBlock(plugins);

        long queuedBefore = getTotalQueued(workloadGroupId);
        long throttledBefore = getThrottled(workloadGroupId);

        // Second search while the first is in-flight: it must be QUEUED (parked), not rejected. Run it async and wait for
        // the park to show up as live depth — total_queued is deliberately NOT the signal here, because it counts a wait
        // when the wait ENDS (on admission), so while the request is still parked it has not moved yet.
        ActionFuture<SearchResponse> secondQueued = blockingSearch(indexName).execute();
        assertBusy(
            () -> assertEquals("second search should be parked in the queue", 1, getQueuedCurrent(workloadGroupId)),
            30,
            TimeUnit.SECONDS
        );
        // It was queued, not throttle-rejected.
        assertEquals("a queued request must not be counted as throttled", throttledBefore, getThrottled(workloadGroupId));
        assertEquals(
            "a still-parked request has not finished waiting, so it is not counted yet",
            queuedBefore,
            getTotalQueued(workloadGroupId)
        );

        // Release the block. The first search completes, frees the permit, and the node-completion drain admits the
        // parked second search — which then also runs (and completes, since blocks are now disabled).
        disableBlocks(plugins);
        assertNotNull(firstBlocked.actionGet(TIMEOUT));
        assertNotNull("the queued search must be admitted and complete once a permit frees", secondQueued.actionGet(TIMEOUT));

        // The queue drains back to empty.
        assertBusy(() -> assertEquals("queue must drain to empty", 0, getQueuedCurrent(workloadGroupId)), 30, TimeUnit.SECONDS);

        // NOW the wait has ended, so total_queued counts it — one genuinely-waiting request, admitted by the node-completion
        // drain. There is no separate wait count: total_queued IS the denominator, written by the same call as the sum.
        // The magnitude is not asserted (the test releases the block as soon as the park is observed, so the actual wait
        // can be sub-millisecond; the arithmetic of sum/mean/max is covered deterministically in unit tests). Total and
        // max must be internally consistent: with a single sample they are equal, and both non-negative.
        long totalWait = getTotalQueueWaitMillis(workloadGroupId);
        long maxWait = getMaxQueueWaitMillis(workloadGroupId);
        assertEquals("exactly one request should have waited", queuedBefore + 1, getTotalQueued(workloadGroupId));
        assertThat("total queue wait must be non-negative", totalWait, greaterThanOrEqualTo(0L));
        assertThat("max queue wait must be non-negative", maxWait, greaterThanOrEqualTo(0L));
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
            long before = getCompletions(workloadGroupId);
            client().prepareSearch(indexName).setQuery(org.opensearch.index.query.QueryBuilders.matchAllQuery()).get();
            long after = getCompletions(workloadGroupId);
            assertTrue("Expected search to be tagged to the workload group", after > before);
        }, 30, TimeUnit.SECONDS);

        List<ScriptedBlockPlugin> plugins = initBlockFactory();

        // First search blocks, holding the permit.
        ActionFuture<SearchResponse> firstBlocked = blockingSearch(indexName).execute();
        awaitForBlock(plugins);

        // Second search parks (fills the single queue slot).
        ActionFuture<SearchResponse> secondQueued = blockingSearch(indexName).execute();
        assertBusy(() -> assertEquals("second search should be parked", 1, getQueuedCurrent(workloadGroupId)), 30, TimeUnit.SECONDS);

        long queueRejectionsBefore = getQueueRejections(workloadGroupId);

        // Third concurrent search: permit taken, queue full -> 429.
        Throwable rejection = expectThrows(Throwable.class, () -> blockingSearch(indexName).get());
        assertTrue(
            "Expected an OpenSearchRejectedExecutionException in the cause chain but was: " + rejection,
            hasRejectedExecutionCause(rejection)
        );
        assertEquals(
            "queue-full rejection should increment total_queue_rejections",
            queueRejectionsBefore + 1,
            getQueueRejections(workloadGroupId)
        );

        // Release; the first and the queued second both complete.
        disableBlocks(plugins);
        assertNotNull(firstBlocked.actionGet(TIMEOUT));
        assertNotNull(secondQueued.actionGet(TIMEOUT));
    }

    public void testGlobalDisableImmediatelyDrainsQueuedSearch() throws Exception {
        String workloadGroupId = "wlm_global_drain_group";
        String ruleId = "wlm_global_drain_rule";
        String indexName = "global_drain_index";

        setWlmMode("enabled");
        WorkloadGroup workloadGroup = createQueueingWorkloadGroup("global_drain_group", workloadGroupId, 1, 5);
        updateWorkloadGroupInClusterState(PUT, workloadGroup);
        FeatureType featureType = AutoTaggingRegistry.getFeatureType(WorkloadGroupFeatureType.NAME);
        createRule(ruleId, "global drain rule", indexName, featureType, workloadGroupId);
        indexDocument(indexName);

        assertBusy(() -> {
            long before = getCompletions(workloadGroupId);
            client().prepareSearch(indexName).setQuery(org.opensearch.index.query.QueryBuilders.matchAllQuery()).get();
            assertTrue("Expected search to be tagged to the workload group", getCompletions(workloadGroupId) > before);
        }, 30, TimeUnit.SECONDS);

        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        ActionFuture<SearchResponse> firstBlocked = blockingSearch(indexName).execute();
        awaitBlockedCount(plugins, 1);

        long totalQueuedBefore = getTotalQueued(workloadGroupId);
        ActionFuture<SearchResponse> secondQueued = blockingSearch(indexName).execute();
        assertBusy(() -> assertEquals("second search should be parked", 1, getQueuedCurrent(workloadGroupId)), 30, TimeUnit.SECONDS);

        // The setting update must submit an immediate queue drain. The first search remains blocked and still owns the
        // only node permit, so the second can reach the script before that permit is released only if it was run
        // untracked by the global ENABLED -> DISABLED callback.
        setWlmMode("disabled");
        awaitBlockedCount(plugins, 2);
        assertBusy(() -> assertEquals("global disable must empty the queue", 0, getQueuedCurrent(workloadGroupId)));
        assertBusy(() -> assertEquals(totalQueuedBefore + 1, getTotalQueued(workloadGroupId)));

        disableBlocks(plugins);
        assertNotNull(firstBlocked.actionGet(TIMEOUT));
        assertNotNull(secondQueued.actionGet(TIMEOUT));
    }

    public void testThrottleTierSwitchImmediatelyDrainsQueuedSearch() throws Exception {
        String workloadGroupId = "wlm_tier_switch_drain_group";
        String ruleId = "wlm_tier_switch_drain_rule";
        String indexName = "tier_switch_drain_index";

        setWlmMode("enabled");
        WorkloadGroup nodeOnly = createQueueingWorkloadGroup("tier_switch_drain_group", workloadGroupId, 1, 5);
        updateWorkloadGroupInClusterState(PUT, nodeOnly);
        FeatureType featureType = AutoTaggingRegistry.getFeatureType(WorkloadGroupFeatureType.NAME);
        createRule(ruleId, "tier switch drain rule", indexName, featureType, workloadGroupId);
        indexDocument(indexName);

        assertBusy(() -> {
            long before = getCompletions(workloadGroupId);
            client().prepareSearch(indexName).setQuery(org.opensearch.index.query.QueryBuilders.matchAllQuery()).get();
            assertTrue("Expected search to be tagged to the workload group", getCompletions(workloadGroupId) > before);
        }, 30, TimeUnit.SECONDS);

        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        ActionFuture<SearchResponse> firstBlocked = blockingSearch(indexName).execute();
        awaitBlockedCount(plugins, 1);

        long totalQueuedBefore = getTotalQueued(workloadGroupId);
        ActionFuture<SearchResponse> secondQueued = blockingSearch(indexName).execute();
        assertBusy(() -> assertEquals("second search should be parked", 1, getQueuedCurrent(workloadGroupId)), 30, TimeUnit.SECONDS);

        Settings sharedOnlyThrottling = Settings.builder()
            .put(WorkloadGroupThrottleSettings.ATTRIBUTE.getKey(), "group")
            .put(WorkloadGroupThrottleSettings.SHARED_LIMIT.getKey(), 1)
            .build();
        WorkloadGroup sharedOnly = createQueueingWorkloadGroup("tier_switch_drain_group", workloadGroupId, sharedOnlyThrottling, 5);
        updateWorkloadGroupInClusterState(PUT, sharedOnly);

        // The old request was queued against the node tier and was never registered with the shared owner. The first
        // search still holds the old node permit, so reaching two blocked scripts proves the cluster-change callback
        // drained that old backlog untracked instead of waiting on either tier.
        awaitBlockedCount(plugins, 2);
        assertBusy(() -> assertEquals("tier switch must empty the old queue", 0, getQueuedCurrent(workloadGroupId)));
        assertBusy(() -> assertEquals(totalQueuedBefore + 1, getTotalQueued(workloadGroupId)));

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

    // Sums a per-group stat across all nodes using the typed WlmStats response (no brittle string parsing).
    private long sumAcrossNodes(String groupId, ToLongFunction<WorkloadGroupStatsHolder> extractor) throws Exception {
        WlmStatsRequest request = new WlmStatsRequest(null, new HashSet<>(Collections.singletonList(groupId)), null);
        WlmStatsResponse response = client().execute(WlmStatsAction.INSTANCE, request).get();
        long total = 0;
        for (WlmStats nodeStats : response.getNodes()) {
            WorkloadGroupStatsHolder holder = nodeStats.getWorkloadGroupStats().getStats().get(groupId);
            if (holder != null) {
                total += extractor.applyAsLong(holder);
            }
        }
        return total;
    }

    private long getCompletions(String groupId) throws Exception {
        return sumAcrossNodes(groupId, WorkloadGroupStatsHolder::getCompletions);
    }

    private long getThrottled(String groupId) throws Exception {
        return sumAcrossNodes(groupId, WorkloadGroupStatsHolder::getThrottled);
    }

    private long getTotalQueued(String groupId) throws Exception {
        return sumAcrossNodes(groupId, WorkloadGroupStatsHolder::getQueued);
    }

    private long getQueuedCurrent(String groupId) throws Exception {
        return sumAcrossNodes(groupId, WorkloadGroupStatsHolder::getQueuedCurrent);
    }

    private long getQueueRejections(String groupId) throws Exception {
        return sumAcrossNodes(groupId, WorkloadGroupStatsHolder::getQueueRejections);
    }

    private long getTotalQueueWaitMillis(String groupId) throws Exception {
        return sumAcrossNodes(groupId, WorkloadGroupStatsHolder::getTotalQueueWaitMillis);
    }

    private long getMaxQueueWaitMillis(String groupId) throws Exception {
        return sumAcrossNodes(groupId, WorkloadGroupStatsHolder::getMaxQueueWaitMillis);
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
        awaitBlockedCount(plugins, 1);
    }

    private void awaitBlockedCount(List<ScriptedBlockPlugin> plugins, int expected) throws Exception {
        assertBusy(() -> {
            int blocked = 0;
            for (ScriptedBlockPlugin plugin : plugins) {
                blocked += plugin.hits.get();
            }
            assertThat("expected searches to reach the blocking script", blocked, greaterThanOrEqualTo(expected));
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
        return createQueueingWorkloadGroup(name, id, throttling, queueSizePerBucket);
    }

    private WorkloadGroup createQueueingWorkloadGroup(String name, String id, Settings throttling, int queueSizePerBucket) {
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
