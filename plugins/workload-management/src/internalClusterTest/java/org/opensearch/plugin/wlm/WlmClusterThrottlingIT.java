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
import org.opensearch.index.query.QueryBuilders;
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

/**
 * End-to-end integration test for cluster-level ({@code shared_limit}) WLM request throttling across a multi-node
 * cluster. Whereas {@link WlmNodeThrottlingIT} proves the per-node tier, this verifies the distributed shared tier:
 * a bucket's cluster-wide in-flight count is enforced by a single owner node regardless of which coordinator a
 * request lands on, so concurrent searches spread across coordinators still cap at {@code shared_limit}.
 * <p>
 * The group sets only {@code shared_limit} (no {@code node_limit}), so every admitted request must consult the
 * bucket owner — exercising both the local-owner short-circuit and the cross-node acquire RPC depending on where
 * the coordinator sits relative to the owner.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 3, numClientNodes = 0, supportsDedicatedMasters = false)
public class WlmClusterThrottlingIT extends OpenSearchIntegTestCase {

    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private static final String PUT = "PUT";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(WlmAutoTaggingIT.TestWorkloadManagementPlugin.class);
        plugins.add(RuleFrameworkPlugin.class);
        plugins.add(ClusterScriptedBlockPlugin.class);
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

    public void testClusterWideCeilingHoldsAcrossCoordinators() throws Exception {
        String workloadGroupId = "wlm_shared_throttle_group";
        String ruleId = "wlm_shared_throttle_rule";
        String indexName = "shared_throttle_index";

        setWlmMode("enabled");

        // shared_limit = 2, node_limit unset -> a cluster-wide ceiling of 2 in-flight for the whole group,
        // enforced by the bucket owner no matter which coordinator receives the request.
        WorkloadGroup workloadGroup = createSharedThrottledGroup("shared_test_group", workloadGroupId, 2);
        updateWorkloadGroupInClusterState(PUT, workloadGroup);

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

        FeatureType featureType = AutoTaggingRegistry.getFeatureType(WorkloadGroupFeatureType.NAME);
        createRule(ruleId, "shared throttle rule", indexName, featureType, workloadGroupId);
        indexDocument(indexName);

        // Wait for rule propagation on EVERY coordinator this test drives, not just a random one. The rule refresh is
        // asynchronous and can reach nodes at different times; a search on an un-refreshed coordinator stays untagged
        // and bypasses admit(), which would break the ceiling assertions below. Poll each named node's own client
        // until a search through it is tagged to the group (its completions advance).
        for (String node : internalCluster().getNodeNames()) {
            assertBusy(() -> {
                long before = getCompletions(workloadGroupId);
                try {
                    client(node).prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).get();
                } catch (Exception e) {
                    // Each probe consults the shared tier (shared_limit=2, node_limit unset) and briefly holds a permit;
                    // remote releases are fire-and-forget, so a burst of probes can transiently exhaust the budget and
                    // throttle this one. That is not a propagation failure — turn the 429 into an AssertionError so
                    // assertBusy retries it (assertBusy only re-runs on AssertionError; a raw 429 would escape). Any
                    // other exception is a real failure and propagates.
                    assertFalse("transient throttle during propagation probe — retry: " + e, hasRejectedExecutionCause(e));
                    throw e;
                }
                long after = getCompletions(workloadGroupId);
                assertTrue("search via [" + node + "] not yet tagged to the throttled workload group", after > before);
            }, 30, TimeUnit.SECONDS);
        }

        List<ClusterScriptedBlockPlugin> plugins = initBlockFactory();

        List<String> coordinators = new ArrayList<>(List.of(internalCluster().getNodeNames()));
        // Fill the cluster-wide budget of 2 using two DIFFERENT coordinators.
        ActionFuture<SearchResponse> first = blockingSearchVia(coordinators.get(0), indexName).execute();
        ActionFuture<SearchResponse> second = blockingSearchVia(coordinators.get(1 % coordinators.size()), indexName).execute();
        awaitBlockedCount(plugins, 2);

        long throttledBefore = getThrottled(workloadGroupId);

        // A third concurrent search on yet another coordinator must be rejected: the cluster-wide ceiling of 2 is
        // reached, and the owner (a single node) sees the count regardless of coordinator.
        String thirdCoordinator = coordinators.get(2 % coordinators.size());
        Throwable rejection = expectThrows(Throwable.class, () -> blockingSearchVia(thirdCoordinator, indexName).get());
        assertTrue(
            "Expected an OpenSearchRejectedExecutionException in the cause chain but was: " + rejection,
            hasRejectedExecutionCause(rejection)
        );
        assertEquals("total_throttled should increment by exactly one", throttledBefore + 1, getThrottled(workloadGroupId));

        // Release the blocks; the two admitted searches complete successfully.
        disableBlocks(plugins);
        assertNotNull(first.actionGet(TIMEOUT));
        assertNotNull(second.actionGet(TIMEOUT));

        // Once the budget drains, a new search is admitted again. The two fills may have released REMOTELY (when the
        // bucket owner is a third node, release is a fire-and-forget RPC with no happens-before to this re-admission),
        // so the owner's in-flight count may not have dropped the instant actionGet() above returned. Poll: retry a
        // non-blocking search until it is admitted (tolerating a transient 429 during the drain window), rather than
        // assuming the drain is immediately visible.
        assertBusy(() -> {
            try {
                client(thirdCoordinator).prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).get();
            } catch (Exception e) {
                assertFalse("still draining (transient 429) — retry", hasRejectedExecutionCause(e));
                throw e;
            }
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * Shared-tier QUEUEING via owner-push, on a shared-ONLY group (no {@code node_limit}). This is the exact scenario
     * that the enqueue-first fix protects: a request denied at the shared limit is PARKED on its coordinator, and the
     * bucket owner pushes it a grant when a slot frees — with no node-tier drain to fall back on. Because the group is
     * shared-only, the ONLY way the parked request can ever complete is the cross-node owner-push path.
     * <p>
     * The enqueue-first ordering matters here: the request is placed in the coordinator's queue BEFORE the shared
     * acquire is sent, so a grant (or owner-push) can never arrive to find an empty queue and deregister the waiter,
     * which would otherwise strand the request (there is no queue timeout to rescue it).
     */
    public void testSharedTierQueuesAndDrainsViaOwnerPush() throws Exception {
        String workloadGroupId = "wlm_shared_queue_group";
        String ruleId = "wlm_shared_queue_rule";
        String indexName = "shared_queue_index";

        setWlmMode("enabled");

        // shared_limit=1, node_limit unset, queue.size_per_bucket=5: one request runs cluster-wide; the rest PARK and drain via
        // owner-push. No node tier exists, so owner-push is the sole drain path.
        WorkloadGroup workloadGroup = createSharedThrottledQueueingGroup("shared_queue_test_group", workloadGroupId, 1, 5);
        updateWorkloadGroupInClusterState(PUT, workloadGroup);

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

        FeatureType featureType = AutoTaggingRegistry.getFeatureType(WorkloadGroupFeatureType.NAME);
        createRule(ruleId, "shared queue rule", indexName, featureType, workloadGroupId);
        indexDocument(indexName);

        // Wait for rule propagation on every coordinator this test drives (same rationale as the throttling test).
        for (String node : internalCluster().getNodeNames()) {
            assertBusy(() -> {
                long before = getCompletions(workloadGroupId);
                try {
                    client(node).prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).get();
                } catch (Exception e) {
                    assertFalse("transient throttle during propagation probe — retry: " + e, hasRejectedExecutionCause(e));
                    throw e;
                }
                long after = getCompletions(workloadGroupId);
                assertTrue("search via [" + node + "] not yet tagged to the workload group", after > before);
            }, 30, TimeUnit.SECONDS);
        }

        List<ClusterScriptedBlockPlugin> plugins = initBlockFactory();
        List<String> coordinators = new ArrayList<>(List.of(internalCluster().getNodeNames()));

        // First search fills the single cluster-wide shared slot and blocks in-flight.
        ActionFuture<SearchResponse> first = blockingSearchVia(coordinators.get(0), indexName).execute();
        awaitBlockedCount(plugins, 1);

        long throttledBefore = getThrottled(workloadGroupId);
        long totalQueuedBefore = getTotalQueued(workloadGroupId);

        // Second search on a DIFFERENT coordinator: the shared limit is reached, so instead of a 429 it must be PARKED
        // (enqueue-first) and registered for owner-push. It is NOT throttled.
        ActionFuture<SearchResponse> second = blockingSearchVia(coordinators.get(1 % coordinators.size()), indexName).execute();
        assertBusy(
            () -> assertEquals("second search should be parked in the queue", 1, getQueuedCurrent(workloadGroupId)),
            30,
            TimeUnit.SECONDS
        );
        assertEquals("a parked request must not be counted as throttled", throttledBefore, getThrottled(workloadGroupId));
        assertEquals("the parked request should be counted as queued", totalQueuedBefore + 1, getTotalQueued(workloadGroupId));

        // Release the blocks. The first completes and frees the single shared slot; the owner pushes a grant to the
        // coordinator holding the parked second search, which then drains and completes. With no node tier, this can
        // ONLY happen via cross-node owner-push — the path the enqueue-first fix keeps race-free.
        disableBlocks(plugins);
        assertNotNull("the first (blocking) search must complete", first.actionGet(TIMEOUT));
        assertNotNull("the parked second search must be admitted via owner-push and complete", second.actionGet(TIMEOUT));

        // The queue drains back to empty.
        assertBusy(() -> assertEquals("queue must drain to empty", 0, getQueuedCurrent(workloadGroupId)), 30, TimeUnit.SECONDS);
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

    private long getCompletions(String groupId) throws Exception {
        return sumAcrossNodes(groupId, WorkloadGroupStatsHolder::getCompletions);
    }

    private long getThrottled(String groupId) throws Exception {
        return sumAcrossNodes(groupId, WorkloadGroupStatsHolder::getThrottled);
    }

    private long getQueuedCurrent(String groupId) throws Exception {
        return sumAcrossNodes(groupId, WorkloadGroupStatsHolder::getQueuedCurrent);
    }

    private long getTotalQueued(String groupId) throws Exception {
        return sumAcrossNodes(groupId, WorkloadGroupStatsHolder::getQueued);
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

    private SearchRequestBuilder blockingSearchVia(String nodeName, String indexName) {
        return client(nodeName).prepareSearch(indexName)
            .setQuery(
                scriptQuery(new Script(ScriptType.INLINE, "mockscript", ClusterScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap()))
            );
    }

    private List<ClusterScriptedBlockPlugin> initBlockFactory() {
        List<ClusterScriptedBlockPlugin> plugins = new ArrayList<>();
        for (PluginsService pluginsService : internalCluster().getDataNodeInstances(PluginsService.class)) {
            plugins.addAll(pluginsService.filterPlugins(ClusterScriptedBlockPlugin.class));
        }
        for (ClusterScriptedBlockPlugin plugin : plugins) {
            plugin.reset();
            plugin.enableBlock();
        }
        return plugins;
    }

    private void awaitBlockedCount(List<ClusterScriptedBlockPlugin> plugins, int expected) throws Exception {
        assertBusy(() -> {
            int blocked = 0;
            for (ClusterScriptedBlockPlugin plugin : plugins) {
                blocked += plugin.hits.get();
            }
            assertEquals("expected exactly " + expected + " searches blocked in-flight", expected, blocked);
        }, 30, TimeUnit.SECONDS);
    }

    private void disableBlocks(List<ClusterScriptedBlockPlugin> plugins) {
        for (ClusterScriptedBlockPlugin plugin : plugins) {
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

    private WorkloadGroup createSharedThrottledGroup(String name, String id, int sharedLimit) {
        Settings throttling = Settings.builder()
            .put(WorkloadGroupThrottleSettings.ATTRIBUTE.getKey(), "group")
            .put(WorkloadGroupThrottleSettings.SHARED_LIMIT.getKey(), sharedLimit)
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

    // Shared-only throttling (no node_limit) WITH queueing enabled, so a denied request parks and drains via owner-push.
    private WorkloadGroup createSharedThrottledQueueingGroup(String name, String id, int sharedLimit, int queueSizePerBucket) {
        Settings throttling = Settings.builder()
            .put(WorkloadGroupThrottleSettings.ATTRIBUTE.getKey(), "group")
            .put(WorkloadGroupThrottleSettings.SHARED_LIMIT.getKey(), sharedLimit)
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
                // One shard so each search produces exactly one blocking script hit, making the in-flight count exact.
                // Throttle admission happens on the coordinator that receives the request, independent of where the
                // shard lives, so a single shard is sufficient to exercise the cross-coordinator ceiling.
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
     * Test script plugin that blocks during the query phase until released, keeping a search in-flight on whichever
     * coordinator dispatched it.
     */
    public static class ClusterScriptedBlockPlugin extends MockScriptPlugin {
        static final String SCRIPT_NAME = "cluster_search_block";

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
                LogManager.getLogger(WlmClusterThrottlingIT.class).info("Blocking on the document {}", fieldsLookup.get("_id"));
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
