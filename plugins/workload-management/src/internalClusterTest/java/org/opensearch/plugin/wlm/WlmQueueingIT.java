/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm;

import org.apache.logging.log4j.LogManager;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.DocWriteResponse;
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
import org.opensearch.core.tasks.TaskCancelledException;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import static org.opensearch.index.query.QueryBuilders.scriptQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

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
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            // Keep the WLM service loop, and with it the 5s queue backstop sweep (WorkloadGroupService#doRun), from running
            // during a test. Otherwise the backstop admits a parked search within seconds even if the primary path a test
            // names (the node-completion handoff, or a live-configuration drain) were broken. The backstop has unit coverage.
            .put(WorkloadManagementSettings.QUERYGROUP_SERVICE_RUN_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1).millis())
            .build();
    }

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
            client().prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).get();
            long after = getCompletions(workloadGroupId);
            assertTrue("Expected search to be tagged to the workload group", after > before);
        }, 30, TimeUnit.SECONDS);

        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        ActionFuture<SearchResponse> firstBlocked;
        ActionFuture<SearchResponse> secondQueued;
        long queuedBefore;
        long waitBefore;
        long secondSubmittedNanos;
        // How long the parked search is kept waiting, once seen parked, before the permit is freed.
        final long holdMillis = 1000;
        try {
            // First search blocks in the query phase, holding the single permit.
            firstBlocked = blockingSearch(indexName).execute();
            awaitForBlock(plugins);

            queuedBefore = getTotalQueued(workloadGroupId);
            waitBefore = getTotalQueueWaitMillis(workloadGroupId);
            long throttledBefore = getThrottled(workloadGroupId);

            // Second search while the first is in-flight: it must be QUEUED (parked), not rejected. Run it async and wait
            // for the park to show up as live depth.
            secondSubmittedNanos = System.nanoTime();
            secondQueued = blockingSearch(indexName).execute();
            assertBusy(
                () -> assertEquals("second search should be parked in the queue", 1, getQueuedCurrent(workloadGroupId)),
                30,
                TimeUnit.SECONDS
            );
            // It was queued, not throttle-rejected.
            assertEquals("a queued request must not be counted as throttled", throttledBefore, getThrottled(workloadGroupId));
            // total_queued counts a request when it enters WAITING, so the still-parked request is counted already. Polled:
            // the count is taken just after the depth moves, under the same bucket lock.
            final long queuedOnPark = queuedBefore + 1;
            assertBusy(
                () -> assertEquals("a parked request is counted when it starts waiting", queuedOnPark, getTotalQueued(workloadGroupId))
            );
            // Keep the permit held for holdMillis, so the parked search waits at least that long: far longer than the rest
            // of its lifetime (reaching the queue, then running once admitted). Its wait is then most of that lifetime.
            final long parkSeenNanos = System.nanoTime();
            assertBusy(
                () -> assertThat(System.nanoTime() - parkSeenNanos, greaterThanOrEqualTo(TimeUnit.MILLISECONDS.toNanos(holdMillis)))
            );
        } finally {
            // Release the block. The first search completes, frees the permit, and the node-completion handoff admits the
            // parked second search — which then also runs (and completes, since blocks are now disabled).
            disableBlocks(plugins);
        }
        assertNotNull(firstBlocked.actionGet(TIMEOUT));
        assertNotNull("the queued search must be admitted and complete once a permit frees", secondQueued.actionGet(TIMEOUT));
        // The second search's lifetime as the client saw it, which contains its wait in the queue.
        final long secondSearchMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - secondSubmittedNanos);

        // The queue drains back to empty.
        assertBusy(() -> assertEquals("queue must drain to empty", 0, getQueuedCurrent(workloadGroupId)), 30, TimeUnit.SECONDS);

        // NOW the wait has ended: the node-completion hand-off admitted the request and recorded its wait, without counting
        // it again (total_queued counted it when it parked). With nothing left waiting, total_queued - queued_current is
        // the number of ended waits, the mean-wait denominator. The wait spans the hold, so it is at least holdMillis, and
        // it cannot exceed the search's lifetime as the client saw it. Since the hold is most of that lifetime, a wait
        // recorded twice would exceed it. (The sum/mean arithmetic, and that every admission path records a wait exactly
        // once, are covered deterministically in unit tests.)
        long recordedWait = getTotalQueueWaitMillis(workloadGroupId) - waitBefore;
        assertEquals("exactly one request waited, counted once", queuedBefore + 1, getTotalQueued(workloadGroupId));
        assertThat("the hand-off records the whole wait, which spans the hold", recordedWait, greaterThanOrEqualTo(holdMillis));
        assertThat("recorded once, the wait fits in the search's lifetime", recordedWait, lessThanOrEqualTo(secondSearchMillis));
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
            client().prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).get();
            long after = getCompletions(workloadGroupId);
            assertTrue("Expected search to be tagged to the workload group", after > before);
        }, 30, TimeUnit.SECONDS);

        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        ActionFuture<SearchResponse> firstBlocked;
        ActionFuture<SearchResponse> secondQueued;
        try {
            // First search blocks, holding the permit.
            firstBlocked = blockingSearch(indexName).execute();
            awaitForBlock(plugins);

            // Second search parks (fills the single queue slot).
            secondQueued = blockingSearch(indexName).execute();
            assertBusy(() -> assertEquals("second search should be parked", 1, getQueuedCurrent(workloadGroupId)), 30, TimeUnit.SECONDS);

            long queueRejectionsBefore = getQueueRejections(workloadGroupId);

            // Third concurrent search: permit taken, queue full -> the throttle 429, naming the full queue.
            Throwable rejection = expectThrows(Throwable.class, () -> blockingSearch(indexName).execute().actionGet(TIMEOUT));
            OpenSearchRejectedExecutionException cause = rejectedExecutionCause(rejection);
            assertNotNull("Expected an OpenSearchRejectedExecutionException in the cause chain but was: " + rejection, cause);
            assertEquals(
                "Request throttled: workload group [queuefull_test_group] reached its per-node limit of 1 concurrent requests, "
                    + "and its request queue is full.",
                cause.getMessage()
            );
            assertEquals(
                "queue-full rejection should increment total_queue_rejections",
                queueRejectionsBefore + 1,
                getQueueRejections(workloadGroupId)
            );
        } finally {
            // Release; the first and the queued second both complete.
            disableBlocks(plugins);
        }
        assertNotNull(firstBlocked.actionGet(TIMEOUT));
        assertNotNull(secondQueued.actionGet(TIMEOUT));
    }

    public void testCancelAfterTimeIntervalEvictsQueuedSearch() throws Exception {
        String workloadGroupId = "wlm_queue_cancel_group";
        String indexName = "queue_cancel_index";
        setWlmMode("enabled");
        updateWorkloadGroupInClusterState(PUT, createQueueingWorkloadGroup("queue_cancel_group", workloadGroupId, 1, 5));
        FeatureType featureType = AutoTaggingRegistry.getFeatureType(WorkloadGroupFeatureType.NAME);
        createRule("wlm_queue_cancel_rule", "queue cancel rule", indexName, featureType, workloadGroupId);
        indexDocument(indexName);
        assertBusy(() -> {
            long before = getCompletions(workloadGroupId);
            client().prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).get();
            assertTrue("Expected search to be tagged to the workload group", getCompletions(workloadGroupId) > before);
        }, 30, TimeUnit.SECONDS);

        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        ActionFuture<SearchResponse> firstBlocked;
        try {
            firstBlocked = blockingSearch(indexName).execute();
            awaitForBlock(plugins);
            long queuedBefore = getTotalQueued(workloadGroupId);
            long throttledBefore = getThrottled(workloadGroupId);
            long rejectionsBefore = getQueueRejections(workloadGroupId);
            long waitBefore = getTotalQueueWaitMillis(workloadGroupId);

            // There is no queue timeout: cancel_after_time_interval, armed in TransportSearchAction#doExecute before
            // admission, bounds a parked request. Long enough to observe the park.
            final long parkedSubmittedNanos = System.nanoTime();
            ActionFuture<SearchResponse> parked = blockingSearch(indexName).setCancelAfterTimeInterval(TimeValue.timeValueSeconds(10))
                .execute();
            assertBusy(() -> assertEquals("second search should be parked", 1, getQueuedCurrent(workloadGroupId)), 30, TimeUnit.SECONDS);

            Exception failure = expectThrows(Exception.class, () -> parked.actionGet(TIMEOUT));
            // The parked search's lifetime as the client saw it, which contains its wait in the queue.
            final long parkedSearchMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - parkedSubmittedNanos);
            Throwable cancelled = ExceptionsHelper.unwrap(failure, TaskCancelledException.class);
            assertNotNull("Expected a TaskCancelledException in the cause chain but was: " + failure, cancelled);
            assertEquals("task cancelled while queued", cancelled.getMessage());
            // The first search still holds the only permit, so only the task-cancellation callback can have completed the
            // parked search (a freed permit would fail it with the same message via admit's cancellation re-check).
            assertFalse("the first search must still hold the only permit", firstBlocked.isDone());

            assertEquals("eviction removes the parked request", 0, getQueuedCurrent(workloadGroupId));
            // The eviction recorded the wait before failing the request, and total_queued counted it when it parked.
            assertEquals(
                "the cancelled request was counted once, when it started waiting",
                queuedBefore + 1,
                getTotalQueued(workloadGroupId)
            );
            long waitAfter = getTotalQueueWaitMillis(workloadGroupId);
            assertThat("the eviction records how long the cancelled request waited", waitAfter, greaterThan(waitBefore));
            // The wait spans nearly all of the 10s interval, and so nearly all of the search's lifetime: recording it twice
            // would exceed that lifetime.
            assertThat(
                "recorded once, the wait fits in the search's lifetime",
                waitAfter - waitBefore,
                lessThanOrEqualTo(parkedSearchMillis)
            );
            assertEquals("a cancelled request is not a throttle rejection", throttledBefore, getThrottled(workloadGroupId));
            assertEquals("a cancelled request is not a queue rejection", rejectionsBefore, getQueueRejections(workloadGroupId));
        } finally {
            disableBlocks(plugins);
        }
        assertNotNull(firstBlocked.actionGet(TIMEOUT));
    }

    public void testGlobalDisableDrainsQueuedSearchUntracked() throws Exception {
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
            client().prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).get();
            assertTrue("Expected search to be tagged to the workload group", getCompletions(workloadGroupId) > before);
        }, 30, TimeUnit.SECONDS);

        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        ActionFuture<SearchResponse> firstBlocked;
        ActionFuture<SearchResponse> secondQueued;
        try {
            firstBlocked = blockingSearch(indexName).execute();
            awaitBlockedCount(plugins, 1);

            long totalQueuedBefore = getTotalQueued(workloadGroupId);
            secondQueued = blockingSearch(indexName).execute();
            assertBusy(() -> assertEquals("second search should be parked", 1, getQueuedCurrent(workloadGroupId)), 30, TimeUnit.SECONDS);

            // The setting update must submit a queue drain without waiting for the periodic service sweep. The first search
            // remains blocked and still owns the only node permit, so the second can reach the script before that permit is
            // released only if it was run untracked by the global ENABLED -> DISABLED callback.
            setWlmMode("disabled");
            awaitBlockedCount(plugins, 2);
            assertBusy(() -> assertEquals("global disable must empty the queue", 0, getQueuedCurrent(workloadGroupId)));
            assertBusy(
                () -> assertEquals(
                    "counted once, when it parked; the drain does not count it again",
                    totalQueuedBefore + 1,
                    getTotalQueued(workloadGroupId)
                )
            );
        } finally {
            disableBlocks(plugins);
        }
        assertNotNull(firstBlocked.actionGet(TIMEOUT));
        assertNotNull(secondQueued.actionGet(TIMEOUT));
    }

    public void testThrottleTierSwitchDrainsQueuedSearchUntracked() throws Exception {
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
            client().prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).get();
            assertTrue("Expected search to be tagged to the workload group", getCompletions(workloadGroupId) > before);
        }, 30, TimeUnit.SECONDS);

        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        ActionFuture<SearchResponse> firstBlocked;
        ActionFuture<SearchResponse> secondQueued;
        try {
            firstBlocked = blockingSearch(indexName).execute();
            awaitBlockedCount(plugins, 1);

            long totalQueuedBefore = getTotalQueued(workloadGroupId);
            secondQueued = blockingSearch(indexName).execute();
            assertBusy(() -> assertEquals("second search should be parked", 1, getQueuedCurrent(workloadGroupId)), 30, TimeUnit.SECONDS);

            Settings sharedOnlyThrottling = Settings.builder().put(WorkloadGroupThrottleSettings.SHARED_LIMIT.getKey(), 1).build();
            WorkloadGroup sharedOnly = createQueueingWorkloadGroup("tier_switch_drain_group", workloadGroupId, sharedOnlyThrottling, 5);
            updateWorkloadGroupInClusterState(PUT, sharedOnly);

            // The old request was queued against the node tier and never registered with the shared owner, the first
            // search still holds the old node permit, and the backstop sweep is off (see nodeSettings). So reaching two
            // blocked scripts proves the cluster-change callback drained that old backlog untracked instead of waiting on
            // either tier.
            awaitBlockedCount(plugins, 2);
            assertBusy(() -> assertEquals("tier switch must empty the old queue", 0, getQueuedCurrent(workloadGroupId)));
            assertBusy(
                () -> assertEquals(
                    "counted once, when it parked; the drain does not count it again",
                    totalQueuedBefore + 1,
                    getTotalQueued(workloadGroupId)
                )
            );
        } finally {
            disableBlocks(plugins);
        }
        assertNotNull(firstBlocked.actionGet(TIMEOUT));
        assertNotNull(secondQueued.actionGet(TIMEOUT));
    }

    public void testScrollPageQueuedThenAdmitted() throws Exception {
        String workloadGroupId = "wlm_queue_scroll_group";
        String indexName = "queue_scroll_index";
        setWlmMode("enabled");
        updateWorkloadGroupInClusterState(PUT, createQueueingWorkloadGroup("queue_scroll_group", workloadGroupId, 1, 5));
        FeatureType featureType = AutoTaggingRegistry.getFeatureType(WorkloadGroupFeatureType.NAME);
        createRule("wlm_queue_scroll_rule", "queue scroll rule", indexName, featureType, workloadGroupId);
        indexDocument(indexName);
        assertBusy(() -> {
            long before = getCompletions(workloadGroupId);
            client().prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).get();
            assertTrue("Expected search to be tagged to the workload group", getCompletions(workloadGroupId) > before);
        }, 30, TimeUnit.SECONDS);

        // Open a scroll context with a cheap query, so the initial search returns its permit at once.
        String scrollId = client().prepareSearch(indexName)
            .setQuery(QueryBuilders.matchAllQuery())
            .setSize(1)
            .setScroll(TIMEOUT)
            .get()
            .getScrollId();
        try {
            List<ScriptedBlockPlugin> plugins = initBlockFactory();
            ActionFuture<SearchResponse> firstBlocked;
            ActionFuture<SearchResponse> page;
            long queuedBefore;
            try {
                // The first search blocks, holding the only permit.
                firstBlocked = blockingSearch(indexName).execute();
                awaitForBlock(plugins);
                queuedBefore = getTotalQueued(workloadGroupId);
                long throttledBefore = getThrottled(workloadGroupId);

                // A scroll page at the limit waits in the queue like any other search instead of being rejected.
                page = client().prepareSearchScroll(scrollId).setScroll(TIMEOUT).execute();
                assertBusy(
                    () -> assertEquals("the scroll page should be parked", 1, getQueuedCurrent(workloadGroupId)),
                    30,
                    TimeUnit.SECONDS
                );
                assertFalse("a parked scroll page must not complete", page.isDone());
                assertEquals("a parked scroll page is not throttled", throttledBefore, getThrottled(workloadGroupId));
            } finally {
                disableBlocks(plugins);
            }
            assertNotNull(firstBlocked.actionGet(TIMEOUT));
            // The first search hands its permit to the parked page, which then reads its still-open scroll context.
            assertNotNull("the parked scroll page must be admitted once the permit frees", page.actionGet(TIMEOUT));
            assertBusy(() -> assertEquals("queue must drain to empty", 0, getQueuedCurrent(workloadGroupId)), 30, TimeUnit.SECONDS);
            assertEquals("the scroll page waited once", queuedBefore + 1, getTotalQueued(workloadGroupId));
        } finally {
            client().prepareClearScroll().addScrollId(scrollId).get();
        }
    }

    // Helpers

    private static OpenSearchRejectedExecutionException rejectedExecutionCause(Throwable t) {
        for (Throwable cur = t; cur != null; cur = cur.getCause()) {
            if (cur instanceof OpenSearchRejectedExecutionException rejection) {
                return rejection;
            }
        }
        return null;
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
        Settings.Builder settings = Settings.builder().put(WorkloadManagementSettings.WLM_MODE_SETTING.getKey(), mode);
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest().persistentSettings(settings);
        assertAcked(client().admin().cluster().updateSettings(request).get());
    }

    private WorkloadGroup createQueueingWorkloadGroup(String name, String id, int nodeLimit, int queueSizePerBucket) {
        Settings throttling = Settings.builder().put(WorkloadGroupThrottleSettings.NODE_LIMIT.getKey(), nodeLimit).build();
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
        assertEquals(DocWriteResponse.Result.CREATED, response.getResult());
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
        // Blocks until released, as the plugin starts armed. A latch releases blocked queries the instant disableBlock() is called.
        private volatile CountDownLatch release = new CountDownLatch(1);

        public void reset() {
            hits.set(0);
        }

        public void disableBlock() {
            release.countDown();
        }

        public void enableBlock() {
            release = new CountDownLatch(1);
        }

        @Override
        public Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap(SCRIPT_NAME, params -> {
                LeafFieldsLookup fieldsLookup = (LeafFieldsLookup) params.get("_fields");
                LogManager.getLogger(WlmQueueingIT.class).info("Blocking on the document {}", fieldsLookup.get("_id"));
                CountDownLatch latch = release;
                hits.incrementAndGet();
                try {
                    // Outlast callers' 30s waits. Throw a RuntimeException (a clean shard failure) on expiry, never an
                    // AssertionError: an Error escapes AbstractRunnable, kills the search thread and never answers the search.
                    if (latch.await(120, TimeUnit.SECONDS) == false) {
                        throw new IllegalStateException("blocking script was never released");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
                return true;
            });
        }
    }
}
