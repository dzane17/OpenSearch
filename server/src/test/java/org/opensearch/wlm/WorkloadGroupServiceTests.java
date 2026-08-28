/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchTask;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.search.backpressure.trackers.NodeDuressTrackers;
import org.opensearch.tasks.Task;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.cancellation.TaskSelectionStrategy;
import org.opensearch.wlm.cancellation.WorkloadGroupTaskCancellationService;
import org.opensearch.wlm.stats.WorkloadGroupState;
import org.opensearch.wlm.tracker.WorkloadGroupResourceUsageTrackerService;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.opensearch.wlm.tracker.ResourceUsageCalculatorTests.createMockTaskWithResourceStats;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WorkloadGroupServiceTests extends OpenSearchTestCase {
    public static final String WORKLOAD_GROUP_ID = "workloadGroupId1";
    private WorkloadGroupService workloadGroupService;
    private WorkloadGroupTaskCancellationService mockCancellationService;
    private ClusterService mockClusterService;
    private ThreadPool mockThreadPool;
    private WorkloadManagementSettings mockWorkloadManagementSettings;
    private Scheduler.Cancellable mockScheduledFuture;
    private Map<String, WorkloadGroupState> mockWorkloadGroupStateMap;
    NodeDuressTrackers mockNodeDuressTrackers;
    WorkloadGroupsStateAccessor mockWorkloadGroupsStateAccessor;

    public void setUp() throws Exception {
        super.setUp();
        mockClusterService = Mockito.mock(ClusterService.class);
        mockThreadPool = Mockito.mock(ThreadPool.class);
        mockScheduledFuture = Mockito.mock(Scheduler.Cancellable.class);
        mockWorkloadManagementSettings = Mockito.mock(WorkloadManagementSettings.class);
        mockWorkloadGroupStateMap = new HashMap<>();
        mockNodeDuressTrackers = Mockito.mock(NodeDuressTrackers.class);
        mockCancellationService = Mockito.mock(TestWorkloadGroupCancellationService.class);
        mockWorkloadGroupsStateAccessor = new WorkloadGroupsStateAccessor();
        when(mockNodeDuressTrackers.isNodeInDuress()).thenReturn(false);

        workloadGroupService = new WorkloadGroupService(
            mockCancellationService,
            mockClusterService,
            mockThreadPool,
            mockWorkloadManagementSettings,
            mockNodeDuressTrackers,
            mockWorkloadGroupsStateAccessor,
            new HashSet<>(),
            new HashSet<>()
        );
    }

    public void tearDown() throws Exception {
        super.tearDown();
        mockThreadPool.shutdown();
    }

    public void testClusterChanged() {
        ClusterChangedEvent mockClusterChangedEvent = Mockito.mock(ClusterChangedEvent.class);
        ClusterState mockPreviousClusterState = Mockito.mock(ClusterState.class);
        ClusterState mockClusterState = Mockito.mock(ClusterState.class);
        Metadata mockPreviousMetadata = Mockito.mock(Metadata.class);
        Metadata mockMetadata = Mockito.mock(Metadata.class);
        WorkloadGroup addedWorkloadGroup = new WorkloadGroup(
            "addedWorkloadGroup",
            "4242",
            new MutableWorkloadGroupFragment(MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED, Map.of(ResourceType.MEMORY, 0.5)),
            1L
        );
        WorkloadGroup deletedWorkloadGroup = new WorkloadGroup(
            "deletedWorkloadGroup",
            "4241",
            new MutableWorkloadGroupFragment(MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED, Map.of(ResourceType.MEMORY, 0.5)),
            1L
        );
        Map<String, WorkloadGroup> previousWorkloadGroups = new HashMap<>();
        previousWorkloadGroups.put("4242", addedWorkloadGroup);
        Map<String, WorkloadGroup> currentWorkloadGroups = new HashMap<>();
        currentWorkloadGroups.put("4241", deletedWorkloadGroup);

        when(mockClusterChangedEvent.previousState()).thenReturn(mockPreviousClusterState);
        when(mockClusterChangedEvent.state()).thenReturn(mockClusterState);
        when(mockPreviousClusterState.metadata()).thenReturn(mockPreviousMetadata);
        when(mockClusterState.metadata()).thenReturn(mockMetadata);
        when(mockPreviousMetadata.workloadGroups()).thenReturn(previousWorkloadGroups);
        when(mockMetadata.workloadGroups()).thenReturn(currentWorkloadGroups);
        workloadGroupService.clusterChanged(mockClusterChangedEvent);

        Set<WorkloadGroup> currentWorkloadGroupsExpected = Set.of(currentWorkloadGroups.get("4241"));
        Set<WorkloadGroup> previousWorkloadGroupsExpected = Set.of(previousWorkloadGroups.get("4242"));

        assertEquals(currentWorkloadGroupsExpected, workloadGroupService.getActiveWorkloadGroups());
        assertEquals(previousWorkloadGroupsExpected, workloadGroupService.getDeletedWorkloadGroups());
    }

    public void testDoStart_SchedulesTask() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        when(mockWorkloadManagementSettings.getWorkloadGroupServiceRunInterval()).thenReturn(TimeValue.timeValueSeconds(1));
        workloadGroupService.doStart();
        Mockito.verify(mockThreadPool).scheduleWithFixedDelay(any(Runnable.class), any(TimeValue.class), eq(ThreadPool.Names.GENERIC));
    }

    public void testDoStop_CancelsScheduledTask() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        when(mockThreadPool.scheduleWithFixedDelay(any(), any(), any())).thenReturn(mockScheduledFuture);
        workloadGroupService.doStart();
        workloadGroupService.doStop();
        Mockito.verify(mockScheduledFuture).cancel();
    }

    public void testDoRun_WhenModeEnabled() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        when(mockNodeDuressTrackers.isNodeInDuress()).thenReturn(true);
        // Call the method
        workloadGroupService.doRun();

        // Verify that refreshWorkloadGroups was called

        // Verify that cancelTasks was called with a BooleanSupplier
        ArgumentCaptor<BooleanSupplier> booleanSupplierCaptor = ArgumentCaptor.forClass(BooleanSupplier.class);
        Mockito.verify(mockCancellationService).cancelTasks(booleanSupplierCaptor.capture(), any(), any());

        // Assert the behavior of the BooleanSupplier
        BooleanSupplier capturedSupplier = booleanSupplierCaptor.getValue();
        assertTrue(capturedSupplier.getAsBoolean());

    }

    public void testDoRun_WhenModeDisabled() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.DISABLED);
        when(mockNodeDuressTrackers.isNodeInDuress()).thenReturn(false);
        workloadGroupService.doRun();
        // Verify that refreshWorkloadGroups was called

        Mockito.verify(mockCancellationService, never()).cancelTasks(any(), any(), any());

    }

    public void testRejectIfNeeded_whenWorkloadGroupIdIsNullOrDefaultOne() {
        WorkloadGroup testWorkloadGroup = new WorkloadGroup(
            "testWorkloadGroup",
            "workloadGroupId1",
            new MutableWorkloadGroupFragment(MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED, Map.of(ResourceType.CPU, 0.10)),
            1L
        );
        Set<WorkloadGroup> activeWorkloadGroups = new HashSet<>() {
            {
                add(testWorkloadGroup);
            }
        };
        mockWorkloadGroupStateMap = new HashMap<>();
        mockWorkloadGroupsStateAccessor = new WorkloadGroupsStateAccessor(mockWorkloadGroupStateMap);
        mockWorkloadGroupStateMap.put("workloadGroupId1", new WorkloadGroupState());

        Map<String, WorkloadGroupState> spyMap = spy(mockWorkloadGroupStateMap);

        workloadGroupService = new WorkloadGroupService(
            mockCancellationService,
            mockClusterService,
            mockThreadPool,
            mockWorkloadManagementSettings,
            mockNodeDuressTrackers,
            mockWorkloadGroupsStateAccessor,
            activeWorkloadGroups,
            new HashSet<>()
        );
        workloadGroupService.rejectIfNeeded(null);

        verify(spyMap, never()).get(any());

        workloadGroupService.rejectIfNeeded(WorkloadGroupTask.DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get());
        verify(spyMap, never()).get(any());
    }

    public void testRejectIfNeeded_whenSoftModeWorkloadGroupIsContendedAndNodeInDuress() {
        Set<WorkloadGroup> activeWorkloadGroups = getActiveWorkloadGroups(
            "testWorkloadGroup",
            WORKLOAD_GROUP_ID,
            MutableWorkloadGroupFragment.ResiliencyMode.SOFT,
            Map.of(ResourceType.CPU, 0.10)
        );
        mockWorkloadGroupStateMap = new HashMap<>();
        mockWorkloadGroupStateMap.put("workloadGroupId1", new WorkloadGroupState());
        WorkloadGroupState state = new WorkloadGroupState();
        WorkloadGroupState.ResourceTypeState cpuResourceState = new WorkloadGroupState.ResourceTypeState(ResourceType.CPU);
        cpuResourceState.setLastRecordedUsage(0.10);
        state.getResourceState().put(ResourceType.CPU, cpuResourceState);
        WorkloadGroupState spyState = spy(state);
        mockWorkloadGroupStateMap.put(WORKLOAD_GROUP_ID, spyState);

        mockWorkloadGroupsStateAccessor = new WorkloadGroupsStateAccessor(mockWorkloadGroupStateMap);

        workloadGroupService = new WorkloadGroupService(
            mockCancellationService,
            mockClusterService,
            mockThreadPool,
            mockWorkloadManagementSettings,
            mockNodeDuressTrackers,
            mockWorkloadGroupsStateAccessor,
            activeWorkloadGroups,
            new HashSet<>()
        );
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        when(mockNodeDuressTrackers.isNodeInDuress()).thenReturn(true);
        assertThrows(OpenSearchRejectedExecutionException.class, () -> workloadGroupService.rejectIfNeeded("workloadGroupId1"));
    }

    public void testRejectIfNeeded_whenWorkloadGroupIsSoftMode() {
        Set<WorkloadGroup> activeWorkloadGroups = getActiveWorkloadGroups(
            "testWorkloadGroup",
            WORKLOAD_GROUP_ID,
            MutableWorkloadGroupFragment.ResiliencyMode.SOFT,
            Map.of(ResourceType.CPU, 0.10)
        );
        mockWorkloadGroupStateMap = new HashMap<>();
        WorkloadGroupState spyState = spy(new WorkloadGroupState());
        mockWorkloadGroupStateMap.put("workloadGroupId1", spyState);

        mockWorkloadGroupsStateAccessor = new WorkloadGroupsStateAccessor(mockWorkloadGroupStateMap);

        workloadGroupService = new WorkloadGroupService(
            mockCancellationService,
            mockClusterService,
            mockThreadPool,
            mockWorkloadManagementSettings,
            mockNodeDuressTrackers,
            mockWorkloadGroupsStateAccessor,
            activeWorkloadGroups,
            new HashSet<>()
        );
        workloadGroupService.rejectIfNeeded("workloadGroupId1");

        verify(spyState, never()).getResourceState();
    }

    public void testRejectIfNeeded_whenWorkloadGroupIsEnforcedMode_andNotBreaching() {
        WorkloadGroup testWorkloadGroup = getWorkloadGroup(
            "testWorkloadGroup",
            "workloadGroupId1",
            MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED,
            Map.of(ResourceType.CPU, 0.10)
        );
        WorkloadGroup spuWorkloadGroup = spy(testWorkloadGroup);
        Set<WorkloadGroup> activeWorkloadGroups = new HashSet<>() {
            {
                add(spuWorkloadGroup);
            }
        };
        mockWorkloadGroupStateMap = new HashMap<>();
        WorkloadGroupState workloadGroupState = new WorkloadGroupState();
        workloadGroupState.getResourceState().get(ResourceType.CPU).setLastRecordedUsage(0.05);

        mockWorkloadGroupStateMap.put("workloadGroupId1", workloadGroupState);

        mockWorkloadGroupsStateAccessor = new WorkloadGroupsStateAccessor(mockWorkloadGroupStateMap);

        workloadGroupService = new WorkloadGroupService(
            mockCancellationService,
            mockClusterService,
            mockThreadPool,
            mockWorkloadManagementSettings,
            mockNodeDuressTrackers,
            mockWorkloadGroupsStateAccessor,
            activeWorkloadGroups,
            new HashSet<>()
        );
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        when(mockWorkloadManagementSettings.getNodeLevelCpuRejectionThreshold()).thenReturn(0.8);
        workloadGroupService.rejectIfNeeded("workloadGroupId1");

        // verify the check to compare the current usage and limit
        // this should happen 3 times => 2 to check whether the resource limit has the TRACKED resource type and 1 to get the value
        verify(spuWorkloadGroup, times(3)).getResourceLimits();
        assertEquals(0, workloadGroupState.getResourceState().get(ResourceType.CPU).rejections.count());
        assertEquals(0, workloadGroupState.totalRejections.count());
    }

    public void testRejectIfNeeded_whenWorkloadGroupIsEnforcedMode_andBreaching() {
        WorkloadGroup testWorkloadGroup = new WorkloadGroup(
            "testWorkloadGroup",
            "workloadGroupId1",
            new MutableWorkloadGroupFragment(
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED,
                Map.of(ResourceType.CPU, 0.10, ResourceType.MEMORY, 0.10)
            ),
            1L
        );
        WorkloadGroup spuWorkloadGroup = spy(testWorkloadGroup);
        Set<WorkloadGroup> activeWorkloadGroups = new HashSet<>() {
            {
                add(spuWorkloadGroup);
            }
        };
        mockWorkloadGroupStateMap = new HashMap<>();
        WorkloadGroupState workloadGroupState = new WorkloadGroupState();
        workloadGroupState.getResourceState().get(ResourceType.CPU).setLastRecordedUsage(0.18);
        workloadGroupState.getResourceState().get(ResourceType.MEMORY).setLastRecordedUsage(0.18);
        WorkloadGroupState spyState = spy(workloadGroupState);

        mockWorkloadGroupsStateAccessor = new WorkloadGroupsStateAccessor(mockWorkloadGroupStateMap);

        mockWorkloadGroupStateMap.put("workloadGroupId1", spyState);

        workloadGroupService = new WorkloadGroupService(
            mockCancellationService,
            mockClusterService,
            mockThreadPool,
            mockWorkloadManagementSettings,
            mockNodeDuressTrackers,
            mockWorkloadGroupsStateAccessor,
            activeWorkloadGroups,
            new HashSet<>()
        );
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        assertThrows(OpenSearchRejectedExecutionException.class, () -> workloadGroupService.rejectIfNeeded("workloadGroupId1"));

        // verify the check to compare the current usage and limit
        // this should happen 3 times => 1 to check whether the resource limit has the TRACKED resource type and 1 to get the value
        // because it will break out of the loop since the limits are breached
        verify(spuWorkloadGroup, times(2)).getResourceLimits();
        assertEquals(
            1,
            workloadGroupState.getResourceState().get(ResourceType.CPU).rejections.count() + workloadGroupState.getResourceState()
                .get(ResourceType.MEMORY).rejections.count()
        );
        assertEquals(1, workloadGroupState.totalRejections.count());
    }

    public void testRejectIfNeeded_whenFeatureIsNotEnabled() {
        WorkloadGroup testWorkloadGroup = new WorkloadGroup(
            "testWorkloadGroup",
            "workloadGroupId1",
            new MutableWorkloadGroupFragment(MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED, Map.of(ResourceType.CPU, 0.10)),
            1L
        );
        Set<WorkloadGroup> activeWorkloadGroups = new HashSet<>() {
            {
                add(testWorkloadGroup);
            }
        };
        mockWorkloadGroupStateMap = new HashMap<>();
        mockWorkloadGroupStateMap.put("workloadGroupId1", new WorkloadGroupState());

        Map<String, WorkloadGroupState> spyMap = spy(mockWorkloadGroupStateMap);

        mockWorkloadGroupsStateAccessor = new WorkloadGroupsStateAccessor(mockWorkloadGroupStateMap);

        workloadGroupService = new WorkloadGroupService(
            mockCancellationService,
            mockClusterService,
            mockThreadPool,
            mockWorkloadManagementSettings,
            mockNodeDuressTrackers,
            mockWorkloadGroupsStateAccessor,
            activeWorkloadGroups,
            new HashSet<>()
        );
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.DISABLED);

        workloadGroupService.rejectIfNeeded(testWorkloadGroup.get_id());
        verify(spyMap, never()).get(any());
    }

    public void testOnTaskCompleted() {
        Task task = new SearchTask(12, "", "", () -> "", null, null);
        mockThreadPool = new TestThreadPool("workloadGroupServiceTests");
        mockThreadPool.getThreadContext().putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, "testId");
        WorkloadGroupState workloadGroupState = new WorkloadGroupState();
        mockWorkloadGroupStateMap.put("testId", workloadGroupState);
        mockWorkloadGroupsStateAccessor = new WorkloadGroupsStateAccessor(mockWorkloadGroupStateMap);
        workloadGroupService = new WorkloadGroupService(
            mockCancellationService,
            mockClusterService,
            mockThreadPool,
            mockWorkloadManagementSettings,
            mockNodeDuressTrackers,
            mockWorkloadGroupsStateAccessor,
            new HashSet<>() {
                {
                    add(
                        new WorkloadGroup(
                            "testWorkloadGroup",
                            "testId",
                            new MutableWorkloadGroupFragment(
                                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED,
                                Map.of(ResourceType.CPU, 0.10, ResourceType.MEMORY, 0.10)
                            ),
                            1L
                        )
                    );
                }
            },
            new HashSet<>()
        );

        ((WorkloadGroupTask) task).setWorkloadGroupId(mockThreadPool.getThreadContext());
        workloadGroupService.onTaskCompleted(task);

        assertEquals(1, workloadGroupState.totalCompletions.count());

        // test non WorkloadGroupTask
        task = new Task(1, "simple", "test", "mock task", null, null);
        workloadGroupService.onTaskCompleted(task);

        // It should still be 1
        assertEquals(1, workloadGroupState.totalCompletions.count());

        mockThreadPool.shutdown();
    }

    public void testGetCurrentWorkloadGroupReturnsNullWhenHeaderMissing() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(mockThreadPool.getThreadContext()).thenReturn(threadContext);
        assertNull(workloadGroupService.getCurrentWorkloadGroup());
    }

    public void testGetCurrentWorkloadGroupReturnsGroupWhenPresent() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, "wg-1");
        when(mockThreadPool.getThreadContext()).thenReturn(threadContext);
        WorkloadGroup wg = new WorkloadGroup(
            "wg-1-name",
            "wg-1",
            new MutableWorkloadGroupFragment(MutableWorkloadGroupFragment.ResiliencyMode.SOFT, Map.of(ResourceType.MEMORY, 0.5)),
            1L
        );
        ClusterState clusterState = Mockito.mock(ClusterState.class);
        Metadata metadata = Mockito.mock(Metadata.class);
        when(mockClusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.workloadGroups()).thenReturn(Map.of("wg-1", wg));
        assertSame(wg, workloadGroupService.getCurrentWorkloadGroup());
    }

    public void testGetCurrentWorkloadGroupReturnsNullWhenGroupMissing() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, "missing-id");
        when(mockThreadPool.getThreadContext()).thenReturn(threadContext);
        ClusterState clusterState = Mockito.mock(ClusterState.class);
        Metadata metadata = Mockito.mock(Metadata.class);
        when(mockClusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.workloadGroups()).thenReturn(Collections.emptyMap());
        assertNull(workloadGroupService.getCurrentWorkloadGroup());
    }

    private void stubClusterStateWithGroup(WorkloadGroup wg) {
        ClusterState clusterState = Mockito.mock(ClusterState.class);
        Metadata metadata = Mockito.mock(Metadata.class);
        when(mockClusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.workloadGroups()).thenReturn(Map.of(wg.get_id(), wg));
    }

    /**
     * Drives the async {@link WorkloadGroupService#acquireThrottlePermit} back into the synchronous "permit or 429"
     * contract these node-tier tests assert on. With no shared throttle service wired, it completes inline: responds
     * with a permit (or null when not throttled) or fails with the 429. Rethrows the failure so {@code expectThrows}
     * works.
     */
    private Releasable acquireThrottlePermitSync(WorkloadGroupService service, String workloadGroupId, String principal) {
        AtomicReference<Releasable> permit = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        // acquireThrottlePermit takes the task (it carries the workload group id and is observed for cancellation while
        // queued). Build a SearchTask whose workload group id is the requested one via a real thread-context header
        // (mockThreadPool is a Mockito mock, so use a self-contained ThreadContext here).
        WorkloadGroupTask task = new SearchTask(1, "", "", () -> "", null, null);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        if (workloadGroupId != null) {
            threadContext.putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, workloadGroupId);
        }
        task.setWorkloadGroupId(threadContext);
        service.acquireThrottlePermit(task, principal, ActionListener.wrap(permit::set, failure::set));
        if (failure.get() != null) {
            if (failure.get() instanceof RuntimeException) {
                throw (RuntimeException) failure.get();
            }
            throw new RuntimeException(failure.get());
        }
        return permit.get();
    }

    private WorkloadGroup throttledGroup(String id, Settings throttling) {
        return throttledGroup(id, throttling, MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED);
    }

    private WorkloadGroup throttledGroup(String id, Settings throttling, MutableWorkloadGroupFragment.ResiliencyMode mode) {
        return new WorkloadGroup(
            id + "-name",
            id,
            new MutableWorkloadGroupFragment(mode, Map.of(ResourceType.MEMORY, 0.5), Settings.EMPTY, throttling),
            1L
        );
    }

    // Same as throttledGroup but with queueing enabled, so a throttle denial can park instead of rejecting.
    private WorkloadGroup queueingGroup(String id, Settings throttling, Settings queue, MutableWorkloadGroupFragment.ResiliencyMode mode) {
        return new WorkloadGroup(
            id + "-name",
            id,
            new MutableWorkloadGroupFragment(mode, Map.of(ResourceType.MEMORY, 0.5), Settings.EMPTY, throttling, queue),
            1L
        );
    }

    // Delivers a clusterChanged event whose CURRENT state holds exactly `currentGroups` (previous state holds `previous`).
    private void deliverWorkloadGroupsChanged(Map<String, WorkloadGroup> previous, Map<String, WorkloadGroup> currentGroups) {
        ClusterChangedEvent event = Mockito.mock(ClusterChangedEvent.class);
        ClusterState previousState = Mockito.mock(ClusterState.class);
        ClusterState currentState = Mockito.mock(ClusterState.class);
        Metadata previousMetadata = Mockito.mock(Metadata.class);
        Metadata currentMetadata = Mockito.mock(Metadata.class);
        when(event.previousState()).thenReturn(previousState);
        when(event.state()).thenReturn(currentState);
        when(previousState.metadata()).thenReturn(previousMetadata);
        when(currentState.metadata()).thenReturn(currentMetadata);
        when(previousMetadata.workloadGroups()).thenReturn(previous);
        when(currentMetadata.workloadGroups()).thenReturn(currentGroups);
        workloadGroupService.clusterChanged(event);
    }

    public void testDisablingThrottlingImmediatelyReleasesTheQueuedBacklog() {
        // Disabling throttling leaves parked requests with nothing to wait for, and an unthrottled group takes the
        // no-permit fast path — so it produces no permit completions to drive a drain chain, and parked requests have no
        // deadline. Without an explicit release they wait forever. Reacting to the config change must free them at once
        // (the sweep is only the backstop). Note disabling throttling necessarily disables queueing in the same update:
        // WorkloadGroup rejects a queue with no throttle limit, and validateMergedConfig rejects an all-unset throttling
        // block, so "throttling off, queueing on" is unreachable and this is the only way to strand a backlog.
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        when(mockThreadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(OpenSearchExecutors.newDirectExecutorService());

        Settings throttling = Settings.builder().put("attribute", "group").put("node_limit", 1).build();
        Settings queue = Settings.builder().put("size_per_bucket", 5).build();
        WorkloadGroup throttled = queueingGroup("wg-1", throttling, queue, MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED);
        stubClusterStateWithGroup(throttled);
        WorkloadGroupQueueService queueService = new WorkloadGroupQueueService(mockThreadPool, mockWorkloadGroupsStateAccessor);
        workloadGroupService.setQueueService(queueService);

        // One request holds the single node slot; two more park behind it.
        assertNotNull(acquireThrottlePermitSync(workloadGroupService, "wg-1", null));
        assertNull(acquireThrottlePermitSync(workloadGroupService, "wg-1", null));
        assertNull(acquireThrottlePermitSync(workloadGroupService, "wg-1", null));
        assertEquals("two requests should be parked", 2, queueService.currentDepth("wg-1"));

        // Operator disables throttling (and therefore queueing) on the group.
        WorkloadGroup unthrottled = new WorkloadGroup(
            "wg-1-name",
            "wg-1",
            new MutableWorkloadGroupFragment(MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED, Map.of(ResourceType.MEMORY, 0.5)),
            2L
        );
        deliverWorkloadGroupsChanged(Map.of("wg-1", throttled), Map.of("wg-1", unthrottled));

        assertEquals("the whole backlog must be released as soon as throttling is disabled", 0, queueService.currentDepth("wg-1"));
    }

    public void testCompletionDrainHonoursALiveNodeLimitDecrease() {
        // Regression: the node-tier drain chain used to capture node_limit when the chain started and reuse it for every
        // subsequent hop. A busy bucket's chain runs one hop per request completion, so it can outlive a live node_limit
        // update — and reusing the captured (higher) value kept admitting above the NEW lower limit until the chain broke.
        // The drain must re-read node_limit from cluster state instead.
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings queue = Settings.builder().put("size_per_bucket", 5).build();

        // Start at node_limit=2 and fill both slots, then park a third request.
        Settings limit2 = Settings.builder().put("attribute", "group").put("node_limit", 2).build();
        stubClusterStateWithGroup(queueingGroup("wg-1", limit2, queue, MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED));
        // This is the only test here that actually admits FROM the queue, so it needs a real executor: admit()
        // dispatches the parked listener off the caller thread. Direct (same-thread) keeps the assertions synchronous —
        // depth is decremented under the bucket lock before admit() runs, so inline dispatch does not affect what we assert.
        when(mockThreadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(OpenSearchExecutors.newDirectExecutorService());
        WorkloadGroupQueueService queueService = new WorkloadGroupQueueService(mockThreadPool, mockWorkloadGroupsStateAccessor);
        workloadGroupService.setQueueService(queueService);

        Releasable first = acquireThrottlePermitSync(workloadGroupService, "wg-1", null);
        Releasable second = acquireThrottlePermitSync(workloadGroupService, "wg-1", null);
        assertNotNull(first);
        assertNotNull(second);
        // The 3rd breaches node_limit=2 with no shared tier configured, so it parks.
        assertNull("third request should be parked, not admitted", acquireThrottlePermitSync(workloadGroupService, "wg-1", null));
        assertEquals(1, queueService.currentDepth("wg-1"));

        // Operator lowers node_limit to 1 while the bucket is busy with a backlog.
        Settings limit1 = Settings.builder().put("attribute", "group").put("node_limit", 1).build();
        stubClusterStateWithGroup(queueingGroup("wg-1", limit1, queue, MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED));

        // Completing one request drops in-flight to 1, which is already AT the new limit — so the drain must not admit
        // the parked request. With the old captured limit of 2 it would have, over-admitting above the configured 1.
        first.close();
        assertEquals("drain must respect the lowered node_limit, leaving the request parked", 1, queueService.currentDepth("wg-1"));

        // Completing the second drops in-flight to 0, leaving room under the new limit -> the parked request drains.
        second.close();
        assertEquals("a slot under the new limit must still drain the backlog", 0, queueService.currentDepth("wg-1"));
    }

    public void testMonitorModeNeverParksEvenWhenQueueingIsEnabled() {
        // MONITOR observes and always admits — it must never park a request. Both queueing entry points are gated on
        // monitorMode == false; without that guard a would-be-throttled monitor request would sit in the queue holding
        // its listener and never be answered (monitor mode is supposed to be a dry run, so this would be a hang, not a
        // rejection). Note a parked request and an admitted monitor request BOTH surface as a null permit here, so the
        // queue depth is what actually distinguishes them.
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("attribute", "group").put("node_limit", 1).build();
        Settings queue = Settings.builder().put("size_per_bucket", 5).build();
        stubClusterStateWithGroup(queueingGroup("wg-1", throttling, queue, MutableWorkloadGroupFragment.ResiliencyMode.MONITOR));
        WorkloadGroupQueueService queueService = new WorkloadGroupQueueService(mockThreadPool, mockWorkloadGroupsStateAccessor);
        workloadGroupService.setQueueService(queueService);

        assertNotNull(acquireThrottlePermitSync(workloadGroupService, "wg-1", null)); // fills node_limit=1
        // The 2nd request breaches node_limit. Under MONITOR it is admitted untracked (null permit), never queued.
        assertNull(acquireThrottlePermitSync(workloadGroupService, "wg-1", null));
        assertEquals("a monitor-mode request must never be parked", 0, queueService.currentDepth("wg-1"));
        assertEquals(0, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());
    }

    public void testAcquireThrottleReturnsNullWhenNodeLimitUnset() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        stubClusterStateWithGroup(throttledGroup("wg-1", Settings.EMPTY)); // throttling not configured
        assertNull(acquireThrottlePermitSync(workloadGroupService, "wg-1", null));
    }

    public void testAcquireThrottleReturnsNullWhenWlmDisabled() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.DISABLED);
        assertNull(acquireThrottlePermitSync(workloadGroupService, "wg-1", null));
    }

    public void testAcquireThrottleRejectsAtLimitAndIncrementsStat() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("attribute", "group").put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));

        Releasable permit = acquireThrottlePermitSync(workloadGroupService, "wg-1", null); // first admit succeeds
        assertNotNull(permit);
        // second admit hits node_limit of 1 -> 429 + total_throttled incremented
        OpenSearchRejectedExecutionException e = expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> acquireThrottlePermitSync(workloadGroupService, "wg-1", null)
        );
        assertEquals("Request throttled: workload group [wg-1-name] reached its per-node limit of 1 concurrent requests.", e.getMessage());
        assertEquals(1, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());

        // releasing the first permit frees the slot so a subsequent acquire succeeds
        permit.close();
        assertNotNull(acquireThrottlePermitSync(workloadGroupService, "wg-1", null));
    }

    public void testMonitorModeObservesButDoesNotThrottle() throws Exception {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("attribute", "group").put("node_limit", 1).build();
        // Same node_limit=1 as the reject test, but the group is in MONITOR resiliency mode.
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling, MutableWorkloadGroupFragment.ResiliencyMode.MONITOR));

        // The would-be-throttle log is at DEBUG; enable it and capture it.
        Logger serviceLogger = LogManager.getLogger(WorkloadGroupService.class);
        Level previousLevel = serviceLogger.getLevel();
        Loggers.setLevel(serviceLogger, Level.DEBUG);
        try (MockLogAppender appender = MockLogAppender.createForLoggers(serviceLogger)) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "monitor would-throttle log",
                    WorkloadGroupService.class.getCanonicalName(),
                    Level.DEBUG,
                    "Request would be throttled (monitor mode, not rejected): workload group [wg-1-name] "
                        + "reached its per-node limit of 1 concurrent requests."
                )
            );

            // First request fills the single node slot (still tracked, so a breach can be observed).
            Releasable permit = acquireThrottlePermitSync(workloadGroupService, "wg-1", null);
            assertNotNull(permit);

            // Second request would breach node_limit -> under MONITOR it is ADMITTED (null permit), not rejected,
            // total_throttled is NOT incremented, and the would-throttle line is logged.
            assertNull(acquireThrottlePermitSync(workloadGroupService, "wg-1", null));
            assertEquals(0, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());
            appender.assertAllExpectationsMatched();

            permit.close();
        } finally {
            Loggers.setLevel(serviceLogger, previousLevel);
        }
    }

    public void testMonitorModeLogNamesTheUsernameAttribute() throws Exception {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("attribute", "username").put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling, MutableWorkloadGroupFragment.ResiliencyMode.MONITOR));

        Logger serviceLogger = LogManager.getLogger(WorkloadGroupService.class);
        Level previousLevel = serviceLogger.getLevel();
        Loggers.setLevel(serviceLogger, Level.DEBUG);
        try (MockLogAppender appender = MockLogAppender.createForLoggers(serviceLogger)) {
            // The monitor log must render the resolved username in the same "for username [alice]" form as the 429.
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "monitor would-throttle log names username",
                    WorkloadGroupService.class.getCanonicalName(),
                    Level.DEBUG,
                    "Request would be throttled (monitor mode, not rejected): workload group [wg-1-name] for username [alice] "
                        + "reached its per-node limit of 1 concurrent requests."
                )
            );

            // alice fills her single per-user slot; her second request would breach -> observed, admitted, no stat.
            Releasable permit = acquireThrottlePermitSync(workloadGroupService, "wg-1", "username|alice");
            assertNotNull(permit);
            assertNull(acquireThrottlePermitSync(workloadGroupService, "wg-1", "username|alice"));
            assertEquals(0, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());
            appender.assertAllExpectationsMatched();

            permit.close();
        } finally {
            Loggers.setLevel(serviceLogger, previousLevel);
        }
    }

    public void testAcquireThrottleUsernameKeepsPerUserBuckets() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("attribute", "username").put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));

        // alice takes her single slot; a second alice request is rejected — and the message names the username.
        Releasable alice = acquireThrottlePermitSync(workloadGroupService, "wg-1", "username|alice");
        assertNotNull(alice);
        OpenSearchRejectedExecutionException e = expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> acquireThrottlePermitSync(workloadGroupService, "wg-1", "username|alice")
        );
        assertEquals(
            "Request throttled: workload group [wg-1-name] for username [alice] reached its per-node limit of 1 concurrent requests.",
            e.getMessage()
        );
        assertEquals(1, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());

        // bob is a different bucket, so he is admitted even while alice is at her limit.
        Releasable bob = acquireThrottlePermitSync(workloadGroupService, "wg-1", "username|bob");
        assertNotNull(bob);

        // releasing alice frees her bucket
        alice.close();
        assertNotNull(acquireThrottlePermitSync(workloadGroupService, "wg-1", "username|alice"));
    }

    public void testAcquireThrottleUsernameWithCommaDoesNotCollide() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("attribute", "username").put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));

        String delim = WorkloadGroupTask.WORKLOAD_GROUP_PRINCIPAL_VALUE_DELIMITER;
        // principal for user "a,b" with a role token appended
        String userAB = "username|a,b" + delim + "role|admin";
        // user "a" is a genuinely different principal
        String userA = "username|a";

        Releasable ab = acquireThrottlePermitSync(workloadGroupService, "wg-1", userAB); // fills "a,b" bucket
        assertNotNull(ab);
        // user "a" must NOT be treated as the same bucket as "a,b" -> still admitted
        assertNotNull(acquireThrottlePermitSync(workloadGroupService, "wg-1", userA));
        // a second "a,b" request hits the "a,b" bucket limit -> rejected
        expectThrows(OpenSearchRejectedExecutionException.class, () -> acquireThrottlePermitSync(workloadGroupService, "wg-1", userAB));
    }

    public void testAcquireThrottleRolePicksMatchingSubfieldFromMultiTokenPrincipal() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("attribute", "role").put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));

        // A principal header may carry both subfields; the role bucket must key off the role token only.
        String delim = WorkloadGroupTask.WORKLOAD_GROUP_PRINCIPAL_VALUE_DELIMITER;
        assertNotNull(acquireThrottlePermitSync(workloadGroupService, "wg-1", "username|alice" + delim + "role|admin"));
        expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> acquireThrottlePermitSync(workloadGroupService, "wg-1", "username|bob" + delim + "role|admin")
        );
        assertEquals(1, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());
    }

    public void testAcquireThrottleFailsOpenWhenPrincipalMissingForUsername() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("attribute", "username").put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));

        // No principal (e.g. security plugin not installed) or no matching subfield -> not throttled (fail open).
        assertNull(acquireThrottlePermitSync(workloadGroupService, "wg-1", null));
        assertNull(acquireThrottlePermitSync(workloadGroupService, "wg-1", ""));
        assertNull(acquireThrottlePermitSync(workloadGroupService, "wg-1", "role|admin")); // no username token
        assertEquals(0, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());
    }

    /**
     * A failure while recording the total_throttled stat must NOT swallow the rejection and admit the over-limit
     * request. Whether the state map lookup returns null (group not yet registered / just deleted) or throws, the
     * 429 must still propagate.
     */
    public void testAcquireThrottleStillRejectsWhenStatUpdateFails() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        Settings throttling = Settings.builder().put("attribute", "group").put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));

        // state map with no entry for wg-1 (as during the state-registration lag) -> raw get(id) returns null
        WorkloadGroupsStateAccessor emptyMapAccessor = Mockito.mock(WorkloadGroupsStateAccessor.class);
        when(emptyMapAccessor.getWorkloadGroupStateMap()).thenReturn(new HashMap<>());
        WorkloadGroupService serviceWithNullState = new WorkloadGroupService(
            mockCancellationService,
            mockClusterService,
            mockThreadPool,
            mockWorkloadManagementSettings,
            mockNodeDuressTrackers,
            emptyMapAccessor,
            new HashSet<>(),
            new HashSet<>()
        );

        assertNotNull(acquireThrottlePermitSync(serviceWithNullState, "wg-1", null)); // first admit fills the single slot
        // second acquire is over the limit; a null state must not let the stat update swallow the 429
        expectThrows(OpenSearchRejectedExecutionException.class, () -> acquireThrottlePermitSync(serviceWithNullState, "wg-1", null));

        // accessor whose state-map lookup throws must also still propagate the 429
        WorkloadGroupsStateAccessor throwingStateAccessor = Mockito.mock(WorkloadGroupsStateAccessor.class);
        when(throwingStateAccessor.getWorkloadGroupStateMap()).thenThrow(new RuntimeException("state map race"));
        WorkloadGroupService serviceWithThrowingState = new WorkloadGroupService(
            mockCancellationService,
            mockClusterService,
            mockThreadPool,
            mockWorkloadManagementSettings,
            mockNodeDuressTrackers,
            throwingStateAccessor,
            new HashSet<>(),
            new HashSet<>()
        );

        assertNotNull(acquireThrottlePermitSync(serviceWithThrowingState, "wg-1", null)); // fills the single slot
        expectThrows(OpenSearchRejectedExecutionException.class, () -> acquireThrottlePermitSync(serviceWithThrowingState, "wg-1", null));
    }

    /**
     * During the state-registration lag a node can enforce a new group's limit before its clusterChanged() registers
     * the state. The rejection stat must not be misattributed to the DEFAULT group in that window.
     */
    public void testAcquireThrottleDoesNotMisattributeToDefaultDuringRegistrationLag() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        Settings throttling = Settings.builder().put("attribute", "group").put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));

        // DEFAULT group state exists, but wg-1 is NOT yet registered (registration lag).
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup(WorkloadGroupTask.DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get());

        assertNotNull(acquireThrottlePermitSync(workloadGroupService, "wg-1", null)); // fills the single slot
        expectThrows(OpenSearchRejectedExecutionException.class, () -> acquireThrottlePermitSync(workloadGroupService, "wg-1", null));

        // the rejection must NOT have landed on the DEFAULT group
        assertEquals(
            0,
            mockWorkloadGroupsStateAccessor.getWorkloadGroupState(WorkloadGroupTask.DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get())
                .getTotalThrottled()
        );
    }

    /**
     * The headline two-tier path: node_limit=1 + shared_limit=1 with a shared throttle service wired in. The first
     * request takes the local slot, the second overflows and takes the shared slot, and the third is rejected (both
     * tiers full) with total_throttled incremented. Uses a single-data-node ring so the shared acquire resolves to
     * this node and runs via the local-owner short-circuit (no real network).
     */
    public void testAdmitFallsThroughNodeTierToSharedTier() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");

        Settings throttling = Settings.builder().put("attribute", "group").put("node_limit", 1).put("shared_limit", 1).build();
        WorkloadGroup group = throttledGroup("wg-1", throttling);

        // Cluster state must expose both the group metadata and a single data node (so this node owns every bucket).
        DiscoveryNode localNode = new DiscoveryNode(
            "local",
            "local",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            org.opensearch.Version.CURRENT
        );
        DiscoveryNodes nodes = DiscoveryNodes.builder().add(localNode).localNodeId("local").build();
        ClusterState clusterState = Mockito.mock(ClusterState.class);
        Metadata metadata = Mockito.mock(Metadata.class);
        when(mockClusterService.state()).thenReturn(clusterState);
        when(mockClusterService.localNode()).thenReturn(localNode);
        when(clusterState.metadata()).thenReturn(metadata);
        when(clusterState.nodes()).thenReturn(nodes);
        when(metadata.workloadGroups()).thenReturn(Map.of(group.get_id(), group));

        WorkloadGroupSharedThrottleService sharedService = new WorkloadGroupSharedThrottleService(
            mockClusterService,
            mockThreadPool,
            Mockito.mock(org.opensearch.transport.TransportService.class)
        );
        // Populate the ring (the constructor starts empty; a nodes-changed event builds it — matches production).
        ClusterState previous = Mockito.mock(ClusterState.class);
        when(previous.nodes()).thenReturn(DiscoveryNodes.EMPTY_NODES);
        sharedService.clusterChanged(new ClusterChangedEvent("test", clusterState, previous));
        workloadGroupService.setSharedThrottleService(sharedService);

        // 1st: local node_limit slot.
        Releasable local = acquireThrottlePermitSync(workloadGroupService, "wg-1", null);
        assertNotNull(local);
        // 2nd: local exhausted -> falls through to shared_limit slot.
        Releasable shared = acquireThrottlePermitSync(workloadGroupService, "wg-1", null);
        assertNotNull(shared);
        // 3rd: both tiers full -> 429 from the shared tier (with the cluster-wide message), and total_throttled bumped.
        OpenSearchRejectedExecutionException e = expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> acquireThrottlePermitSync(workloadGroupService, "wg-1", null)
        );
        assertEquals(
            "Request throttled: workload group [wg-1-name] reached its cluster-wide limit of 1 concurrent requests.",
            e.getMessage()
        );
        assertEquals(1, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());

        // Releasing the shared permit frees the shared slot so a subsequent request is admitted again.
        shared.close();
        assertNotNull(acquireThrottlePermitSync(workloadGroupService, "wg-1", null));
    }

    public void testMonitorModeObservesButDoesNotThrottleOnSharedTier() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");

        Settings throttling = Settings.builder().put("attribute", "group").put("node_limit", 1).put("shared_limit", 1).build();
        // Same two-tier config as testAdmitFallsThroughNodeTierToSharedTier, but MONITOR resiliency mode.
        WorkloadGroup group = throttledGroup("wg-1", throttling, MutableWorkloadGroupFragment.ResiliencyMode.MONITOR);

        DiscoveryNode localNode = new DiscoveryNode(
            "local",
            "local",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            org.opensearch.Version.CURRENT
        );
        DiscoveryNodes nodes = DiscoveryNodes.builder().add(localNode).localNodeId("local").build();
        ClusterState clusterState = Mockito.mock(ClusterState.class);
        Metadata metadata = Mockito.mock(Metadata.class);
        when(mockClusterService.state()).thenReturn(clusterState);
        when(mockClusterService.localNode()).thenReturn(localNode);
        when(clusterState.metadata()).thenReturn(metadata);
        when(clusterState.nodes()).thenReturn(nodes);
        when(metadata.workloadGroups()).thenReturn(Map.of(group.get_id(), group));

        WorkloadGroupSharedThrottleService sharedService = new WorkloadGroupSharedThrottleService(
            mockClusterService,
            mockThreadPool,
            Mockito.mock(org.opensearch.transport.TransportService.class)
        );
        ClusterState previous = Mockito.mock(ClusterState.class);
        when(previous.nodes()).thenReturn(DiscoveryNodes.EMPTY_NODES);
        sharedService.clusterChanged(new ClusterChangedEvent("test", clusterState, previous));
        workloadGroupService.setSharedThrottleService(sharedService);

        // 1st fills the local slot, 2nd fills the shared slot.
        assertNotNull(acquireThrottlePermitSync(workloadGroupService, "wg-1", null));
        assertNotNull(acquireThrottlePermitSync(workloadGroupService, "wg-1", null));
        // 3rd would breach the shared limit -> under MONITOR it is ADMITTED (null), not rejected, and no stat update.
        assertNull(acquireThrottlePermitSync(workloadGroupService, "wg-1", null));
        assertEquals(0, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());
    }

    public void testMonitorModeNeverParksOnSharedTierWithQueueingEnabled() {
        // The shared tier takes the ENQUEUE-FIRST path, which parks the request BEFORE asking the owner for a slot. That
        // path is gated on `queueSizePerBucket > 0 && monitorMode == false`; this pins the monitorMode half. Without it a
        // monitor-mode request would be parked before the acquire even happens — i.e. MONITOR would silently start
        // holding requests instead of being a dry run. The node-tier equivalent is guarded separately inside
        // onThrottleBreach, so this is the case that actually covers the enqueue-first gate.
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");

        Settings throttling = Settings.builder().put("attribute", "group").put("node_limit", 1).put("shared_limit", 1).build();
        Settings queue = Settings.builder().put("size_per_bucket", 5).build();
        WorkloadGroup group = queueingGroup("wg-1", throttling, queue, MutableWorkloadGroupFragment.ResiliencyMode.MONITOR);

        DiscoveryNode localNode = new DiscoveryNode(
            "local",
            "local",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            org.opensearch.Version.CURRENT
        );
        DiscoveryNodes nodes = DiscoveryNodes.builder().add(localNode).localNodeId("local").build();
        ClusterState clusterState = Mockito.mock(ClusterState.class);
        Metadata metadata = Mockito.mock(Metadata.class);
        when(mockClusterService.state()).thenReturn(clusterState);
        when(mockClusterService.localNode()).thenReturn(localNode);
        when(clusterState.metadata()).thenReturn(metadata);
        when(clusterState.nodes()).thenReturn(nodes);
        when(metadata.workloadGroups()).thenReturn(Map.of(group.get_id(), group));

        WorkloadGroupSharedThrottleService sharedService = new WorkloadGroupSharedThrottleService(
            mockClusterService,
            mockThreadPool,
            Mockito.mock(org.opensearch.transport.TransportService.class)
        );
        ClusterState previous = Mockito.mock(ClusterState.class);
        when(previous.nodes()).thenReturn(DiscoveryNodes.EMPTY_NODES);
        sharedService.clusterChanged(new ClusterChangedEvent("test", clusterState, previous));
        workloadGroupService.setSharedThrottleService(sharedService);
        WorkloadGroupQueueService queueService = new WorkloadGroupQueueService(mockThreadPool, mockWorkloadGroupsStateAccessor);
        workloadGroupService.setQueueService(queueService);

        // 1st fills the local slot, 2nd fills the shared slot.
        assertNotNull(acquireThrottlePermitSync(workloadGroupService, "wg-1", null));
        assertNotNull(acquireThrottlePermitSync(workloadGroupService, "wg-1", null));
        // 3rd overflows to the shared tier at its limit. Under MONITOR it must be admitted untracked, never parked.
        assertNull(acquireThrottlePermitSync(workloadGroupService, "wg-1", null));
        assertEquals("a monitor-mode request must never be parked on the shared tier", 0, queueService.currentDepth("wg-1"));
        assertEquals(0, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());
    }

    public void testShouldSBPHandle() {
        SearchTask task = createMockTaskWithResourceStats(SearchTask.class, 100, 200, 0, 12);
        WorkloadGroupState workloadGroupState = new WorkloadGroupState();
        Set<WorkloadGroup> activeWorkloadGroups = new HashSet<>();
        mockWorkloadGroupStateMap.put("testId", workloadGroupState);
        mockWorkloadGroupsStateAccessor = new WorkloadGroupsStateAccessor(mockWorkloadGroupStateMap);
        workloadGroupService = new WorkloadGroupService(
            mockCancellationService,
            mockClusterService,
            mockThreadPool,
            mockWorkloadManagementSettings,
            mockNodeDuressTrackers,
            mockWorkloadGroupsStateAccessor,
            activeWorkloadGroups,
            Collections.emptySet()
        );

        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);

        // Default workloadGroupId
        mockThreadPool = new TestThreadPool("workloadGroupServiceTests");
        mockThreadPool.getThreadContext()
            .putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, WorkloadGroupTask.DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get());
        // we haven't set the workloadGroupId yet SBP should still track the task for cancellation
        assertTrue(workloadGroupService.shouldSBPHandle(task));
        task.setWorkloadGroupId(mockThreadPool.getThreadContext());
        assertTrue(workloadGroupService.shouldSBPHandle(task));

        mockThreadPool.shutdownNow();

        // invalid workloadGroup task
        mockThreadPool = new TestThreadPool("workloadGroupServiceTests");
        mockThreadPool.getThreadContext().putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, "testId");
        task.setWorkloadGroupId(mockThreadPool.getThreadContext());
        assertTrue(workloadGroupService.shouldSBPHandle(task));

        // Valid workload group task but wlm not enabled
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.DISABLED);
        activeWorkloadGroups.add(
            new WorkloadGroup(
                "testWorkloadGroup",
                "testId",
                new MutableWorkloadGroupFragment(
                    MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED,
                    Map.of(ResourceType.CPU, 0.10, ResourceType.MEMORY, 0.10)
                ),
                1L
            )
        );
        assertTrue(workloadGroupService.shouldSBPHandle(task));

        mockThreadPool.shutdownNow();

        // test the case when SBP should not track the task
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        task = new SearchTask(1, "", "test", () -> "", null, null);
        mockThreadPool = new TestThreadPool("workloadGroupServiceTests");
        mockThreadPool.getThreadContext().putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, "testId");
        task.setWorkloadGroupId(mockThreadPool.getThreadContext());
        assertFalse(workloadGroupService.shouldSBPHandle(task));
    }

    private static Set<WorkloadGroup> getActiveWorkloadGroups(
        String name,
        String id,
        MutableWorkloadGroupFragment.ResiliencyMode mode,
        Map<ResourceType, Double> resourceLimits
    ) {
        WorkloadGroup testWorkloadGroup = getWorkloadGroup(name, id, mode, resourceLimits);
        Set<WorkloadGroup> activeWorkloadGroups = new HashSet<>() {
            {
                add(testWorkloadGroup);
            }
        };
        return activeWorkloadGroups;
    }

    private static WorkloadGroup getWorkloadGroup(
        String name,
        String id,
        MutableWorkloadGroupFragment.ResiliencyMode mode,
        Map<ResourceType, Double> resourceLimits
    ) {
        WorkloadGroup testWorkloadGroup = new WorkloadGroup(name, id, new MutableWorkloadGroupFragment(mode, resourceLimits), 1L);
        return testWorkloadGroup;
    }

    // This is needed to test the behavior of WorkloadGroupService#doRun method
    static class TestWorkloadGroupCancellationService extends WorkloadGroupTaskCancellationService {
        public TestWorkloadGroupCancellationService(
            WorkloadManagementSettings workloadManagementSettings,
            TaskSelectionStrategy taskSelectionStrategy,
            WorkloadGroupResourceUsageTrackerService resourceUsageTrackerService,
            WorkloadGroupsStateAccessor workloadGroupsStateAccessor,
            Collection<WorkloadGroup> activeWorkloadGroups,
            Collection<WorkloadGroup> deletedWorkloadGroups
        ) {
            super(workloadManagementSettings, taskSelectionStrategy, resourceUsageTrackerService, workloadGroupsStateAccessor);
        }

        @Override
        public void cancelTasks(
            BooleanSupplier isNodeInDuress,
            Collection<WorkloadGroup> activeWorkloadGroups,
            Collection<WorkloadGroup> deletedWorkloadGroups
        ) {

        }
    }
}
