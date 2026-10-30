/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.Version;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchExecutionGate;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchScrollAction;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.action.search.SearchTask;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.search.backpressure.trackers.NodeDuressTrackers;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.wlm.cancellation.TaskSelectionStrategy;
import org.opensearch.wlm.cancellation.WorkloadGroupTaskCancellationService;
import org.opensearch.wlm.stats.WorkloadGroupState;
import org.opensearch.wlm.tracker.WorkloadGroupResourceUsageTrackerService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

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
    private BiConsumer<WlmMode, WlmMode> wlmModeChangeListener;
    NodeDuressTrackers mockNodeDuressTrackers;
    WorkloadGroupsStateAccessor mockWorkloadGroupsStateAccessor;

    public void setUp() throws Exception {
        super.setUp();
        mockClusterService = Mockito.mock(ClusterService.class);
        mockThreadPool = Mockito.mock(ThreadPool.class);
        // The queue wraps retained listeners to restore the caller's thread context, so the mock needs a real one.
        when(mockThreadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        mockScheduledFuture = Mockito.mock(Scheduler.Cancellable.class);
        mockWorkloadManagementSettings = Mockito.mock(WorkloadManagementSettings.class);
        mockWorkloadGroupStateMap = new HashMap<>();
        mockNodeDuressTrackers = Mockito.mock(NodeDuressTrackers.class);
        mockCancellationService = Mockito.mock(TestWorkloadGroupCancellationService.class);
        mockWorkloadGroupsStateAccessor = new WorkloadGroupsStateAccessor();
        when(mockNodeDuressTrackers.isNodeInDuress()).thenReturn(false);
        Mockito.doAnswer(invocation -> {
            wlmModeChangeListener = invocation.getArgument(0);
            return null;
        }).when(mockWorkloadManagementSettings).addWlmModeChangeListener(any());

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

    public void testIsThrottlingActiveFollowsGroupThrottlingAndWlmMode() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        assertFalse("no cluster state applied yet", workloadGroupService.isThrottlingActive());

        WorkloadGroup unthrottled = throttledGroup("wg-1", Settings.EMPTY);
        WorkloadGroup throttled = throttledGroup("wg-1", Settings.builder().put("node_limit", 1).build());
        applyWorkloadGroups(Map.of(), Map.of("wg-1", unthrottled));
        assertFalse("no group configures throttling", workloadGroupService.isThrottlingActive());

        applyWorkloadGroups(Map.of("wg-1", unthrottled), Map.of("wg-1", throttled));
        assertTrue(workloadGroupService.isThrottlingActive());
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.MONITOR_ONLY);
        assertFalse("throttling only applies in enabled mode", workloadGroupService.isThrottlingActive());
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.DISABLED);
        assertFalse(workloadGroupService.isThrottlingActive());
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        assertTrue(workloadGroupService.isThrottlingActive());

        applyWorkloadGroups(Map.of("wg-1", throttled), Map.of());
        assertFalse("the throttled group was deleted", workloadGroupService.isThrottlingActive());
    }

    private void applyWorkloadGroups(Map<String, WorkloadGroup> previous, Map<String, WorkloadGroup> current) {
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
        when(currentMetadata.workloadGroups()).thenReturn(current);
        workloadGroupService.clusterChanged(event);
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

    public void testQueueBackstopRunsEveryFiveSecondsWithoutSlowingEnforcement() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        WorkloadGroupQueueService queueService = Mockito.mock(WorkloadGroupQueueService.class);
        workloadGroupService.setQueueService(queueService);
        when(mockThreadPool.relativeTimeInNanos()).thenReturn(
            0L,
            WorkloadGroupService.QUEUE_BACKSTOP_SWEEP_INTERVAL_NANOS - 1L,
            WorkloadGroupService.QUEUE_BACKSTOP_SWEEP_INTERVAL_NANOS
        );

        workloadGroupService.doRun();
        workloadGroupService.doRun();
        workloadGroupService.doRun();

        verify(queueService, times(2)).sweep(any());
        verify(mockCancellationService, times(3)).cancelTasks(any(), any(), any());
        verify(mockCancellationService, times(3)).pruneDeletedWorkloadGroups(any());
    }

    public void testQueueBackstopCadenceHoldsForNegativeRelativeClock() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        WorkloadGroupQueueService queueService = Mockito.mock(WorkloadGroupQueueService.class);
        workloadGroupService.setQueueService(queueService);
        // System.nanoTime's origin is arbitrary (possibly in the future), so the relative clock may read negative.
        final long origin = -10 * WorkloadGroupService.QUEUE_BACKSTOP_SWEEP_INTERVAL_NANOS;
        when(mockThreadPool.relativeTimeInNanos()).thenReturn(
            origin,
            origin + WorkloadGroupService.QUEUE_BACKSTOP_SWEEP_INTERVAL_NANOS - 1L,
            origin + WorkloadGroupService.QUEUE_BACKSTOP_SWEEP_INTERVAL_NANOS
        );

        workloadGroupService.doRun();
        workloadGroupService.doRun();
        workloadGroupService.doRun();

        verify(queueService, times(2)).sweep(any());
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

    public void testRejectIfNeeded_whenWorkloadGroupIsMonitorMode() {
        Set<WorkloadGroup> activeWorkloadGroups = getActiveWorkloadGroups(
            "testWorkloadGroup",
            WORKLOAD_GROUP_ID,
            MutableWorkloadGroupFragment.ResiliencyMode.MONITOR,
            Map.of(ResourceType.CPU, 0.10)
        );
        mockWorkloadGroupStateMap = new HashMap<>();
        WorkloadGroupState workloadGroupState = new WorkloadGroupState();
        workloadGroupState.getResourceState().get(ResourceType.CPU).setLastRecordedUsage(0.18);
        mockWorkloadGroupStateMap.put(WORKLOAD_GROUP_ID, workloadGroupState);
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
        when(mockNodeDuressTrackers.isNodeInDuress()).thenReturn(true);

        workloadGroupService.rejectIfNeeded(WORKLOAD_GROUP_ID);

        assertEquals(0, workloadGroupState.getResourceState().get(ResourceType.CPU).rejections.count());
        assertEquals(0, workloadGroupState.totalRejections.count());
    }

    public void testRejectIfNeeded_whenWorkloadGroupIsAbsentFromActiveWorkloadGroups() {
        Set<WorkloadGroup> activeWorkloadGroups = getActiveWorkloadGroups(
            "otherWorkloadGroup",
            "otherWorkloadGroupId",
            MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED,
            Map.of(ResourceType.CPU, 0.10)
        );
        mockWorkloadGroupStateMap = new HashMap<>();
        WorkloadGroupState workloadGroupState = new WorkloadGroupState();
        workloadGroupState.getResourceState().get(ResourceType.CPU).setLastRecordedUsage(0.18);
        mockWorkloadGroupStateMap.put(WORKLOAD_GROUP_ID, workloadGroupState);
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

        workloadGroupService.rejectIfNeeded(WORKLOAD_GROUP_ID);

        assertEquals(0, workloadGroupState.getResourceState().get(ResourceType.CPU).rejections.count());
        assertEquals(0, workloadGroupState.totalRejections.count());
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
        String workloadGroupId = wg.get_id();
        when(mockClusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.workloadGroups()).thenReturn(Map.of(workloadGroupId, wg));
    }

    private void stubClusterStateWithNoGroups() {
        ClusterState clusterState = Mockito.mock(ClusterState.class);
        Metadata metadata = Mockito.mock(Metadata.class);
        when(mockClusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.workloadGroups()).thenReturn(Map.of());
    }

    private WorkloadGroupSharedThrottleService stubLocalSharedService(WorkloadGroup workloadGroup) {
        return stubLocalSharedService(workloadGroup, Set.of(DiscoveryNodeRole.DATA_ROLE));
    }

    // A local node without the data role leaves the ring with no eligible owner.
    private WorkloadGroupSharedThrottleService stubLocalSharedService(WorkloadGroup workloadGroup, Set<DiscoveryNodeRole> localRoles) {
        DiscoveryNode localNode = node("local", localRoles);
        return stubSharedService(workloadGroup, DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).build());
    }

    // A real shared service on this node with `nodes` applied, over a transport that is connected to no other node.
    private WorkloadGroupSharedThrottleService stubSharedService(WorkloadGroup workloadGroup, DiscoveryNodes nodes) {
        stubClusterStateWithGroup(workloadGroup);
        ClusterState clusterState = mockClusterService.state();
        when(clusterState.nodes()).thenReturn(nodes);
        when(mockClusterService.localNode()).thenReturn(nodes.getLocalNode());
        when(mockClusterService.getSettings()).thenReturn(Settings.EMPTY);
        WorkloadGroupSharedThrottleService sharedService = new WorkloadGroupSharedThrottleService(
            mockClusterService,
            mockThreadPool,
            Mockito.mock(TransportService.class)
        );
        ClusterState previous = Mockito.mock(ClusterState.class);
        when(previous.nodes()).thenReturn(DiscoveryNodes.EMPTY_NODES);
        sharedService.clusterChanged(new ClusterChangedEvent("test", clusterState, previous));
        workloadGroupService.setSharedThrottleService(sharedService);
        return sharedService;
    }

    private static DiscoveryNode node(String id, Set<DiscoveryNodeRole> roles) {
        return new DiscoveryNode(id, id, buildNewFakeTransportAddress(), Collections.emptyMap(), roles, Version.CURRENT);
    }

    private Releasable acquireThrottlePermitSync(WorkloadGroupTask task, BooleanSupplier parentAlreadyCounted) {
        return acquireThrottlePermitSync(workloadGroupService, task, parentAlreadyCounted);
    }

    // Admits a fresh top-level task carrying the given principal, exercising bucket resolution and the limit directly.
    private Releasable acquireThrottle(String workloadGroupId, String principal) {
        return acquireThrottle(workloadGroupService, workloadGroupId, principal);
    }

    private Releasable acquireThrottle(WorkloadGroupService service, String workloadGroupId, String principal) {
        WorkloadGroupTask task = throttleTask(workloadGroupId);
        task.setThrottlePrincipal(principal);
        return acquireThrottlePermitSync(service, task, () -> false);
    }

    private static Releasable acquireThrottlePermitSync(
        WorkloadGroupService service,
        WorkloadGroupTask task,
        BooleanSupplier parentAlreadyCounted
    ) {
        AtomicReference<Releasable> permit = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        AtomicBoolean completed = new AtomicBoolean(false);
        service.acquireThrottlePermit(task, parentAlreadyCounted, ActionListener.wrap(grantedPermit -> {
            permit.set(grantedPermit);
            completed.set(true);
        }, exception -> {
            failure.set(exception);
            completed.set(true);
        }));
        assertTrue("local-owner admission must complete inline", completed.get());
        if (failure.get() instanceof RuntimeException exception) {
            throw exception;
        }
        if (failure.get() != null) {
            throw new AssertionError(failure.get());
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

    private WorkloadGroup deserializedThrottledGroup(String id, Settings throttling) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        out.writeString(id + "-name");
        out.writeString(id);
        new MutableWorkloadGroupFragment(
            MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED,
            Map.of(ResourceType.MEMORY, 0.5),
            Settings.EMPTY,
            throttling
        ).writeTo(out);
        out.writeLong(1L);

        StreamInput in = out.bytes().streamInput();
        return new WorkloadGroup(in);
    }

    public void testAcquireThrottleAdmitsNestedRequestWithoutASecondPermit() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));

        // The outer request takes the group's only permit and is marked as counted.
        WorkloadGroupTask outer = throttleTask("wg-1");
        Releasable outerPermit = acquireThrottlePermitSync(outer, () -> false);
        assertNotNull(outerPermit);
        assertTrue("a successful acquire must mark the task as counted", outer.isThrottleCounted());

        // A nested rewrite search has a counted parent, so it's admitted without a permit (null = nothing to release).
        WorkloadGroupTask nested = throttleTask("wg-1");
        assertNull(acquireThrottlePermitSync(nested, () -> true));
        // Exempt but still marked counted, so a grandchild search doesn't take a fresh permit.
        assertTrue("an exempt request must be marked as counted, so the accounting propagates transitively", nested.isThrottleCounted());
        assertEquals(
            "an exemption is not a throttle",
            0,
            mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled()
        );

        // An independent request still hits the limit.
        WorkloadGroupTask independent = throttleTask("wg-1");
        expectThrows(OpenSearchRejectedExecutionException.class, () -> acquireThrottlePermitSync(independent, () -> false));

        outerPermit.close();
    }

    public void testAcquireThrottleChargesASearchWhoseParentWasNotCounted() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));

        // An _msearch parent never goes through admission, so each sub-search is charged.
        WorkloadGroupTask firstSubSearch = throttleTask("wg-1");
        Releasable firstPermit = acquireThrottlePermitSync(firstSubSearch, () -> false);
        assertNotNull("the first sub-search of an _msearch must take its own permit", firstPermit);
        assertTrue(firstSubSearch.isThrottleCounted());

        WorkloadGroupTask secondSubSearch = throttleTask("wg-1");
        expectThrows(OpenSearchRejectedExecutionException.class, () -> acquireThrottlePermitSync(secondSubSearch, () -> false));
        assertFalse("a rejected request must not be marked as counted", secondSubSearch.isThrottleCounted());
        assertEquals(
            "an _msearch sub-search rejected by the limit is a real throttle",
            1,
            mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled()
        );

        firstPermit.close();
    }

    public void testAcquireThrottleExemptionIsTransitiveAcrossTwoLevelsOfNesting() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));

        // Two levels of nesting: root A pays, and B and C ride on that one permit.
        WorkloadGroupTask rootA = throttleTask("wg-1");
        Releasable rootPermit = acquireThrottlePermitSync(rootA, () -> false);
        assertNotNull(rootPermit);
        assertTrue(rootA.isThrottleCounted());

        WorkloadGroupTask nestedB = throttleTask("wg-1");
        assertNull(acquireThrottlePermitSync(nestedB, () -> rootA.isThrottleCounted()));

        // C reads only B; if exempt B recorded nothing, C would take a fresh permit and self-reject.
        WorkloadGroupTask grandchildC = throttleTask("wg-1");
        assertNull(
            "a second level of nesting must inherit the accounting through the exempt middle task",
            acquireThrottlePermitSync(grandchildC, () -> nestedB.isThrottleCounted())
        );
        assertEquals(
            "no level of a single request's own nesting may be counted as a throttle",
            0,
            mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled()
        );

        // The accounting must not leak into unrelated requests: the group is still at its limit for anyone else.
        WorkloadGroupTask independent = throttleTask("wg-1");
        expectThrows(OpenSearchRejectedExecutionException.class, () -> acquireThrottlePermitSync(independent, () -> false));

        rootPermit.close();
    }

    private WorkloadGroupTask throttleTask(String workloadGroupId) {
        return throttleTask(workloadGroupId, 1L);
    }

    private WorkloadGroupTask throttleTask(String workloadGroupId, long taskId) {
        WorkloadGroupTask task = new WorkloadGroupTask(taskId, "transport", "Search", "test task", TaskId.EMPTY_TASK_ID, Map.of());
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, workloadGroupId);
        task.setWorkloadGroupId(threadContext);
        return task;
    }

    // --- queueing ---------------------------------------------------------------------------------------------------

    // Whole-group bucket key, as WorkloadGroupService builds it.
    private static String groupBucket(String groupId) {
        return groupId + ":group:group";
    }

    // Same as throttledGroup but with a queue bag, so a throttle denial can park instead of rejecting.
    private WorkloadGroup queueingGroup(String id, Settings throttling, Settings queue, MutableWorkloadGroupFragment.ResiliencyMode mode) {
        return new WorkloadGroup(
            id + "-name",
            id,
            new MutableWorkloadGroupFragment(mode, Map.of(ResourceType.MEMORY, 0.5), Settings.EMPTY, throttling, queue),
            1L
        );
    }

    private static Settings queueOf(int sizePerBucket) {
        return Settings.builder().put("size_per_bucket", sizePerBucket).build();
    }

    // Installs a real queue service whose GENERIC executor runs inline, so queued-request completions are synchronous.
    private WorkloadGroupQueueService newDirectQueueService() {
        when(mockThreadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(OpenSearchExecutors.newDirectExecutorService());
        WorkloadGroupQueueService queueService = new WorkloadGroupQueueService(mockThreadPool, mockWorkloadGroupsStateAccessor);
        workloadGroupService.setQueueService(queueService);
        return queueService;
    }

    private void enqueueWaitingRequest(WorkloadGroupQueueService queueService, String groupId, String bucketKey, AtomicInteger admissions) {
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup(groupId);
        assertTrue(queueService.tryEnqueue(groupId, bucketKey, throttleTask(groupId), 10, ActionListener.wrap(permit -> {
            admissions.incrementAndGet();
            permit.close();
        }, e -> { throw new AssertionError("queue-drain request failed instead of being admitted", e); })));
    }

    private void registerPendingRequest(
        WorkloadGroupQueueService queueService,
        String groupId,
        String bucketKey,
        AtomicInteger admissions
    ) {
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup(groupId);
        assertNotNull(queueService.tryRegisterPendingAcquire(groupId, bucketKey, throttleTask(groupId), ActionListener.wrap(permit -> {
            admissions.incrementAndGet();
            permit.close();
        }, e -> { throw new AssertionError("pending queue-drain request failed instead of being admitted", e); })));
    }

    /** What one admission attempt produced. A parked request is neither admitted nor failed (yet). */
    private static final class Admission {
        final WorkloadGroupTask task;
        final AtomicReference<Releasable> permit = new AtomicReference<>();
        final AtomicReference<Exception> failure = new AtomicReference<>();
        final AtomicBoolean completed = new AtomicBoolean();

        Admission(WorkloadGroupTask task) {
            this.task = task;
        }
    }

    private Admission admitOrPark(String workloadGroupId) {
        return admitOrPark(workloadGroupId, null);
    }

    private Admission admitOrPark(String workloadGroupId, String principal) {
        WorkloadGroupTask task = throttleTask(workloadGroupId);
        task.setThrottlePrincipal(principal);
        return admitOrPark(task);
    }

    private Admission admitOrPark(WorkloadGroupTask task) {
        Admission admission = new Admission(task);
        workloadGroupService.acquireThrottlePermit(admission.task, () -> false, ActionListener.wrap(p -> {
            admission.permit.set(p);
            admission.completed.set(true);
        }, e -> {
            admission.failure.set(e);
            admission.completed.set(true);
        }));
        return admission;
    }

    public void testNodeTierBreachParksAndHandsTheCompletingPermitToTheQueue() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        WorkloadGroupQueueService queueService = newDirectQueueService();
        stubClusterStateWithGroup(
            queueingGroup(
                "wg-1",
                Settings.builder().put("node_limit", 1).build(),
                queueOf(5),
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
            )
        );

        Admission holder = admitOrPark("wg-1");
        assertNotNull(holder.permit.get());
        Admission parked = admitOrPark("wg-1");
        assertFalse("a node-tier breach with queue room parks the request", parked.completed.get());
        assertEquals(1, queueService.waitingDepth("wg-1"));
        assertEquals("parking is not a rejection", 0, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());
        assertEquals("parking counts it as queued", 1, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalQueued());

        holder.permit.get().close();
        assertNotNull("the completing request's slot goes to the parked one", parked.permit.get());
        assertTrue("a handed-off admission carries the throttle charge", parked.task.isThrottleCounted());
        assertEquals(
            "the hand-off does not count it again",
            1,
            mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalQueued()
        );
        parked.permit.get().close();
        assertNotNull("the slot is released once the queue is empty", admitOrPark("wg-1").permit.get());
    }

    public void testNodeTierQueueFullRejectsWithTheQueueFullMessage() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        WorkloadGroupQueueService queueService = newDirectQueueService();
        stubClusterStateWithGroup(
            queueingGroup(
                "wg-1",
                Settings.builder().put("node_limit", 1).build(),
                queueOf(1),
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
            )
        );

        assertNotNull(admitOrPark("wg-1").permit.get());
        assertFalse(admitOrPark("wg-1").completed.get());
        Admission rejected = admitOrPark("wg-1");
        assertTrue(rejected.failure.get() instanceof OpenSearchRejectedExecutionException);
        assertEquals(
            "Request throttled: workload group [wg-1-name] reached its per-node limit of 1 concurrent requests, "
                + "and its request queue is full.",
            rejected.failure.get().getMessage()
        );
        assertEquals(1, queueService.waitingDepth("wg-1"));
        assertEquals(1, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalQueueRejections());
        assertEquals(1, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());
    }

    public void testIsQueuedIsTrueOnlyWhileANodeTierDeniedSearchIsParked() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        newDirectQueueService();
        stubClusterStateWithGroup(
            queueingGroup(
                "wg-1",
                Settings.builder().put("node_limit", 1).build(),
                queueOf(5),
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
            )
        );

        Admission holder = admitOrPark(throttleTask("wg-1", 1L));
        assertNotNull(holder.permit.get());
        assertFalse("a running request is not queued", workloadGroupService.isQueued("wg-1", 1L));
        Admission parked = admitOrPark(throttleTask("wg-1", 2L));
        assertFalse(parked.completed.get());
        assertTrue("a node-tier-denied search that parked is queued", workloadGroupService.isQueued("wg-1", 2L));
        assertFalse("the wrong group id is not asked", workloadGroupService.isQueued("wg-2", 2L));
        assertFalse("a task with no group id is not queued", workloadGroupService.isQueued(null, 2L));

        holder.permit.get().close();
        assertNotNull("the freed permit admits the parked request", parked.permit.get());
        assertFalse("an admitted request is no longer queued", workloadGroupService.isQueued("wg-1", 2L));
        parked.permit.get().close();
    }

    public void testIsQueuedIsFalseWithoutAQueueService() {
        assertFalse(workloadGroupService.isQueued("wg-1", 1L));
    }

    // shared_limit 0 is explicit but disables the shared tier: the 429 names only the node tier, and with no queue
    // configured it says nothing about a queue.
    public void testNodeOnlyGroupWithExplicitZeroSharedLimitRejectsWithThePerNodeMessage() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        newDirectQueueService();
        stubLocalSharedService(throttledGroup("wg-1", Settings.builder().put("node_limit", 2).put("shared_limit", 0).build()));

        assertNotNull(admitOrPark("wg-1").permit.get());
        assertNotNull(admitOrPark("wg-1").permit.get());
        Admission rejected = admitOrPark("wg-1");
        assertTrue(rejected.failure.get() instanceof OpenSearchRejectedExecutionException);
        assertEquals(
            "Request throttled: workload group [wg-1-name] reached its per-node limit of 2 concurrent requests.",
            rejected.failure.get().getMessage()
        );
        assertEquals(0, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalQueueRejections());
        assertEquals(1, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());
    }

    public void testScrollPageParksAndIsAdmittedWhenASlotFrees() {
        assertParksThroughTheSearchGateAndIsAdmittedWhenASlotFrees(SearchScrollAction.NAME, new SearchScrollRequest("scroll-id"));
    }

    public void testPointInTimeSearchParksAndIsAdmittedWhenASlotFrees() {
        SearchRequest pitSearch = new SearchRequest().source(
            new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder("pit-id"))
        );
        assertParksThroughTheSearchGateAndIsAdmittedWhenASlotFrees(SearchAction.NAME, pitSearch);
    }

    // Runs one request for `action` through the real search gate while the group's only node permit is held: it must wait
    // in the queue, not run or be rejected, and run on the permit the holder frees.
    private void assertParksThroughTheSearchGateAndIsAdmittedWhenASlotFrees(String action, ActionRequest request) {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        WorkloadGroupQueueService queueService = newDirectQueueService();
        stubClusterStateWithGroup(
            queueingGroup(
                "wg-1",
                Settings.builder().put("node_limit", 1).build(),
                queueOf(5),
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
            )
        );
        Admission holder = admitOrPark("wg-1");
        assertNotNull(holder.permit.get());

        GatedRequest gated = executeThroughSearchGate(searchTaskFor("wg-1", action), request);
        assertNull("a request at the limit must wait, not run", gated.running.get());
        assertNull("nor be rejected", gated.failure.get());
        assertEquals(1, queueService.retainedDepth("wg-1"));
        assertEquals(0, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());

        holder.permit.get().close();
        assertNotNull("the freed slot admits the parked request", gated.running.get());
        assertTrue("it runs on the handed-off permit", gated.task.isThrottleCounted());
        assertEquals(0, queueService.retainedDepth("wg-1"));
        assertEquals(1, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalQueued());
        gated.running.get().onResponse("done");
        assertEquals("done", gated.response.get());
        Admission next = admitOrPark("wg-1");
        assertNotNull("the request returned its slot when it completed", next.permit.get());
        next.permit.get().close();
    }

    public void testParkedScrollPageIsEvictedWhenItsTaskIsCancelled() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        WorkloadGroupQueueService queueService = newDirectQueueService();
        stubClusterStateWithGroup(
            queueingGroup(
                "wg-1",
                Settings.builder().put("node_limit", 1).build(),
                queueOf(5),
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
            )
        );
        Admission holder = admitOrPark("wg-1");
        GatedRequest page = executeThroughSearchGate(searchTaskFor("wg-1", SearchScrollAction.NAME), new SearchScrollRequest("scroll-id"));
        assertEquals(1, queueService.retainedDepth("wg-1"));

        page.task.cancel("client gone");
        assertTrue(page.failure.get() instanceof TaskCancelledException);
        assertEquals("task cancelled while queued", page.failure.get().getMessage());
        assertNull("an evicted page never runs", page.running.get());
        assertEquals(0, queueService.retainedDepth("wg-1"));
        assertEquals(
            "a cancelled page is not a throttle",
            0,
            mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled()
        );
        assertEquals(
            "it was counted as queued when it parked, and its eviction does not count it again",
            1,
            mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalQueued()
        );

        holder.permit.get().close();
        Admission next = admitOrPark("wg-1");
        assertNotNull("the evicted page holds no place in the queue", next.permit.get());
        next.permit.get().close();
    }

    /** One request run through the search gate: the listener its body got once admitted, and the request's outcome. */
    private static final class GatedRequest {
        final SearchTask task;
        final AtomicReference<ActionListener<String>> running = new AtomicReference<>();
        final AtomicReference<String> response = new AtomicReference<>();
        final AtomicReference<Exception> failure = new AtomicReference<>();

        GatedRequest(SearchTask task) {
            this.task = task;
        }
    }

    private GatedRequest executeThroughSearchGate(SearchTask task, ActionRequest request) {
        GatedRequest gated = new GatedRequest(task);
        activeSearchGate().execute(task, request, ActionListener.<String>wrap(gated.response::set, gated.failure::set), gated.running::set);
        return gated;
    }

    // A search task for `action`, tagged to the workload group the way the search actions tag it.
    private static SearchTask searchTaskFor(String workloadGroupId, String action) {
        SearchTask task = new SearchTask(1L, "transport", action, () -> "test", TaskId.EMPTY_TASK_ID, Map.of());
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, workloadGroupId);
        task.setWorkloadGroupId(threadContext);
        return task;
    }

    public void testParkedRequestResumesInItsOwnThreadContextAfterANodeHandoff() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(mockThreadPool.getThreadContext()).thenReturn(threadContext);
        newDirectQueueService();
        stubClusterStateWithGroup(
            queueingGroup(
                "wg-1",
                Settings.builder().put("node_limit", 1).build(),
                queueOf(5),
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
            )
        );

        Admission holder = admitOrPark("wg-1");
        AtomicReference<String> seen = new AtomicReference<>();
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader("X", "parked");
            workloadGroupService.acquireThrottlePermit(throttleTask("wg-1"), () -> false, ActionListener.wrap(p -> {
                seen.set(threadContext.getHeader("X"));
                p.close();
            }, e -> fail(e.toString())));
        }
        assertNull("parked", seen.get());
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader("X", "releaser");
            holder.permit.get().close();
        }
        assertEquals("the admitted search must not inherit the releasing request's context", "parked", seen.get());
    }

    public void testCountedParentIsExemptBeforeTheQueue() {
        // The nested-search exemption runs before any enqueue: a search whose parent already holds the charge must not
        // park behind (or deadlock with) that parent.
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        WorkloadGroupQueueService queueService = newDirectQueueService();
        stubClusterStateWithGroup(
            queueingGroup(
                "wg-1",
                Settings.builder().put("node_limit", 1).build(),
                queueOf(5),
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
            )
        );

        WorkloadGroupTask root = throttleTask("wg-1");
        Releasable rootPermit = acquireThrottlePermitSync(root, () -> false);
        assertNotNull(rootPermit);
        WorkloadGroupTask nested = throttleTask("wg-1");
        assertNull(acquireThrottlePermitSync(nested, root::isThrottleCounted));
        assertEquals("an exempt nested search never parks", 0, queueService.retainedDepth("wg-1"));
        rootPermit.close();
    }

    public void testRemovingFinalThrottleTierSchedulesQueuedBacklogDrain() {
        // Removing the final tier leaves parked requests with no permit source, so the config callback must run them all
        // untracked instead of waiting for the periodic service loop.
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        WorkloadGroupQueueService queueService = newDirectQueueService();
        WorkloadGroup throttled = queueingGroup(
            "wg-1",
            Settings.builder().put("node_limit", 1).build(),
            queueOf(5),
            MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
        );
        stubClusterStateWithGroup(throttled);

        assertNotNull(admitOrPark("wg-1").permit.get());
        Admission parkedA = admitOrPark("wg-1");
        Admission parkedB = admitOrPark("wg-1");
        assertEquals("two requests should be parked", 2, queueService.waitingDepth("wg-1"));

        // Queue settings may remain configured, but queueing becomes inert; the backlog retained under the old admission
        // policy must run without waiting for the periodic service sweep.
        WorkloadGroup unthrottled = queueingGroup("wg-1", Settings.EMPTY, queueOf(5), MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED);
        applyWorkloadGroups(Map.of("wg-1", throttled), Map.of("wg-1", unthrottled));

        assertEquals("the scheduled policy drain must release the whole backlog", 0, queueService.waitingDepth("wg-1"));
        assertTrue(parkedA.completed.get() && parkedB.completed.get());
        assertFalse("an untracked drain holds no permit, so it is not charged", parkedA.task.isThrottleCounted());
    }

    public void testDeletingGroupDrainsWaitingAndPendingRequests() {
        WorkloadGroupQueueService queueService = newDirectQueueService();
        AtomicInteger admissions = new AtomicInteger();
        enqueueWaitingRequest(queueService, "wg-1", groupBucket("wg-1"), admissions);
        registerPendingRequest(queueService, "wg-1", groupBucket("wg-1"), admissions);
        assertEquals(1, queueService.waitingDepth("wg-1"));
        assertEquals(2, queueService.retainedDepth("wg-1"));

        WorkloadGroup group = queueingGroup(
            "wg-1",
            Settings.builder().put("node_limit", 1).build(),
            queueOf(10),
            MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
        );
        applyWorkloadGroups(Map.of("wg-1", group), Map.of());

        assertEquals(0, queueService.retainedDepth("wg-1"));
        assertEquals(2, admissions.get());
    }

    public void testThrottleByChangeDrainsQueue() {
        WorkloadGroupQueueService queueService = newDirectQueueService();
        AtomicInteger admissions = new AtomicInteger();
        enqueueWaitingRequest(queueService, "wg-1", groupBucket("wg-1"), admissions);

        WorkloadGroup groupScope = queueingGroup(
            "wg-1",
            Settings.builder().put("node_limit", 1).build(),
            queueOf(10),
            MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
        );
        WorkloadGroup byUsername = queueingGroup(
            "wg-1",
            Settings.builder().put("by", "username").put("node_limit", 1).build(),
            queueOf(10),
            MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
        );
        applyWorkloadGroups(Map.of("wg-1", groupScope), Map.of("wg-1", byUsername));

        assertEquals(0, queueService.retainedDepth("wg-1"));
        assertEquals(1, admissions.get());
    }

    public void testSwitchingThrottleTierDrainsQueue() {
        WorkloadGroupQueueService queueService = newDirectQueueService();
        AtomicInteger admissions = new AtomicInteger();
        enqueueWaitingRequest(queueService, "wg-1", groupBucket("wg-1"), admissions);

        WorkloadGroup nodeOnly = queueingGroup(
            "wg-1",
            Settings.builder().put("node_limit", 1).build(),
            queueOf(10),
            MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
        );
        WorkloadGroup sharedOnly = queueingGroup(
            "wg-1",
            Settings.builder().put("shared_limit", 1).build(),
            queueOf(10),
            MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
        );
        applyWorkloadGroups(Map.of("wg-1", nodeOnly), Map.of("wg-1", sharedOnly));

        assertEquals(0, queueService.retainedDepth("wg-1"));
        assertEquals(1, admissions.get());
    }

    public void testAddingThrottleTierDrainsQueue() {
        WorkloadGroupQueueService queueService = newDirectQueueService();
        AtomicInteger admissions = new AtomicInteger();
        enqueueWaitingRequest(queueService, "wg-1", groupBucket("wg-1"), admissions);

        WorkloadGroup nodeOnly = queueingGroup(
            "wg-1",
            Settings.builder().put("node_limit", 1).build(),
            queueOf(10),
            MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
        );
        WorkloadGroup bothTiers = queueingGroup(
            "wg-1",
            Settings.builder().put("node_limit", 1).put("shared_limit", 1).build(),
            queueOf(10),
            MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
        );
        applyWorkloadGroups(Map.of("wg-1", nodeOnly), Map.of("wg-1", bothTiers));

        assertEquals(0, queueService.retainedDepth("wg-1"));
        assertEquals(1, admissions.get());
    }

    public void testZeroToUnsetLimitIsNotATierChange() throws IOException {
        // A limit below 1 never admits through its tier, so moving between 0 and unset changes no admission contract.
        WorkloadGroupQueueService queueService = newDirectQueueService();
        AtomicInteger admissions = new AtomicInteger();
        enqueueWaitingRequest(queueService, "wg-1", groupBucket("wg-1"), admissions);

        WorkloadGroup zeroNodeLimit = deserializedThrottledGroup(
            "wg-1",
            Settings.builder().put("node_limit", 0).put("shared_limit", 2).build()
        );
        WorkloadGroup unsetNodeLimit = queueingGroup(
            "wg-1",
            Settings.builder().put("shared_limit", 2).build(),
            queueOf(10),
            MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
        );
        applyWorkloadGroups(Map.of("wg-1", zeroNodeLimit), Map.of("wg-1", unsetNodeLimit));

        assertEquals(1, queueService.retainedDepth("wg-1"));
        assertEquals(0, admissions.get());
    }

    public void testEnteringMonitorModeDrainsQueue() {
        WorkloadGroupQueueService queueService = newDirectQueueService();
        AtomicInteger admissions = new AtomicInteger();
        enqueueWaitingRequest(queueService, "wg-1", groupBucket("wg-1"), admissions);

        Settings throttling = Settings.builder().put("node_limit", 1).build();
        WorkloadGroup enforced = queueingGroup("wg-1", throttling, queueOf(10), MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED);
        WorkloadGroup monitor = queueingGroup("wg-1", throttling, queueOf(10), MutableWorkloadGroupFragment.ResiliencyMode.MONITOR);
        applyWorkloadGroups(Map.of("wg-1", enforced), Map.of("wg-1", monitor));

        assertEquals(0, queueService.retainedDepth("wg-1"));
        assertEquals(1, admissions.get());
    }

    public void testConfigChangeDispatchesQueueDrainOffTheClusterApplierThread() {
        ExecutorService delayedExecutor = Mockito.mock(ExecutorService.class);
        when(mockThreadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(delayedExecutor);
        WorkloadGroupQueueService queueService = new WorkloadGroupQueueService(mockThreadPool, mockWorkloadGroupsStateAccessor);
        workloadGroupService.setQueueService(queueService);
        AtomicInteger admissions = new AtomicInteger();
        enqueueWaitingRequest(queueService, "wg-1", groupBucket("wg-1"), admissions);

        Settings throttling = Settings.builder().put("node_limit", 1).build();
        WorkloadGroup enforced = queueingGroup("wg-1", throttling, queueOf(10), MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED);
        WorkloadGroup monitor = queueingGroup("wg-1", throttling, queueOf(10), MutableWorkloadGroupFragment.ResiliencyMode.MONITOR);
        applyWorkloadGroups(Map.of("wg-1", enforced), Map.of("wg-1", monitor));

        ArgumentCaptor<Runnable> drainTask = ArgumentCaptor.forClass(Runnable.class);
        verify(delayedExecutor).execute(drainTask.capture());
        assertEquals("the cluster-applier callback must only schedule the drain", 1, queueService.retainedDepth("wg-1"));
        assertEquals(0, admissions.get());

        when(mockThreadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(OpenSearchExecutors.newDirectExecutorService());
        drainTask.getValue().run();

        assertEquals(0, queueService.retainedDepth("wg-1"));
        assertEquals(1, admissions.get());
    }

    public void testNumericLimitChangesWithSameTiersDoNotDrainQueue() {
        WorkloadGroupQueueService queueService = newDirectQueueService();
        AtomicInteger admissions = new AtomicInteger();
        enqueueWaitingRequest(queueService, "wg-1", groupBucket("wg-1"), admissions);

        WorkloadGroup previous = queueingGroup(
            "wg-1",
            Settings.builder().put("node_limit", 1).put("shared_limit", 2).build(),
            queueOf(10),
            MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
        );
        WorkloadGroup current = queueingGroup(
            "wg-1",
            Settings.builder().put("node_limit", 3).put("shared_limit", 4).build(),
            queueOf(10),
            MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
        );
        applyWorkloadGroups(Map.of("wg-1", previous), Map.of("wg-1", current));

        assertEquals(1, queueService.retainedDepth("wg-1"));
        assertEquals(0, admissions.get());
    }

    public void testQueueDepthAndOtherNonMonitorChangesDoNotDrainQueue() {
        WorkloadGroupQueueService queueService = newDirectQueueService();
        AtomicInteger admissions = new AtomicInteger();
        enqueueWaitingRequest(queueService, "wg-1", groupBucket("wg-1"), admissions);

        Settings throttling = Settings.builder().put("node_limit", 1).build();
        WorkloadGroup original = queueingGroup("wg-1", throttling, queueOf(10), MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED);
        WorkloadGroup resizedQueue = queueingGroup("wg-1", throttling, queueOf(20), MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED);
        applyWorkloadGroups(Map.of("wg-1", original), Map.of("wg-1", resizedQueue));
        assertEquals(1, queueService.retainedDepth("wg-1"));

        WorkloadGroup soft = queueingGroup("wg-1", throttling, queueOf(20), MutableWorkloadGroupFragment.ResiliencyMode.SOFT);
        applyWorkloadGroups(Map.of("wg-1", resizedQueue), Map.of("wg-1", soft));

        assertEquals(1, queueService.retainedDepth("wg-1"));
        assertEquals(0, admissions.get());
    }

    public void testGlobalModeLeavingEnabledDrainsAllQueues() {
        assertNotNull(wlmModeChangeListener);
        WorkloadGroupQueueService queueService = newDirectQueueService();
        AtomicInteger admissions = new AtomicInteger();

        enqueueWaitingRequest(queueService, "wg-1", groupBucket("wg-1"), admissions);
        registerPendingRequest(queueService, "wg-2", groupBucket("wg-2"), admissions);
        wlmModeChangeListener.accept(WlmMode.ENABLED, WlmMode.DISABLED);
        assertEquals(0, queueService.retainedDepth("wg-1"));
        assertEquals(0, queueService.retainedDepth("wg-2"));
        assertEquals(2, admissions.get());

        registerPendingRequest(queueService, "wg-3", groupBucket("wg-3"), admissions);
        wlmModeChangeListener.accept(WlmMode.ENABLED, WlmMode.MONITOR_ONLY);
        assertEquals(0, queueService.retainedDepth("wg-3"));
        assertEquals(3, admissions.get());
    }

    public void testServiceLoopReleasesARequestParkedAfterTheModeChangeDrainWhileDisabled() {
        assertServiceLoopReleasesARequestParkedAfterTheModeChangeDrain(WlmMode.DISABLED);
        Mockito.verifyNoInteractions(mockCancellationService); // the enforcement work DISABLED skips still never runs
    }

    public void testServiceLoopReleasesARequestParkedAfterTheModeChangeDrainInMonitorMode() {
        assertServiceLoopReleasesARequestParkedAfterTheModeChangeDrain(WlmMode.MONITOR_ONLY);
        // Unlike DISABLED, MONITOR_ONLY still runs the enforcement work after the release.
        verify(mockCancellationService).cancelTasks(any(), any(), any());
        verify(mockCancellationService).pruneDeletedWorkloadGroups(any());
    }

    // A request that resolved its plan while WLM was ENABLED parks just after the one-shot drain that ran when WLM left
    // ENABLED. Nothing else is bound to admit it (while DISABLED the queue backstop never runs), so the next run of the
    // service loop releases it, untracked, as the drain would have.
    private void assertServiceLoopReleasesARequestParkedAfterTheModeChangeDrain(WlmMode notEnabled) {
        assertNotNull(wlmModeChangeListener);
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        WorkloadGroupQueueService queueService = newDirectQueueService();
        stubClusterStateWithGroup(
            queueingGroup(
                "wg-1",
                Settings.builder().put("node_limit", 1).build(),
                queueOf(5),
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
            )
        );
        Admission holder = admitOrPark("wg-1");
        assertNotNull(holder.permit.get());

        // The straggler's plan is resolved on the first mode read, still ENABLED; every later read sees the new mode.
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED, notEnabled);
        wlmModeChangeListener.accept(WlmMode.ENABLED, notEnabled); // the one-shot drain finds the queue empty
        Admission straggler = admitOrPark("wg-1");
        assertFalse("the request parked after the drain", straggler.completed.get());
        assertEquals(1, queueService.waitingDepth("wg-1"));

        workloadGroupService.doRun();

        assertTrue("the service loop must release the straggler", straggler.completed.get());
        assertNull(straggler.failure.get());
        assertNotNull("released with an untracked no-op permit", straggler.permit.get());
        assertFalse("an untracked release holds no throttle slot", straggler.task.isThrottleCounted());
        assertEquals(0, queueService.retainedDepth("wg-1"));
        holder.permit.get().close();
    }

    public void testServiceLoopRunsNothingWhileDisabledWithAnEmptyQueue() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.DISABLED);
        when(mockThreadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(OpenSearchExecutors.newDirectExecutorService());
        WorkloadGroupQueueService queueService = spy(new WorkloadGroupQueueService(mockThreadPool, mockWorkloadGroupsStateAccessor));
        workloadGroupService.setQueueService(queueService);
        // A group that held a request keeps its emptied queue object: that must not cost the loop anything either.
        AtomicInteger admissions = new AtomicInteger();
        enqueueWaitingRequest(queueService, "wg-1", groupBucket("wg-1"), admissions);
        assertEquals(1, queueService.admitAllUntracked("wg-1"));
        assertEquals(1, admissions.get());
        Mockito.clearInvocations(queueService, mockThreadPool);

        workloadGroupService.doRun();

        verify(queueService).hasRetainedRequests();
        Mockito.verifyNoMoreInteractions(queueService);
        Mockito.verifyNoInteractions(mockCancellationService, mockThreadPool);
    }

    public void testGlobalModeTransitionNotLeavingEnabledDoesNotDrainQueue() {
        assertNotNull(wlmModeChangeListener);
        WorkloadGroupQueueService queueService = newDirectQueueService();
        AtomicInteger admissions = new AtomicInteger();
        enqueueWaitingRequest(queueService, "wg-1", groupBucket("wg-1"), admissions);

        wlmModeChangeListener.accept(WlmMode.MONITOR_ONLY, WlmMode.DISABLED);
        wlmModeChangeListener.accept(WlmMode.DISABLED, WlmMode.ENABLED);

        assertEquals(1, queueService.retainedDepth("wg-1"));
        assertEquals(0, admissions.get());
    }

    public void testNodePermitCompletionHandsSlotToQueueBeforeRacingArrival() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        when(mockThreadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(OpenSearchExecutors.newDirectExecutorService());
        stubClusterStateWithGroup(
            queueingGroup(
                "wg-1",
                Settings.builder().put("node_limit", 1).build(),
                queueOf(5),
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
            )
        );

        AtomicBoolean injectedRacingArrival = new AtomicBoolean(false);
        AtomicReference<Admission> racingArrival = new AtomicReference<>();
        WorkloadGroupQueueService queueService = new WorkloadGroupQueueService(mockThreadPool, mockWorkloadGroupsStateAccessor) {
            @Override
            boolean handoffNodePermit(String groupId, String bucketKey, Releasable permit) {
                if (injectedRacingArrival.compareAndSet(false, true)) {
                    // Run a new arrival at the exact point where a release-then-reacquire implementation would already have
                    // freed the raw tracker slot. The handoff still owns that slot, so this request must queue instead of
                    // barging ahead.
                    racingArrival.set(admitOrPark("wg-1"));
                    assertFalse("the racing request must not acquire the queued request's slot", racingArrival.get().completed.get());
                    assertEquals("oldest waiter plus racing arrival", 2, retainedDepth("wg-1"));
                }
                return super.handoffNodePermit(groupId, bucketKey, permit);
            }
        };
        workloadGroupService.setQueueService(queueService);

        Admission holder = admitOrPark("wg-1");
        assertNotNull(holder.permit.get());
        AtomicReference<Releasable> oldestQueuedPermit = new AtomicReference<>();
        assertTrue(
            queueService.tryEnqueue(
                "wg-1",
                groupBucket("wg-1"),
                throttleTask("wg-1"),
                5,
                ActionListener.wrap(oldestQueuedPermit::set, e -> fail("oldest queued request failed: " + e))
            )
        );

        holder.permit.get().close();
        holder.permit.get().close(); // the wrapper itself must be one-shot; no second handoff from a duplicate completion

        assertTrue(injectedRacingArrival.get());
        assertNotNull("the oldest queued request receives the still-held node slot", oldestQueuedPermit.get());
        assertNull("the racing request remains queued behind it", racingArrival.get().permit.get());
        assertEquals(1, queueService.retainedDepth("wg-1"));

        oldestQueuedPermit.get().close();
        assertNotNull("the same underlying slot chains to the next queued request", racingArrival.get().permit.get());
        assertEquals(0, queueService.retainedDepth("wg-1"));
        racingArrival.get().permit.get().close();

        Admission afterDrain = admitOrPark("wg-1");
        assertNotNull("the underlying tracker slot must be released when the queue empties", afterDrain.permit.get());
        afterDrain.permit.get().close();
    }

    public void testCompletionDrainHonoursALiveNodeLimitDecrease() {
        // The node-tier drain chain must re-read node_limit from cluster state on every hop instead of reusing the value
        // captured when the chain started; otherwise a busy bucket keeps admitting above a lowered limit.
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        WorkloadGroupQueueService queueService = newDirectQueueService();
        stubClusterStateWithGroup(
            queueingGroup(
                "wg-1",
                Settings.builder().put("node_limit", 2).build(),
                queueOf(5),
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
            )
        );

        Admission first = admitOrPark("wg-1");
        Admission second = admitOrPark("wg-1");
        assertNotNull(first.permit.get());
        assertNotNull(second.permit.get());
        Admission third = admitOrPark("wg-1");
        assertFalse("third request should be parked, not admitted", third.completed.get());
        assertEquals(1, queueService.waitingDepth("wg-1"));

        // Operator lowers node_limit to 1 while the bucket is busy with a backlog.
        stubClusterStateWithGroup(
            queueingGroup(
                "wg-1",
                Settings.builder().put("node_limit", 1).build(),
                queueOf(5),
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
            )
        );

        // Completing one request drops in-flight to 1, already AT the new limit, so the drain must not admit.
        first.permit.get().close();
        assertEquals("drain must respect the lowered node_limit, leaving the request parked", 1, queueService.waitingDepth("wg-1"));

        // Completing the second leaves room under the new limit -> the parked request drains.
        second.permit.get().close();
        assertEquals("a slot under the new limit must still drain the backlog", 0, queueService.waitingDepth("wg-1"));
        assertNotNull(third.permit.get());
    }

    public void testQueueBackstopReleasesARequestParkedAfterTheFinalTierWasRemoved() {
        assertQueueBackstopReleasesStrandedRequest(
            () -> stubClusterStateWithGroup(
                queueingGroup("wg-1", Settings.EMPTY, queueOf(5), MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED)
            )
        );
    }

    public void testQueueBackstopReleasesARequestParkedBeforeItsGroupWasDeleted() {
        assertQueueBackstopReleasesStrandedRequest(this::stubClusterStateWithNoGroups);
    }

    // A request that resolved the old plan parks just after the one-shot live-config drain (modeled by changing the
    // applied config without delivering clusterChanged, as if that drain had already run against an empty queue). With no
    // tier left nothing can ever admit it, so the backstop releases it untracked, as the drain and a new arrival would.
    private void assertQueueBackstopReleasesStrandedRequest(Runnable removeEveryTier) {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        WorkloadGroupQueueService queueService = newDirectQueueService();
        stubClusterStateWithGroup(
            queueingGroup(
                "wg-1",
                Settings.builder().put("node_limit", 1).build(),
                queueOf(5),
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
            )
        );
        Admission holder = admitOrPark("wg-1");
        assertNotNull(holder.permit.get());
        Admission parked = admitOrPark("wg-1");
        assertEquals(1, queueService.waitingDepth("wg-1"));

        removeEveryTier.run();
        holder.permit.get().close();
        assertFalse("with no tier left the completion has no slot to hand off", parked.completed.get());

        workloadGroupService.doRun(); // the mock clock reads 0, so the first backstop pass is due

        assertTrue("the backstop must release the stranded request", parked.completed.get());
        assertNull(parked.failure.get());
        assertNotNull("released with an untracked no-op permit", parked.permit.get());
        assertFalse("an untracked release holds no throttle slot", parked.task.isThrottleCounted());
        assertEquals(0, queueService.retainedDepth("wg-1"));
    }

    public void testQueueBackstopLeavesASharedOnlyBucketToOwnerPushUntilItsTierIsGone() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        AtomicLong clock = new AtomicLong();
        when(mockThreadPool.relativeTimeInNanos()).thenAnswer(invocation -> clock.get());
        WorkloadGroupQueueService queueService = newDirectQueueService();
        stubClusterStateWithGroup(
            queueingGroup(
                "wg-1",
                Settings.builder().put("shared_limit", 1).build(),
                queueOf(5),
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
            )
        );
        AtomicInteger admissions = new AtomicInteger();
        enqueueWaitingRequest(queueService, "wg-1", groupBucket("wg-1"), admissions);

        workloadGroupService.doRun();
        assertEquals("a shared-only bucket is left to owner push", 0, admissions.get());

        stubClusterStateWithGroup(queueingGroup("wg-1", Settings.EMPTY, queueOf(5), MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED));
        clock.addAndGet(WorkloadGroupService.QUEUE_BACKSTOP_SWEEP_INTERVAL_NANOS);
        workloadGroupService.doRun();

        assertEquals("with no tier left the backstop releases it", 1, admissions.get());
        assertEquals(0, queueService.retainedDepth("wg-1"));
    }

    public void testBackstopSweepFillsNodeSlotsAddedByALimitIncrease() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        WorkloadGroupQueueService queueService = newDirectQueueService();
        stubClusterStateWithGroup(
            queueingGroup(
                "wg-1",
                Settings.builder().put("node_limit", 1).build(),
                queueOf(5),
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
            )
        );
        Admission holder = admitOrPark("wg-1");
        assertNotNull(holder.permit.get());
        Admission[] parked = new Admission[3];
        for (int i = 0; i < parked.length; i++) {
            parked[i] = admitOrPark("wg-1");
        }
        assertEquals(3, queueService.waitingDepth("wg-1"));

        // A numeric node_limit increase is not a queue drain (see requiresQueueDrain), and no completion hands the
        // added slots to the backlog.
        stubClusterStateWithGroup(
            queueingGroup(
                "wg-1",
                Settings.builder().put("node_limit", 4).build(),
                queueOf(5),
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
            )
        );
        workloadGroupService.doRun(); // the mock clock reads 0, so the first backstop pass is due

        assertEquals("one backstop pass must fill every added slot", 0, queueService.waitingDepth("wg-1"));
        for (Admission admission : parked) {
            assertNotNull(admission.permit.get());
        }
        Admission next = admitOrPark("wg-1");
        assertFalse("the bucket is at its new limit", next.completed.get());
        holder.permit.get().close();
        assertNotNull("a freed slot goes to the next waiter", next.permit.get());
        next.permit.get().close();
        for (Admission admission : parked) {
            admission.permit.get().close();
        }
    }

    public void testNodeTierParkRecoversASlotFreedBeforeItWasRetained() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        when(mockThreadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(OpenSearchExecutors.newDirectExecutorService());
        stubClusterStateWithGroup(
            queueingGroup(
                "wg-1",
                Settings.builder().put("node_limit", 1).build(),
                queueOf(5),
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
            )
        );
        AtomicReference<Releasable> completesBeforeEnqueue = new AtomicReference<>();
        WorkloadGroupQueueService queueService = new WorkloadGroupQueueService(mockThreadPool, mockWorkloadGroupsStateAccessor) {
            @Override
            boolean tryEnqueue(
                String groupId,
                String bucketKey,
                WorkloadGroupTask task,
                int sizePerBucket,
                ActionListener<Releasable> listener
            ) {
                // The bucket's only in-flight request completes after this arrival's acquire failed but before it is
                // retained: its close sees an empty queue and releases the slot to nobody.
                Releasable completing = completesBeforeEnqueue.getAndSet(null);
                if (completing != null) {
                    completing.close();
                }
                return super.tryEnqueue(groupId, bucketKey, task, sizePerBucket, listener);
            }
        };
        workloadGroupService.setQueueService(queueService);

        Admission holder = admitOrPark("wg-1");
        assertNotNull(holder.permit.get());
        completesBeforeEnqueue.set(holder.permit.get());
        Admission parked = admitOrPark("wg-1");
        assertNotNull("a slot freed while the request was being parked must admit it at once", parked.permit.get());
        assertEquals(0, queueService.retainedDepth("wg-1"));
        assertEquals(1, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalQueued());

        // The recovered slot is a real one: node_limit=1 still holds and the slot chains to the next waiter.
        Admission next = admitOrPark("wg-1");
        assertFalse(next.completed.get());
        parked.permit.get().close();
        assertNotNull(next.permit.get());
        next.permit.get().close();
        Admission afterDrain = admitOrPark("wg-1");
        assertNotNull("the tracker slot is released once the queue empties", afterDrain.permit.get());
        afterDrain.permit.get().close();
    }

    public void testNodePermitReleaseSkipsQueueWorkWhenOnlyAnotherBucketIsBacklogged() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        when(mockThreadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(OpenSearchExecutors.newDirectExecutorService());
        stubClusterStateWithGroup(
            queueingGroup(
                "wg-1",
                Settings.builder().put("by", "username").put("node_limit", 1).build(),
                queueOf(5),
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
            )
        );
        List<String> queueWork = new ArrayList<>();
        WorkloadGroupQueueService queueService = new WorkloadGroupQueueService(mockThreadPool, mockWorkloadGroupsStateAccessor) {
            @Override
            boolean handoffNodePermit(String groupId, String bucketKey, Releasable permit) {
                queueWork.add(bucketKey);
                return super.handoffNodePermit(groupId, bucketKey, permit);
            }

            @Override
            boolean drainNode(String groupId, String bucketKey, Function<String, Releasable> nodeAcquire) {
                queueWork.add(bucketKey);
                return super.drainNode(groupId, bucketKey, nodeAcquire);
            }
        };
        workloadGroupService.setQueueService(queueService);

        Admission alice = admitOrPark("wg-1", "username|alice");
        assertNotNull(alice.permit.get());
        Admission aliceQueued = admitOrPark("wg-1", "username|alice");
        assertFalse(aliceQueued.completed.get());
        Admission bob = admitOrPark("wg-1", "username|bob");
        assertNotNull(bob.permit.get());
        queueWork.clear(); // the park's own drain attempt is not what this test measures

        bob.permit.get().close();
        assertEquals("a release in a bucket with no backlog does no queue work", List.of(), queueWork);

        alice.permit.get().close();
        assertEquals(List.of("wg-1:username:alice"), queueWork);
        assertNotNull("alice's freed slot goes to her parked request", aliceQueued.permit.get());
        aliceQueued.permit.get().close();
        Admission fresh = admitOrPark("wg-1", "username|alice");
        assertNotNull("the slot is released once alice's queue empties", fresh.permit.get());
        fresh.permit.get().close();
    }

    public void testMonitorModeNeverParksEvenWhenQueueingIsEnabled() {
        // MONITOR observes and always admits — it must never park a request (that would turn a dry run into a hang).
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        WorkloadGroupQueueService queueService = newDirectQueueService();
        stubClusterStateWithGroup(
            queueingGroup(
                "wg-1",
                Settings.builder().put("node_limit", 1).build(),
                queueOf(5),
                MutableWorkloadGroupFragment.ResiliencyMode.MONITOR
            )
        );

        assertNotNull(admitOrPark("wg-1").permit.get());
        Admission observed = admitOrPark("wg-1");
        assertTrue("MONITOR admits immediately", observed.completed.get());
        assertNull(observed.permit.get());
        assertEquals("a monitor-mode request must never be parked", 0, queueService.retainedDepth("wg-1"));
        assertEquals(0, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());
        assertEquals(1, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalWouldThrottle());
        assertEquals(
            "MONITOR never queues, so the queue stats stay 0",
            0,
            mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalQueued()
        );
        assertEquals(0, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalQueueRejections());
    }

    public void testMonitorModeNeverParksOnSharedTierWithQueueingEnabled() {
        // The shared tier takes the ENQUEUE-FIRST path, which retains the request BEFORE asking the owner. That path is
        // gated on MONITOR being off; without the gate a monitor-mode request would be held instead of observed.
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        stubLocalSharedService(
            queueingGroup(
                "wg-1",
                Settings.builder().put("node_limit", 1).put("shared_limit", 1).build(),
                queueOf(5),
                MutableWorkloadGroupFragment.ResiliencyMode.MONITOR
            )
        );
        WorkloadGroupQueueService queueService = newDirectQueueService();

        assertNotNull(admitOrPark("wg-1").permit.get()); // the local slot
        assertNotNull(admitOrPark("wg-1").permit.get()); // the shared slot
        Admission observed = admitOrPark("wg-1");
        assertTrue(observed.completed.get());
        assertNull("admitted untracked, never parked", observed.permit.get());
        assertEquals("a monitor-mode request must never be parked on the shared tier", 0, queueService.retainedDepth("wg-1"));
        assertEquals(0, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());
        assertEquals(1, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalWouldThrottle());
        assertEquals(0, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalQueueRejections());
    }

    public void testSharedTierDenialParksAndOwnerPushAdmitsIt() {
        // End to end on a local owner: a shared denial with queue room registers this node as a waiter and parks the
        // request; releasing the shared permit pushes it a grant.
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        WorkloadGroupSharedThrottleService sharedService = stubLocalSharedService(
            queueingGroup(
                "wg-1",
                Settings.builder().put("shared_limit", 1).build(),
                queueOf(5),
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
            )
        );
        WorkloadGroupQueueService queueService = newDirectQueueService();
        sharedService.setQueueService(queueService);

        Admission holder = admitOrPark("wg-1");
        assertNotNull(holder.permit.get());
        Admission parked = admitOrPark("wg-1");
        assertFalse(parked.completed.get());
        assertEquals(1, queueService.waitingDepth("wg-1"));
        assertEquals(1, sharedService.waiterCount(groupBucket("wg-1")));

        holder.permit.get().close();
        assertNotNull("owner-push admitted the parked request", parked.permit.get());
        assertTrue(parked.task.isThrottleCounted());
        assertEquals(1, sharedService.tracker().inFlight(groupBucket("wg-1")));
        assertEquals(0, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());
        parked.permit.get().close();
        assertEquals(0, sharedService.tracker().inFlight(groupBucket("wg-1")));
    }

    public void testUnavailableSharedTierParksTheRequestWhileItsOwnerIsUnreachable() {
        // With queueing on, an owner this coordinator is not connected to answers nothing, so the request parks instead of
        // getting the unavailable 429, and a backstop pass that still cannot reach the owner leaves it parked.
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        stubRemoteOwnerSharedService(MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED);
        WorkloadGroupQueueService queueService = newDirectQueueService();

        Admission parked = admitOrPark("wg-1");
        assertFalse("neither admitted nor rejected", parked.completed.get());
        assertEquals(1, queueService.waitingDepth("wg-1"));
        assertEquals(0, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());

        workloadGroupService.doRun(); // the mock clock reads 0, so the first backstop pass is due
        assertFalse("a pass that cannot reach the owner leaves the request parked", parked.completed.get());
        assertEquals(1, queueService.waitingDepth("wg-1"));
        assertEquals(0, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());
    }

    public void testUnavailableSharedTierInMonitorStillAdmitsWithAQueueConfigured() {
        // MONITOR never parks: an owner this coordinator cannot reach is observed (would_throttle) and the request admitted.
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        stubRemoteOwnerSharedService(MutableWorkloadGroupFragment.ResiliencyMode.MONITOR);
        WorkloadGroupQueueService queueService = newDirectQueueService();

        Admission observed = admitOrPark("wg-1");
        assertTrue(observed.completed.get());
        assertNull("admitted untracked", observed.permit.get());
        assertNull(observed.failure.get());
        assertEquals(0, queueService.retainedDepth("wg-1"));
        assertEquals(1, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalWouldThrottle());
        assertEquals(0, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());
    }

    // A shared-only queueing group whose every bucket is owned by a remote data node this coordinator is not connected to.
    private void stubRemoteOwnerSharedService(MutableWorkloadGroupFragment.ResiliencyMode mode) {
        stubSharedService(
            queueingGroup("wg-1", Settings.builder().put("shared_limit", 1).build(), queueOf(5), mode),
            DiscoveryNodes.builder()
                .add(node("local", Set.of()))
                .add(node("owner", Set.of(DiscoveryNodeRole.DATA_ROLE)))
                .localNodeId("local")
                .build()
        );
    }

    public void testAcquireThrottleReturnsNullWhenNodeLimitUnset() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        stubClusterStateWithGroup(throttledGroup("wg-1", Settings.EMPTY)); // throttling not configured
        assertNull(acquireThrottle("wg-1", null));
    }

    public void testAcquireThrottleReturnsNullWhenNodeLimitIsZero() throws IOException {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("node_limit", 0).build();
        // Deserialization can keep a zero limit; admission must treat it as disabled before reaching the tracker.
        stubClusterStateWithGroup(deserializedThrottledGroup("wg-1", throttling));

        assertNull(acquireThrottle("wg-1", null));
        assertNull(acquireThrottle("wg-1", null));
        assertEquals(0, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());
    }

    public void testAcquireThrottleFailsOpenForUnsupportedThrottlingSchema() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        Map<String, Settings> unsupportedConfigs = Map.of(
            "explicit-group",
            Settings.builder().put("by", "group").put("node_limit", 1).build(),
            "legacy-attribute",
            Settings.builder().put("attribute", "username").put("node_limit", 1).build()
        );

        for (Map.Entry<String, Settings> entry : unsupportedConfigs.entrySet()) {
            WorkloadGroup workloadGroup = Mockito.mock(WorkloadGroup.class);
            MutableWorkloadGroupFragment fragment = Mockito.mock(MutableWorkloadGroupFragment.class);
            when(workloadGroup.get_id()).thenReturn(entry.getKey());
            when(workloadGroup.getMutableWorkloadGroupFragment()).thenReturn(fragment);
            when(fragment.getThrottling()).thenReturn(entry.getValue());
            stubClusterStateWithGroup(workloadGroup);

            assertNull(acquireThrottle(entry.getKey(), "username|alice"));
            assertNull(acquireThrottle(entry.getKey(), "username|alice"));
        }
    }

    public void testAcquireThrottleReturnsNullWhenWlmDisabled() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.DISABLED);
        assertNull(acquireThrottle("wg-1", null));
    }

    public void testAcquireThrottleRejectsAtLimitAndIncrementsStat() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));

        Releasable permit = acquireThrottle("wg-1", null); // first admit succeeds
        assertNotNull(permit);
        // second admit hits node_limit of 1 -> 429 + total_throttled incremented
        expectThrows(OpenSearchRejectedExecutionException.class, () -> acquireThrottle("wg-1", null));
        assertEquals(1, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());

        // releasing the first permit frees the slot so a subsequent acquire succeeds
        permit.close();
        assertNotNull(acquireThrottle("wg-1", null));
    }

    public void testSearchGateReleasesPermitBeforeNotifying() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        stubClusterStateWithGroup(throttledGroup("wg-1", Settings.builder().put("node_limit", 1).build()));

        AtomicReference<ActionListener<String>> admittedListener = new AtomicReference<>();
        AtomicReference<Releasable> admittedFromCompletion = new AtomicReference<>();
        activeSearchGate().execute(
            throttleTask("wg-1"),
            new SearchRequest(),
            ActionListener.<String>wrap(response -> admittedFromCompletion.set(acquireThrottle("wg-1", null)), e -> {
                throw new AssertionError("unexpected failure", e);
            }),
            admittedListener::set
        );
        assertNotNull("an admitted request must run", admittedListener.get());
        // The running request holds the only slot.
        expectThrows(OpenSearchRejectedExecutionException.class, () -> acquireThrottle("wg-1", null));

        admittedListener.get().onResponse("done");
        // Work started from the completion listener (an _msearch's next sub-search) finds the slot already free.
        assertNotNull(admittedFromCompletion.get());
        admittedFromCompletion.get().close();
    }

    public void testSearchGateRejectsWithoutRunningTheRequest() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        stubClusterStateWithGroup(throttledGroup("wg-1", Settings.builder().put("node_limit", 1).build()));
        Releasable held = acquireThrottle("wg-1", null);

        AtomicBoolean ran = new AtomicBoolean();
        AtomicReference<Exception> failure = new AtomicReference<>();
        activeSearchGate().execute(
            throttleTask("wg-1"),
            new SearchRequest(),
            ActionListener.<String>wrap(
                response -> fail("must not succeed"),
                e -> assertTrue("notified more than once", failure.compareAndSet(null, e))
            ),
            listener -> ran.set(true)
        );
        assertFalse("a throttled request must not run", ran.get());
        assertTrue(failure.get() instanceof OpenSearchRejectedExecutionException);
        assertEquals(1, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());
        held.close();
    }

    public void testSearchGateRunsWithoutAChargeWhenNotThrottled() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.DISABLED);
        AtomicReference<String> response = new AtomicReference<>();
        activeSearchGate().execute(throttleTask("wg-1"), new SearchRequest(), ActionListener.<String>wrap(response::set, e -> {
            throw new AssertionError("unexpected failure", e);
        }), listener -> listener.onResponse("done"));
        assertEquals("done", response.get());
    }

    // A gate over this service's admission, consulted even though the fixture never applies cluster state (so
    // isThrottlingActive() stays false).
    private SearchExecutionGate activeSearchGate() {
        return new SearchExecutionGate(
            new WorkloadGroupSearchAdmission(workloadGroupService, mockClusterService, Mockito.mock(TaskManager.class))::admit
        );
    }

    public void testAcquireThrottleMonitorModeObservesWithoutRejecting() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling, MutableWorkloadGroupFragment.ResiliencyMode.MONITOR));

        assertNotNull(acquireThrottle("wg-1", null)); // first admit takes the only slot
        // MONITOR admits over-limit requests and counts them in total_would_throttle, not total_throttled.
        assertNull(acquireThrottle("wg-1", null));
        assertEquals(0, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());
        assertEquals(1, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalWouldThrottle());
    }

    public void testAcquireThrottleMonitorModeDoesNotDoubleCountNestedSearch() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling, MutableWorkloadGroupFragment.ResiliencyMode.MONITOR));

        try (Releasable occupyingPermit = acquireThrottlePermitSync(throttleTask("wg-1"), () -> false)) {
            assertNotNull(occupyingPermit);
            WorkloadGroupTask outer = throttleTask("wg-1");
            assertNull(acquireThrottlePermitSync(outer, () -> false));
            assertTrue(outer.isThrottleCounted());

            WorkloadGroupTask nested = throttleTask("wg-1");
            assertNull(acquireThrottlePermitSync(nested, outer::isThrottleCounted));
            assertTrue(nested.isThrottleCounted());
            assertEquals(1, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalWouldThrottle());
            assertEquals(0, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());

            WorkloadGroupTask independent = throttleTask("wg-1");
            assertNull(acquireThrottlePermitSync(independent, () -> false));
            assertEquals(2, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalWouldThrottle());
        }
    }

    public void testAcquireThrottleEnforcedModeDoesNotCountWouldThrottle() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling, MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED));

        assertNotNull(acquireThrottle("wg-1", null));
        // total_would_throttle is exclusive to MONITOR.
        expectThrows(OpenSearchRejectedExecutionException.class, () -> acquireThrottle("wg-1", null));
        assertEquals(1, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());
        assertEquals(0, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalWouldThrottle());
    }

    public void testAcquireThrottleSoftModeStillRejects() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling, MutableWorkloadGroupFragment.ResiliencyMode.SOFT));

        assertNotNull(acquireThrottle("wg-1", null));
        // Only MONITOR is observe-only; SOFT enforces the throttle like ENFORCED does.
        expectThrows(OpenSearchRejectedExecutionException.class, () -> acquireThrottle("wg-1", null));
        assertEquals(1, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());
    }

    public void testAcquireThrottleUsernameKeepsPerUserBuckets() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("by", "username").put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));

        // alice takes her single slot; a second alice request is rejected.
        Releasable alice = acquireThrottle("wg-1", "username|alice");
        assertNotNull(alice);
        expectThrows(OpenSearchRejectedExecutionException.class, () -> acquireThrottle("wg-1", "username|alice"));
        assertEquals(1, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());

        // bob is a different bucket, so he is admitted even while alice is at her limit.
        Releasable bob = acquireThrottle("wg-1", "username|bob");
        assertNotNull(bob);

        // releasing alice frees her bucket
        alice.close();
        assertNotNull(acquireThrottle("wg-1", "username|alice"));
    }

    public void testAcquireThrottleUsernameWithCommaDoesNotCollide() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("by", "username").put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));

        String delim = WorkloadGroupTask.WORKLOAD_GROUP_PRINCIPAL_VALUE_DELIMITER;
        // principal for user "a,b" with a role token appended
        String userAB = "username|a,b" + delim + "role|admin";
        // user "a" is a genuinely different principal
        String userA = "username|a";

        Releasable ab = acquireThrottle("wg-1", userAB); // fills "a,b" bucket
        assertNotNull(ab);
        // user "a" must NOT be treated as the same bucket as "a,b" -> still admitted
        assertNotNull(acquireThrottle("wg-1", userA));
        // a second "a,b" request hits the "a,b" bucket limit -> rejected
        expectThrows(OpenSearchRejectedExecutionException.class, () -> acquireThrottle("wg-1", userAB));
    }

    public void testAcquireThrottleRolePicksMatchingSubfieldFromMultiTokenPrincipal() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("by", "role").put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));

        // A principal header may carry both subfields; the role bucket must key off the role token only.
        String delim = WorkloadGroupTask.WORKLOAD_GROUP_PRINCIPAL_VALUE_DELIMITER;
        assertNotNull(acquireThrottle("wg-1", "username|alice" + delim + "role|admin"));
        expectThrows(OpenSearchRejectedExecutionException.class, () -> acquireThrottle("wg-1", "username|bob" + delim + "role|admin"));
        assertEquals(1, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());
    }

    public void testAcquireThrottleRoleBucketIsStableAcrossTokenOrder() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("by", "role").put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));

        // The same roles in any order must land in the same bucket.
        String delim = WorkloadGroupTask.WORKLOAD_GROUP_PRINCIPAL_VALUE_DELIMITER;
        assertNotNull(acquireThrottle("wg-1", "role|admin" + delim + "role|analyst"));
        expectThrows(OpenSearchRejectedExecutionException.class, () -> acquireThrottle("wg-1", "role|analyst" + delim + "role|admin"));
        assertEquals(1, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());
    }

    public void testAcquireThrottleRoleKeepsPerRoleBuckets() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("by", "role").put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));

        String delim = WorkloadGroupTask.WORKLOAD_GROUP_PRINCIPAL_VALUE_DELIMITER;
        Releasable analyst = acquireThrottle("wg-1", "username|alice" + delim + "role|analyst");
        assertNotNull(analyst);
        expectThrows(OpenSearchRejectedExecutionException.class, () -> acquireThrottle("wg-1", "username|bob" + delim + "role|analyst"));

        assertNotNull(acquireThrottle("wg-1", "username|carol" + delim + "role|admin"));
        assertEquals(1, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());

        analyst.close();
        assertNotNull(acquireThrottle("wg-1", "username|bob" + delim + "role|analyst"));
    }

    /** {@code by: role} charges only the smallest role, so it does not cap a role (documented limitation). */
    public void testAcquireThrottleRoleChargesOnlySmallestRole() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("by", "role").put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));

        String delim = WorkloadGroupTask.WORKLOAD_GROUP_PRINCIPAL_VALUE_DELIMITER;
        // Charged to all_access, not readall.
        assertNotNull(acquireThrottle("wg-1", "role|all_access" + delim + "role|readall"));
        // readall's bucket is still empty.
        assertNotNull(acquireThrottle("wg-1", "role|readall"));
        expectThrows(OpenSearchRejectedExecutionException.class, () -> acquireThrottle("wg-1", "role|all_access" + delim + "role|zzz"));
        assertEquals(1, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());
    }

    public void testAcquireThrottleFailsOpenWhenPrincipalMissingForRole() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("by", "role").put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));

        assertNull(acquireThrottle("wg-1", null));
        assertNull(acquireThrottle("wg-1", ""));
        assertNull(acquireThrottle("wg-1", "username|alice")); // no role token
        assertNull(acquireThrottle("wg-1", "role|")); // empty role value
        assertEquals(0, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());

        // Control: proves the nulls above are fail-open, not throttling being off.
        assertNotNull(acquireThrottle("wg-1", "role|admin"));
        expectThrows(OpenSearchRejectedExecutionException.class, () -> acquireThrottle("wg-1", "role|admin"));
    }

    public void testThrottleRejectionNamesGroupAndByValue() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("by", "username").put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));

        assertNotNull(acquireThrottle("wg-1", "username|alice"));
        OpenSearchRejectedExecutionException e = expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> acquireThrottle("wg-1", "username|alice")
        );
        // The operator (and the caller) must be able to tell which group and which principal was throttled.
        assertTrue(e.getMessage(), e.getMessage().contains("workload group [wg-1-name]"));
        assertTrue(e.getMessage(), e.getMessage().contains("username [alice]"));
        assertTrue(e.getMessage(), e.getMessage().contains("per-node limit of 1"));
    }

    public void testThrottleRejectionForWholeGroupOmitsByClause() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));

        assertNotNull(acquireThrottle("wg-1", null));
        OpenSearchRejectedExecutionException e = expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> acquireThrottle("wg-1", null)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("workload group [wg-1-name]"));
        assertFalse(e.getMessage(), e.getMessage().contains(" for group "));
    }

    public void testIncrementFailuresForUntaggedRequestDoesNotThrow() {
        // The failure listener passes null for an untagged request, and ConcurrentHashMap rejects null keys.
        workloadGroupService.incrementFailuresFor(null);
        assertEquals(
            1,
            mockWorkloadGroupsStateAccessor.getWorkloadGroupState(WorkloadGroupTask.DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get()).getFailures()
        );
    }

    public void testAcquireThrottleFailsOpenWhenPrincipalMissingForUsername() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("by", "username").put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));

        // No principal (e.g. security plugin not installed) or no matching subfield -> not throttled (fail open).
        assertNull(acquireThrottle("wg-1", null));
        assertNull(acquireThrottle("wg-1", ""));
        assertNull(acquireThrottle("wg-1", "role|admin")); // no username token
        assertEquals(0, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());
    }

    /**
     * A failure while recording the total_throttled stat must NOT swallow the rejection and admit the over-limit
     * request. Whether the state map lookup returns null (group not yet registered / just deleted) or throws, the
     * 429 must still propagate.
     */
    public void testAcquireThrottleStillRejectsWhenStatUpdateFails() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        Settings throttling = Settings.builder().put("node_limit", 1).build();
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

        assertNotNull(acquireThrottle(serviceWithNullState, "wg-1", null)); // first admit fills the single slot
        // second acquire is over the limit; a null state must not let the stat update swallow the 429
        expectThrows(OpenSearchRejectedExecutionException.class, () -> acquireThrottle(serviceWithNullState, "wg-1", null));

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

        assertNotNull(acquireThrottle(serviceWithThrowingState, "wg-1", null)); // fills the single slot
        expectThrows(OpenSearchRejectedExecutionException.class, () -> acquireThrottle(serviceWithThrowingState, "wg-1", null));
    }

    /**
     * During the state-registration lag a node can enforce a new group's limit before its clusterChanged() registers
     * the state. The rejection stat must not be misattributed to the DEFAULT group in that window.
     */
    public void testAcquireThrottleDoesNotMisattributeToDefaultDuringRegistrationLag() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        Settings throttling = Settings.builder().put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));

        // DEFAULT group state exists, but wg-1 is NOT yet registered (registration lag).
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup(WorkloadGroupTask.DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get());

        assertNotNull(acquireThrottle("wg-1", null)); // fills the single slot
        expectThrows(OpenSearchRejectedExecutionException.class, () -> acquireThrottle("wg-1", null));

        // the rejection must NOT have landed on the DEFAULT group
        assertEquals(
            0,
            mockWorkloadGroupsStateAccessor.getWorkloadGroupState(WorkloadGroupTask.DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get())
                .getTotalThrottled()
        );
    }

    public void testSharedTierOverflowEnforcesClusterLimit() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("node_limit", 1).put("shared_limit", 1).build();
        stubLocalSharedService(throttledGroup("wg-1", throttling));

        WorkloadGroupTask firstTask = throttleTask("wg-1");
        Releasable localPermit = acquireThrottlePermitSync(firstTask, () -> false);
        assertNotNull(localPermit);
        assertTrue(firstTask.isThrottleCounted());

        WorkloadGroupTask secondTask = throttleTask("wg-1");
        Releasable sharedPermit = acquireThrottlePermitSync(secondTask, () -> false);
        assertNotNull(sharedPermit);
        assertTrue(secondTask.isThrottleCounted());

        WorkloadGroupTask thirdTask = throttleTask("wg-1");
        OpenSearchRejectedExecutionException rejection = expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> acquireThrottlePermitSync(thirdTask, () -> false)
        );
        assertEquals(
            "Request throttled: workload group [wg-1-name] reached its per-node limit of 1 and shared limit of 1 concurrent requests.",
            rejection.getMessage()
        );
        assertFalse(thirdTask.isThrottleCounted());
        assertEquals(1, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());

        sharedPermit.close();
        Releasable replacement = acquireThrottlePermitSync(throttleTask("wg-1"), () -> false);
        assertNotNull(replacement);
        replacement.close();
        localPermit.close();
    }

    public void testSharedTierSupportsSharedOnlyAndZeroNodeLimit() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        for (Settings throttling : new Settings[] {
            Settings.builder().put("shared_limit", 1).build(),
            Settings.builder().put("node_limit", 0).put("shared_limit", 1).build() }) {
            stubLocalSharedService(throttledGroup("wg-1", throttling));
            Releasable permit = acquireThrottlePermitSync(throttleTask("wg-1"), () -> false);
            assertNotNull(permit);
            expectThrows(OpenSearchRejectedExecutionException.class, () -> acquireThrottlePermitSync(throttleTask("wg-1"), () -> false));
            permit.close();
        }
    }

    public void testSharedTierInheritsParentCharge() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        stubLocalSharedService(throttledGroup("wg-1", Settings.builder().put("shared_limit", 1).build()));

        WorkloadGroupTask root = throttleTask("wg-1");
        Releasable rootPermit = acquireThrottlePermitSync(root, () -> false);
        assertNotNull(rootPermit);
        WorkloadGroupTask nested = throttleTask("wg-1");
        assertNull(acquireThrottlePermitSync(nested, root::isThrottleCounted));
        assertTrue(nested.isThrottleCounted());
        WorkloadGroupTask grandchild = throttleTask("wg-1");
        assertNull(acquireThrottlePermitSync(grandchild, nested::isThrottleCounted));
        assertTrue(grandchild.isThrottleCounted());
        expectThrows(OpenSearchRejectedExecutionException.class, () -> acquireThrottlePermitSync(throttleTask("wg-1"), () -> false));
        rootPermit.close();
    }

    public void testSharedOnlyGroupIsNotThrottledWithoutAnEligibleOwner() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        stubLocalSharedService(
            throttledGroup("wg-1", Settings.builder().put("shared_limit", 1).build()),
            Set.of(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE)
        );

        // No node counts shared_limit, so the group acts as if it had none: every request runs, uncounted.
        for (int i = 0; i < 3; i++) {
            WorkloadGroupTask task = throttleTask("wg-1");
            assertNull(acquireThrottlePermitSync(task, () -> false));
            assertFalse(task.isThrottleCounted());
        }
        assertEquals(0, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());
    }

    public void testNodeLimitAloneDecidesWithoutAnEligibleOwner() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        stubLocalSharedService(
            throttledGroup("wg-1", Settings.builder().put("node_limit", 1).put("shared_limit", 5).build()),
            Set.of(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE)
        );

        Releasable nodePermit = acquireThrottlePermitSync(throttleTask("wg-1"), () -> false);
        assertNotNull(nodePermit);
        // Rejected by node_limit alone, as if shared_limit were unset, rather than reported as an unavailable shared tier.
        OpenSearchRejectedExecutionException rejection = expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> acquireThrottlePermitSync(throttleTask("wg-1"), () -> false)
        );
        assertEquals(
            "Request throttled: workload group [wg-1-name] reached its per-node limit of 1 concurrent requests.",
            rejection.getMessage()
        );
        assertEquals(1, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());
        nodePermit.close();
        Releasable next = acquireThrottlePermitSync(throttleTask("wg-1"), () -> false);
        assertNotNull("the freed node permit admits the next request", next);
        next.close();
    }

    public void testSearchPoolRejectionOfSharedHandOffIsNotCountedAsThrottle() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        stubClusterStateWithGroup(throttledGroup("wg-1", Settings.builder().put("shared_limit", 1).build()));
        // The shared service delivers a search-pool rejection (not its denial marker) when the hand-off is rejected.
        WorkloadGroupSharedThrottleService sharedService = Mockito.mock(WorkloadGroupSharedThrottleService.class);
        Mockito.when(sharedService.hasEligibleOwner()).thenReturn(true);
        OpenSearchRejectedExecutionException poolRejection = new OpenSearchRejectedExecutionException("search pool full");
        Mockito.doAnswer(invocation -> {
            ActionListener<Releasable> listener = invocation.getArgument(4);
            listener.onFailure(poolRejection);
            return null;
        }).when(sharedService).acquireAsync(any(), Mockito.anyInt(), Mockito.anyLong(), Mockito.anyBoolean(), any());
        workloadGroupService.setSharedThrottleService(sharedService);

        WorkloadGroupTask task = throttleTask("wg-1");
        OpenSearchRejectedExecutionException rejection = expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> acquireThrottlePermitSync(task, () -> false)
        );
        assertSame("the pool rejection must pass through unchanged", poolRejection, rejection);
        assertEquals(
            "a pool rejection is not a throttle breach",
            0,
            mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled()
        );
    }

    public void testMonitorNeverRejectsOnSharedHandOffRejection() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        stubClusterStateWithGroup(
            throttledGroup("wg-1", Settings.builder().put("shared_limit", 1).build(), MutableWorkloadGroupFragment.ResiliencyMode.MONITOR)
        );
        WorkloadGroupSharedThrottleService sharedService = Mockito.mock(WorkloadGroupSharedThrottleService.class);
        Mockito.when(sharedService.hasEligibleOwner()).thenReturn(true);
        AtomicBoolean proceedsOnDenial = new AtomicBoolean();
        Mockito.doAnswer(invocation -> {
            proceedsOnDenial.set(invocation.getArgument(3));
            ActionListener<Releasable> listener = invocation.getArgument(4);
            // The shared service never does this for MONITOR; the caller must still not turn it into a 429.
            listener.onFailure(new OpenSearchRejectedExecutionException("search pool full"));
            return null;
        }).when(sharedService).acquireAsync(any(), Mockito.anyInt(), Mockito.anyLong(), Mockito.anyBoolean(), any());
        workloadGroupService.setSharedThrottleService(sharedService);

        WorkloadGroupTask task = throttleTask("wg-1");
        assertNull("MONITOR never rejects", acquireThrottlePermitSync(task, () -> false));
        assertTrue("MONITOR must ask the shared tier to deliver denials where the search continues", proceedsOnDenial.get());
        assertEquals(1, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalWouldThrottle());
        assertEquals(0, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());
    }

    public void testSharedTierUsesTaskPrincipalForBuckets() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("by", "username").put("shared_limit", 1).build();
        stubLocalSharedService(throttledGroup("wg-1", throttling));

        WorkloadGroupTask alice = throttleTask("wg-1");
        alice.setThrottlePrincipal("username|alice");
        Releasable alicePermit = acquireThrottlePermitSync(alice, () -> false);
        assertNotNull(alicePermit);

        WorkloadGroupTask secondAlice = throttleTask("wg-1");
        secondAlice.setThrottlePrincipal("username|alice");
        OpenSearchRejectedExecutionException rejection = expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> acquireThrottlePermitSync(secondAlice, () -> false)
        );
        assertEquals(
            "Request throttled: workload group [wg-1-name] for username [alice] reached its shared limit of 1 concurrent requests.",
            rejection.getMessage()
        );

        WorkloadGroupTask bob = throttleTask("wg-1");
        bob.setThrottlePrincipal("username|bob");
        Releasable bobPermit = acquireThrottlePermitSync(bob, () -> false);
        assertNotNull(bobPermit);
        bobPermit.close();
        alicePermit.close();
    }

    public void testSharedTierFailsClosedWhenUnavailable() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        // No shared service wired: the shared tier cannot answer, exactly like an unreachable owner.
        stubClusterStateWithGroup(throttledGroup("wg-1", Settings.builder().put("shared_limit", 1).build()));

        WorkloadGroupTask task = throttleTask("wg-1");
        OpenSearchRejectedExecutionException rejection = expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> acquireThrottlePermitSync(task, () -> false)
        );
        assertEquals(
            "Request throttled: workload group [wg-1-name] could not check its shared limit of 1 concurrent requests "
                + "(cluster-wide throttle unavailable).",
            rejection.getMessage()
        );
        assertFalse(task.isThrottleCounted());
        assertEquals(1, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());
    }

    public void testSharedTierUnavailableStillAdmitsWithinNodeLimit() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        stubClusterStateWithGroup(throttledGroup("wg-1", Settings.builder().put("node_limit", 1).put("shared_limit", 1).build()));

        Releasable local = acquireThrottlePermitSync(throttleTask("wg-1"), () -> false);
        assertNotNull("the node tier is unaffected by the shared tier being unavailable", local);
        OpenSearchRejectedExecutionException rejection = expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> acquireThrottlePermitSync(throttleTask("wg-1"), () -> false)
        );
        assertEquals(
            "Request throttled: workload group [wg-1-name] reached its per-node limit of 1 concurrent requests and could not "
                + "check its shared limit of 1 concurrent requests (cluster-wide throttle unavailable).",
            rejection.getMessage()
        );
        local.close();
    }

    public void testSharedTierUnavailableInMonitorAdmitsAndCountsWouldThrottle() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        stubClusterStateWithGroup(
            throttledGroup("wg-1", Settings.builder().put("shared_limit", 1).build(), MutableWorkloadGroupFragment.ResiliencyMode.MONITOR)
        );

        WorkloadGroupTask task = throttleTask("wg-1");
        assertNull("MONITOR never rejects", acquireThrottlePermitSync(task, () -> false));
        assertTrue(task.isThrottleCounted());
        assertEquals(1, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalWouldThrottle());
        assertEquals(0, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());
    }

    public void testUnexpectedSharedTierErrorFailsClosed() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        stubClusterStateWithGroup(throttledGroup("wg-1", Settings.builder().put("shared_limit", 1).build()));
        WorkloadGroupSharedThrottleService sharedService = Mockito.mock(WorkloadGroupSharedThrottleService.class);
        Mockito.when(sharedService.hasEligibleOwner()).thenReturn(true);
        Mockito.doAnswer(invocation -> {
            ActionListener<Releasable> listener = invocation.getArgument(4);
            listener.onFailure(new IllegalStateException("boom"));
            return null;
        }).when(sharedService).acquireAsync(any(), Mockito.anyInt(), Mockito.anyLong(), Mockito.anyBoolean(), any());
        workloadGroupService.setSharedThrottleService(sharedService);

        OpenSearchRejectedExecutionException rejection = expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> acquireThrottlePermitSync(throttleTask("wg-1"), () -> false)
        );
        assertTrue(rejection.getMessage(), rejection.getMessage().contains("cluster-wide throttle unavailable"));
    }

    public void testSharedMonitorDenialAdmitsAndCountsWouldThrottle() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("shared_limit", 1).build();
        stubLocalSharedService(throttledGroup("wg-1", throttling, MutableWorkloadGroupFragment.ResiliencyMode.MONITOR));

        Releasable permit = acquireThrottlePermitSync(throttleTask("wg-1"), () -> false);
        assertNotNull(permit);
        WorkloadGroupTask second = throttleTask("wg-1");
        assertNull("MONITOR never rejects", acquireThrottlePermitSync(second, () -> false));
        assertTrue(second.isThrottleCounted());
        assertEquals(1, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalWouldThrottle());
        assertEquals(0, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());
        permit.close();
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
