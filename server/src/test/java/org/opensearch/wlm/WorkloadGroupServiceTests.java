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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;

import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.opensearch.wlm.tracker.ResourceUsageCalculatorTests.createMockTaskWithResourceStats;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
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
        WorkloadGroupTask task = taskForGroup(workloadGroupId);
        service.acquireThrottlePermit(task, principal, ActionListener.wrap(permit::set, failure::set));
        if (failure.get() != null) {
            if (failure.get() instanceof RuntimeException) {
                throw (RuntimeException) failure.get();
            }
            throw new RuntimeException(failure.get());
        }
        return permit.get();
    }

    private static WorkloadGroupTask taskForGroup(String workloadGroupId) {
        // Build a SearchTask whose workload group id is supplied via a real thread-context header. The test thread pool
        // is usually a Mockito mock, so this helper is self-contained.
        WorkloadGroupTask task = new SearchTask(randomNonNegativeLong(), "", "", () -> "", null, null);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        if (workloadGroupId != null) {
            threadContext.putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, workloadGroupId);
        }
        task.setWorkloadGroupId(threadContext);
        return task;
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

    private WorkloadGroupQueueService newDirectQueueService() {
        when(mockThreadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(OpenSearchExecutors.newDirectExecutorService());
        WorkloadGroupQueueService queueService = new WorkloadGroupQueueService(mockThreadPool, mockWorkloadGroupsStateAccessor);
        workloadGroupService.setQueueService(queueService);
        return queueService;
    }

    private void enqueueWaitingRequest(WorkloadGroupQueueService queueService, String groupId, String bucketKey, AtomicInteger admissions) {
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup(groupId);
        WorkloadGroupTask task = new SearchTask(randomNonNegativeLong(), "", "", () -> "", null, null);
        assertTrue(queueService.tryEnqueue(groupId, bucketKey, task, 10, ActionListener.wrap(permit -> {
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
        WorkloadGroupTask task = new SearchTask(randomNonNegativeLong(), "", "", () -> "", null, null);
        assertNotNull(queueService.tryRegisterPendingAcquire(groupId, bucketKey, task, ActionListener.wrap(permit -> {
            admissions.incrementAndGet();
            permit.close();
        }, e -> { throw new AssertionError("pending queue-drain request failed instead of being admitted", e); })));
    }

    public void testRemovingFinalThrottleTierSchedulesQueuedBacklogDrain() {
        // Removing the final tier leaves parked requests with no permit source, so the config callback must run them all
        // untracked instead of waiting for the periodic service loop.
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        when(mockThreadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(OpenSearchExecutors.newDirectExecutorService());

        Settings throttling = Settings.builder().put("group.node_limit", 1).build();
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

        // Operator removes the final throttle tier. Queue settings may remain configured, but queueing becomes inert;
        // any backlog retained under the old admission policy must run without waiting for the periodic service sweep.
        WorkloadGroup unthrottled = new WorkloadGroup(
            "wg-1-name",
            "wg-1",
            new MutableWorkloadGroupFragment(MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED, Map.of(ResourceType.MEMORY, 0.5)),
            2L
        );
        deliverWorkloadGroupsChanged(Map.of("wg-1", throttled), Map.of("wg-1", unthrottled));

        assertEquals("the scheduled policy drain must release the whole backlog", 0, queueService.currentDepth("wg-1"));
    }

    public void testDeletingGroupDrainsWaitingAndPendingRequests() {
        WorkloadGroupQueueService queueService = newDirectQueueService();
        AtomicInteger admissions = new AtomicInteger();
        enqueueWaitingRequest(queueService, "wg-1", "wg-1:group", admissions);
        registerPendingRequest(queueService, "wg-1", "wg-1:group", admissions);
        assertEquals(1, queueService.currentDepth("wg-1"));
        assertEquals(2, queueService.retainedDepth("wg-1"));

        Settings throttling = Settings.builder().put("group.node_limit", 1).build();
        Settings queue = Settings.builder().put("size_per_bucket", 10).build();
        WorkloadGroup group = queueingGroup("wg-1", throttling, queue, MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED);
        deliverWorkloadGroupsChanged(Map.of("wg-1", group), Map.of());

        assertEquals(0, queueService.retainedDepth("wg-1"));
        assertEquals(2, admissions.get());
    }

    public void testThrottleAttributeChangeDrainsQueue() {
        WorkloadGroupQueueService queueService = newDirectQueueService();
        AtomicInteger admissions = new AtomicInteger();
        enqueueWaitingRequest(queueService, "wg-1", "wg-1:group", admissions);

        Settings queue = Settings.builder().put("size_per_bucket", 10).build();
        WorkloadGroup groupAttribute = queueingGroup(
            "wg-1",
            Settings.builder().put("group.node_limit", 1).build(),
            queue,
            MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
        );
        WorkloadGroup usernameAttribute = queueingGroup(
            "wg-1",
            Settings.builder().put("username.node_limit", 1).build(),
            queue,
            MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
        );
        deliverWorkloadGroupsChanged(Map.of("wg-1", groupAttribute), Map.of("wg-1", usernameAttribute));

        assertEquals(0, queueService.retainedDepth("wg-1"));
        assertEquals(1, admissions.get());
    }

    public void testSwitchingThrottleTierDrainsQueue() {
        WorkloadGroupQueueService queueService = newDirectQueueService();
        AtomicInteger admissions = new AtomicInteger();
        enqueueWaitingRequest(queueService, "wg-1", "wg-1:group", admissions);

        Settings queue = Settings.builder().put("size_per_bucket", 10).build();
        WorkloadGroup nodeOnly = queueingGroup(
            "wg-1",
            Settings.builder().put("group.node_limit", 1).build(),
            queue,
            MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
        );
        WorkloadGroup sharedOnly = queueingGroup(
            "wg-1",
            Settings.builder().put("group.shared_limit", 1).build(),
            queue,
            MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
        );
        deliverWorkloadGroupsChanged(Map.of("wg-1", nodeOnly), Map.of("wg-1", sharedOnly));

        assertEquals(0, queueService.retainedDepth("wg-1"));
        assertEquals(1, admissions.get());
    }

    public void testAddingThrottleTierDrainsQueue() {
        WorkloadGroupQueueService queueService = newDirectQueueService();
        AtomicInteger admissions = new AtomicInteger();
        enqueueWaitingRequest(queueService, "wg-1", "wg-1:group", admissions);

        Settings queue = Settings.builder().put("size_per_bucket", 10).build();
        WorkloadGroup nodeOnly = queueingGroup(
            "wg-1",
            Settings.builder().put("group.node_limit", 1).build(),
            queue,
            MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
        );
        WorkloadGroup bothTiers = queueingGroup(
            "wg-1",
            Settings.builder().put("group.node_limit", 1).put("group.shared_limit", 1).build(),
            queue,
            MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
        );
        deliverWorkloadGroupsChanged(Map.of("wg-1", nodeOnly), Map.of("wg-1", bothTiers));

        assertEquals(0, queueService.retainedDepth("wg-1"));
        assertEquals(1, admissions.get());
    }

    public void testEnteringMonitorModeDrainsQueue() {
        WorkloadGroupQueueService queueService = newDirectQueueService();
        AtomicInteger admissions = new AtomicInteger();
        enqueueWaitingRequest(queueService, "wg-1", "wg-1:group", admissions);

        Settings throttling = Settings.builder().put("group.node_limit", 1).build();
        Settings queue = Settings.builder().put("size_per_bucket", 10).build();
        WorkloadGroup enforced = queueingGroup("wg-1", throttling, queue, MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED);
        WorkloadGroup monitor = queueingGroup("wg-1", throttling, queue, MutableWorkloadGroupFragment.ResiliencyMode.MONITOR);
        deliverWorkloadGroupsChanged(Map.of("wg-1", enforced), Map.of("wg-1", monitor));

        assertEquals(0, queueService.retainedDepth("wg-1"));
        assertEquals(1, admissions.get());
    }

    public void testConfigChangeDispatchesQueueDrainOffTheClusterApplierThread() {
        ExecutorService delayedExecutor = Mockito.mock(ExecutorService.class);
        when(mockThreadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(delayedExecutor);
        WorkloadGroupQueueService queueService = new WorkloadGroupQueueService(mockThreadPool, mockWorkloadGroupsStateAccessor);
        workloadGroupService.setQueueService(queueService);
        AtomicInteger admissions = new AtomicInteger();
        enqueueWaitingRequest(queueService, "wg-1", "wg-1:group", admissions);

        Settings throttling = Settings.builder().put("group.node_limit", 1).build();
        Settings queue = Settings.builder().put("size_per_bucket", 10).build();
        WorkloadGroup enforced = queueingGroup("wg-1", throttling, queue, MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED);
        WorkloadGroup monitor = queueingGroup("wg-1", throttling, queue, MutableWorkloadGroupFragment.ResiliencyMode.MONITOR);
        deliverWorkloadGroupsChanged(Map.of("wg-1", enforced), Map.of("wg-1", monitor));

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
        enqueueWaitingRequest(queueService, "wg-1", "wg-1:group", admissions);

        Settings queue = Settings.builder().put("size_per_bucket", 10).build();
        WorkloadGroup previous = queueingGroup(
            "wg-1",
            Settings.builder().put("group.node_limit", 1).put("group.shared_limit", 2).build(),
            queue,
            MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
        );
        WorkloadGroup current = queueingGroup(
            "wg-1",
            Settings.builder().put("group.node_limit", 3).put("group.shared_limit", 4).build(),
            queue,
            MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
        );
        deliverWorkloadGroupsChanged(Map.of("wg-1", previous), Map.of("wg-1", current));

        assertEquals(1, queueService.retainedDepth("wg-1"));
        assertEquals(0, admissions.get());
    }

    public void testQueueDepthAndOtherNonMonitorChangesDoNotDrainQueue() {
        WorkloadGroupQueueService queueService = newDirectQueueService();
        AtomicInteger admissions = new AtomicInteger();
        enqueueWaitingRequest(queueService, "wg-1", "wg-1:group", admissions);

        Settings throttling = Settings.builder().put("group.node_limit", 1).build();
        WorkloadGroup original = queueingGroup(
            "wg-1",
            throttling,
            Settings.builder().put("size_per_bucket", 10).build(),
            MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
        );
        WorkloadGroup resizedQueue = queueingGroup(
            "wg-1",
            throttling,
            Settings.builder().put("size_per_bucket", 20).build(),
            MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
        );
        deliverWorkloadGroupsChanged(Map.of("wg-1", original), Map.of("wg-1", resizedQueue));
        assertEquals(1, queueService.retainedDepth("wg-1"));

        WorkloadGroup soft = queueingGroup(
            "wg-1",
            throttling,
            Settings.builder().put("size_per_bucket", 20).build(),
            MutableWorkloadGroupFragment.ResiliencyMode.SOFT
        );
        deliverWorkloadGroupsChanged(Map.of("wg-1", resizedQueue), Map.of("wg-1", soft));

        assertEquals(1, queueService.retainedDepth("wg-1"));
        assertEquals(0, admissions.get());
    }

    public void testGlobalModeLeavingEnabledDrainsAllQueues() {
        assertNotNull(wlmModeChangeListener);
        WorkloadGroupQueueService queueService = newDirectQueueService();
        AtomicInteger admissions = new AtomicInteger();

        enqueueWaitingRequest(queueService, "wg-1", "wg-1:group", admissions);
        registerPendingRequest(queueService, "wg-2", "wg-2:group", admissions);
        wlmModeChangeListener.accept(WlmMode.ENABLED, WlmMode.DISABLED);
        assertEquals(0, queueService.retainedDepth("wg-1"));
        assertEquals(0, queueService.retainedDepth("wg-2"));
        assertEquals(2, admissions.get());

        registerPendingRequest(queueService, "wg-3", "wg-3:group", admissions);
        wlmModeChangeListener.accept(WlmMode.ENABLED, WlmMode.MONITOR_ONLY);
        assertEquals(0, queueService.retainedDepth("wg-3"));
        assertEquals(3, admissions.get());
    }

    public void testGlobalModeTransitionNotLeavingEnabledDoesNotDrainQueue() {
        assertNotNull(wlmModeChangeListener);
        WorkloadGroupQueueService queueService = newDirectQueueService();
        AtomicInteger admissions = new AtomicInteger();
        enqueueWaitingRequest(queueService, "wg-1", "wg-1:group", admissions);

        wlmModeChangeListener.accept(WlmMode.MONITOR_ONLY, WlmMode.DISABLED);
        wlmModeChangeListener.accept(WlmMode.DISABLED, WlmMode.ENABLED);

        assertEquals(1, queueService.retainedDepth("wg-1"));
        assertEquals(0, admissions.get());
    }

    public void testNodePermitCompletionHandsSlotToQueueBeforeRacingArrival() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        when(mockThreadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(OpenSearchExecutors.newDirectExecutorService());

        Settings throttling = Settings.builder().put("group.node_limit", 1).build();
        Settings queue = Settings.builder().put("size_per_bucket", 5).build();
        stubClusterStateWithGroup(queueingGroup("wg-1", throttling, queue, MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED));

        AtomicBoolean injectedRacingArrival = new AtomicBoolean(false);
        AtomicReference<Releasable> racingArrivalPermit = new AtomicReference<>();
        AtomicReference<Exception> racingArrivalFailure = new AtomicReference<>();
        WorkloadGroupQueueService queueService = new WorkloadGroupQueueService(mockThreadPool, mockWorkloadGroupsStateAccessor) {
            @Override
            public boolean handoffNodePermit(String groupId, String bucketKey, Releasable permit) {
                if (injectedRacingArrival.compareAndSet(false, true)) {
                    // Run a new arrival at the exact point where the old implementation had already released the raw
                    // tracker slot but had not yet reacquired it for the queue. The handoff implementation still owns
                    // that slot, so this request must queue instead of barging ahead.
                    workloadGroupService.acquireThrottlePermit(
                        taskForGroup("wg-1"),
                        null,
                        ActionListener.wrap(racingArrivalPermit::set, racingArrivalFailure::set)
                    );
                    assertNull("the racing request must not acquire the queued request's slot", racingArrivalPermit.get());
                    assertNull(racingArrivalFailure.get());
                    assertEquals("oldest waiter plus racing arrival", 2, retainedDepth("wg-1"));
                }
                return super.handoffNodePermit(groupId, bucketKey, permit);
            }
        };
        workloadGroupService.setQueueService(queueService);

        Releasable holder = acquireThrottlePermitSync(workloadGroupService, "wg-1", null);
        assertNotNull(holder);
        AtomicReference<Releasable> oldestQueuedPermit = new AtomicReference<>();
        assertTrue(
            queueService.tryEnqueue(
                "wg-1",
                "wg-1:group",
                taskForGroup("wg-1"),
                5,
                ActionListener.wrap(oldestQueuedPermit::set, e -> fail("oldest queued request failed: " + e))
            )
        );

        holder.close();
        holder.close(); // the wrapper itself must be one-shot; no second handoff from a duplicate completion callback

        assertTrue(injectedRacingArrival.get());
        assertNotNull("the oldest queued request receives the still-held node slot", oldestQueuedPermit.get());
        assertNull("the racing request remains queued behind it", racingArrivalPermit.get());
        assertEquals(1, queueService.retainedDepth("wg-1"));

        oldestQueuedPermit.get().close();
        assertNotNull("the same underlying slot chains to the next queued request", racingArrivalPermit.get());
        assertEquals(0, queueService.retainedDepth("wg-1"));
        racingArrivalPermit.get().close();

        Releasable afterDrain = acquireThrottlePermitSync(workloadGroupService, "wg-1", null);
        assertNotNull("the underlying tracker slot must be released when the queue empties", afterDrain);
        afterDrain.close();
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
        Settings limit2 = Settings.builder().put("group.node_limit", 2).build();
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
        Settings limit1 = Settings.builder().put("group.node_limit", 1).build();
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
        Settings throttling = Settings.builder().put("group.node_limit", 1).build();
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
        Settings throttling = Settings.builder().put("group.node_limit", 1).build();
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
        Settings throttling = Settings.builder().put("group.node_limit", 1).build();
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
        Settings throttling = Settings.builder().put("username.node_limit", 1).build();
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
        Settings throttling = Settings.builder().put("username.node_limit", 1).build();
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
        Settings throttling = Settings.builder().put("username.node_limit", 1).build();
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
        Settings throttling = Settings.builder().put("role.node_limit", 1).build();
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
        Settings throttling = Settings.builder().put("username.node_limit", 1).build();
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
        Settings throttling = Settings.builder().put("group.node_limit", 1).build();
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
        Settings throttling = Settings.builder().put("group.node_limit", 1).build();
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

        Settings throttling = Settings.builder().put("group.node_limit", 1).put("group.shared_limit", 1).build();
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

        Settings throttling = Settings.builder().put("group.node_limit", 1).put("group.shared_limit", 1).build();
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

        Settings throttling = Settings.builder().put("group.node_limit", 1).put("group.shared_limit", 1).build();
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

    // Starts an admission and records how it completes, so a test can tell "parked" (not completed) from "admitted
    // untracked" (completed with a null permit).
    private static final class PendingAdmission {
        final AtomicBoolean completed = new AtomicBoolean();
        final AtomicReference<Releasable> permit = new AtomicReference<>();
        final AtomicReference<Exception> failure = new AtomicReference<>();
    }

    private PendingAdmission startAdmission(String workloadGroupId, String principal) {
        PendingAdmission admission = new PendingAdmission();
        workloadGroupService.acquireThrottlePermit(taskForGroup(workloadGroupId), principal, ActionListener.wrap(p -> {
            admission.permit.set(p);
            admission.completed.set(true);
        }, e -> {
            admission.failure.set(e);
            admission.completed.set(true);
        }));
        return admission;
    }

    private static String principal(String... tokens) {
        return String.join(WorkloadGroupTask.WORKLOAD_GROUP_PRINCIPAL_VALUE_DELIMITER, tokens);
    }

    public void testMultiRuleRequestNeedsAPermitFromEveryRule() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("username.node_limit", 1).put("role.node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));

        Releasable aliceAnalyst = acquireThrottlePermitSync(workloadGroupService, "wg-1", principal("username|alice", "role|analyst"));
        assertNotNull(aliceAnalyst);

        // bob has room under the username rule but the analyst role is full: rejected, naming the role rule.
        OpenSearchRejectedExecutionException e = expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> acquireThrottlePermitSync(workloadGroupService, "wg-1", principal("username|bob", "role|analyst"))
        );
        assertEquals(
            "Request throttled: workload group [wg-1-name] for role [analyst] reached its per-node limit of 1 concurrent requests.",
            e.getMessage()
        );
        assertEquals(1, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());

        // The rejection released the username permit bob took first, so bob still fits under a different role.
        Releasable bobAdmin = acquireThrottlePermitSync(workloadGroupService, "wg-1", principal("username|bob", "role|admin"));
        assertNotNull(bobAdmin);

        // Closing alice's combined permit frees both of her buckets.
        aliceAnalyst.close();
        aliceAnalyst.close(); // idempotent
        assertNotNull(acquireThrottlePermitSync(workloadGroupService, "wg-1", principal("username|alice", "role|analyst")));
        bobAdmin.close();
    }

    public void testUserWithSeveralRolesIsChargedToTheirSmallestRole() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        stubClusterStateWithGroup(throttledGroup("wg-1", Settings.builder().put("role.node_limit", 1).build()));

        // alice holds [analyst, admin]: she lands in the lexicographically smallest role's bucket, admin, only.
        Releasable alice = acquireThrottlePermitSync(
            workloadGroupService,
            "wg-1",
            principal("username|alice", "role|analyst", "role|admin")
        );
        assertNotNull(alice);
        OpenSearchRejectedExecutionException e = expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> acquireThrottlePermitSync(workloadGroupService, "wg-1", principal("username|carol", "role|admin"))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("for role [admin]"));
        // The analyst bucket was never charged, so bob (analyst only) is admitted.
        Releasable bob = acquireThrottlePermitSync(workloadGroupService, "wg-1", principal("username|bob", "role|analyst"));
        assertNotNull(bob);

        alice.close();
        bob.close();
        Releasable carol = acquireThrottlePermitSync(workloadGroupService, "wg-1", principal("username|carol", "role|admin"));
        assertNotNull(carol);
        carol.close();
    }

    public void testRulesAreAcquiredNarrowestFirst() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("group.node_limit", 1).put("username.node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));

        Releasable alice = acquireThrottlePermitSync(workloadGroupService, "wg-1", "username|alice");
        assertNotNull(alice);
        // Both of alice's buckets are full; the username rule is checked first, so it is the one named.
        OpenSearchRejectedExecutionException aliceAgain = expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> acquireThrottlePermitSync(workloadGroupService, "wg-1", "username|alice")
        );
        assertTrue(aliceAgain.getMessage(), aliceAgain.getMessage().contains("for username [alice] reached its per-node limit"));
        // bob passes his own username rule and is stopped by the group rule.
        OpenSearchRejectedExecutionException bob = expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> acquireThrottlePermitSync(workloadGroupService, "wg-1", "username|bob")
        );
        assertEquals(
            "Request throttled: workload group [wg-1-name] reached its per-node limit of 1 concurrent requests.",
            bob.getMessage()
        );
        alice.close();
    }

    public void testRuleWithoutAValueIsSkippedButOtherRulesStillApply() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("username.node_limit", 1).put("group.node_limit", 2).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));

        // No principal: the username rule can't attribute the request, but the group rule still counts it.
        Releasable first = acquireThrottlePermitSync(workloadGroupService, "wg-1", null);
        Releasable second = acquireThrottlePermitSync(workloadGroupService, "wg-1", null);
        assertNotNull(first);
        assertNotNull(second);
        expectThrows(OpenSearchRejectedExecutionException.class, () -> acquireThrottlePermitSync(workloadGroupService, "wg-1", null));
        first.close();
        second.close();
    }

    public void testMonitorModeObservesEachRuleAndKeepsGoing() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("username.node_limit", 1).put("group.node_limit", 5).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling, MutableWorkloadGroupFragment.ResiliencyMode.MONITOR));

        Releasable first = acquireThrottlePermitSync(workloadGroupService, "wg-1", "username|alice");
        assertNotNull(first);
        // alice's username bucket would breach: observed and skipped, but the group rule still tracks the request.
        Releasable second = acquireThrottlePermitSync(workloadGroupService, "wg-1", "username|alice");
        assertNotNull("the group rule must still hand out a tracked permit", second);
        assertEquals(0, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());
        first.close();
        second.close();
    }

    public void testRequestQueuedAtALaterRuleHoldsEarlierPermitsAndResumes() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("username.node_limit", 1).put("group.node_limit", 1).build();
        Settings queue = Settings.builder().put("size_per_bucket", 5).build();
        stubClusterStateWithGroup(queueingGroup("wg-1", throttling, queue, MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED));
        WorkloadGroupQueueService queueService = newDirectQueueService();

        Releasable bob = acquireThrottlePermitSync(workloadGroupService, "wg-1", "username|bob");
        assertNotNull(bob);

        // alice gets her username permit, then parks on the full group bucket while holding it.
        PendingAdmission alice = startAdmission("wg-1", "username|alice");
        assertFalse("alice must be parked on the group bucket", alice.completed.get());
        assertEquals(1, queueService.currentDepth("wg-1"));
        PendingAdmission aliceAgain = startAdmission("wg-1", "username|alice");
        assertFalse("alice's second request waits on her own (held) username bucket", aliceAgain.completed.get());

        // bob finishing hands the group slot to alice, who then completes with both permits.
        bob.close();
        assertTrue(alice.completed.get());
        assertNull(alice.failure.get());
        assertNotNull(alice.permit.get());
        assertFalse("alice's second request is still waiting behind her first", aliceAgain.completed.get());

        // alice finishing releases both buckets: her queued second request takes the username slot, then the group slot.
        alice.permit.get().close();
        assertTrue(aliceAgain.completed.get());
        assertNotNull(aliceAgain.permit.get());
        aliceAgain.permit.get().close();
        assertEquals(0, queueService.retainedDepth("wg-1"));

        // Nothing leaked: both buckets admit again.
        Releasable after = acquireThrottlePermitSync(workloadGroupService, "wg-1", "username|alice");
        assertNotNull(after);
        after.close();
    }

    public void testRejectionAfterQueueingReleasesEarlierPermits() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("username.node_limit", 1).put("group.node_limit", 1).build();
        Settings queue = Settings.builder().put("size_per_bucket", 1).build();
        stubClusterStateWithGroup(queueingGroup("wg-1", throttling, queue, MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED));
        WorkloadGroupQueueService queueService = newDirectQueueService();

        Releasable bob = acquireThrottlePermitSync(workloadGroupService, "wg-1", "username|bob");
        PendingAdmission carol = startAdmission("wg-1", "username|carol"); // parks on the group bucket (fills it)
        assertFalse(carol.completed.get());

        // dave: username ok, group bucket full and its queue full -> 429, and dave's username permit must be returned.
        expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> acquireThrottlePermitSync(workloadGroupService, "wg-1", "username|dave")
        );
        bob.close(); // carol is admitted
        assertNotNull(carol.permit.get());
        carol.permit.get().close();
        Releasable dave = acquireThrottlePermitSync(workloadGroupService, "wg-1", "username|dave");
        assertNotNull("dave's username permit from the rejected attempt must have been released", dave);
        dave.close();
        assertEquals(0, queueService.retainedDepth("wg-1"));
    }

    public void testLiveConfigDrainReleasesTheBucketButKeepsUnchangedRules() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("username.node_limit", 1).put("group.node_limit", 1).build();
        Settings queue = Settings.builder().put("size_per_bucket", 5).build();
        WorkloadGroup original = queueingGroup("wg-1", throttling, queue, MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED);
        stubClusterStateWithGroup(original);
        WorkloadGroupQueueService queueService = newDirectQueueService();

        Releasable alice = acquireThrottlePermitSync(workloadGroupService, "wg-1", "username|alice");
        assertNotNull(alice);
        // alice's second request parks on her username bucket, before reaching the (also full) group bucket.
        PendingAdmission parked = startAdmission("wg-1", "username|alice");
        assertFalse(parked.completed.get());

        // Removing the username rule changes the group's admission shape, so the backlog is released from the username
        // bucket. The unchanged group rule still applies: the request moves on and waits for the group bucket.
        WorkloadGroup groupOnly = queueingGroup(
            "wg-1",
            Settings.builder().put("group.node_limit", 1).build(),
            queue,
            MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
        );
        stubClusterStateWithGroup(groupOnly);
        deliverWorkloadGroupsChanged(Map.of("wg-1", original), Map.of("wg-1", groupOnly));

        assertFalse("the unchanged group rule must still hold the request back", parked.completed.get());
        assertEquals(1, queueService.currentDepth("wg-1"));
        alice.close();
        assertTrue(parked.completed.get());
        assertNull(parked.failure.get());
        assertNotNull("admitted against the group bucket", parked.permit.get());
        parked.permit.get().close();
        assertEquals(0, queueService.retainedDepth("wg-1"));
    }

    public void testLaterBucketsUseLiveLimits() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings queue = Settings.builder().put("size_per_bucket", 5).build();
        WorkloadGroup wide = queueingGroup(
            "wg-1",
            Settings.builder().put("username.node_limit", 1).put("group.node_limit", 100).build(),
            queue,
            MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
        );
        stubClusterStateWithGroup(wide);
        WorkloadGroupQueueService queueService = newDirectQueueService();

        Releasable alice = acquireThrottlePermitSync(workloadGroupService, "wg-1", "username|alice"); // group 1/100
        Releasable bob = acquireThrottlePermitSync(workloadGroupService, "wg-1", "username|bob");     // group 2/100
        PendingAdmission parked = startAdmission("wg-1", "username|alice"); // waits on alice's username bucket
        assertFalse(parked.completed.get());

        // The operator lowers group.node_limit to 2 (same shape, so nothing drains). When alice's queued request resumes,
        // it must acquire the group bucket against the live limit of 2, which is full — not the 100 seen on arrival.
        stubClusterStateWithGroup(
            queueingGroup(
                "wg-1",
                Settings.builder().put("username.node_limit", 1).put("group.node_limit", 2).build(),
                queue,
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
            )
        );
        alice.close(); // hands alice's username slot to the parked request; the group bucket is now at 1/2 (bob)
        assertTrue("1 of 2 group slots is free, so it is admitted", parked.completed.get());
        assertNotNull(parked.permit.get());
        // group is now 2/2: a third request (carol) must wait at the live limit of 2.
        PendingAdmission carol = startAdmission("wg-1", "username|carol");
        assertFalse("the live group limit of 2 is reached", carol.completed.get());
        bob.close();
        assertTrue(carol.completed.get());
        carol.permit.get().close();
        parked.permit.get().close();
        assertEquals(0, queueService.retainedDepth("wg-1"));
    }

    public void testRuleRemovedMidChainIsSkippedNotWaitedOn() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings queue = Settings.builder().put("size_per_bucket", 5).build();
        WorkloadGroup both = queueingGroup(
            "wg-1",
            Settings.builder().put("username.node_limit", 1).put("group.node_limit", 1).build(),
            queue,
            MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
        );
        stubClusterStateWithGroup(both);
        WorkloadGroupQueueService queueService = newDirectQueueService();

        Releasable alice = acquireThrottlePermitSync(workloadGroupService, "wg-1", "username|alice");
        PendingAdmission parked = startAdmission("wg-1", "username|alice");
        assertFalse(parked.completed.get());

        // The group rule is removed before this node processes the change (no drain): the resumed request must skip it.
        stubClusterStateWithGroup(
            queueingGroup(
                "wg-1",
                Settings.builder().put("username.node_limit", 1).build(),
                queue,
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
            )
        );
        alice.close();
        assertTrue(parked.completed.get());
        assertNull(parked.failure.get());
        assertNotNull("holds only its username permit", parked.permit.get());
        parked.permit.get().close();
        assertEquals(0, queueService.retainedDepth("wg-1"));
    }

    public void testCancelledRequestStopsAcquiringAndReleasesHeldPermits() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("username.node_limit", 1).put("group.node_limit", 1).build();
        Settings queue = Settings.builder().put("size_per_bucket", 5).build();
        stubClusterStateWithGroup(queueingGroup("wg-1", throttling, queue, MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED));
        WorkloadGroupQueueService queueService = newDirectQueueService();

        Releasable bob = acquireThrottlePermitSync(workloadGroupService, "wg-1", "username|bob");
        // alice takes her username permit and parks on the group bucket while holding it.
        WorkloadGroupTask aliceTask = taskForGroup("wg-1");
        PendingAdmission alice = new PendingAdmission();
        workloadGroupService.acquireThrottlePermit(aliceTask, "username|alice", ActionListener.wrap(p -> {
            alice.permit.set(p);
            alice.completed.set(true);
        }, e -> {
            alice.failure.set(e);
            alice.completed.set(true);
        }));
        assertFalse(alice.completed.get());

        // Cancelling the parked task evicts it and must release the username permit it was holding.
        aliceTask.cancel("client went away");
        assertTrue(alice.completed.get());
        assertNotNull(alice.failure.get());
        assertEquals(0, queueService.retainedDepth("wg-1"));
        bob.close();
        // If the cancelled request had leaked its username permit, alice's next request would be held at username.
        Releasable aliceAgain = acquireThrottlePermitSync(workloadGroupService, "wg-1", "username|alice");
        assertNotNull(aliceAgain);
        aliceAgain.close();
    }

    public void testAddingARuleDrainsTheQueue() {
        WorkloadGroupQueueService queueService = newDirectQueueService();
        AtomicInteger admissions = new AtomicInteger();
        enqueueWaitingRequest(queueService, "wg-1", "wg-1:group", admissions);

        Settings queue = Settings.builder().put("size_per_bucket", 10).build();
        WorkloadGroup groupOnly = queueingGroup(
            "wg-1",
            Settings.builder().put("group.node_limit", 1).build(),
            queue,
            MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
        );
        WorkloadGroup withUsername = queueingGroup(
            "wg-1",
            Settings.builder().put("group.node_limit", 1).put("username.node_limit", 3).build(),
            queue,
            MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
        );
        deliverWorkloadGroupsChanged(Map.of("wg-1", groupOnly), Map.of("wg-1", withUsername));
        assertEquals(1, admissions.get());
    }

    public void testChangingOneRulesLimitKeepsTheQueue() {
        WorkloadGroupQueueService queueService = newDirectQueueService();
        AtomicInteger admissions = new AtomicInteger();
        enqueueWaitingRequest(queueService, "wg-1", "wg-1:group", admissions);

        Settings queue = Settings.builder().put("size_per_bucket", 10).build();
        WorkloadGroup previous = queueingGroup(
            "wg-1",
            Settings.builder().put("group.node_limit", 1).put("username.node_limit", 3).build(),
            queue,
            MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
        );
        WorkloadGroup current = queueingGroup(
            "wg-1",
            Settings.builder().put("group.node_limit", 1).put("username.node_limit", 7).build(),
            queue,
            MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
        );
        deliverWorkloadGroupsChanged(Map.of("wg-1", previous), Map.of("wg-1", current));
        assertEquals("a numeric change with the same rules and tiers must not drain", 0, admissions.get());
        assertEquals(1, queueService.currentDepth("wg-1"));
    }

    // A shared-throttle service whose ring holds only this node, so every shared acquire takes the local-owner path.
    private WorkloadGroupSharedThrottleService localSharedThrottleService(WorkloadGroup group) {
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
        return sharedService;
    }

    public void testHeldSharedPermitsAreKeptAliveWhileWaitingForALaterBucket() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("username.shared_limit", 1).put("group.node_limit", 1).build();
        Settings queue = Settings.builder().put("size_per_bucket", 5).build();
        WorkloadGroup group = queueingGroup("wg-1", throttling, queue, MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED);
        WorkloadGroupSharedThrottleService sharedService = localSharedThrottleService(group);
        WorkloadGroupQueueService queueService = newDirectQueueService();

        Releasable bob = acquireThrottlePermitSync(workloadGroupService, "wg-1", "username|bob");
        assertNotNull(bob);
        assertEquals("an admitted request's permits are not kept alive", 0, sharedService.keptAliveCountForTest());

        // alice takes her shared username permit, then waits on the full group bucket: that permit must be renewed.
        PendingAdmission alice = startAdmission("wg-1", "username|alice");
        assertFalse(alice.completed.get());
        assertEquals(1, sharedService.keptAliveCountForTest());
        sharedService.renewKeptAlivePermits();

        bob.close(); // hands the group slot to alice
        assertTrue(alice.completed.get());
        assertNotNull(alice.permit.get());
        assertEquals("admitted: no longer kept alive", 0, sharedService.keptAliveCountForTest());
        alice.permit.get().close();
        assertEquals(0, queueService.retainedDepth("wg-1"));
    }

    public void testExceptionAtALaterStepReleasesHeldPermitsAndFailsOnce() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("username.node_limit", 1).put("group.shared_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));
        WorkloadGroupSharedThrottleService sharedService = Mockito.mock(WorkloadGroupSharedThrottleService.class);
        when(sharedService.keepAlive(any(), anyLong())).thenReturn(() -> {});
        doThrow(new IllegalStateException("boom")).doAnswer(invocation -> {
            ActionListener<Releasable> l = invocation.getArgument(2);
            l.onResponse(null); // owner unavailable -> fail open
            return null;
        }).when(sharedService).acquireAsync(anyString(), anyInt(), any());
        workloadGroupService.setSharedThrottleService(sharedService);

        AtomicInteger completions = new AtomicInteger();
        AtomicReference<Exception> failure = new AtomicReference<>();
        workloadGroupService.acquireThrottlePermit(taskForGroup("wg-1"), "username|alice", ActionListener.wrap(p -> {
            completions.incrementAndGet();
        }, e -> {
            completions.incrementAndGet();
            failure.set(e);
        }));
        assertEquals(1, completions.get());
        assertTrue(failure.get() instanceof IllegalStateException);

        // The username permit taken before the throw was released: alice is admitted again.
        Releasable again = acquireThrottlePermitSync(workloadGroupService, "wg-1", "username|alice");
        assertNotNull(again);
        again.close();
    }

    public void testKeepAliveDeadlineIsPerRequestNotPerStep() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder()
            .put("username.shared_limit", 1)
            .put("role.node_limit", 1)
            .put("group.node_limit", 1)
            .build();
        Settings queue = Settings.builder().put("size_per_bucket", 5).build();
        stubClusterStateWithGroup(queueingGroup("wg-1", throttling, queue, MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED));
        WorkloadGroupQueueService queueService = newDirectQueueService();
        WorkloadGroupSharedThrottleService sharedService = Mockito.mock(WorkloadGroupSharedThrottleService.class);
        when(sharedService.keepAlive(any(), anyLong())).thenReturn(() -> {});
        doAnswer(invocation -> {
            ActionListener<Releasable> l = invocation.getArgument(2);
            l.onResponse(() -> {}); // a granted shared permit
            return null;
        }).when(sharedService).acquireAsync(anyString(), anyInt(), any(), any());
        workloadGroupService.setSharedThrottleService(sharedService);

        Releasable bob = acquireThrottlePermitSync(workloadGroupService, "wg-1", principal("username|bob", "role|reader"));
        assertNotNull(bob);
        // alice holds her shared username permit and waits at the role bucket, then moves on to the group bucket.
        PendingAdmission alice = startAdmission("wg-1", principal("username|alice", "role|reader"));
        assertFalse(alice.completed.get());
        bob.close();
        assertTrue(alice.completed.get());

        ArgumentCaptor<Long> deadlines = ArgumentCaptor.forClass(Long.class);
        // Two steps after the first for each of bob and alice: each request reuses one deadline across its steps.
        verify(sharedService, times(4)).keepAlive(any(), deadlines.capture());
        List<Long> values = deadlines.getAllValues();
        assertEquals("bob's steps share one deadline", values.get(0), values.get(1));
        assertEquals("alice's steps share one deadline", values.get(2), values.get(3));
        alice.permit.get().close();
        assertEquals(0, queueService.retainedDepth("wg-1"));
    }

    public void testRequestEnqueuedJustAfterItsTierWasRemovedIsReleased() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings queue = Settings.builder().put("size_per_bucket", 5).build();
        WorkloadGroup withRule = queueingGroup(
            "wg-1",
            Settings.builder().put("group.node_limit", 1).build(),
            queue,
            MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
        );
        WorkloadGroup withoutRule = queueingGroup(
            "wg-1",
            Settings.builder().put("username.node_limit", 1).build(),
            queue,
            MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED
        );
        WorkloadGroupQueueService queueService = newDirectQueueService();
        ClusterState clusterState = Mockito.mock(ClusterState.class);
        Metadata metadata = Mockito.mock(Metadata.class);
        when(mockClusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.workloadGroups()).thenReturn(Map.of("wg-1", withRule));
        Releasable holder = acquireThrottlePermitSync(workloadGroupService, "wg-1", null);
        assertNotNull(holder);

        // Another request already waits in the bucket (as one queued under a newer config would); it must not be released.
        AtomicInteger otherAdmissions = new AtomicInteger();
        enqueueWaitingRequest(queueService, "wg-1", "wg-1:group", otherAdmissions);

        // The second request resolves its plan with the group rule, but the rule is gone by the time it enqueues (and
        // the change's queue drain has already run): it must be released rather than wait forever.
        when(metadata.workloadGroups()).thenReturn(Map.of("wg-1", withRule), Map.of("wg-1", withoutRule));
        PendingAdmission late = startAdmission("wg-1", null);
        assertTrue("released instead of stranded", late.completed.get());
        assertNull(late.failure.get());
        assertEquals("only the late request is released", 0, otherAdmissions.get());
        assertEquals(1, queueService.retainedDepth("wg-1"));
        holder.close();
    }

    public void testSynchronousSharedAcquireFailureDoesNotOrphanThePendingEntry() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("group.shared_limit", 1).build();
        Settings queue = Settings.builder().put("size_per_bucket", 5).build();
        stubClusterStateWithGroup(queueingGroup("wg-1", throttling, queue, MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED));
        WorkloadGroupQueueService queueService = newDirectQueueService();
        WorkloadGroupSharedThrottleService sharedService = Mockito.mock(WorkloadGroupSharedThrottleService.class);
        doThrow(new IllegalStateException("boom")).when(sharedService).acquireAsync(anyString(), anyInt(), any(), any());
        workloadGroupService.setSharedThrottleService(sharedService);

        PendingAdmission admission = startAdmission("wg-1", null);
        assertTrue(admission.completed.get());
        assertTrue(admission.failure.get() instanceof IllegalStateException);
        assertEquals("the provisional queue entry is removed with the failed send", 0, queueService.retainedDepth("wg-1"));
    }

    public void testRejectionStillReachesTheCallerWhenReleasingAHeldPermitThrows() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("username.shared_limit", 5).put("group.node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));
        WorkloadGroupSharedThrottleService sharedService = Mockito.mock(WorkloadGroupSharedThrottleService.class);
        when(sharedService.keepAlive(any(), anyLong())).thenReturn(() -> {});
        AtomicInteger grants = new AtomicInteger();
        doAnswer(invocation -> {
            ActionListener<Releasable> l = invocation.getArgument(2);
            boolean second = grants.incrementAndGet() == 2;
            l.onResponse(() -> {
                if (second) {
                    throw new IllegalStateException("release failed (e.g. executor shut down)");
                }
            });
            return null;
        }).when(sharedService).acquireAsync(anyString(), anyInt(), any());
        workloadGroupService.setSharedThrottleService(sharedService);

        Releasable holder = acquireThrottlePermitSync(workloadGroupService, "wg-1", "username|alice"); // takes the group slot
        OpenSearchRejectedExecutionException e = expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> acquireThrottlePermitSync(workloadGroupService, "wg-1", "username|bob")
        );
        assertEquals(1, e.getSuppressed().length);
        holder.close();
    }

    public void testRequestEnqueuedJustAfterASwitchToMonitorIsReleased() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("group.node_limit", 1).build();
        Settings queue = Settings.builder().put("size_per_bucket", 5).build();
        WorkloadGroup enforced = queueingGroup("wg-1", throttling, queue, MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED);
        WorkloadGroup monitor = queueingGroup("wg-1", throttling, queue, MutableWorkloadGroupFragment.ResiliencyMode.MONITOR);
        WorkloadGroupQueueService queueService = newDirectQueueService();
        ClusterState clusterState = Mockito.mock(ClusterState.class);
        Metadata metadata = Mockito.mock(Metadata.class);
        when(mockClusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.workloadGroups()).thenReturn(Map.of("wg-1", enforced));
        Releasable holder = acquireThrottlePermitSync(workloadGroupService, "wg-1", null);

        // Resolved as ENFORCED, but the group is in MONITOR (already drained) when the request enqueues.
        when(metadata.workloadGroups()).thenReturn(Map.of("wg-1", enforced), Map.of("wg-1", monitor));
        PendingAdmission late = startAdmission("wg-1", null);
        assertTrue("MONITOR never keeps a request waiting", late.completed.get());
        assertEquals(0, queueService.retainedDepth("wg-1"));
        holder.close();
    }

    public void testFullGroupQueueRejectionSaysTheQueueIsFull() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("group.shared_limit", 1).build();
        Settings queue = Settings.builder().put("size_per_bucket", 5).build();
        stubClusterStateWithGroup(queueingGroup("wg-1", throttling, queue, MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED));
        WorkloadGroupQueueService queueService = newDirectQueueService();
        workloadGroupService.setSharedThrottleService(Mockito.mock(WorkloadGroupSharedThrottleService.class));
        for (int i = 0; i < WorkloadGroupQueueSettings.MAX_GROUP_QUEUE_DEPTH; i++) {
            assertNotNull(
                queueService.tryRegisterPendingAcquire("wg-1", "wg-1:group", taskForGroup("wg-1"), ActionListener.wrap(p -> {}, e -> {}))
            );
        }
        OpenSearchRejectedExecutionException e = expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> acquireThrottlePermitSync(workloadGroupService, "wg-1", null)
        );
        assertEquals(
            "Request throttled: workload group [wg-1-name] already has the maximum of "
                + WorkloadGroupQueueSettings.MAX_GROUP_QUEUE_DEPTH
                + " queued requests.",
            e.getMessage()
        );
    }
}
