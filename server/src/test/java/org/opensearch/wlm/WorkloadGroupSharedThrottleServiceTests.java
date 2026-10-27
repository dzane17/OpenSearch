/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.Version;
import org.opensearch.action.search.SearchTask;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.TaskManager;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;
import org.opensearch.wlm.WorkloadGroupSharedThrottleService.TaskCheckRequest;
import org.opensearch.wlm.WorkloadGroupSharedThrottleService.TaskCheckResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import org.mockito.Mockito;

import static org.opensearch.wlm.SharedThrottleTestPermits.ADMITTED;
import static org.opensearch.wlm.SharedThrottleTestPermits.NOT_ADMITTED;
import static org.opensearch.wlm.SharedThrottleTestPermits.tryAcquire;
import static org.mockito.Mockito.when;

public class WorkloadGroupSharedThrottleServiceTests extends OpenSearchTestCase {

    private ClusterService clusterService;
    private ThreadPool threadPool;
    private TransportService transportService;
    private DiscoveryNode localNode;
    private Metadata metadata;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        localNode = new DiscoveryNode(
            "local",
            "local",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
        clusterService = Mockito.mock(ClusterService.class);
        threadPool = Mockito.mock(ThreadPool.class);
        transportService = Mockito.mock(TransportService.class);

        ClusterState state = Mockito.mock(ClusterState.class);
        DiscoveryNodes singleDataNode = DiscoveryNodes.builder().add(localNode).localNodeId("local").build();
        when(state.nodes()).thenReturn(singleDataNode);
        // Owner-push resolves a bucket's live shared_limit from cluster-state metadata. Default to "no workload groups";
        // tests that need a real limit re-stub workloadGroups().
        metadata = Mockito.mock(Metadata.class);
        when(metadata.workloadGroups()).thenReturn(Map.of());
        when(state.metadata()).thenReturn(metadata);
        when(clusterService.state()).thenReturn(state);
        when(clusterService.localNode()).thenReturn(localNode);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
    }

    // The owner resolves a bucket's shared_limit from its OWN cluster state (it never travels on the RELEASE RPC), so
    // any test that expects owner-push to be driven has to model the group. Without this the limit reads as unset and
    // offerSharedSlots correctly declines to grant.
    private void stubGroupFor(String bucketKey, int sharedLimit) {
        final int idx = bucketKey.indexOf(':');
        final String groupId = idx < 0 ? bucketKey : bucketKey.substring(0, idx);
        when(metadata.workloadGroups()).thenReturn(Map.of(groupId, workloadGroup(groupId, sharedLimit)));
    }

    private static WorkloadGroup workloadGroup(String groupId, int sharedLimit) {
        return workloadGroup(groupId, Settings.builder().put("shared_limit", sharedLimit).build());
    }

    private static WorkloadGroup workloadGroup(String groupId, Settings throttling) {
        return new WorkloadGroup(
            groupId + "-name",
            groupId,
            new MutableWorkloadGroupFragment(
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.EMPTY,
                throttling
            ),
            1L
        );
    }

    private static ClusterState stateWithGroups(DiscoveryNodes nodes, Map<String, WorkloadGroup> groups) {
        Metadata stateMetadata = Mockito.mock(Metadata.class);
        when(stateMetadata.workloadGroups()).thenReturn(groups);
        ClusterState state = Mockito.mock(ClusterState.class);
        when(state.nodes()).thenReturn(nodes);
        when(state.metadata()).thenReturn(stateMetadata);
        return state;
    }

    private WorkloadGroupSharedThrottleService newServiceWithClock(AtomicLong nanos) {
        WorkloadGroupSharedThrottleService service = new WorkloadGroupSharedThrottleService(
            clusterService,
            threadPool,
            transportService,
            WorkloadGroupSharedThrottleService.ACQUIRE_TIMEOUT,
            nanos::get
        );
        deliverNodesChanged(service, clusterService.state().nodes());
        return service;
    }

    // Issues a queueing acquire that the (local) owner must deny and register.
    private static void denyAndRegister(WorkloadGroupSharedThrottleService service, String bucket, int sharedLimit) {
        AtomicReference<Exception> denial = new AtomicReference<>();
        service.acquireAsync(
            bucket,
            sharedLimit,
            SharedThrottleTestPermits.NO_SUCH_TASK,
            false,
            ActionListener.wrap(p -> fail("must be denied while at limit"), denial::set),
            () -> fail("the local owner must always be able to register itself as a waiter")
        );
        assertTrue("acquire at limit must be denied", WorkloadGroupSharedThrottleService.isDenial(denial.get()));
    }

    // Runs task checks inline against a mocked task manager that knows no task, so an expired permit's check reports its
    // search finished. Call before building the service, which reads the task manager at construction.
    private TaskManager inlineTaskChecks() {
        TaskManager taskManager = Mockito.mock(TaskManager.class);
        when(transportService.getTaskManager()).thenReturn(taskManager);
        when(threadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(OpenSearchExecutors.newDirectExecutorService());
        return taskManager;
    }

    private WorkloadGroupSharedThrottleService newService() {
        // Single data node => this node owns every bucket => acquire uses the local short-circuit (no real network).
        WorkloadGroupSharedThrottleService service = new WorkloadGroupSharedThrottleService(clusterService, threadPool, transportService);
        // The ring is empty until a cluster-state change populates it (matches production: the constructor does not
        // read cluster state). Deliver a nodesChanged event with the current nodes.
        deliverNodesChanged(service, clusterService.state().nodes());
        return service;
    }

    private void deliverNodesChanged(WorkloadGroupSharedThrottleService service, DiscoveryNodes nodes) {
        ClusterState previous = Mockito.mock(ClusterState.class);
        when(previous.nodes()).thenReturn(DiscoveryNodes.EMPTY_NODES);
        ClusterState current = Mockito.mock(ClusterState.class);
        when(current.nodes()).thenReturn(nodes);
        ClusterChangedEvent event = new ClusterChangedEvent("test", current, previous);
        service.clusterChanged(event);
    }

    private static Releasable awaitGrant(WorkloadGroupSharedThrottleService service, String bucket, int limit) {
        AtomicReference<Releasable> permit = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        service.acquireAsync(bucket, limit, SharedThrottleTestPermits.NO_SUCH_TASK, false, ActionListener.wrap(permit::set, failure::set));
        if (failure.get() != null) {
            throw failure.get() instanceof RuntimeException re ? re : new RuntimeException(failure.get());
        }
        return permit.get();
    }

    public void testTtlSweepDrivesOwnerPushForAnExpiredPermitSlot() {
        // A permit whose holder crashed, or whose release was lost, frees a shared slot with NO release RPC behind it.
        // With no subsequent acquire or waiter re-registration, the sweep is the only observer of that expired permit: it
        // starts the permit's task check, and the check that finds the search gone must drive owner-push; otherwise a
        // retained request stays registered while the slot sits idle.
        final int sharedLimit = 1;
        final String bucket = "g1:group";
        final AtomicLong nanos = new AtomicLong(0L);
        inlineTaskChecks();
        WorkloadGroupSharedThrottleService service = newServiceWithClock(nanos);
        stubGroupFor(bucket, sharedLimit);

        final AtomicInteger admits = new AtomicInteger(0);
        service.setGrantConsumer((bucketKey, reservedPermit) -> {
            admits.incrementAndGet();
            return ADMITTED; // stands in for the queue service admitting one parked request
        });

        assertNotNull("first acquire takes the only shared slot", awaitGrant(service, bucket, sharedLimit));
        denyAndRegister(service, bucket, sharedLimit);
        assertEquals("coordinator is registered as a waiter", 1, service.waiterCount(bucket));

        // The holder vanishes: advance past the permit TTL WITHOUT any release RPC.
        nanos.set(WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS + 1);
        assertEquals("nothing can have been admitted before the sweep runs", 0, admits.get());

        service.tracker().sweepExpired();
        assertEquals("the expiry check that freed the slot must drive owner-push", 1, admits.get());
    }

    public void testLocalGrantReKeysItsReservationSoARunningSearchStaysCounted() {
        // A grant's reservation names the RESERVATION_TASK_ID placeholder only until the grant admits a queued request: a
        // local grant then re-keys it to that request's task. A search that runs past PERMIT_TTL is therefore renewed by
        // its expiry check and keeps its slot, and the slot frees, for the next queued request, only once its task is gone.
        final String bucket = "reservation-ttl:group";
        final long firstTask = 42;
        final AtomicLong nanos = new AtomicLong(0L);
        TaskManager taskManager = inlineTaskChecks();
        WorkloadGroupSharedThrottleService service = newServiceWithClock(nanos);
        stubGroupFor(bucket, 1);
        final List<Releasable> admitted = new ArrayList<>();
        service.setGrantConsumer((bucketKey, permit) -> {
            if (admitted.size() == 2) {
                return NOT_ADMITTED; // two queued requests
            }
            admitted.add(permit);
            return OptionalLong.of(firstTask + admitted.size() - 1);
        });
        Releasable holder = awaitGrant(service, bucket, 1);
        denyAndRegister(service, bucket, 1);
        holder.close();
        assertEquals(1, admitted.size());
        assertEquals(1, service.tracker().inFlight(bucket));

        // Still running past its TTL: the check asks about the admitted task, not the placeholder, and renews the permit.
        when(taskManager.getTask(firstTask)).thenReturn(
            new SearchTask(firstTask, "transport", "indices:data/read/search", () -> "search", TaskId.EMPTY_TASK_ID, Map.of())
        );
        nanos.set(WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS + 1);
        service.tracker().sweepExpired();
        assertEquals("the running search keeps its slot: nothing is re-granted", 1, admitted.size());
        Mockito.verify(taskManager).getTask(firstTask);
        Mockito.verify(taskManager, Mockito.never()).getTask(WorkloadGroupSharedThrottleService.RESERVATION_TASK_ID);
        assertEquals(1, service.tracker().inFlight(bucket));
        assertEquals(0, service.tracker().pendingChecks());

        // It finishes without its release arriving: the next check frees the slot and hands it to the second request.
        when(taskManager.getTask(firstTask)).thenReturn(null);
        nanos.set(2 * WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS + 2);
        service.tracker().sweepExpired();
        assertEquals("the freed slot goes to the second queued request", 2, admitted.size());
        assertEquals(1, service.tracker().inFlight(bucket));
        admitted.get(0).close();
        assertEquals("the reclaimed search's late release is a no-op", 1, service.tracker().inFlight(bucket));
        admitted.get(1).close();
        assertEquals(0, service.tracker().inFlight(bucket));
        assertEquals("the unused follow-up grant deregisters the drained waiter", 0, service.waiterCount(bucket));
    }

    public void testLocalGrantWhoseSearchAlreadyReleasedIsNotReKeyedBack() {
        // The local admission hands the permit to the request before the reservation is re-keyed, and a request that
        // completes in between (here: inline, from the consumer) has already released it. The re-key must not re-create it.
        final String bucket = "rekey-after-release:group";
        WorkloadGroupSharedThrottleService service = newService();
        stubGroupFor(bucket, 1);
        final AtomicInteger admits = new AtomicInteger();
        service.setGrantConsumer((bucketKey, permit) -> {
            if (admits.getAndIncrement() > 0) {
                return NOT_ADMITTED;
            }
            permit.close(); // the admitted search finishes at once; its release re-drives the (now empty) queue
            return OptionalLong.of(7);
        });
        Releasable holder = awaitGrant(service, bucket, 1);
        denyAndRegister(service, bucket, 1);
        holder.close();
        assertEquals("one admission, then the empty queue's grant", 2, admits.get());
        assertEquals("the released reservation stays released", 0, service.tracker().inFlight(bucket));
        assertEquals(0, service.waiterCount(bucket));
    }

    public void testGrantReplyNamesTheAdmittedTaskOrNone() {
        // Coordinator side of a remote grant: the reply carries the admitted request's task id, or none when there was no
        // request, no consumer is wired yet, or dispatch failed.
        WorkloadGroupSharedThrottleService service = newService();
        final WorkloadGroupSharedThrottleService.GrantPermitRequest grant = new WorkloadGroupSharedThrottleService.GrantPermitRequest(
            "g1:group",
            "permit-g"
        );
        assertEquals("no consumer wired yet", OptionalLong.empty(), service.handleGrant(grant).admittedTaskId);

        service.setGrantConsumer((bucketKey, permit) -> OptionalLong.of(42));
        assertEquals(OptionalLong.of(42), service.handleGrant(grant).admittedTaskId);

        service.setGrantConsumer((bucketKey, permit) -> NOT_ADMITTED);
        assertEquals(OptionalLong.empty(), service.handleGrant(grant).admittedTaskId);

        service.setGrantConsumer((bucketKey, permit) -> { throw new IllegalStateException("dispatch unavailable"); });
        assertEquals(OptionalLong.empty(), service.handleGrant(grant).admittedTaskId);
    }

    public void testTaskCheckThatFindsTheSearchFinishedDrivesOwnerPush() {
        // A permit is not freed by the sweep itself: it stays counted until the coordinator's task check answers. When
        // that check finds the search gone, the slot frees with no release RPC behind it, so the check must drive
        // owner-push; otherwise the parked request stays stranded. A still-running search keeps its slot.
        final int sharedLimit = 1;
        final String bucket = "task-checked:group";
        final long taskId = 42;
        final AtomicLong nanos = new AtomicLong(0L);
        TaskManager taskManager = inlineTaskChecks();
        WorkloadGroupSharedThrottleService service = newServiceWithClock(nanos);
        stubGroupFor(bucket, sharedLimit);

        final AtomicInteger admits = new AtomicInteger(0);
        service.setGrantConsumer((bucketKey, reservedPermit) -> {
            admits.incrementAndGet();
            return ADMITTED;
        });

        AtomicReference<Releasable> held = new AtomicReference<>();
        service.acquireAsync(bucket, sharedLimit, taskId, false, ActionListener.wrap(held::set, e -> fail("must be granted")));
        assertNotNull("the search takes the only shared slot", held.get());
        denyAndRegister(service, bucket, sharedLimit);
        assertEquals(1, service.waiterCount(bucket));

        // Still running at TTL expiry: the check renews the permit and nothing is offered.
        SearchTask running = new SearchTask(
            taskId,
            "transport",
            "indices:data/read/search",
            () -> "search",
            TaskId.EMPTY_TASK_ID,
            Map.of()
        );
        when(taskManager.getTask(taskId)).thenReturn(running);
        nanos.set(WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS + 1);
        service.tracker().sweepExpired();
        assertEquals("an active search keeps its slot", 0, admits.get());
        assertEquals(1, service.tracker().inFlight(bucket));

        // Finished without its release arriving: the next expiry check frees the slot and hands it to the waiter.
        when(taskManager.getTask(taskId)).thenReturn(null);
        nanos.set(2 * WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS + 2);
        service.tracker().sweepExpired();
        assertEquals("the check that freed the slot must drive owner-push", 1, admits.get());
    }

    public void testTtlSweepOffersEveryPermitReclaimedInOneBucket() {
        // One sweep finds every expired permit in the bucket; their batched task check frees each slot, and each freed slot
        // must be offered.
        final int sharedLimit = 3;
        final String bucket = "multi-expiry:group";
        final AtomicLong nanos = new AtomicLong(0L);
        inlineTaskChecks();
        WorkloadGroupSharedThrottleService service = newServiceWithClock(nanos);
        stubGroupFor(bucket, sharedLimit);

        final AtomicInteger parked = new AtomicInteger(3);
        final AtomicInteger admits = new AtomicInteger();
        final List<Releasable> grantedPermits = new ArrayList<>();
        service.setGrantConsumer((bucketKey, permit) -> {
            if (parked.getAndDecrement() <= 0) {
                return NOT_ADMITTED;
            }
            admits.incrementAndGet();
            grantedPermits.add(permit);
            return ADMITTED;
        });

        final List<Releasable> expiredHolders = new ArrayList<>();
        for (int i = 0; i < sharedLimit; i++) {
            expiredHolders.add(awaitGrant(service, bucket, sharedLimit));
        }
        for (int i = 0; i < sharedLimit; i++) {
            denyAndRegister(service, bucket, sharedLimit);
        }

        nanos.set(WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS + 1L);
        service.tracker().sweepExpired();

        assertEquals("one sweep must offer all three slots reclaimed in the same bucket", sharedLimit, admits.get());
        assertEquals("the replacement grants refill, but never exceed, the live limit", sharedLimit, service.tracker().inFlight(bucket));

        expiredHolders.forEach(Releasable::close); // already expired: idempotent no-ops
        while (grantedPermits.isEmpty() == false) {
            grantedPermits.remove(0).close();
        }
        assertEquals(0, service.tracker().inFlight(bucket));
    }

    public void testSharedLimitIncreaseOffersOnlyTheAddedCapacityFromClusterChange() {
        final String groupId = "limit-increase";
        final String bucket = groupId + ":group";
        final int previousLimit = 1;
        final int currentLimit = 4;
        WorkloadGroupSharedThrottleService service = newService();
        stubGroupFor(bucket, previousLimit);
        when(threadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(OpenSearchExecutors.newDirectExecutorService());

        final AtomicInteger parked = new AtomicInteger(5);
        final AtomicInteger admits = new AtomicInteger();
        final List<Releasable> grantedPermits = new ArrayList<>();
        service.setGrantConsumer((bucketKey, permit) -> {
            if (parked.getAndDecrement() <= 0) {
                return NOT_ADMITTED;
            }
            admits.incrementAndGet();
            grantedPermits.add(permit);
            return ADMITTED;
        });

        final Releasable originalHolder = awaitGrant(service, bucket, previousLimit);
        for (int i = 0; i < 5; i++) {
            denyAndRegister(service, bucket, previousLimit);
        }

        final DiscoveryNodes nodes = clusterService.state().nodes();
        final ClusterState previousState = stateWithGroups(nodes, Map.of(groupId, workloadGroup(groupId, previousLimit)));
        final ClusterState currentState = stateWithGroups(nodes, Map.of(groupId, workloadGroup(groupId, currentLimit)));
        when(clusterService.state()).thenReturn(currentState);

        // A metadata-only event, delivered after the ring was initialized: the ring update returns early on it, but the
        // metadata-driven hook must still run.
        final ClusterChangedEvent event = new ClusterChangedEvent("shared limit increased", currentState, previousState);
        assertFalse("precondition: a metadata-only event", event.nodesChanged());
        service.clusterChanged(event);

        assertEquals("the hook must grant exactly newLimit - oldLimit requests", currentLimit - previousLimit, admits.get());
        assertEquals("the owner must stop at the new live limit", currentLimit, service.tracker().inFlight(bucket));
        assertEquals("requests beyond the one-time added-capacity budget remain queued", 2, parked.get());

        originalHolder.close();
        while (grantedPermits.isEmpty() == false) {
            grantedPermits.remove(0).close();
        }
        assertEquals(0, service.tracker().inFlight(bucket));
    }

    public void testClusterChangePrunesWaitersForDeletedGroup() {
        final String groupId = "deleted-group";
        final String bucket = groupId + ":group";
        final int sharedLimit = 1;
        WorkloadGroupSharedThrottleService service = newService();
        stubGroupFor(bucket, sharedLimit);
        AtomicReference<Runnable> cleanupTask = new AtomicReference<>();
        ExecutorService genericExecutor = Mockito.mock(ExecutorService.class);
        Mockito.doAnswer(invocation -> {
            cleanupTask.set((Runnable) invocation.getArgument(0));
            return null;
        }).when(genericExecutor).execute(Mockito.any(Runnable.class));
        when(threadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(genericExecutor);

        Releasable holder = awaitGrant(service, bucket, sharedLimit);
        denyAndRegister(service, bucket, sharedLimit);
        assertEquals(1, service.waiterCount(bucket));

        DiscoveryNodes nodes = clusterService.state().nodes();
        ClusterState previousState = stateWithGroups(nodes, Map.of(groupId, workloadGroup(groupId, sharedLimit)));
        ClusterState currentState = stateWithGroups(nodes, Map.of());
        when(clusterService.state()).thenReturn(currentState);

        service.clusterChanged(new ClusterChangedEvent("workload group deleted", currentState, previousState));

        assertNotNull("cleanup must be scheduled off the cluster-applier thread", cleanupTask.get());
        assertEquals("the asynchronous cleanup must not run inline", 1, service.waiterCount(bucket));
        cleanupTask.get().run();
        assertEquals("deleting the group must prune its owner-side waiter membership", 0, service.waiterCount(bucket));
        holder.close();
        assertEquals(0, service.tracker().inFlight(bucket));
    }

    public void testClusterChangePrunesWaitersWhenSharedTierIsRemoved() {
        final String groupId = "shared-tier-removed";
        final String bucket = groupId + ":group";
        final int sharedLimit = 1;
        WorkloadGroupSharedThrottleService service = newService();
        stubGroupFor(bucket, sharedLimit);
        when(threadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(OpenSearchExecutors.newDirectExecutorService());

        Releasable holder = awaitGrant(service, bucket, sharedLimit);
        denyAndRegister(service, bucket, sharedLimit);
        assertEquals(1, service.waiterCount(bucket));

        DiscoveryNodes nodes = clusterService.state().nodes();
        ClusterState previousState = stateWithGroups(nodes, Map.of(groupId, workloadGroup(groupId, sharedLimit)));
        Settings nodeOnlyThrottling = Settings.builder().put("node_limit", 1).build();
        ClusterState currentState = stateWithGroups(nodes, Map.of(groupId, workloadGroup(groupId, nodeOnlyThrottling)));
        when(clusterService.state()).thenReturn(currentState);

        // Metadata-only (see testSharedLimitIncreaseOffersOnlyTheAddedCapacityFromClusterChange).
        final ClusterChangedEvent event = new ClusterChangedEvent("shared tier removed", currentState, previousState);
        assertFalse("precondition: a metadata-only event", event.nodesChanged());
        service.clusterChanged(event);

        assertEquals("removing shared_limit must prune the owner-side waiter membership", 0, service.waiterCount(bucket));
        holder.close();
        assertEquals(0, service.tracker().inFlight(bucket));
    }

    public void testOrdinaryReleaseOffersOneSlotEvenWhenLiveLimitIsHigher() {
        final String bucket = "one-slot-release:group";
        WorkloadGroupSharedThrottleService service = newService();
        stubGroupFor(bucket, 1);

        final AtomicInteger parked = new AtomicInteger(4);
        final AtomicInteger admits = new AtomicInteger();
        final List<Releasable> grantedPermits = new ArrayList<>();
        service.setGrantConsumer((bucketKey, permit) -> {
            if (parked.getAndDecrement() <= 0) {
                return NOT_ADMITTED;
            }
            admits.incrementAndGet();
            grantedPermits.add(permit);
            return ADMITTED;
        });

        final Releasable holder = awaitGrant(service, bucket, 1);
        for (int i = 0; i < 4; i++) {
            denyAndRegister(service, bucket, 1);
        }

        // Model the new state being visible without invoking its cluster-change hook. The release path now sees limit 4,
        // but the single completed request still represents only one newly-freed slot and must not perform a bulk fill.
        stubGroupFor(bucket, 4);
        holder.close();

        assertEquals("a normal return must hand off only its one freed slot", 1, admits.get());
        assertEquals(1, service.tracker().inFlight(bucket));
        assertEquals(3, parked.get());

        while (grantedPermits.isEmpty() == false) {
            grantedPermits.remove(0).close();
        }
        assertEquals(0, service.tracker().inFlight(bucket));
    }

    public void testReleaseWithNoWaiterSkipsTheOwnershipCheck() {
        final String bucket = "no-waiter-release:group";
        WorkloadGroupSharedThrottleService service = newService();
        stubGroupFor(bucket, 1);
        Releasable permit = awaitGrant(service, bucket, 1);
        assertEquals(0, service.waiterCount(bucket));
        Mockito.clearInvocations(clusterService);
        permit.close();
        assertEquals("the release frees the slot", 0, service.tracker().inFlight(bucket));
        // Without a queue there is never a waiter, so a release must not pay for the ring lookup (isStillOwner reads localNode()).
        Mockito.verify(clusterService, Mockito.never()).localNode();
    }

    public void testWaiterReconcileFindsAnExpiredPermitWithoutTheOwnerSweep() {
        // The offer a waiter re-registration triggers runs tryAcquire on the full bucket, which finds the expired permit and
        // starts its task check, so a lost permit in a bucket with parked requests does not wait for the owner's sweep.
        final String bucket = "reconcile-expiry:group";
        final AtomicLong nanos = new AtomicLong(0L);
        inlineTaskChecks();
        WorkloadGroupSharedThrottleService service = newServiceWithClock(nanos);
        stubGroupFor(bucket, 1);
        final AtomicInteger admits = new AtomicInteger();
        service.setGrantConsumer((bucketKey, permit) -> {
            admits.incrementAndGet();
            return ADMITTED;
        });
        assertNotNull("the leaked holder: never closed", awaitGrant(service, bucket, 1));

        nanos.set(WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS + 1);
        service.reconcileRetainedWaiters(bucket); // no tracker().sweepExpired()

        assertEquals("the check that found the search gone must offer the freed slot", 1, admits.get());
        assertEquals(0, service.tracker().pendingChecks());
        assertEquals(1, service.tracker().inFlight(bucket));
    }

    public void testRingPopulatesWhenNodeSetUnchangedVsPreviousState() {
        // Single-node regression: the first clusterChanged reports no node change, yet the ring must still populate.
        WorkloadGroupSharedThrottleService service = new WorkloadGroupSharedThrottleService(clusterService, threadPool, transportService);
        DiscoveryNodes nodes = DiscoveryNodes.builder().add(localNode).localNodeId("local").build();
        ClusterState previous = Mockito.mock(ClusterState.class);
        when(previous.nodes()).thenReturn(nodes); // same node set as current -> nodesChanged() == false
        ClusterState current = Mockito.mock(ClusterState.class);
        when(current.nodes()).thenReturn(nodes);
        ClusterChangedEvent event = new ClusterChangedEvent("test", current, previous);
        assertFalse("precondition: this event reports no node change", event.nodesChanged());

        service.clusterChanged(event);

        // shared_limit=1 must now actually enforce (grant then reject), not report the shared tier unavailable.
        assertNotNull(awaitGrant(service, "b", 1));
        expectThrows(OpenSearchRejectedExecutionException.class, () -> awaitGrant(service, "b", 1));
    }

    public void testSameIdRestartRebuildsRingWithFreshNode() {
        // A restart keeps the persistent id but gets a new ephemeral id: the ring must rebuild with the fresh node.
        WorkloadGroupSharedThrottleService service = new WorkloadGroupSharedThrottleService(clusterService, threadPool, transportService);

        DiscoveryNode first = new DiscoveryNode(
            "n1",
            "n1",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
        deliverNodesChanged(service, DiscoveryNodes.builder().add(first).localNodeId("n1").build());
        assertSame("ring should hold the first node instance", first, service.ring().ownerFor("b").orElseThrow());

        // Same persistent id "n1", but a brand-new DiscoveryNode instance (fresh ephemeral id + transport address).
        DiscoveryNode restarted = new DiscoveryNode(
            "n1",
            "n1",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
        assertNotEquals("restarted node must not equal the old instance", first, restarted);
        deliverNodesChanged(service, DiscoveryNodes.builder().add(restarted).localNodeId("n1").build());

        assertSame("ring must have rebuilt with the restarted node instance", restarted, service.ring().ownerFor("b").orElseThrow());
    }

    public void testOnlyNodeChangesRebuildRingAfterFirstEvent() {
        WorkloadGroupSharedThrottleService service = newService();
        ThrottleOwnerSelector initial = service.ring();
        assertEquals(Set.of(localNode), initial.eligibleNodeSet());

        DiscoveryNode other = new DiscoveryNode(
            "other",
            "other",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
        DiscoveryNodes withOther = DiscoveryNodes.builder().add(localNode).add(other).localNodeId("local").build();
        // Deliberately inconsistent with the ring (a real applier never produces this): an event reporting no node change
        // whose state nevertheless holds another eligible node, so a skipped scan is observable as an unchanged ring.
        ClusterState state = Mockito.mock(ClusterState.class);
        when(state.nodes()).thenReturn(withOther);
        ClusterChangedEvent unchanged = new ClusterChangedEvent("test", state, state);
        assertFalse("precondition: this event reports no node change", unchanged.nodesChanged());
        service.clusterChanged(unchanged);
        assertSame("an event without node changes must not rescan or rebuild the ring", initial, service.ring());

        deliverNodesChanged(service, withOther);
        assertEquals("a node change must rebuild the ring", Set.of(localNode, other), service.ring().eligibleNodeSet());
    }

    public void testFormerOwnerPrunesWaitersAndCannotIssueOrPushPermits() {
        final int sharedLimit = 1;
        final DiscoveryNode remote = dataNode("remote");
        final DiscoveryNodes bothNodes = DiscoveryNodes.builder().add(localNode).add(remote).localNodeId(localNode.getId()).build();
        final String bucket = bucketOwnedBy(serviceWithRing(bothNodes), remote) + ":group";

        WorkloadGroupSharedThrottleService service = newService();
        stubGroupFor(bucket, sharedLimit);
        AtomicInteger admits = new AtomicInteger();
        service.setGrantConsumer((bucketKey, permit) -> {
            admits.incrementAndGet();
            return ADMITTED;
        });

        Releasable oldPermit = awaitGrant(service, bucket, sharedLimit);
        denyAndRegister(service, bucket, sharedLimit);
        assertEquals(1, service.waiterCount(bucket));

        deliverNodesChanged(service, bothNodes);
        assertEquals(remote, service.ring().ownerFor(bucket).orElseThrow());
        assertEquals("the former owner must drop stale waiter routing state", 0, service.waiterCount(bucket));

        WorkloadGroupSharedThrottleService.AcquirePermitResponse staleAcquire = service.handleAcquire(
            acquireRequest(bucket, sharedLimit, "stale-acquire", localNode, true)
        );
        assertFalse("a former owner must not mint another permit", staleAcquire.granted);
        assertTrue("the refusal is not_owner (unavailable to the caller), not a denial", staleAcquire.notOwner);
        assertFalse("a former owner must not register another waiter", staleAcquire.registered);
        assertEquals("only the pre-remap permit may remain", 1, service.tracker().inFlight(bucket));

        oldPermit.close();
        assertEquals("the old permit may drain locally but must not create a pushed grant", 0, service.tracker().inFlight(bucket));
        assertEquals(0, admits.get());
    }

    public void testOwnerChangeDuringPushReservationReclaimsPermitBeforeGrant() {
        final int sharedLimit = 1;
        final DiscoveryNode remote = dataNode("remote-race");
        final DiscoveryNodes bothNodes = DiscoveryNodes.builder().add(localNode).add(remote).localNodeId(localNode.getId()).build();
        final String bucket = bucketOwnedBy(serviceWithRing(bothNodes), remote) + ":group";

        RingMovingClock clock = new RingMovingClock();
        WorkloadGroupSharedThrottleService service = newService(clock);
        stubGroupFor(bucket, sharedLimit);
        AtomicInteger admits = new AtomicInteger();
        service.setGrantConsumer((bucketKey, permit) -> {
            admits.incrementAndGet();
            return ADMITTED;
        });

        Releasable holder = awaitGrant(service, bucket, sharedLimit);
        denyAndRegister(service, bucket, sharedLimit);
        assertEquals(1, service.waiterCount(bucket));

        // The release below reads no clock until the owner-push reservation's tracker acquire, which reads it once, after
        // the offer's ownership check and before the post-reservation fence (as in the acquire-side fence test). Moving
        // the bucket away on that read lands the move inside the reservation window.
        clock.onNextRead(() -> deliverNodesChanged(service, bothNodes));
        holder.close();

        assertTrue("the reservation window was exercised", clock.fired());
        assertEquals(remote, service.ring().ownerFor(bucket).orElseThrow());
        assertEquals("the post-reservation owner fence must reclaim the stale permit", 0, service.tracker().inFlight(bucket));
        assertEquals("the former owner must not expose the stale permit to its local waiter", 0, admits.get());
        assertEquals(0, service.waiterCount(bucket));
    }

    public void testReleaseCarriesNoDeregistrationSignal() {
        // A grant that admitted nothing is returned by its reply, so a RELEASE carries no deregistration signal: one for an
        // unknown permit id (say, issued by a previous owner) frees nothing and removes nobody, and a real one only offers
        // its slot to the waiter, which stays registered after an admission since it may have more queued requests.
        final String bucket = "unknown-release:group";
        final int sharedLimit = 1;
        WorkloadGroupSharedThrottleService service = newService();
        stubGroupFor(bucket, sharedLimit);
        final AtomicInteger admits = new AtomicInteger();
        service.setGrantConsumer((bucketKey, permit) -> {
            admits.incrementAndGet();
            return ADMITTED;
        });
        assertTrue(
            tryAcquire(
                service.tracker(),
                bucket,
                sharedLimit,
                "current-owner-permit",
                WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS,
                "other-coordinator"
            )
        );
        denyAndRegister(service, bucket, sharedLimit);
        assertEquals(1, service.waiterCount(bucket));

        service.handleRelease(new WorkloadGroupSharedThrottleService.ReleasePermitRequest(bucket, "permit-from-former-owner"));
        assertEquals("an unknown permit id must not remove a waiter", 1, service.waiterCount(bucket));
        assertEquals("the real permit remains live", 1, service.tracker().inFlight(bucket));
        assertEquals("nothing freed, so nothing is offered", 0, admits.get());

        service.handleRelease(new WorkloadGroupSharedThrottleService.ReleasePermitRequest(bucket, "current-owner-permit"));
        assertEquals("the freed slot is pushed to the waiter", 1, admits.get());
        assertEquals("an admitted waiter stays registered", 1, service.waiterCount(bucket));
        assertEquals(1, service.tracker().inFlight(bucket));
    }

    public void testRegisterWaiterImmediatelyDrivesFreeCapacity() {
        final String bucket = "re-register:group";
        final int sharedLimit = 1;
        WorkloadGroupSharedThrottleService service = newService();
        stubGroupFor(bucket, sharedLimit);
        AtomicReference<Releasable> admittedPermit = new AtomicReference<>();
        AtomicBoolean firstGrant = new AtomicBoolean(true);
        AtomicInteger admits = new AtomicInteger();
        service.setGrantConsumer((bucketKey, permit) -> {
            if (firstGrant.compareAndSet(true, false)) {
                admittedPermit.set(permit);
                admits.incrementAndGet();
                return ADMITTED;
            }
            return NOT_ADMITTED;
        });

        WorkloadGroupSharedThrottleService.RegisterWaiterResponse response = service.handleRegisterWaiter(
            new WorkloadGroupSharedThrottleService.RegisterWaiterRequest(bucket, localNode.getEphemeralId())
        );

        assertTrue(response.registered);
        assertEquals("registration must drive already-free capacity without waiting for a future release", 1, admits.get());
        assertEquals(1, service.waiterCount(bucket));
        assertEquals(1, service.tracker().inFlight(bucket));

        admittedPermit.get().close();
        assertEquals("the next unused grant reconciles the now-empty coordinator out of the waiter set", 0, service.waiterCount(bucket));
        assertEquals(0, service.tracker().inFlight(bucket));
    }

    public void testRegisterWaiterIsRefusedWithoutAPositiveSharedLimit() {
        final String bucket = "re-register-unset:group";
        WorkloadGroupSharedThrottleService service = newService(); // no group stubbed: the shared limit reads as unset
        WorkloadGroupSharedThrottleService.RegisterWaiterResponse response = service.handleRegisterWaiter(
            new WorkloadGroupSharedThrottleService.RegisterWaiterRequest(bucket, localNode.getEphemeralId())
        );
        assertFalse(response.registered);
        assertEquals(0, service.waiterCount(bucket));
    }

    public void testWaiterRegistrationResolvesTheExactCoordinatorProcess() {
        // The owner looks a waiter up by the ephemeral id its message carries. A coordinator that restarted (same persistent
        // id, new ephemeral id) is therefore absent until the owner applies its rejoin, and is refused even while its
        // previous incarnation is still a member here and looks connected: never registered as that other process.
        final String groupId = "exact-process";
        final String bucket = groupId + ":group";
        final DiscoveryNode previous = coordinatingOnlyNode("restarting");
        final DiscoveryNode restarted = coordinatingOnlyNode("restarting");
        assertEquals("a restart keeps the persistent id", previous.getId(), restarted.getId());
        assertNotEquals("but is a new process", previous, restarted);
        final DiscoveryNodes beforeRejoin = DiscoveryNodes.builder().add(localNode).add(previous).localNodeId("local").build();
        final DiscoveryNodes afterRejoin = DiscoveryNodes.builder().add(localNode).add(restarted).localNodeId("local").build();
        final Map<String, WorkloadGroup> groups = Map.of(groupId, workloadGroup(groupId, 1));
        final ClusterState stateBeforeRejoin = stateWithGroups(beforeRejoin, groups);
        when(clusterService.state()).thenReturn(stateBeforeRejoin);
        when(transportService.nodeConnected(Mockito.any(DiscoveryNode.class))).thenReturn(true);
        WorkloadGroupSharedThrottleService service = newService();
        assertTrue(tryAcquire(service.tracker(), bucket, 1, "held", WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS, "other"));

        WorkloadGroupSharedThrottleService.AcquirePermitResponse denial = service.handleAcquire(
            acquireRequest(bucket, 1, "restarted-denied", restarted, true)
        );
        assertFalse(denial.granted);
        assertFalse("a process the owner has not applied is refused, not resolved to its previous incarnation", denial.registered);
        assertFalse(
            service.handleRegisterWaiter(
                new WorkloadGroupSharedThrottleService.RegisterWaiterRequest(bucket, restarted.getEphemeralId())
            ).registered
        );
        assertEquals(0, service.waiterCount(bucket));
        assertTrue(
            "the previous incarnation registers as itself",
            service.handleRegisterWaiter(
                new WorkloadGroupSharedThrottleService.RegisterWaiterRequest(bucket, previous.getEphemeralId())
            ).registered
        );
        assertEquals(1, service.waiterCount(bucket));

        // The owner applies the rejoin: the previous process is purged, and the next pass registers the restarted one.
        final ClusterState stateAfterRejoin = stateWithGroups(afterRejoin, groups);
        when(clusterService.state()).thenReturn(stateAfterRejoin);
        deliverNodesChanged(service, beforeRejoin, afterRejoin);
        assertEquals("the departed incarnation's registration is purged", 0, service.waiterCount(bucket));
        assertTrue(
            service.handleRegisterWaiter(
                new WorkloadGroupSharedThrottleService.RegisterWaiterRequest(bucket, restarted.getEphemeralId())
            ).registered
        );
        assertEquals(1, service.waiterCount(bucket));
        deliverNodesChanged(service, afterRejoin, DiscoveryNodes.builder().add(localNode).localNodeId("local").build());
        assertEquals("the registered waiter was exactly the restarted process", 0, service.waiterCount(bucket));
    }

    public void testWaiterRegistrationRefusesAMemberCoordinatorThatIsNotConnected() {
        // Membership is not enough: a grant is pushed over the owner's connection to the coordinator, so a member this
        // node is not connected to (say, before fault detection removes it) is refused, on a denial and on REGISTER_WAITER
        // alike, and a later pass registers it once connected.
        final String groupId = "not-connected";
        final String bucket = groupId + ":group";
        final DiscoveryNode coordinator = coordinatingOnlyNode("coordinator");
        final DiscoveryNodes nodes = DiscoveryNodes.builder().add(localNode).add(coordinator).localNodeId("local").build();
        final ClusterState state = stateWithGroups(nodes, Map.of(groupId, workloadGroup(groupId, 1)));
        when(clusterService.state()).thenReturn(state);
        when(transportService.nodeConnected(coordinator)).thenReturn(false);
        WorkloadGroupSharedThrottleService service = newService(); // applies the membership, which holds the coordinator
        assertTrue(tryAcquire(service.tracker(), bucket, 1, "held", WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS, "other"));
        final WorkloadGroupSharedThrottleService.RegisterWaiterRequest registration =
            new WorkloadGroupSharedThrottleService.RegisterWaiterRequest(bucket, coordinator.getEphemeralId());

        WorkloadGroupSharedThrottleService.AcquirePermitResponse denial = service.handleAcquire(
            acquireRequest(bucket, 1, "denied", coordinator, true)
        );
        assertFalse(denial.granted);
        assertFalse("a member this node is not connected to is not registered on a denial", denial.registered);
        assertFalse("nor by its backstop pass", service.handleRegisterWaiter(registration).registered);
        assertEquals(0, service.waiterCount(bucket));

        when(transportService.nodeConnected(coordinator)).thenReturn(true);
        assertTrue("the first pass once connected registers it", service.handleRegisterWaiter(registration).registered);
        assertEquals(1, service.waiterCount(bucket));
        assertEquals("the full bucket offers it nothing yet", 1, service.tracker().inFlight(bucket));
    }

    public void testLocalOwnerGrantsThenDeniesAtLimit() {
        WorkloadGroupSharedThrottleService service = newService();
        Releasable p1 = awaitGrant(service, "b", 1);
        assertNotNull(p1);
        expectThrows(OpenSearchRejectedExecutionException.class, () -> awaitGrant(service, "b", 1));
        // release frees the shared slot
        p1.close();
        assertNotNull(awaitGrant(service, "b", 1));
    }

    public void testOwnerPushDrainsEveryParkedRequestOnOneCoordinator() {
        // One coordinator (here the local owner) parks SEVERAL requests for a shared bucket, but the owner waiter registry
        // holds one Set membership per coordinator. A grant must NOT deregister the coordinator on a successful admit — it
        // may still have more queued requests — so each successive freed slot drains the next parked request.
        final int sharedLimit = 1;
        final String bucket = "b";
        WorkloadGroupSharedThrottleService service = newService();
        stubGroupFor(bucket, sharedLimit);

        // A stubbed coordinator-side consumer standing in for the queue service: it holds `parked` requests and admits
        // one per grant, capturing the reserved permit so the test can "complete" that request by closing it.
        final AtomicInteger parked = new AtomicInteger(3);
        final AtomicInteger admits = new AtomicInteger(0);
        final List<Releasable> heldPermits = new ArrayList<>();
        service.setGrantConsumer((bucketKey, reservedPermit) -> {
            if (parked.get() <= 0) {
                return NOT_ADMITTED; // nothing left to admit: the grant goes unused and this waiter is deregistered
            }
            parked.decrementAndGet();
            admits.incrementAndGet();
            heldPermits.add(reservedPermit);
            return ADMITTED;
        });

        Releasable slotHolder = awaitGrant(service, bucket, sharedLimit);
        assertNotNull("first acquire takes the only shared slot", slotHolder);
        for (int i = 0; i < 3; i++) {
            denyAndRegister(service, bucket, sharedLimit);
        }
        assertEquals("one Set membership for the coordinator regardless of parked count", 1, service.waiterCount(bucket));

        slotHolder.close();
        assertEquals("first freed slot drains exactly one parked request", 1, admits.get());
        assertEquals("coordinator must remain registered while it still has parked requests", 1, service.waiterCount(bucket));

        heldPermits.remove(0).close();
        assertEquals("second freed slot drains the second parked request", 2, admits.get());
        assertEquals(1, service.waiterCount(bucket));

        heldPermits.remove(0).close();
        assertEquals("third freed slot drains the third (last) parked request", 3, admits.get());

        // The last request completes with nothing left queued: the next grant comes back unused, so the coordinator
        // self-deregisters and the freed slot returns to the pool. No stranding, no leaked permit.
        heldPermits.remove(0).close();
        assertEquals("no further admits once the queue is empty", 3, admits.get());
        assertEquals("coordinator self-reconciles out of the registry when it has nothing queued", 0, service.waiterCount(bucket));
        assertEquals("no shared permit leaked after the burst fully drains", 0, service.tracker().inFlight(bucket));
    }

    public void testDoubleCloseReleasesOnce() {
        WorkloadGroupSharedThrottleService service = newService();
        Releasable p = awaitGrant(service, "b", 2);
        assertNotNull(awaitGrant(service, "b", 2)); // second slot
        p.close();
        p.close(); // must not double-release
        // one slot still held, so only one more grant is available
        assertNotNull(awaitGrant(service, "b", 2));
        expectThrows(OpenSearchRejectedExecutionException.class, () -> awaitGrant(service, "b", 2));
    }

    public void testHasEligibleOwnerOnlyWhileTheRingHasADataNode() {
        WorkloadGroupSharedThrottleService service = new WorkloadGroupSharedThrottleService(clusterService, threadPool, transportService);
        assertFalse("no cluster state applied yet", service.hasEligibleOwner());

        DiscoveryNodes withData = DiscoveryNodes.builder().add(dataNode("data")).localNodeId("data").build();
        deliverNodesChanged(service, DiscoveryNodes.EMPTY_NODES, withData);
        assertTrue(service.hasEligibleOwner());

        DiscoveryNodes withoutData = DiscoveryNodes.builder().add(coordinatingOnlyNode("coord")).localNodeId("coord").build();
        deliverNodesChanged(service, withData, withoutData);
        assertFalse("a cluster without data nodes has no owner", service.hasEligibleOwner());
    }

    public void testEmptyRingIsUnavailable() {
        // Cluster with no eligible data node (coordinating/manager-only) => empty ring => unavailable. WorkloadGroupService
        // checks hasEligibleOwner first and treats shared_limit as unset, so this covers a ring that empties after that check.
        DiscoveryNode managerOnly = new DiscoveryNode(
            "m",
            "m",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            Version.CURRENT
        );
        ClusterState state = Mockito.mock(ClusterState.class);
        when(state.nodes()).thenReturn(DiscoveryNodes.builder().add(managerOnly).localNodeId("m").build());
        when(clusterService.state()).thenReturn(state);
        when(clusterService.localNode()).thenReturn(managerOnly);

        WorkloadGroupSharedThrottleService service = newService();
        AtomicReference<Exception> failure = new AtomicReference<>();
        service.acquireAsync(
            "b",
            1,
            SharedThrottleTestPermits.NO_SUCH_TASK,
            false,
            ActionListener.wrap(p -> fail("must not admit: " + p), failure::set)
        );
        assertNotNull("listener must be invoked inline", failure.get());
        assertTrue("empty ring must report the shared tier unavailable", WorkloadGroupSharedThrottleService.isUnavailable(failure.get()));
    }

    public void testDisconnectedOwnerIsUnavailableWithoutSendingRequest() {
        // Ring owner is a remote data node (not the local node), so this is NOT the local short-circuit path.
        DiscoveryNode remote = new DiscoveryNode(
            "remote",
            "remote",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
        ClusterState state = Mockito.mock(ClusterState.class);
        // Only the remote node is a data node, so every bucket hashes to it; local is a coordinating-only node.
        when(state.nodes()).thenReturn(DiscoveryNodes.builder().add(remote).localNodeId("local").build());
        when(clusterService.state()).thenReturn(state);
        when(clusterService.localNode()).thenReturn(localNode);
        // The owner is known-disconnected.
        when(transportService.nodeConnected(remote)).thenReturn(false);

        WorkloadGroupSharedThrottleService service = newService();
        AtomicReference<Exception> failure = new AtomicReference<>();
        service.acquireAsync(
            "b",
            1,
            SharedThrottleTestPermits.NO_SUCH_TASK,
            false,
            ActionListener.wrap(p -> fail("must not admit: " + p), failure::set)
        );

        // An inline result proves the nodeConnected() pre-check fired: the mock cannot intercept the final sendRequest,
        // so sending would throw rather than complete the listener.
        assertNotNull("listener must be invoked inline (no RTT to a dead node)", failure.get());
        assertTrue("disconnected owner -> shared tier unavailable", WorkloadGroupSharedThrottleService.isUnavailable(failure.get()));
    }

    public void testAcquirePermitRequestSerializationRoundTrip() throws Exception {
        for (boolean wantsQueue : new boolean[] { false, true }) {
            WorkloadGroupSharedThrottleService.AcquirePermitRequest original = new WorkloadGroupSharedThrottleService.AcquirePermitRequest(
                "grp1:username:alice",
                42,
                "permit-xyz",
                123_456_789L,
                "coordinator-ephemeral-id",
                randomLongBetween(0, Long.MAX_VALUE),
                wantsQueue
            );
            WorkloadGroupSharedThrottleService.AcquirePermitRequest copy = copyWriteable(
                original,
                writableRegistry(),
                WorkloadGroupSharedThrottleService.AcquirePermitRequest::new
            );
            assertEquals(original.bucketKey, copy.bucketKey);
            assertEquals(original.sharedLimit, copy.sharedLimit);
            assertEquals(original.permitId, copy.permitId);
            assertEquals(
                "the owner applies its own TTL, whatever the sender held",
                WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS,
                copy.ttlNanos
            );
            assertEquals(original.coordinatorId, copy.coordinatorId);
            assertEquals(original.coordinatorTaskId, copy.coordinatorTaskId);
            assertEquals(wantsQueue, copy.wantsQueue);
            // The ephemeral id is the coordinator's only identity on the wire: no persistent node id is sent.
            assertEquals(fullAcquireRequestBody().keySet(), writtenRequestKeys(original));
        }
    }

    public void testAcquirePermitResponseSerializationRoundTrip() throws Exception {
        for (WorkloadGroupSharedThrottleService.AcquirePermitResponse original : List.of(
            WorkloadGroupSharedThrottleService.AcquirePermitResponse.GRANTED,
            WorkloadGroupSharedThrottleService.AcquirePermitResponse.DENIED,
            WorkloadGroupSharedThrottleService.AcquirePermitResponse.DENIED_AND_REGISTERED,
            WorkloadGroupSharedThrottleService.AcquirePermitResponse.NOT_OWNER
        )) {
            WorkloadGroupSharedThrottleService.AcquirePermitResponse copy = copyWriteable(
                original,
                writableRegistry(),
                WorkloadGroupSharedThrottleService.AcquirePermitResponse::new
            );
            assertEquals(original.granted, copy.granted);
            assertEquals(original.notOwner, copy.notOwner);
            assertEquals(original.registered, copy.registered);
        }
    }

    /** Only the explicit registered-denial response may claim a registration (never silently "registered"). */
    public void testAcquireResponseConstantsOnlyRegisterOnARegisteredDenial() {
        assertFalse(WorkloadGroupSharedThrottleService.AcquirePermitResponse.GRANTED.registered);
        assertFalse(WorkloadGroupSharedThrottleService.AcquirePermitResponse.DENIED.registered);
        assertFalse(WorkloadGroupSharedThrottleService.AcquirePermitResponse.NOT_OWNER.registered);
        assertTrue(WorkloadGroupSharedThrottleService.AcquirePermitResponse.DENIED_AND_REGISTERED.registered);
    }

    public void testFormerOwnerRefusesAcquireAndKeepsItsRecords() {
        DiscoveryNode newOwner = dataNode("other");
        DiscoveryNodes moved = DiscoveryNodes.builder().add(localNode).add(newOwner).localNodeId("local").build();
        String key = bucketOwnedBy(serviceWithRing(moved), newOwner);

        // Single data node: this node owns the bucket and grants a permit that is still in flight.
        WorkloadGroupSharedThrottleService service = newService();
        assertTrue(service.handleAcquire(acquireRequest(key, "held")).granted);

        // The bucket moves to another node; a coordinator still on the old ring keeps routing acquires here.
        deliverNodesChanged(service, moved);
        WorkloadGroupSharedThrottleService.AcquirePermitResponse response = service.handleAcquire(acquireRequest(key, "stale"));
        assertFalse("a former owner must not grant against its stale counter", response.granted);
        assertTrue("the refusal must say not_owner, not denied", response.notOwner);
        assertEquals("existing records drain by release or TTL, never cleared", 1, service.tracker().inFlight(key));
    }

    public void testJoiningNodeGrantsBeforeItsFirstClusterStateInsteadOfRefusing() {
        // Coordinators that applied this node's join route its buckets here before it applies its own first cluster state.
        WorkloadGroupSharedThrottleService joining = new WorkloadGroupSharedThrottleService(clusterService, threadPool, transportService);
        WorkloadGroupSharedThrottleService.AcquirePermitResponse early = joining.handleAcquire(acquireRequest("b", "early"));
        assertTrue("an empty, never-initialized ring must not refuse as not_owner", early.granted);
        assertEquals(1, joining.tracker().inFlight("b"));

        // Once its ring is built, ownership is enforced again, and the early record stays counted until it drains.
        DiscoveryNode other = dataNode("other");
        DiscoveryNodes nodes = DiscoveryNodes.builder().add(localNode).add(other).localNodeId("local").build();
        String notOwned = bucketOwnedBy(serviceWithRing(nodes), other);
        deliverNodesChanged(joining, nodes);
        assertTrue(joining.handleAcquire(acquireRequest(notOwned, "after")).notOwner);
        assertEquals(0, joining.tracker().inFlight(notOwned));
        assertEquals(1, joining.tracker().inFlight("b"));
    }

    public void testOwnershipLostAfterTrackerAcquireReturnsThePermit() {
        DiscoveryNode newOwner = dataNode("other");
        DiscoveryNodes moved = DiscoveryNodes.builder().add(localNode).add(newOwner).localNodeId("local").build();
        String key = bucketOwnedBy(serviceWithRing(moved), newOwner); // owned locally now, by newOwner after the move

        // The tracker reads its clock once per acquire, after the ownership check and before it grants, so a clock that
        // moves the bucket on that read lands the move between the grant and the post-acquire re-check.
        // Owner side (handleAcquire).
        RingMovingClock ownerClock = new RingMovingClock();
        WorkloadGroupSharedThrottleService owner = newService(ownerClock);
        ownerClock.onNextRead(() -> deliverNodesChanged(owner, moved));
        Mockito.clearInvocations(clusterService);
        WorkloadGroupSharedThrottleService.AcquirePermitResponse response = owner.handleAcquire(acquireRequest(key, "racy"));
        assertTrue("the bucket must move during the tracker acquire", ownerClock.fired());
        // Once for the ownership check and once for the post-acquire re-check; a refusal before the grant reads it once.
        Mockito.verify(clusterService, Mockito.times(2)).localNode();
        assertTrue(response.notOwner);
        assertEquals("the permit acquired before the swap must be returned", 0, owner.tracker().inFlight(key));

        // Coordinator-side local short-circuit (acquireAsync).
        RingMovingClock localClock = new RingMovingClock();
        WorkloadGroupSharedThrottleService local = newService(localClock);
        localClock.onNextRead(() -> deliverNodesChanged(local, moved));
        Mockito.clearInvocations(clusterService);
        AtomicReference<Exception> failure = new AtomicReference<>();
        local.acquireAsync(
            key,
            5,
            SharedThrottleTestPermits.NO_SUCH_TASK,
            false,
            ActionListener.wrap(p -> fail("must not admit: " + p), failure::set)
        );
        assertTrue("the bucket must move during the tracker acquire", localClock.fired());
        // Once to build the request and once for the post-acquire re-check.
        Mockito.verify(clusterService, Mockito.times(2)).localNode();
        assertTrue(
            "lost ownership mid-acquire must report the shared tier unavailable",
            WorkloadGroupSharedThrottleService.isUnavailable(failure.get())
        );
        assertEquals(0, local.tracker().inFlight(key));
    }

    private WorkloadGroupSharedThrottleService newService(LongSupplier clock) {
        WorkloadGroupSharedThrottleService service = new WorkloadGroupSharedThrottleService(
            clusterService,
            threadPool,
            transportService,
            WorkloadGroupSharedThrottleService.ACQUIRE_TIMEOUT,
            clock
        );
        deliverNodesChanged(service, clusterService.state().nodes());
        return service;
    }

    // A clock that runs a one-shot action on its next read; always reads zero.
    private static final class RingMovingClock implements LongSupplier {
        private final AtomicReference<Runnable> next = new AtomicReference<>();
        private final AtomicBoolean fired = new AtomicBoolean();

        void onNextRead(Runnable action) {
            next.set(action);
        }

        boolean fired() {
            return fired.get();
        }

        @Override
        public long getAsLong() {
            Runnable action = next.getAndSet(null);
            if (action != null) {
                action.run();
                fired.set(true);
            }
            return 0L;
        }
    }

    private static WorkloadGroupSharedThrottleService.AcquirePermitRequest acquireRequest(String key, String permitId) {
        return acquireRequest(key, permitId, "coord");
    }

    private static WorkloadGroupSharedThrottleService.AcquirePermitRequest acquireRequest(
        String key,
        String permitId,
        String coordinatorId
    ) {
        return new WorkloadGroupSharedThrottleService.AcquirePermitRequest(
            key,
            5,
            permitId,
            WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS,
            coordinatorId,
            SharedThrottleTestPermits.NO_SUCH_TASK,
            false
        );
    }

    // An acquire as the given coordinator sends it; wantsQueue asks the owner to register it as a waiter on a denial.
    private static WorkloadGroupSharedThrottleService.AcquirePermitRequest acquireRequest(
        String key,
        int sharedLimit,
        String permitId,
        DiscoveryNode coordinator,
        boolean wantsQueue
    ) {
        return new WorkloadGroupSharedThrottleService.AcquirePermitRequest(
            key,
            sharedLimit,
            permitId,
            WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS,
            coordinator.getEphemeralId(),
            SharedThrottleTestPermits.NO_SUCH_TASK,
            wantsQueue
        );
    }

    private static DiscoveryNode coordinatingOnlyNode(String id) {
        return new DiscoveryNode(id, id, buildNewFakeTransportAddress(), Collections.emptyMap(), Set.of(), Version.CURRENT);
    }

    private void deliverNodesChanged(WorkloadGroupSharedThrottleService service, DiscoveryNodes previousNodes, DiscoveryNodes nodes) {
        service.clusterChanged(nodesChangedEvent(previousNodes, nodes));
    }

    private static ClusterChangedEvent nodesChangedEvent(DiscoveryNodes previousNodes, DiscoveryNodes nodes) {
        ClusterState previous = Mockito.mock(ClusterState.class);
        when(previous.nodes()).thenReturn(previousNodes);
        ClusterState current = Mockito.mock(ClusterState.class);
        when(current.nodes()).thenReturn(nodes);
        return new ClusterChangedEvent("test", current, previous);
    }

    public void testCheckQueueOverflowExtendsPermitsInsteadOfStrandingThem() {
        AtomicLong now = new AtomicLong();
        // An executor that never runs the drain, so the queue fills up.
        when(threadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(Mockito.mock(ExecutorService.class));
        WorkloadGroupSharedThrottleService service = new WorkloadGroupSharedThrottleService(
            clusterService,
            threadPool,
            transportService,
            WorkloadGroupSharedThrottleService.ACQUIRE_TIMEOUT,
            now::get
        );
        deliverNodesChanged(service, clusterService.state().nodes());
        int permits = WorkloadGroupSharedThrottleService.MAX_QUEUED_CHECKS + 1;
        for (int i = 0; i < permits; i++) {
            assertTrue(
                tryAcquire(
                    service.tracker(),
                    "b",
                    permits,
                    "p" + i,
                    WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS,
                    localNode.getEphemeralId(),
                    i + 1
                )
            );
        }

        now.addAndGet(WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS + 1);
        service.tracker().sweepExpired();
        assertEquals(WorkloadGroupSharedThrottleService.MAX_QUEUED_CHECKS, service.queuedChecks().size());
        assertEquals("the overflowing permit stays counted", permits, service.tracker().inFlight("b"));

        // The overflowing permit was extended rather than left pending: once the extension passes, a scan collects it again.
        service.queuedChecks().clear();
        now.addAndGet(WorkloadGroupSharedThrottleService.FAILED_CHECK_TTL.nanos() + 1);
        service.tracker().sweepExpired();
        assertEquals(1, service.queuedChecks().size());
    }

    public void testRemovedCoordinatorPermitsArePurged() {
        // Coordinating-only peers: the ring stays on the single local data node, so this node owns every bucket.
        DiscoveryNode leaving = coordinatingOnlyNode("leaving");
        DiscoveryNode staying = coordinatingOnlyNode("staying");
        DiscoveryNodes before = DiscoveryNodes.builder().add(localNode).add(leaving).add(staying).localNodeId("local").build();
        WorkloadGroupSharedThrottleService service = newService();
        deliverNodesChanged(service, clusterService.state().nodes(), before);

        assertTrue(service.handleAcquire(acquireRequest("b", "from-leaving-1", leaving.getEphemeralId())).granted);
        assertTrue(service.handleAcquire(acquireRequest("c", "from-leaving-2", leaving.getEphemeralId())).granted);
        assertTrue(service.handleAcquire(acquireRequest("b", "from-staying", staying.getEphemeralId())).granted);
        assertTrue(service.handleAcquire(acquireRequest("b", "from-other", "other-coordinator")).granted);
        assertNotNull(awaitGrant(service, "b", 5)); // local coordinator

        deliverNodesChanged(service, before, DiscoveryNodes.builder().add(localNode).add(staying).localNodeId("local").build());
        assertEquals("only the departed coordinator's permits are purged", 3, service.tracker().inFlight("b"));
        assertEquals("its permits in every bucket are purged", 0, service.tracker().inFlight("c"));
    }

    public void testSameIdRestartPurgesTheOldIncarnationsPermits() {
        DiscoveryNode firstIncarnation = coordinatingOnlyNode("restarting");
        DiscoveryNode secondIncarnation = coordinatingOnlyNode("restarting"); // same persistent id, new ephemeral id
        assertNotEquals(firstIncarnation.getEphemeralId(), secondIncarnation.getEphemeralId());
        DiscoveryNodes before = DiscoveryNodes.builder().add(localNode).add(firstIncarnation).localNodeId("local").build();
        DiscoveryNodes after = DiscoveryNodes.builder().add(localNode).add(secondIncarnation).localNodeId("local").build();
        WorkloadGroupSharedThrottleService service = newService();
        deliverNodesChanged(service, clusterService.state().nodes(), before);

        assertTrue(service.handleAcquire(acquireRequest("b", "old", firstIncarnation.getEphemeralId())).granted);
        // The new incarnation can reach the owner before the owner applies the restart: its permit must survive the purge.
        assertTrue(
            tryAcquire(
                service.tracker(),
                "b",
                5,
                "new",
                WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS,
                secondIncarnation.getEphemeralId()
            )
        );

        ClusterChangedEvent restart = nodesChangedEvent(before, after);
        assertTrue("precondition: the restart must register as a node change", restart.nodesChanged());
        service.clusterChanged(restart);
        assertEquals("only the old incarnation's permit is purged", 1, service.tracker().inFlight("b"));
        assertEquals(
            "the survivor belongs to the new incarnation",
            Map.of("b", 1),
            service.tracker().releaseAllFrom(Set.of(secondIncarnation.getEphemeralId()))
        );
    }

    public void testCoordinatorPurgeDropsItsWaitersAndOffersTheFreedSlot() {
        final String groupId = "purge";
        final String bucket = groupId + ":group";
        DiscoveryNode leaving = coordinatingOnlyNode("leaving");
        DiscoveryNodes before = DiscoveryNodes.builder().add(localNode).add(leaving).localNodeId("local").build();
        DiscoveryNodes after = DiscoveryNodes.builder().add(localNode).localNodeId("local").build();
        ClusterState state = stateWithGroups(before, Map.of(groupId, workloadGroup(groupId, 1)));
        when(clusterService.state()).thenReturn(state);
        when(transportService.nodeConnected(leaving)).thenReturn(true);
        when(threadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(OpenSearchExecutors.newDirectExecutorService());
        WorkloadGroupSharedThrottleService service = newService();
        AtomicInteger admits = new AtomicInteger();
        service.setGrantConsumer((bucketKey, permit) -> {
            admits.incrementAndGet();
            return ADMITTED;
        });

        assertTrue(service.handleAcquire(acquireRequest(bucket, 1, "held-by-leaving", leaving, false)).granted);
        WorkloadGroupSharedThrottleService.AcquirePermitResponse leavingWaits = service.handleAcquire(
            acquireRequest(bucket, 1, "leaving-denied", leaving, true)
        );
        assertTrue("precondition: the leaving coordinator is a registered waiter", leavingWaits.registered);
        denyAndRegister(service, bucket, 1);
        assertEquals(2, service.waiterCount(bucket));

        deliverNodesChanged(service, before, after);
        assertEquals("the departed coordinator's waiter registration is dropped", 1, service.waiterCount(bucket));
        assertEquals("the purged permit's slot is pushed to the remaining waiter", 1, admits.get());
        assertEquals("the pushed grant holds the only slot", 1, service.tracker().inFlight(bucket));
    }

    public void testTaskCheckUsesExistingTaskManagerAndCountsCancelledTasks() {
        TaskManager taskManager = Mockito.mock(TaskManager.class);
        when(transportService.getTaskManager()).thenReturn(taskManager);
        SearchTask task = new SearchTask(42, "transport", "indices:data/read/search", () -> "search", TaskId.EMPTY_TASK_ID, Map.of());
        task.cancel("still stopping");
        when(taskManager.getTask(42)).thenReturn(task);
        WorkloadGroupSharedThrottleService service = newService();
        TaskCheckResponse response = service.checkTasks(new TaskCheckRequest(localNode.getEphemeralId(), new long[] { 41, 42, 43 }));
        assertArrayEquals(new boolean[] { false, true, false }, response.active);
        Mockito.verify(taskManager).getTask(41);
        Mockito.verify(taskManager).getTask(42);
        Mockito.verify(taskManager).getTask(43);
        Mockito.verifyNoMoreInteractions(taskManager);
    }

    public void testTaskCheckReportsATaskWaitingInTheQueueInactive() {
        // A WAITING request holds no shared permit, so a permit recorded for its task is a grant whose reply was lost:
        // reported active, it would be renewed for as long as the request waits. A running task is still reported active.
        TaskManager taskManager = Mockito.mock(TaskManager.class);
        when(transportService.getTaskManager()).thenReturn(taskManager);
        when(taskManager.getTask(41)).thenReturn(
            new SearchTask(41, "transport", "indices:data/read/search", () -> "parked", TaskId.EMPTY_TASK_ID, Map.of())
        );
        when(taskManager.getTask(42)).thenReturn(
            new SearchTask(42, "transport", "indices:data/read/search", () -> "running", TaskId.EMPTY_TASK_ID, Map.of())
        );
        WorkloadGroupSharedThrottleService service = newService();
        TaskCheckRequest request = new TaskCheckRequest(localNode.getEphemeralId(), new long[] { 41, 42, 43 });
        assertArrayEquals(
            "before a queue is wired, no task is waiting",
            new boolean[] { true, true, false },
            service.checkTasks(request).active
        );

        service.setTaskWaitingInQueue(taskId -> taskId == 41 || taskId == 43);
        assertArrayEquals(new boolean[] { false, true, false }, service.checkTasks(request).active);
    }

    public void testTaskCheckRejectsAnotherCoordinatorIncarnation() {
        TaskManager taskManager = Mockito.mock(TaskManager.class);
        when(transportService.getTaskManager()).thenReturn(taskManager);
        WorkloadGroupSharedThrottleService service = newService();
        expectThrows(IllegalStateException.class, () -> service.checkTasks(new TaskCheckRequest("old-incarnation", new long[] { 42 })));
        Mockito.verifyNoInteractions(taskManager);
    }

    public void testTaskCheckRequestSerializationRoundTrip() throws Exception {
        long[] ids = new long[WorkloadGroupSharedThrottleService.MAX_TASKS_PER_CHECK];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = randomLongBetween(0, Long.MAX_VALUE);
        }
        TaskCheckRequest original = new TaskCheckRequest("coordinator", ids);
        TaskCheckRequest copy = copyWriteable(original, writableRegistry(), TaskCheckRequest::new);
        assertEquals(original.coordinatorId, copy.coordinatorId);
        assertArrayEquals(original.taskIds, copy.taskIds);
    }

    public void testTaskCheckResponseSerializationRoundTrip() throws Exception {
        boolean[] active = new boolean[WorkloadGroupSharedThrottleService.MAX_TASKS_PER_CHECK];
        for (int i = 0; i < active.length; i++) {
            active[i] = randomBoolean();
        }
        TaskCheckResponse original = new TaskCheckResponse(active);
        TaskCheckResponse copy = copyWriteable(original, writableRegistry(), TaskCheckResponse::new);
        assertArrayEquals(original.active, copy.active);
    }

    public void testTaskCheckRequestIgnoresFieldsAddedByANewerPeer() throws Exception {
        Map<String, Object> body = new HashMap<>();
        body.put(TaskCheckRequest.KEY_COORDINATOR, "coordinator");
        body.put(TaskCheckRequest.KEY_TASK_IDS, List.of(7L, 8L));
        body.put("future_include_cancelled", true);
        body.put("future_list", List.of("a", "b"));
        StreamInput in = bodyBytes(body, true);
        TaskCheckRequest parsed = new TaskCheckRequest(in);
        assertEquals("coordinator", parsed.coordinatorId);
        assertArrayEquals(new long[] { 7L, 8L }, parsed.taskIds);
        assertEquals("unknown keys must be consumed, not left as trailing bytes", 0, in.available());
    }

    public void testTaskCheckResponseIgnoresFieldsAddedByANewerPeer() throws Exception {
        Map<String, Object> body = new HashMap<>();
        body.put(TaskCheckResponse.KEY_ACTIVE, List.of(false, true));
        body.put("future_cancelled", List.of(false, false));
        StreamInput in = bodyBytes(body, false);
        assertArrayEquals(new boolean[] { false, true }, new TaskCheckResponse(in).active);
        assertEquals(0, in.available());
    }

    public void testTaskCheckMessagesAreStrict() throws Exception {
        Map<String, Object> full = new HashMap<>();
        full.put(TaskCheckRequest.KEY_COORDINATOR, "coordinator");
        full.put(TaskCheckRequest.KEY_TASK_IDS, List.of(7L));
        for (String omitted : full.keySet()) {
            Map<String, Object> partial = new HashMap<>(full);
            partial.remove(omitted);
            IllegalStateException e = expectThrows(IllegalStateException.class, () -> new TaskCheckRequest(bodyBytes(partial, true)));
            assertTrue(e.getMessage(), e.getMessage().contains(omitted));
        }
        expectThrows(IllegalStateException.class, () -> new TaskCheckResponse(bodyBytes(new HashMap<>(), false)));
    }

    public void testTaskCheckMessagesRejectElementsOfTheWrongType() throws Exception {
        Map<String, Object> request = new HashMap<>();
        request.put(TaskCheckRequest.KEY_COORDINATOR, "coordinator");
        request.put(TaskCheckRequest.KEY_TASK_IDS, List.of(7L, "eight"));
        expectThrows(IllegalStateException.class, () -> new TaskCheckRequest(bodyBytes(request, true)));
        Map<String, Object> response = new HashMap<>();
        response.put(TaskCheckResponse.KEY_ACTIVE, List.of(true, 1));
        expectThrows(IllegalStateException.class, () -> new TaskCheckResponse(bodyBytes(response, false)));
    }

    public void testTaskCheckRejectsOversizedPayloads() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            new TaskCheckRequest("coordinator", new long[WorkloadGroupSharedThrottleService.MAX_TASKS_PER_CHECK + 1]).writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                expectThrows(IOException.class, () -> new TaskCheckRequest(in));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            new TaskCheckResponse(new boolean[WorkloadGroupSharedThrottleService.MAX_TASKS_PER_CHECK + 1]).writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                expectThrows(IOException.class, () -> new TaskCheckResponse(in));
            }
        }
    }

    public void testReleasePermitResponseIgnoresFieldsAddedByANewerPeer() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            WorkloadGroupSharedThrottleService.ReleasePermitResponse.INSTANCE.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                new WorkloadGroupSharedThrottleService.ReleasePermitResponse(in);
                assertEquals(0, in.available());
            }
        }
        Map<String, Object> body = new HashMap<>();
        body.put("future_was_known", true);
        StreamInput in = bodyBytes(body, false);
        new WorkloadGroupSharedThrottleService.ReleasePermitResponse(in);
        assertEquals("unknown keys must be consumed, not left as trailing bytes", 0, in.available());
    }

    private static DiscoveryNode dataNode(String id) {
        return new DiscoveryNode(
            id,
            id,
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
    }

    private WorkloadGroupSharedThrottleService serviceWithRing(DiscoveryNodes nodes) {
        WorkloadGroupSharedThrottleService service = new WorkloadGroupSharedThrottleService(clusterService, threadPool, transportService);
        deliverNodesChanged(service, nodes);
        return service;
    }

    private static String bucketOwnedBy(WorkloadGroupSharedThrottleService service, DiscoveryNode owner) {
        for (int i = 0; i < 10000; i++) {
            String candidate = "bucket-" + i;
            if (service.ring().ownerFor(candidate).filter(owner::equals).isPresent()) {
                return candidate;
            }
        }
        throw new AssertionError("no bucket owned by " + owner);
    }

    public void testReleasePermitRequestSerializationRoundTrip() throws Exception {
        WorkloadGroupSharedThrottleService.ReleasePermitRequest original = new WorkloadGroupSharedThrottleService.ReleasePermitRequest(
            "grp1:group",
            "permit-abc"
        );
        WorkloadGroupSharedThrottleService.ReleasePermitRequest copy = copyWriteable(
            original,
            writableRegistry(),
            WorkloadGroupSharedThrottleService.ReleasePermitRequest::new
        );
        assertEquals(original.bucketKey, copy.bucketKey);
        assertEquals(original.permitId, copy.permitId);

        // A release only names the permit: an unused grant is returned by the GRANT's reply, not by a flagged RELEASE.
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                TaskId.readFromStream(in); // TransportRequest preamble
                assertEquals(
                    Set.of(
                        WorkloadGroupSharedThrottleService.ReleasePermitRequest.KEY_BUCKET,
                        WorkloadGroupSharedThrottleService.ReleasePermitRequest.KEY_PERMIT_ID
                    ),
                    in.readMap(StreamInput::readString, StreamInput::readGenericValue).keySet()
                );
            }
        }
    }

    public void testGrantPermitRequestSerializationRoundTrip() throws Exception {
        WorkloadGroupSharedThrottleService.GrantPermitRequest original = new WorkloadGroupSharedThrottleService.GrantPermitRequest(
            "grp1:group",
            "permit-g"
        );
        WorkloadGroupSharedThrottleService.GrantPermitRequest copy = copyWriteable(
            original,
            writableRegistry(),
            WorkloadGroupSharedThrottleService.GrantPermitRequest::new
        );
        assertEquals("grp1:group", copy.bucketKey);
        assertEquals("permit-g", copy.permitId);

        // The coordinator needs no limit, so the grant carries none: a dead strict field would bind every future owner.
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                TaskId.readFromStream(in); // TransportRequest preamble
                assertEquals(
                    Set.of(
                        WorkloadGroupSharedThrottleService.GrantPermitRequest.KEY_BUCKET,
                        WorkloadGroupSharedThrottleService.GrantPermitRequest.KEY_PERMIT_ID
                    ),
                    in.readMap(StreamInput::readString, StreamInput::readGenericValue).keySet()
                );
            }
        }
    }

    public void testGrantPermitResponseSerializationRoundTrip() throws Exception {
        final long taskId = randomLongBetween(1, Long.MAX_VALUE);
        for (OptionalLong admitted : List.of(OptionalLong.empty(), OptionalLong.of(taskId))) {
            WorkloadGroupSharedThrottleService.GrantPermitResponse original = new WorkloadGroupSharedThrottleService.GrantPermitResponse(
                admitted
            );
            WorkloadGroupSharedThrottleService.GrantPermitResponse copy = copyWriteable(
                original,
                writableRegistry(),
                WorkloadGroupSharedThrottleService.GrantPermitResponse::new
            );
            assertEquals(admitted, copy.admittedTaskId);

            // Always written, as a list of zero or one ids: "nothing admitted" is an empty list, never a sentinel id.
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                original.writeTo(out);
                try (StreamInput in = out.bytes().streamInput()) {
                    assertEquals(
                        Map.of(
                            WorkloadGroupSharedThrottleService.GrantPermitResponse.KEY_ADMITTED_TASK_IDS,
                            admitted.isPresent() ? List.of(taskId) : List.of()
                        ),
                        in.readMap(StreamInput::readString, StreamInput::readGenericValue)
                    );
                }
            }
        }
        assertEquals(OptionalLong.empty(), WorkloadGroupSharedThrottleService.GrantPermitResponse.NOT_ADMITTED.admittedTaskId);
    }

    public void testRegisterWaiterRequestSerializationRoundTrip() throws Exception {
        WorkloadGroupSharedThrottleService.RegisterWaiterRequest original = new WorkloadGroupSharedThrottleService.RegisterWaiterRequest(
            "grp1:group",
            "coordinator-ephemeral-id"
        );
        WorkloadGroupSharedThrottleService.RegisterWaiterRequest copy = copyWriteable(
            original,
            writableRegistry(),
            WorkloadGroupSharedThrottleService.RegisterWaiterRequest::new
        );
        assertEquals(original.bucketKey, copy.bucketKey);
        assertEquals(original.coordinatorId, copy.coordinatorId);
        // The sender is named by its ephemeral id, as on an acquire: no persistent node id is sent.
        assertEquals(
            Set.of(
                WorkloadGroupSharedThrottleService.RegisterWaiterRequest.KEY_BUCKET,
                WorkloadGroupSharedThrottleService.RegisterWaiterRequest.KEY_COORDINATOR
            ),
            writtenRequestKeys(original)
        );
        assertEquals(
            "the same key as the acquire's",
            WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_COORDINATOR,
            WorkloadGroupSharedThrottleService.RegisterWaiterRequest.KEY_COORDINATOR
        );
    }

    public void testRegisterWaiterResponseSerializationRoundTrip() throws Exception {
        for (boolean registered : new boolean[] { true, false }) {
            WorkloadGroupSharedThrottleService.RegisterWaiterResponse original =
                new WorkloadGroupSharedThrottleService.RegisterWaiterResponse(registered);
            WorkloadGroupSharedThrottleService.RegisterWaiterResponse copy = copyWriteable(
                original,
                writableRegistry(),
                WorkloadGroupSharedThrottleService.RegisterWaiterResponse::new
            );
            assertEquals(registered, copy.registered);
        }
    }

    // Serde tolerance: bodies are written by hand to stand in for another build's writer.

    private static StreamInput bodyBytes(Map<String, Object> body, boolean withTaskPreamble) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        if (withTaskPreamble) {
            TaskId.EMPTY_TASK_ID.writeTo(out); // TransportRequest preamble that super(in) consumes; responses have none
        }
        out.writeMap(body, StreamOutput::writeString, StreamOutput::writeGenericValue);
        return out.bytes().streamInput();
    }

    // The body keys a request writes.
    private static Set<String> writtenRequestKeys(TransportRequest request) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                TaskId.readFromStream(in); // TransportRequest preamble
                return in.readMap(StreamInput::readString, StreamInput::readGenericValue).keySet();
            }
        }
    }

    // Every key a current writer sends, so tolerance and strictness tests start from a complete body.
    private static Map<String, Object> fullAcquireRequestBody() {
        Map<String, Object> body = new HashMap<>();
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_BUCKET, "grp1:group");
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_SHARED_LIMIT, 5);
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_PERMIT_ID, "permit-abc");
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_COORDINATOR, "eph-coordinator");
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_COORDINATOR_TASK_ID, 7L);
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_WANTS_QUEUE, true);
        return body;
    }

    private static Map<String, Object> fullAcquireResponseBody() {
        Map<String, Object> body = new HashMap<>();
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitResponse.KEY_GRANTED, false);
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitResponse.KEY_NOT_OWNER, false);
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitResponse.KEY_REGISTERED, true);
        return body;
    }

    private static Map<String, Object> fullReleaseRequestBody() {
        Map<String, Object> body = new HashMap<>();
        body.put(WorkloadGroupSharedThrottleService.ReleasePermitRequest.KEY_BUCKET, "grp1:group");
        body.put(WorkloadGroupSharedThrottleService.ReleasePermitRequest.KEY_PERMIT_ID, "permit-abc");
        return body;
    }

    private static Map<String, Object> fullGrantRequestBody() {
        Map<String, Object> body = new HashMap<>();
        body.put(WorkloadGroupSharedThrottleService.GrantPermitRequest.KEY_BUCKET, "grp1:group");
        body.put(WorkloadGroupSharedThrottleService.GrantPermitRequest.KEY_PERMIT_ID, "permit-g");
        return body;
    }

    private static Map<String, Object> fullGrantResponseBody(List<?> admittedTaskIds) {
        Map<String, Object> body = new HashMap<>();
        body.put(WorkloadGroupSharedThrottleService.GrantPermitResponse.KEY_ADMITTED_TASK_IDS, admittedTaskIds);
        return body;
    }

    private static Map<String, Object> fullRegisterWaiterRequestBody() {
        Map<String, Object> body = new HashMap<>();
        body.put(WorkloadGroupSharedThrottleService.RegisterWaiterRequest.KEY_BUCKET, "grp1:group");
        body.put(WorkloadGroupSharedThrottleService.RegisterWaiterRequest.KEY_COORDINATOR, "eph-coordinator");
        return body;
    }

    private static Map<String, Object> fullRegisterWaiterResponseBody() {
        Map<String, Object> body = new HashMap<>();
        body.put(WorkloadGroupSharedThrottleService.RegisterWaiterResponse.KEY_REGISTERED, true);
        return body;
    }

    // Adds fields a newer peer might send, of every type readGenericValue knows.
    private static Map<String, Object> withFutureFields(Map<String, Object> body) {
        Map<String, Object> extended = new HashMap<>(body);
        extended.put("future_flag", true);
        extended.put("future_reason", "at_limit");
        extended.put("future_retry_after_millis", 250L);
        extended.put("future_list", List.of("a", "b"));
        extended.put("future_null", null);
        return extended;
    }

    // Every key of a complete body is required: dropping any one fails decoding, naming it, rather than being guessed.
    private static void assertEveryKeyRequired(
        Map<String, Object> full,
        boolean request,
        CheckedFunction<StreamInput, ?, IOException> reader
    ) throws IOException {
        for (String omitted : full.keySet()) {
            Map<String, Object> partial = new HashMap<>(full);
            partial.remove(omitted);
            StreamInput in = bodyBytes(partial, request);
            IllegalStateException e = expectThrows(IllegalStateException.class, () -> reader.apply(in));
            assertTrue(e.getMessage(), e.getMessage().contains(omitted));
        }
    }

    public void testAcquirePermitRequestIgnoresFieldsAddedByANewerPeer() throws Exception {
        Map<String, Object> body = withFutureFields(fullAcquireRequestBody());
        body.put("ttl_nanos", 1_000L); // not part of the format: the owner applies its own TTL
        body.put("requesting_node_id", "node-coordinator"); // not part of the format either: an older dev build sent it
        StreamInput in = bodyBytes(body, true);
        WorkloadGroupSharedThrottleService.AcquirePermitRequest parsed = new WorkloadGroupSharedThrottleService.AcquirePermitRequest(in);
        assertEquals("grp1:group", parsed.bucketKey);
        assertEquals(5, parsed.sharedLimit);
        assertEquals("permit-abc", parsed.permitId);
        assertEquals(WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS, parsed.ttlNanos);
        assertEquals("eph-coordinator", parsed.coordinatorId);
        assertEquals(7L, parsed.coordinatorTaskId);
        assertTrue(parsed.wantsQueue);
        assertEquals("unknown keys must be consumed, not left as trailing bytes", 0, in.available());
    }

    public void testAcquirePermitRequestIsStrict() throws Exception {
        // Every field ships with throttling itself, so a missing one fails decoding (reported unavailable).
        assertEveryKeyRequired(fullAcquireRequestBody(), true, WorkloadGroupSharedThrottleService.AcquirePermitRequest::new);
    }

    public void testAcquirePermitRequestAcceptsAWidenedNumericType() throws Exception {
        // A peer that widens shared_limit to a long must still interoperate.
        Map<String, Object> body = fullAcquireRequestBody();
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_SHARED_LIMIT, 5L);
        WorkloadGroupSharedThrottleService.AcquirePermitRequest parsed = new WorkloadGroupSharedThrottleService.AcquirePermitRequest(
            bodyBytes(body, true)
        );
        assertEquals(5, parsed.sharedLimit);
    }

    public void testAcquirePermitResponseIgnoresFieldsAddedByANewerPeer() throws Exception {
        StreamInput in = bodyBytes(withFutureFields(fullAcquireResponseBody()), false); // TransportResponse has no TaskId preamble
        WorkloadGroupSharedThrottleService.AcquirePermitResponse parsed = new WorkloadGroupSharedThrottleService.AcquirePermitResponse(in);
        assertFalse(parsed.granted);
        assertFalse(parsed.notOwner);
        assertTrue(parsed.registered);
        assertEquals(0, in.available());
    }

    public void testAcquirePermitResponseIsStrict() throws Exception {
        // In particular a missing registered key is rejected, never read as "not registered".
        assertEveryKeyRequired(fullAcquireResponseBody(), false, WorkloadGroupSharedThrottleService.AcquirePermitResponse::new);
    }

    public void testReleasePermitRequestIgnoresFieldsAddedByANewerPeer() throws Exception {
        Map<String, Object> body = withFutureFields(fullReleaseRequestBody());
        body.put("shared_limit", 5);
        // Not part of the format: a release carries no deregistration signal, whoever sends it.
        body.put("requesting_node_id", "node-1");
        body.put("unused_grant", true);
        StreamInput in = bodyBytes(body, true);
        WorkloadGroupSharedThrottleService.ReleasePermitRequest parsed = new WorkloadGroupSharedThrottleService.ReleasePermitRequest(in);
        assertEquals("grp1:group", parsed.bucketKey);
        assertEquals("permit-abc", parsed.permitId);
        assertEquals(0, in.available());
    }

    public void testReleasePermitRequestIsStrictLikeTheOthers() throws Exception {
        assertEveryKeyRequired(fullReleaseRequestBody(), true, WorkloadGroupSharedThrottleService.ReleasePermitRequest::new);
    }

    public void testGrantPermitRequestIgnoresFieldsAddedByANewerPeer() throws Exception {
        Map<String, Object> body = withFutureFields(fullGrantRequestBody());
        body.put("shared_limit", 4); // not part of the format: the coordinator needs no limit
        StreamInput in = bodyBytes(body, true);
        WorkloadGroupSharedThrottleService.GrantPermitRequest parsed = new WorkloadGroupSharedThrottleService.GrantPermitRequest(in);
        assertEquals("grp1:group", parsed.bucketKey);
        assertEquals("permit-g", parsed.permitId);
        assertEquals(0, in.available());
    }

    public void testGrantPermitRequestIsStrict() throws Exception {
        assertEveryKeyRequired(fullGrantRequestBody(), true, WorkloadGroupSharedThrottleService.GrantPermitRequest::new);
    }

    public void testGrantPermitResponseIgnoresFieldsAddedByANewerPeer() throws Exception {
        for (List<?> admittedTaskIds : List.of(List.of(), List.of(7L))) {
            StreamInput in = bodyBytes(withFutureFields(fullGrantResponseBody(admittedTaskIds)), false);
            WorkloadGroupSharedThrottleService.GrantPermitResponse parsed = new WorkloadGroupSharedThrottleService.GrantPermitResponse(in);
            assertEquals(admittedTaskIds.isEmpty() ? OptionalLong.empty() : OptionalLong.of(7L), parsed.admittedTaskId);
            assertEquals("unknown keys must be consumed, not left as trailing bytes", 0, in.available());
        }
    }

    public void testGrantPermitResponseIsStrict() throws Exception {
        // A missing outcome is rejected, never read as "nothing admitted" (which would reclaim a slot a search holds).
        assertEveryKeyRequired(fullGrantResponseBody(List.of(7L)), false, WorkloadGroupSharedThrottleService.GrantPermitResponse::new);
        assertEveryKeyRequired(fullGrantResponseBody(List.of()), false, WorkloadGroupSharedThrottleService.GrantPermitResponse::new);
    }

    public void testGrantPermitResponseRejectsMoreThanOneAdmittedTask() throws Exception {
        // One grant admits at most one request.
        IOException e = expectThrows(
            IOException.class,
            () -> new WorkloadGroupSharedThrottleService.GrantPermitResponse(bodyBytes(fullGrantResponseBody(List.of(7L, 8L)), false))
        );
        assertTrue(e.getMessage(), e.getMessage().contains(WorkloadGroupSharedThrottleService.GrantPermitResponse.KEY_ADMITTED_TASK_IDS));
    }

    public void testGrantPermitResponseRejectsAMalformedOutcome() throws Exception {
        for (Object malformed : List.<Object>of(7L, "7", List.of("seven"), List.of(true))) {
            Map<String, Object> body = new HashMap<>();
            body.put(WorkloadGroupSharedThrottleService.GrantPermitResponse.KEY_ADMITTED_TASK_IDS, malformed);
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> new WorkloadGroupSharedThrottleService.GrantPermitResponse(bodyBytes(body, false))
            );
            assertTrue(
                e.getMessage(),
                e.getMessage().contains(WorkloadGroupSharedThrottleService.GrantPermitResponse.KEY_ADMITTED_TASK_IDS)
            );
        }
    }

    public void testRegisterWaiterRequestIgnoresFieldsAddedByANewerPeer() throws Exception {
        Map<String, Object> body = withFutureFields(fullRegisterWaiterRequestBody());
        body.put("requesting_node_id", "node-1"); // not part of the format: an older dev build sent it
        StreamInput in = bodyBytes(body, true);
        WorkloadGroupSharedThrottleService.RegisterWaiterRequest parsed = new WorkloadGroupSharedThrottleService.RegisterWaiterRequest(in);
        assertEquals("grp1:group", parsed.bucketKey);
        assertEquals("eph-coordinator", parsed.coordinatorId);
        assertEquals(0, in.available());
    }

    public void testRegisterWaiterRequestIsStrict() throws Exception {
        assertEveryKeyRequired(fullRegisterWaiterRequestBody(), true, WorkloadGroupSharedThrottleService.RegisterWaiterRequest::new);
        // An older dev build named itself by its persistent id only: that id is ignored, and the missing key is reported.
        Map<String, Object> older = new HashMap<>();
        older.put(WorkloadGroupSharedThrottleService.RegisterWaiterRequest.KEY_BUCKET, "grp1:group");
        older.put("requesting_node_id", "node-1");
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> new WorkloadGroupSharedThrottleService.RegisterWaiterRequest(bodyBytes(older, true))
        );
        assertTrue(e.getMessage(), e.getMessage().contains(WorkloadGroupSharedThrottleService.RegisterWaiterRequest.KEY_COORDINATOR));
    }

    public void testRegisterWaiterResponseIgnoresFieldsAddedByANewerPeer() throws Exception {
        StreamInput in = bodyBytes(withFutureFields(fullRegisterWaiterResponseBody()), false);
        assertTrue(new WorkloadGroupSharedThrottleService.RegisterWaiterResponse(in).registered);
        assertEquals(0, in.available());
    }

    public void testRegisterWaiterResponseIsStrict() throws Exception {
        assertEveryKeyRequired(fullRegisterWaiterResponseBody(), false, WorkloadGroupSharedThrottleService.RegisterWaiterResponse::new);
    }

    /**
     * LOCAL-OWNER path. Registration there looks nothing up (the waiter IS this node), so it can never report "not
     * registered". Verified even with the local node ABSENT from its own applied state: the ring still routes the bucket
     * here, the local short-circuit registers {@code clusterService.localNode()} directly, and the no-waiter callback must
     * stay unused.
     */
    public void testLocalOwnerAlwaysRegistersEvenWhenAbsentFromItsOwnAppliedState() {
        final String bucket = "grp-local:group";
        final int sharedLimit = 1;
        WorkloadGroupSharedThrottleService service = newService(); // ring built while the local node IS present
        stubGroupFor(bucket, sharedLimit);

        ClusterState nodeless = Mockito.mock(ClusterState.class);
        when(nodeless.nodes()).thenReturn(DiscoveryNodes.EMPTY_NODES);
        when(nodeless.metadata()).thenReturn(metadata);
        when(clusterService.state()).thenReturn(nodeless);
        assertNull("precondition: the local node must not resolve from its own state", clusterService.state().nodes().get("local"));

        assertNotNull("first acquire takes the only shared slot", awaitGrant(service, bucket, sharedLimit));
        denyAndRegister(service, bucket, sharedLimit);
        assertEquals("the local waiter must still be registered", 1, service.waiterCount(bucket));
    }

    /**
     * The local-owner denial must be reported through the listener (the denial marker), never by diverting to the
     * no-waiter callback, for BOTH the queueing and non-queueing overloads.
     */
    public void testLocalOwnerDenialUsesTheListenerForBothOverloads() {
        final String bucket = "grp-local2:group";
        final int sharedLimit = 1;
        WorkloadGroupSharedThrottleService service = newService();
        stubGroupFor(bucket, sharedLimit);
        assertNotNull(awaitGrant(service, bucket, sharedLimit));

        // Non-queueing overload: null callback, so a divert here would NPE.
        AtomicReference<Exception> plain = new AtomicReference<>();
        service.acquireAsync(
            bucket,
            sharedLimit,
            SharedThrottleTestPermits.NO_SUCH_TASK,
            false,
            ActionListener.wrap(p -> fail("must be denied"), plain::set)
        );
        assertTrue(WorkloadGroupSharedThrottleService.isDenial(plain.get()));
        assertEquals("wantsQueue=false must not register a waiter", 0, service.waiterCount(bucket));

        // Queueing overload: the callback is supplied but must remain unused.
        denyAndRegister(service, bucket, sharedLimit);
        assertEquals("now a local waiter is registered", 1, service.waiterCount(bucket));
    }

    public void testQueueingAcquireToAnUnreachableOwnerIsUnavailableNotAnUnregisteredDenial() {
        // Fail-closed for the queueing overload: when the shared tier cannot answer, the outcome is the unavailable
        // marker through the listener — never the no-waiter callback (a denial) and never a waiter registration.
        final DiscoveryNode remote = dataNode("remote-unreachable");
        final DiscoveryNodes bothNodes = DiscoveryNodes.builder().add(localNode).add(remote).localNodeId(localNode.getId()).build();
        WorkloadGroupSharedThrottleService service = serviceWithRing(bothNodes);
        final String bucket = bucketOwnedBy(service, remote) + ":group";
        when(transportService.nodeConnected(remote)).thenReturn(false);

        AtomicReference<Exception> failure = new AtomicReference<>();
        service.acquireAsync(
            bucket,
            1,
            SharedThrottleTestPermits.NO_SUCH_TASK,
            false,
            ActionListener.wrap(p -> fail("must not admit"), failure::set),
            () -> fail("not a denial")
        );
        assertTrue(WorkloadGroupSharedThrottleService.isUnavailable(failure.get()));
        assertEquals(0, service.waiterCount(bucket));
    }

    public void testSlotFreedBetweenLocalDenialAndRegistrationStillWakesTheWaiter() {
        // The slot frees after the failed tryAcquire but before registerWaiter: it sees no waiter and is offered to
        // nobody. Without the post-registration re-check the request would wait for an unrelated later release.
        // The denying acquire itself opens that window: the holder's permit has expired, so the acquire collects it and,
        // once the bucket lock is released (after the denial, before registration), runs its task check inline. The check
        // finds the holder's search finished and frees the slot while this coordinator is not yet a waiter.
        final String bucket = "deny-register-race:group";
        final AtomicLong nanos = new AtomicLong(0L);
        inlineTaskChecks();
        WorkloadGroupSharedThrottleService service = newServiceWithClock(nanos);
        stubGroupFor(bucket, 1);
        AtomicInteger admits = new AtomicInteger();
        service.setGrantConsumer((bucketKey, permit) -> {
            admits.incrementAndGet();
            return ADMITTED;
        });
        assertNotNull(awaitGrant(service, bucket, 1));
        nanos.set(WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS + 1);

        denyAndRegister(service, bucket, 1);

        assertEquals("the expired holder's slot was freed inside the denial window", 0, service.tracker().pendingChecks());
        assertEquals("the slot freed inside the window must be offered to the new waiter", 1, admits.get());
        assertEquals("only the pushed grant holds the slot", 1, service.tracker().inFlight(bucket));
    }

    public void testReconcileRetainedWaitersWithAnEmptyRingLeavesThemToTheNodeTier() {
        DiscoveryNode managerOnly = new DiscoveryNode(
            "m",
            "m",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            Version.CURRENT
        );
        ClusterState state = Mockito.mock(ClusterState.class);
        when(state.nodes()).thenReturn(DiscoveryNodes.builder().add(managerOnly).localNodeId("m").build());
        when(clusterService.state()).thenReturn(state);
        when(clusterService.localNode()).thenReturn(managerOnly);
        WorkloadGroupSharedThrottleService service = newService();
        List<String> noOwner = new ArrayList<>();
        service.setNoEligibleOwnerHandler(noOwner::add);

        service.reconcileRetainedWaiters("g1:group");

        // No node counts shared_limit, so the node tier decides for the waiters.
        assertEquals(List.of("g1:group"), noOwner);
    }

    public void testReconcileRetainedWaitersWithTheLocalOwner() {
        final String bucket = "g1:group";
        WorkloadGroupSharedThrottleService service = newService();

        // No shared tier in this node's view: the local owner refuses the registration and registers nobody.
        service.reconcileRetainedWaiters(bucket);
        assertEquals(0, service.waiterCount(bucket));

        stubGroupFor(bucket, 1);
        Releasable holder = awaitGrant(service, bucket, 1);
        service.reconcileRetainedWaiters(bucket);
        assertEquals("the backstop restores a dropped waiter membership", 1, service.waiterCount(bucket));
        holder.close();
    }

    public void testOwnerRingEmptyingLeavesRetainedWaitersToTheNodeTier() {
        final String bucket = "g1:group";
        stubGroupFor(bucket, 1);
        when(threadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(OpenSearchExecutors.newDirectExecutorService());
        WorkloadGroupSharedThrottleService service = newService();
        service.setRetainedBucketKeysSupplier(() -> Set.of(bucket));
        List<String> noOwner = new ArrayList<>();
        service.setNoEligibleOwnerHandler(noOwner::add);

        ClusterState previous = clusterService.state();
        ClusterState current = Mockito.mock(ClusterState.class);
        when(current.nodes()).thenReturn(DiscoveryNodes.EMPTY_NODES);
        service.clusterChanged(new ClusterChangedEvent("every owner left", current, previous));

        assertEquals("with no owner left no node counts shared_limit, so the node tier decides", List.of(bucket), noOwner);
    }

    public void testOwnerReturningReRegistersRetainedWaitersAndAdmitsThem() {
        final String bucket = "g1:group";
        stubGroupFor(bucket, 1);
        when(threadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(OpenSearchExecutors.newDirectExecutorService());
        WorkloadGroupSharedThrottleService service = newService();
        service.setRetainedBucketKeysSupplier(() -> Set.of(bucket));
        service.setNoEligibleOwnerHandler(bucketKey -> {});
        // One retained request: the first grant admits it, and a later one finds nothing to admit.
        List<Releasable> admitted = new ArrayList<>();
        service.setGrantConsumer((bucketKey, permit) -> {
            if (admitted.isEmpty()) {
                admitted.add(permit);
                return ADMITTED;
            }
            return NOT_ADMITTED;
        });
        ClusterState withOwner = clusterService.state();
        ClusterState noOwner = Mockito.mock(ClusterState.class);
        when(noOwner.nodes()).thenReturn(DiscoveryNodes.EMPTY_NODES);
        service.clusterChanged(new ClusterChangedEvent("every owner left", noOwner, withOwner));
        assertFalse(service.hasEligibleOwner());

        service.clusterChanged(new ClusterChangedEvent("an owner joined", withOwner, noOwner));

        // The retained bucket registers with its new owner, here this node, whose free slot then admits the request.
        assertEquals("the new owner's grant admits the retained request", 1, admitted.size());
        assertEquals(1, service.waiterCount(bucket));
        assertEquals(1, service.tracker().inFlight(bucket));
        admitted.get(0).close();
        assertEquals(0, service.tracker().inFlight(bucket));
        assertEquals("the unused follow-up grant deregisters the drained coordinator", 0, service.waiterCount(bucket));
    }
}
