/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.Version;
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
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.mockito.Mockito;

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
            new SharedThrottleTracker(nanos::get)
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
            false,
            ActionListener.wrap(p -> fail("must be denied while at limit"), denial::set),
            () -> fail("the local owner must always be able to register itself as a waiter")
        );
        assertTrue("acquire at limit must be denied", WorkloadGroupSharedThrottleService.isDenial(denial.get()));
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
        service.acquireAsync(bucket, limit, false, ActionListener.wrap(permit::set, failure::set));
        if (failure.get() != null) {
            throw failure.get() instanceof RuntimeException re ? re : new RuntimeException(failure.get());
        }
        return permit.get();
    }

    public void testTtlSweepDrivesOwnerPushForAnExpiredPermitSlot() {
        // A permit reclaimed by the TTL sweep frees a shared slot with NO release RPC behind it (the holder crashed, or its
        // release was lost). With no subsequent acquire, the sweep is the only observer of that free slot, so it must
        // drive owner-push; otherwise a retained request stays registered while the slot sits idle.
        final int sharedLimit = 1;
        final String bucket = "g1:group";
        final AtomicLong nanos = new AtomicLong(0L);
        WorkloadGroupSharedThrottleService service = newServiceWithClock(nanos);
        stubGroupFor(bucket, sharedLimit);

        final AtomicInteger admits = new AtomicInteger(0);
        service.setGrantConsumer((bucketKey, reservedPermit) -> {
            admits.incrementAndGet();
            return true; // stands in for the queue service admitting one parked request
        });

        assertNotNull("first acquire takes the only shared slot", awaitGrant(service, bucket, sharedLimit));
        denyAndRegister(service, bucket, sharedLimit);
        assertEquals("coordinator is registered as a waiter", 1, service.waiterCountForTest(bucket));

        // The holder vanishes: advance past the permit TTL WITHOUT any release RPC.
        nanos.set(WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS + 1);
        assertEquals("nothing can have been admitted before the sweep runs", 0, admits.get());

        service.sweepExpiredAndDrive();
        assertEquals("the sweep must drive owner-push for the slot it freed", 1, admits.get());
    }

    public void testTtlSweepOffersEveryPermitReclaimedInOneBucket() {
        final int sharedLimit = 3;
        final String bucket = "multi-expiry:group";
        final AtomicLong nanos = new AtomicLong(0L);
        WorkloadGroupSharedThrottleService service = newServiceWithClock(nanos);
        stubGroupFor(bucket, sharedLimit);

        final AtomicInteger parked = new AtomicInteger(3);
        final AtomicInteger admits = new AtomicInteger();
        final List<Releasable> grantedPermits = new ArrayList<>();
        service.setGrantConsumer((bucketKey, permit) -> {
            if (parked.getAndDecrement() <= 0) {
                return false;
            }
            admits.incrementAndGet();
            grantedPermits.add(permit);
            return true;
        });

        final List<Releasable> expiredHolders = new ArrayList<>();
        for (int i = 0; i < sharedLimit; i++) {
            expiredHolders.add(awaitGrant(service, bucket, sharedLimit));
        }
        for (int i = 0; i < sharedLimit; i++) {
            denyAndRegister(service, bucket, sharedLimit);
        }

        nanos.set(WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS + 1L);
        service.sweepExpiredAndDrive();

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
                return false;
            }
            admits.incrementAndGet();
            grantedPermits.add(permit);
            return true;
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
        assertEquals(1, service.waiterCountForTest(bucket));

        DiscoveryNodes nodes = clusterService.state().nodes();
        ClusterState previousState = stateWithGroups(nodes, Map.of(groupId, workloadGroup(groupId, sharedLimit)));
        ClusterState currentState = stateWithGroups(nodes, Map.of());
        when(clusterService.state()).thenReturn(currentState);

        service.clusterChanged(new ClusterChangedEvent("workload group deleted", currentState, previousState));

        assertNotNull("cleanup must be scheduled off the cluster-applier thread", cleanupTask.get());
        assertEquals("the asynchronous cleanup must not run inline", 1, service.waiterCountForTest(bucket));
        cleanupTask.get().run();
        assertEquals("deleting the group must prune its owner-side waiter membership", 0, service.waiterCountForTest(bucket));
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
        assertEquals(1, service.waiterCountForTest(bucket));

        DiscoveryNodes nodes = clusterService.state().nodes();
        ClusterState previousState = stateWithGroups(nodes, Map.of(groupId, workloadGroup(groupId, sharedLimit)));
        Settings nodeOnlyThrottling = Settings.builder().put("node_limit", 1).build();
        ClusterState currentState = stateWithGroups(nodes, Map.of(groupId, workloadGroup(groupId, nodeOnlyThrottling)));
        when(clusterService.state()).thenReturn(currentState);

        // Metadata-only (see testSharedLimitIncreaseOffersOnlyTheAddedCapacityFromClusterChange).
        final ClusterChangedEvent event = new ClusterChangedEvent("shared tier removed", currentState, previousState);
        assertFalse("precondition: a metadata-only event", event.nodesChanged());
        service.clusterChanged(event);

        assertEquals("removing shared_limit must prune the owner-side waiter membership", 0, service.waiterCountForTest(bucket));
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
                return false;
            }
            admits.incrementAndGet();
            grantedPermits.add(permit);
            return true;
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
            return true;
        });

        Releasable oldPermit = awaitGrant(service, bucket, sharedLimit);
        denyAndRegister(service, bucket, sharedLimit);
        assertEquals(1, service.waiterCountForTest(bucket));

        deliverNodesChanged(service, bothNodes);
        assertEquals(remote, service.ring().ownerFor(bucket).orElseThrow());
        assertEquals("the former owner must drop stale waiter routing state", 0, service.waiterCountForTest(bucket));

        WorkloadGroupSharedThrottleService.AcquirePermitResponse staleAcquire = service.handleAcquire(
            new WorkloadGroupSharedThrottleService.AcquirePermitRequest(
                bucket,
                sharedLimit,
                "stale-acquire",
                WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS,
                localNode.getEphemeralId(),
                localNode.getId(),
                true
            )
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

        WorkloadGroupSharedThrottleService service = newService();
        stubGroupFor(bucket, sharedLimit);
        AtomicInteger admits = new AtomicInteger();
        service.setGrantConsumer((bucketKey, permit) -> {
            admits.incrementAndGet();
            return true;
        });

        Releasable holder = awaitGrant(service, bucket, sharedLimit);
        denyAndRegister(service, bucket, sharedLimit);
        assertEquals(1, service.waiterCountForTest(bucket));

        // The next successful tracker acquire is the owner-push reservation triggered by the release below; move the
        // bucket away inside that window (the same test seam as the acquire-side fence).
        AtomicBoolean swapped = new AtomicBoolean();
        service.afterTrackerAcquire = () -> {
            if (swapped.compareAndSet(false, true)) {
                deliverNodesChanged(service, bothNodes);
            }
        };
        holder.close();

        assertTrue("the reservation window was exercised", swapped.get());
        assertEquals(remote, service.ring().ownerFor(bucket).orElseThrow());
        assertEquals("the post-reservation owner fence must reclaim the stale permit", 0, service.tracker().inFlight(bucket));
        assertEquals("the former owner must not expose the stale permit to its local waiter", 0, admits.get());
        assertEquals(0, service.waiterCountForTest(bucket));
    }

    public void testUnknownUnusedGrantReleaseDoesNotDeregisterCurrentOwnerWaiter() {
        final String bucket = "unknown-release:group";
        final int sharedLimit = 1;
        WorkloadGroupSharedThrottleService service = newService();
        stubGroupFor(bucket, sharedLimit);
        assertTrue(
            service.tracker()
                .tryAcquire(
                    bucket,
                    sharedLimit,
                    "current-owner-permit",
                    WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS,
                    SharedThrottleTracker.UNKNOWN_COORDINATOR
                )
        );
        denyAndRegister(service, bucket, sharedLimit);
        assertEquals(1, service.waiterCountForTest(bucket));

        service.handleRelease(
            new WorkloadGroupSharedThrottleService.ReleasePermitRequest(bucket, "permit-from-former-owner", localNode.getId())
        );

        assertEquals("an unknown old-owner permit id must not remove a valid new-owner waiter", 1, service.waiterCountForTest(bucket));
        assertEquals("the current owner's real permit remains live", 1, service.tracker().inFlight(bucket));
        assertTrue(service.tracker().release(bucket, "current-owner-permit"));
    }

    public void testReRegisterWaiterAfterOwnerChangeImmediatelyDrivesFreeCapacity() {
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
                return true;
            }
            return false;
        });

        WorkloadGroupSharedThrottleService.ReRegisterWaiterAfterOwnerChangeResponse response = service
            .handleReRegisterWaiterAfterOwnerChange(
                new WorkloadGroupSharedThrottleService.ReRegisterWaiterAfterOwnerChangeRequest(bucket, localNode.getId())
            );

        assertTrue(response.registered);
        assertEquals("registration must drive already-free capacity without waiting for a future release", 1, admits.get());
        assertEquals(1, service.waiterCountForTest(bucket));
        assertEquals(1, service.tracker().inFlight(bucket));

        admittedPermit.get().close();
        assertEquals(
            "the next unused grant reconciles the now-empty coordinator out of the waiter set",
            0,
            service.waiterCountForTest(bucket)
        );
        assertEquals(0, service.tracker().inFlight(bucket));
    }

    public void testReRegisterWaiterIsRefusedWithoutAPositiveSharedLimit() {
        final String bucket = "re-register-unset:group";
        WorkloadGroupSharedThrottleService service = newService(); // no group stubbed: the shared limit reads as unset
        WorkloadGroupSharedThrottleService.ReRegisterWaiterAfterOwnerChangeResponse response = service
            .handleReRegisterWaiterAfterOwnerChange(
                new WorkloadGroupSharedThrottleService.ReRegisterWaiterAfterOwnerChangeRequest(bucket, localNode.getId())
            );
        assertFalse(response.registered);
        assertEquals(0, service.waiterCountForTest(bucket));
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
                return false; // nothing left to admit -> caller returns the unused grant and deregisters this waiter
            }
            parked.decrementAndGet();
            admits.incrementAndGet();
            heldPermits.add(reservedPermit);
            return true;
        });

        Releasable slotHolder = awaitGrant(service, bucket, sharedLimit);
        assertNotNull("first acquire takes the only shared slot", slotHolder);
        for (int i = 0; i < 3; i++) {
            denyAndRegister(service, bucket, sharedLimit);
        }
        assertEquals("one Set membership for the coordinator regardless of parked count", 1, service.waiterCountForTest(bucket));

        slotHolder.close();
        assertEquals("first freed slot drains exactly one parked request", 1, admits.get());
        assertEquals("coordinator must remain registered while it still has parked requests", 1, service.waiterCountForTest(bucket));

        heldPermits.remove(0).close();
        assertEquals("second freed slot drains the second parked request", 2, admits.get());
        assertEquals(1, service.waiterCountForTest(bucket));

        heldPermits.remove(0).close();
        assertEquals("third freed slot drains the third (last) parked request", 3, admits.get());

        // The last request completes with nothing left queued: the next grant comes back unused, so the coordinator
        // self-deregisters and the freed slot returns to the pool. No stranding, no leaked permit.
        heldPermits.remove(0).close();
        assertEquals("no further admits once the queue is empty", 3, admits.get());
        assertEquals("coordinator self-reconciles out of the registry when it has nothing queued", 0, service.waiterCountForTest(bucket));
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

    public void testEmptyRingIsUnavailable() {
        // Cluster with no eligible data node (coordinating/manager-only) => empty ring => unavailable (caller rejects).
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
        service.acquireAsync("b", 1, false, ActionListener.wrap(p -> fail("must not admit: " + p), failure::set));
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
        service.acquireAsync("b", 1, false, ActionListener.wrap(p -> fail("must not admit: " + p), failure::set));

        // An inline result proves the nodeConnected() pre-check fired: the mock cannot intercept the final sendRequest,
        // so sending would throw rather than complete the listener.
        assertNotNull("listener must be invoked inline (no RTT to a dead node)", failure.get());
        assertTrue("disconnected owner -> shared tier unavailable", WorkloadGroupSharedThrottleService.isUnavailable(failure.get()));
    }

    public void testAcquirePermitRequestSerializationRoundTrip() throws Exception {
        WorkloadGroupSharedThrottleService.AcquirePermitRequest original = new WorkloadGroupSharedThrottleService.AcquirePermitRequest(
            "grp1:username:alice",
            42,
            "permit-xyz",
            123_456_789L,
            "coordinator-ephemeral-id"
        );
        WorkloadGroupSharedThrottleService.AcquirePermitRequest copy = copyWriteable(
            original,
            writableRegistry(),
            WorkloadGroupSharedThrottleService.AcquirePermitRequest::new
        );
        assertEquals(original.bucketKey, copy.bucketKey);
        assertEquals(original.sharedLimit, copy.sharedLimit);
        assertEquals(original.permitId, copy.permitId);
        assertEquals(original.ttlNanos, copy.ttlNanos);
        assertEquals(original.coordinatorId, copy.coordinatorId);
    }

    public void testAcquirePermitResponseSerializationRoundTrip() throws Exception {
        for (WorkloadGroupSharedThrottleService.AcquirePermitResponse original : List.of(
            WorkloadGroupSharedThrottleService.AcquirePermitResponse.GRANTED,
            WorkloadGroupSharedThrottleService.AcquirePermitResponse.DENIED,
            WorkloadGroupSharedThrottleService.AcquirePermitResponse.NOT_OWNER
        )) {
            WorkloadGroupSharedThrottleService.AcquirePermitResponse copy = copyWriteable(
                original,
                writableRegistry(),
                WorkloadGroupSharedThrottleService.AcquirePermitResponse::new
            );
            assertEquals(original.granted, copy.granted);
            assertEquals(original.notOwner, copy.notOwner);
        }
    }

    public void testAcquirePermitResponseWithoutNotOwnerReadsAsDenial() throws Exception {
        // A same-version peer that predates the former-owner fence sends only "granted"; it must decode, as a denial.
        Map<String, Object> body = new HashMap<>();
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitResponse.KEY_GRANTED, false);
        WorkloadGroupSharedThrottleService.AcquirePermitResponse parsed = new WorkloadGroupSharedThrottleService.AcquirePermitResponse(
            bodyBytes(body, false)
        );
        assertFalse(parsed.granted);
        assertFalse(parsed.notOwner);
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

    public void testOwnershipLostAfterTrackerAcquireReturnsThePermit() {
        DiscoveryNode newOwner = dataNode("other");
        DiscoveryNodes moved = DiscoveryNodes.builder().add(localNode).add(newOwner).localNodeId("local").build();

        // Owner side (handleAcquire).
        WorkloadGroupSharedThrottleService owner = newService();
        String key = bucketOwnedBy(serviceWithRing(moved), newOwner); // owned locally now, by newOwner after the move
        owner.afterTrackerAcquire = () -> deliverNodesChanged(owner, moved);
        WorkloadGroupSharedThrottleService.AcquirePermitResponse response = owner.handleAcquire(acquireRequest(key, "racy"));
        assertTrue(response.notOwner);
        assertEquals("the permit acquired before the swap must be returned", 0, owner.tracker().inFlight(key));

        // Coordinator-side local short-circuit (acquireAsync).
        WorkloadGroupSharedThrottleService local = newService();
        local.afterTrackerAcquire = () -> deliverNodesChanged(local, moved);
        AtomicReference<Exception> failure = new AtomicReference<>();
        local.acquireAsync(key, 5, false, ActionListener.wrap(p -> fail("must not admit: " + p), failure::set));
        assertTrue(
            "lost ownership mid-acquire must report the shared tier unavailable",
            WorkloadGroupSharedThrottleService.isUnavailable(failure.get())
        );
        assertEquals(0, local.tracker().inFlight(key));
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
            coordinatorId
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
        assertTrue(service.handleAcquire(acquireRequest("b", "from-unknown", SharedThrottleTracker.UNKNOWN_COORDINATOR)).granted);
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
            service.tracker()
                .tryAcquire("b", 5, "new", WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS, secondIncarnation.getEphemeralId())
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
            return true;
        });

        assertTrue(
            service.handleAcquire(
                new WorkloadGroupSharedThrottleService.AcquirePermitRequest(
                    bucket,
                    1,
                    "held-by-leaving",
                    WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS,
                    leaving.getEphemeralId()
                )
            ).granted
        );
        WorkloadGroupSharedThrottleService.AcquirePermitResponse leavingWaits = service.handleAcquire(
            new WorkloadGroupSharedThrottleService.AcquirePermitRequest(
                bucket,
                1,
                "leaving-denied",
                WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS,
                leaving.getEphemeralId(),
                leaving.getId(),
                true
            )
        );
        assertTrue("precondition: the leaving coordinator is a registered waiter", leavingWaits.registered);
        denyAndRegister(service, bucket, 1);
        assertEquals(2, service.waiterCountForTest(bucket));

        deliverNodesChanged(service, before, after);
        assertEquals("the departed coordinator's waiter registration is dropped", 1, service.waiterCountForTest(bucket));
        assertEquals("the purged permit's slot is pushed to the remaining waiter", 1, admits.get());
        assertEquals("the pushed grant holds the only slot", 1, service.tracker().inFlight(bucket));
    }

    public void testAcquirePermitRequestWithoutCoordinatorReadsAsUnknown() throws Exception {
        // A same-version peer that predates the coordinator field sends only the baseline keys; it must decode, with the
        // coordinator unknown (such a permit is never purged on node removal, only by its TTL).
        Map<String, Object> body = new HashMap<>();
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_BUCKET, "grp1:group");
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_SHARED_LIMIT, 5);
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_PERMIT_ID, "permit-abc");
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_TTL_NANOS, 1_000L);
        StreamInput in = bodyBytes(body, true);
        WorkloadGroupSharedThrottleService.AcquirePermitRequest parsed = new WorkloadGroupSharedThrottleService.AcquirePermitRequest(in);
        assertEquals("grp1:group", parsed.bucketKey);
        assertEquals("permit-abc", parsed.permitId);
        assertEquals(SharedThrottleTracker.UNKNOWN_COORDINATOR, parsed.coordinatorId);
        assertEquals(0, in.available());
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

    public void testQueueingAcquireRequestSerializationRoundTrip() throws Exception {
        WorkloadGroupSharedThrottleService.AcquirePermitRequest original = new WorkloadGroupSharedThrottleService.AcquirePermitRequest(
            "grp1:group",
            3,
            "permit-q",
            99L,
            "node-7-ephemeral",
            "node-7",
            true
        );
        WorkloadGroupSharedThrottleService.AcquirePermitRequest copy = copyWriteable(
            original,
            writableRegistry(),
            WorkloadGroupSharedThrottleService.AcquirePermitRequest::new
        );
        assertEquals("node-7-ephemeral", copy.coordinatorId);
        assertEquals("node-7", copy.requestingNodeId);
        assertTrue(copy.wantsQueue);
        assertEquals(original.permitId, copy.permitId);
    }

    public void testAcquirePermitRequestWithoutQueueFieldsReadsAsNotQueueing() throws Exception {
        // A same-version peer that predates queueing sends only the baseline keys; it must decode as a plain acquire.
        Map<String, Object> body = new HashMap<>();
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_BUCKET, "grp1:group");
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_SHARED_LIMIT, 5);
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_PERMIT_ID, "permit-abc");
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_TTL_NANOS, 1_000L);
        WorkloadGroupSharedThrottleService.AcquirePermitRequest parsed = new WorkloadGroupSharedThrottleService.AcquirePermitRequest(
            bodyBytes(body, true)
        );
        assertFalse(parsed.wantsQueue);
        assertEquals("", parsed.requestingNodeId);
    }

    public void testAcquirePermitResponseRegisteredRoundTripsAndDefaultsToFalse() throws Exception {
        WorkloadGroupSharedThrottleService.AcquirePermitResponse copy = copyWriteable(
            WorkloadGroupSharedThrottleService.AcquirePermitResponse.DENIED_AND_REGISTERED,
            writableRegistry(),
            WorkloadGroupSharedThrottleService.AcquirePermitResponse::new
        );
        assertFalse(copy.granted);
        assertFalse(copy.notOwner);
        assertTrue(copy.registered);

        // A pre-queueing owner never sends the key: the coordinator must read "not registered" and reject, not wait.
        Map<String, Object> body = new HashMap<>();
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitResponse.KEY_GRANTED, false);
        assertFalse(new WorkloadGroupSharedThrottleService.AcquirePermitResponse(bodyBytes(body, false)).registered);
    }

    public void testReleasePermitRequestQueueEmptyOnNodeIdRoundTripsAndDefaultsToEmpty() throws Exception {
        WorkloadGroupSharedThrottleService.ReleasePermitRequest copy = copyWriteable(
            new WorkloadGroupSharedThrottleService.ReleasePermitRequest("grp1:group", "permit-abc", "node-1"),
            writableRegistry(),
            WorkloadGroupSharedThrottleService.ReleasePermitRequest::new
        );
        assertEquals("node-1", copy.queueEmptyOnNodeId);

        Map<String, Object> body = new HashMap<>();
        body.put(WorkloadGroupSharedThrottleService.ReleasePermitRequest.KEY_BUCKET, "grp1:group");
        body.put(WorkloadGroupSharedThrottleService.ReleasePermitRequest.KEY_PERMIT_ID, "permit-abc");
        assertEquals("", new WorkloadGroupSharedThrottleService.ReleasePermitRequest(bodyBytes(body, true)).queueEmptyOnNodeId);
    }

    public void testGrantPermitRequestSerializationRoundTrip() throws Exception {
        WorkloadGroupSharedThrottleService.GrantPermitRequest copy = copyWriteable(
            new WorkloadGroupSharedThrottleService.GrantPermitRequest("grp1:group", 4, "permit-g"),
            writableRegistry(),
            WorkloadGroupSharedThrottleService.GrantPermitRequest::new
        );
        assertEquals("grp1:group", copy.bucketKey);
        assertEquals(4, copy.sharedLimit);
        assertEquals("permit-g", copy.permitId);
    }

    public void testReRegisterWaiterAfterOwnerChangeRequestSerializationRoundTrip() throws Exception {
        WorkloadGroupSharedThrottleService.ReRegisterWaiterAfterOwnerChangeRequest original =
            new WorkloadGroupSharedThrottleService.ReRegisterWaiterAfterOwnerChangeRequest("grp1:group", "node-1");
        WorkloadGroupSharedThrottleService.ReRegisterWaiterAfterOwnerChangeRequest copy = copyWriteable(
            original,
            writableRegistry(),
            WorkloadGroupSharedThrottleService.ReRegisterWaiterAfterOwnerChangeRequest::new
        );
        assertEquals(original.bucketKey, copy.bucketKey);
        assertEquals(original.requestingNodeId, copy.requestingNodeId);
    }

    public void testReRegisterWaiterAfterOwnerChangeResponseSerializationRoundTrip() throws Exception {
        for (boolean registered : new boolean[] { true, false }) {
            WorkloadGroupSharedThrottleService.ReRegisterWaiterAfterOwnerChangeResponse original =
                new WorkloadGroupSharedThrottleService.ReRegisterWaiterAfterOwnerChangeResponse(registered);
            WorkloadGroupSharedThrottleService.ReRegisterWaiterAfterOwnerChangeResponse copy = copyWriteable(
                original,
                writableRegistry(),
                WorkloadGroupSharedThrottleService.ReRegisterWaiterAfterOwnerChangeResponse::new
            );
            assertEquals(registered, copy.registered);
        }
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

    public void testAcquirePermitRequestIgnoresFieldsAddedByANewerPeer() throws Exception {
        Map<String, Object> body = new HashMap<>();
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_BUCKET, "grp1:group");
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_SHARED_LIMIT, 5);
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_PERMIT_ID, "permit-abc");
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_TTL_NANOS, 1_000L);
        body.put("requesting_node", "eph-1");
        body.put("future_flag", true);
        body.put("future_list", List.of("a", "b"));
        body.put("future_null", null);

        StreamInput in = bodyBytes(body, true);
        WorkloadGroupSharedThrottleService.AcquirePermitRequest parsed = new WorkloadGroupSharedThrottleService.AcquirePermitRequest(in);
        assertEquals("grp1:group", parsed.bucketKey);
        assertEquals(5, parsed.sharedLimit);
        assertEquals("permit-abc", parsed.permitId);
        assertEquals(1_000L, parsed.ttlNanos);
        assertEquals("unknown keys must be consumed, not left as trailing bytes", 0, in.available());
    }

    public void testAcquirePermitRequestIsStrict() throws Exception {
        // A missing baseline field fails decoding (reported unavailable) rather than being guessed.
        Map<String, Object> full = new HashMap<>();
        full.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_BUCKET, "grp1:group");
        full.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_SHARED_LIMIT, 5);
        full.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_PERMIT_ID, "permit-abc");
        full.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_TTL_NANOS, 1_000L);
        for (String omitted : full.keySet()) {
            Map<String, Object> partial = new HashMap<>(full);
            partial.remove(omitted);
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> new WorkloadGroupSharedThrottleService.AcquirePermitRequest(bodyBytes(partial, true))
            );
            assertTrue(e.getMessage(), e.getMessage().contains(omitted));
        }
    }

    public void testAcquirePermitRequestAcceptsAWidenedNumericType() throws Exception {
        // A peer that widens shared_limit to a long, or narrows ttl to an int, must still interoperate.
        Map<String, Object> body = new HashMap<>();
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_BUCKET, "grp1:group");
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_SHARED_LIMIT, 5L);
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_PERMIT_ID, "permit-abc");
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_TTL_NANOS, 1_000);
        WorkloadGroupSharedThrottleService.AcquirePermitRequest parsed = new WorkloadGroupSharedThrottleService.AcquirePermitRequest(
            bodyBytes(body, true)
        );
        assertEquals(5, parsed.sharedLimit);
        assertEquals(1_000L, parsed.ttlNanos);
    }

    public void testAcquirePermitResponseIgnoresFieldsAddedByANewerPeer() throws Exception {
        Map<String, Object> body = new HashMap<>();
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitResponse.KEY_GRANTED, true);
        body.put("future_reason", "at_limit");
        body.put("future_retry_after_millis", 250L);
        StreamInput in = bodyBytes(body, false); // TransportResponse has no TaskId preamble
        WorkloadGroupSharedThrottleService.AcquirePermitResponse parsed = new WorkloadGroupSharedThrottleService.AcquirePermitResponse(in);
        assertTrue(parsed.granted);
        assertEquals(0, in.available());
    }

    public void testAcquirePermitResponseIsStrict() throws Exception {
        expectThrows(
            IllegalStateException.class,
            () -> new WorkloadGroupSharedThrottleService.AcquirePermitResponse(bodyBytes(new HashMap<>(), false))
        );
    }

    public void testReleasePermitRequestIgnoresFieldsAddedByANewerPeer() throws Exception {
        Map<String, Object> body = new HashMap<>();
        body.put(WorkloadGroupSharedThrottleService.ReleasePermitRequest.KEY_BUCKET, "grp1:group");
        body.put(WorkloadGroupSharedThrottleService.ReleasePermitRequest.KEY_PERMIT_ID, "permit-abc");
        body.put("shared_limit", 5); // fields a newer peer might add
        body.put("queue_empty_on_node", "eph-1");
        StreamInput in = bodyBytes(body, true);
        WorkloadGroupSharedThrottleService.ReleasePermitRequest parsed = new WorkloadGroupSharedThrottleService.ReleasePermitRequest(in);
        assertEquals("grp1:group", parsed.bucketKey);
        assertEquals("permit-abc", parsed.permitId);
        assertEquals(0, in.available());
    }

    public void testReleasePermitRequestIsStrictLikeTheOthers() throws Exception {
        Map<String, Object> full = new HashMap<>();
        full.put(WorkloadGroupSharedThrottleService.ReleasePermitRequest.KEY_BUCKET, "grp1:group");
        full.put(WorkloadGroupSharedThrottleService.ReleasePermitRequest.KEY_PERMIT_ID, "permit-abc");
        for (String omitted : full.keySet()) {
            Map<String, Object> partial = new HashMap<>(full);
            partial.remove(omitted);
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> new WorkloadGroupSharedThrottleService.ReleasePermitRequest(bodyBytes(partial, true))
            );
            assertTrue(e.getMessage(), e.getMessage().contains(omitted));
        }
    }

    /**
     * LOCAL-OWNER path. Registration there does not go through the cluster-state lookup at all (the waiter IS this node),
     * so it can never report "not registered". Verified even with the local node ABSENT from its own applied state, which
     * is the condition that defeats the remote path: the ring still routes the bucket here, the local short-circuit
     * registers {@code clusterService.localNode()} directly, and the no-waiter callback must stay unused.
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
        assertEquals("the local waiter must still be registered", 1, service.waiterCountForTest(bucket));
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
        service.acquireAsync(bucket, sharedLimit, false, ActionListener.wrap(p -> fail("must be denied"), plain::set));
        assertTrue(WorkloadGroupSharedThrottleService.isDenial(plain.get()));
        assertEquals("wantsQueue=false must not register a waiter", 0, service.waiterCountForTest(bucket));

        // Queueing overload: the callback is supplied but must remain unused.
        denyAndRegister(service, bucket, sharedLimit);
        assertEquals("now a local waiter is registered", 1, service.waiterCountForTest(bucket));
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
        service.acquireAsync(bucket, 1, false, ActionListener.wrap(p -> fail("must not admit"), failure::set), () -> fail("not a denial"));
        assertTrue(WorkloadGroupSharedThrottleService.isUnavailable(failure.get()));
        assertEquals(0, service.waiterCountForTest(bucket));
    }

    public void testReleaseBetweenLocalDenialAndRegistrationStillWakesTheWaiter() {
        // The release lands after the failed tryAcquire but before registerWaiter: it sees no waiter and offers its slot
        // to nobody. Without the post-registration re-check the request would wait for an unrelated later release.
        final String bucket = "deny-register-race:group";
        WorkloadGroupSharedThrottleService service = newService();
        stubGroupFor(bucket, 1);
        AtomicInteger admits = new AtomicInteger();
        service.setGrantConsumer((bucketKey, permit) -> {
            admits.incrementAndGet();
            return true;
        });
        Releasable holder = awaitGrant(service, bucket, 1);
        service.afterTrackerDenial = holder::close;

        denyAndRegister(service, bucket, 1);

        assertEquals("the slot freed inside the window must be offered to the new waiter", 1, admits.get());
        assertEquals(1, service.tracker().inFlight(bucket));
    }

    public void testReconcileRetainedWaitersWithAnEmptyRingRejectsThem() {
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
        List<String> unavailable = new ArrayList<>();
        service.setSharedTierUnavailableHandler(unavailable::add);

        service.reconcileRetainedWaiters("g1:group");

        assertEquals(List.of("g1:group"), unavailable);
    }

    public void testReconcileRetainedWaitersWithADisconnectedOwnerRejectsThemWithoutSending() {
        DiscoveryNode remote = new DiscoveryNode(
            "remote",
            "remote",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
        ClusterState state = Mockito.mock(ClusterState.class);
        when(state.nodes()).thenReturn(DiscoveryNodes.builder().add(remote).localNodeId("local").build());
        when(clusterService.state()).thenReturn(state);
        when(transportService.nodeConnected(remote)).thenReturn(false);
        WorkloadGroupSharedThrottleService service = newService();
        List<String> unavailable = new ArrayList<>();
        service.setSharedTierUnavailableHandler(unavailable::add);

        service.reconcileRetainedWaiters("g1:group");

        // Inline: the mocked transport never calls back, so only the nodeConnected() pre-check can have reported this.
        assertEquals(List.of("g1:group"), unavailable);
    }

    public void testReconcileRetainedWaitersWithTheLocalOwner() {
        final String bucket = "g1:group";
        WorkloadGroupSharedThrottleService service = newService();
        List<String> unavailable = new ArrayList<>();
        service.setSharedTierUnavailableHandler(unavailable::add);

        // No shared tier in this node's view: the owner refuses the registration, so the waiters cannot be woken.
        service.reconcileRetainedWaiters(bucket);
        assertEquals(List.of(bucket), unavailable);

        unavailable.clear();
        stubGroupFor(bucket, 1);
        Releasable holder = awaitGrant(service, bucket, 1);
        service.reconcileRetainedWaiters(bucket);
        assertTrue("a confirmed registration rejects nothing", unavailable.isEmpty());
        assertEquals("the backstop restores a dropped waiter membership", 1, service.waiterCountForTest(bucket));
        holder.close();
    }

    public void testOwnerRingEmptyingRejectsRetainedWaiters() {
        final String bucket = "g1:group";
        stubGroupFor(bucket, 1);
        when(threadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(OpenSearchExecutors.newDirectExecutorService());
        WorkloadGroupSharedThrottleService service = newService();
        service.setRetainedBucketKeysSupplier(() -> Set.of(bucket));
        List<String> unavailable = new ArrayList<>();
        service.setSharedTierUnavailableHandler(unavailable::add);

        ClusterState previous = clusterService.state();
        ClusterState current = Mockito.mock(ClusterState.class);
        when(current.nodes()).thenReturn(DiscoveryNodes.EMPTY_NODES);
        service.clusterChanged(new ClusterChangedEvent("every owner left", current, previous));

        assertEquals("with no owner left no grant can come, so the waiting requests are rejected", List.of(bucket), unavailable);
    }
}
