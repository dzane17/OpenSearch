/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

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
            org.opensearch.Version.CURRENT
        );
        clusterService = Mockito.mock(ClusterService.class);
        threadPool = Mockito.mock(ThreadPool.class);
        transportService = Mockito.mock(TransportService.class);

        ClusterState state = Mockito.mock(ClusterState.class);
        DiscoveryNodes singleDataNode = DiscoveryNodes.builder().add(localNode).localNodeId("local").build();
        when(state.nodes()).thenReturn(singleDataNode);
        // The TTL sweep resolves a bucket's live shared_limit from cluster-state metadata, so every test needs a
        // metadata stub. Default to "no workload groups"; tests that need a real limit re-stub workloadGroups().
        metadata = Mockito.mock(Metadata.class);
        when(metadata.workloadGroups()).thenReturn(Map.of());
        when(state.metadata()).thenReturn(metadata);
        when(clusterService.state()).thenReturn(state);
        when(clusterService.localNode()).thenReturn(localNode);
    }

    // The owner resolves a bucket's shared_limit from its OWN cluster state (it no longer arrives on the RELEASE RPC), so
    // any test that expects owner-push to be driven has to model the group. Without this the limit reads as UNSET and
    // offerSharedSlots correctly declines to grant.
    private void stubGroupFor(String bucketKey, int sharedLimit) {
        final int idx = bucketKey.indexOf(':');
        final String groupId = idx < 0 ? bucketKey : bucketKey.substring(0, idx);
        when(metadata.workloadGroups()).thenReturn(Map.of(groupId, workloadGroup(groupId, sharedLimit)));
    }

    private WorkloadGroup workloadGroup(String groupId, int sharedLimit) {
        Settings throttling = Settings.builder().put("attribute", "group").put("shared_limit", sharedLimit).build();
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
        service.acquireAsync(bucket, limit, ActionListener.wrap(permit::set, failure::set));
        if (failure.get() != null) {
            throw failure.get() instanceof RuntimeException re ? re : new RuntimeException(failure.get());
        }
        return permit.get();
    }

    public void testTtlSweepDrivesOwnerPushForALeaseExpiredSlot() {
        // Regression: a permit reclaimed by the TTL sweep frees a shared slot with NO release RPC behind it (the holder
        // crashed, or its release was lost), so the sweep is the ONLY observer of that free slot. It must drive
        // owner-push. Previously sweepExpired() returned void and the sweep ignored the freed capacity, so a coordinator
        // with a parked request stayed registered as a waiter while the slot sat idle — and since parked requests have no
        // deadline, it stranded until some unrelated release happened to re-drive the bucket.
        final int sharedLimit = 1;
        final String groupId = "g1";
        final String bucket = groupId + ":group";

        // Controllable clock so the permit can be expired deterministically instead of sleeping past the 5-minute TTL.
        final AtomicLong nanos = new AtomicLong(0L);
        WorkloadGroupSharedThrottleService service = new WorkloadGroupSharedThrottleService(
            clusterService,
            threadPool,
            transportService,
            new SharedThrottleTracker(nanos::get)
        );
        deliverNodesChanged(service, clusterService.state().nodes());

        // The sweep resolves shared_limit from cluster state (an expiry carries no limit, unlike a release).
        Settings throttling = Settings.builder().put("attribute", "group").put("shared_limit", sharedLimit).build();
        WorkloadGroup group = new WorkloadGroup(
            "g1-name",
            groupId,
            new MutableWorkloadGroupFragment(
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.EMPTY,
                throttling
            ),
            1L
        );
        when(metadata.workloadGroups()).thenReturn(Map.of(groupId, group));

        final AtomicInteger admits = new AtomicInteger(0);
        service.setGrantConsumer((bucketKey, reservedPermit) -> {
            admits.incrementAndGet();
            return true; // stands in for the queue service admitting one parked request
        });

        // Take the only shared slot, then deny an acquire with wantsQueue=true so this coordinator is a registered waiter
        // with a parked request. Both happen at t=0, while the holder's permit is still live.
        assertNotNull("first acquire takes the only shared slot", awaitGrant(service, bucket, sharedLimit));
        AtomicReference<Exception> denial = new AtomicReference<>();
        service.acquireAsync(
            bucket,
            sharedLimit,
            ActionListener.wrap(p -> fail("must be denied while at limit"), denial::set),
            () -> fail("local owner must always be able to register itself as a waiter")
        );
        assertTrue("acquire at limit must be denied", denial.get() instanceof OpenSearchRejectedExecutionException);
        assertEquals("coordinator is registered as a waiter", 1, service.waiterCountForTest(bucket));

        // Simulate the holder vanishing: advance past the permit TTL WITHOUT any release RPC. Nothing has pruned the
        // permit yet (no acquire has touched the bucket), so the slot is expired-but-unreclaimed.
        nanos.set(WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS + 1);
        assertEquals("nothing can have been admitted before the sweep runs", 0, admits.get());

        // One sweep pass must reclaim the expired permit AND hand the freed slot to the waiting coordinator.
        service.sweepExpiredAndDrive();
        assertEquals("the sweep must drive owner-push for the slot it freed", 1, admits.get());
    }

    public void testTtlSweepOffersEveryPermitReclaimedInOneBucket() {
        final int sharedLimit = 3;
        final String groupId = "multi-expiry";
        final String bucket = groupId + ":group";
        final AtomicLong nanos = new AtomicLong(0L);
        WorkloadGroupSharedThrottleService service = new WorkloadGroupSharedThrottleService(
            clusterService,
            threadPool,
            transportService,
            new SharedThrottleTracker(nanos::get)
        );
        deliverNodesChanged(service, clusterService.state().nodes());
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
            AtomicReference<Exception> denial = new AtomicReference<>();
            service.acquireAsync(
                bucket,
                sharedLimit,
                ActionListener.wrap(p -> fail("the bucket is full"), denial::set),
                () -> fail("the local owner must register itself")
            );
            assertTrue(denial.get() instanceof OpenSearchRejectedExecutionException);
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
            AtomicReference<Exception> denial = new AtomicReference<>();
            service.acquireAsync(
                bucket,
                previousLimit,
                ActionListener.wrap(p -> fail("the old shared limit is full"), denial::set),
                () -> fail("the local owner must register itself")
            );
            assertTrue(denial.get() instanceof OpenSearchRejectedExecutionException);
        }

        final DiscoveryNodes nodes = clusterService.state().nodes();
        final Metadata previousMetadata = Mockito.mock(Metadata.class);
        when(previousMetadata.workloadGroups()).thenReturn(Map.of(groupId, workloadGroup(groupId, previousLimit)));
        final Metadata currentMetadata = Mockito.mock(Metadata.class);
        when(currentMetadata.workloadGroups()).thenReturn(Map.of(groupId, workloadGroup(groupId, currentLimit)));
        final ClusterState previousState = Mockito.mock(ClusterState.class);
        when(previousState.nodes()).thenReturn(nodes);
        when(previousState.metadata()).thenReturn(previousMetadata);
        final ClusterState currentState = Mockito.mock(ClusterState.class);
        when(currentState.nodes()).thenReturn(nodes);
        when(currentState.metadata()).thenReturn(currentMetadata);
        when(clusterService.state()).thenReturn(currentState);

        service.clusterChanged(new ClusterChangedEvent("shared limit increased", currentState, previousState));

        assertEquals("the hook must grant exactly newLimit - oldLimit requests", currentLimit - previousLimit, admits.get());
        assertEquals("the owner must stop at the new live limit", currentLimit, service.tracker().inFlight(bucket));
        assertEquals("requests beyond the one-time added-capacity budget remain queued", 2, parked.get());

        originalHolder.close();
        while (grantedPermits.isEmpty() == false) {
            grantedPermits.remove(0).close();
        }
        assertEquals(0, service.tracker().inFlight(bucket));
    }

    public void testOrdinaryReleaseOffersOneSlotEvenWhenLiveLimitIsHigher() {
        final String groupId = "one-slot-release";
        final String bucket = groupId + ":group";
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
            AtomicReference<Exception> denial = new AtomicReference<>();
            service.acquireAsync(
                bucket,
                1,
                ActionListener.wrap(p -> fail("the old shared limit is full"), denial::set),
                () -> fail("the local owner must register itself")
            );
            assertTrue(denial.get() instanceof OpenSearchRejectedExecutionException);
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
        // Regression for the single-node no-op: the coordinator seeds the initial applied state already containing the
        // local node, so the first real clusterChanged has previous.nodes() == current.nodes() and nodesChanged() is
        // false. The ring must still populate (we compare eligible owner sets, not the delta), otherwise shared
        // throttling silently does nothing on a single-node cluster.
        WorkloadGroupSharedThrottleService service = new WorkloadGroupSharedThrottleService(clusterService, threadPool, transportService);
        DiscoveryNodes nodes = DiscoveryNodes.builder().add(localNode).localNodeId("local").build();
        ClusterState previous = Mockito.mock(ClusterState.class);
        when(previous.nodes()).thenReturn(nodes); // same node set as current -> nodesChanged() == false
        ClusterState current = Mockito.mock(ClusterState.class);
        when(current.nodes()).thenReturn(nodes);
        ClusterChangedEvent event = new ClusterChangedEvent("test", current, previous);
        assertFalse("precondition: this event reports no node change", event.nodesChanged());

        service.clusterChanged(event);

        // shared_limit=1 must now actually enforce (grant then reject), not fail open.
        assertNotNull(awaitGrant(service, "b", 1));
        expectThrows(OpenSearchRejectedExecutionException.class, () -> awaitGrant(service, "b", 1));
    }

    public void testSameIdRestartRebuildsRingWithFreshNode() {
        // A node can restart keeping its persistent id but with a new ephemeral id/address. Comparing only persistent
        // ids would treat this as "no change" and keep the stale DiscoveryNode in the ring, so nodeConnected(stale)
        // would fail open forever. The ring must rebuild and hold the FRESH node object.
        WorkloadGroupSharedThrottleService service = new WorkloadGroupSharedThrottleService(clusterService, threadPool, transportService);

        DiscoveryNode first = new DiscoveryNode(
            "n1",
            "n1",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            org.opensearch.Version.CURRENT
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
            org.opensearch.Version.CURRENT
        );
        assertNotEquals("restarted node must not equal the old instance", first, restarted);
        deliverNodesChanged(service, DiscoveryNodes.builder().add(restarted).localNodeId("n1").build());

        assertSame("ring must have rebuilt with the restarted node instance", restarted, service.ring().ownerFor("b").orElseThrow());
    }

    public void testFormerOwnerPrunesWaitersAndCannotIssueOrPushPermits() {
        final int sharedLimit = 1;
        final DiscoveryNode remote = new DiscoveryNode(
            "remote",
            "remote",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            org.opensearch.Version.CURRENT
        );
        final DiscoveryNodes bothNodes = DiscoveryNodes.builder().add(localNode).add(remote).localNodeId(localNode.getId()).build();
        final ThrottleOwnerSelector twoNodeRing = ThrottleOwnerSelector.fromDiscoveryNodes(bothNodes);
        String movedBucket = null;
        for (int i = 0; i < 10_000 && movedBucket == null; i++) {
            String candidate = "moved-" + i;
            if (twoNodeRing.ownerFor(candidate).filter(remote::equals).isPresent()) {
                movedBucket = candidate;
            }
        }
        assertNotNull("could not find a bucket that moves from the single local owner to the remote node", movedBucket);
        final String bucket = movedBucket;

        WorkloadGroupSharedThrottleService service = newService();
        stubGroupFor(bucket, sharedLimit);
        AtomicInteger admits = new AtomicInteger();
        service.setGrantConsumer((bucketKey, permit) -> {
            admits.incrementAndGet();
            return true;
        });

        Releasable oldPermit = awaitGrant(service, bucket, sharedLimit);
        AtomicReference<Exception> denial = new AtomicReference<>();
        service.acquireAsync(
            bucket,
            sharedLimit,
            ActionListener.wrap(p -> fail("must be denied while the old permit is live"), denial::set),
            () -> fail("the local owner must register itself")
        );
        assertTrue(denial.get() instanceof OpenSearchRejectedExecutionException);
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
                localNode.getId(),
                true
            )
        );
        assertFalse("a former owner must not mint another permit", staleAcquire.granted);
        assertFalse("a former owner must not register another waiter", staleAcquire.registered);
        assertEquals("only the pre-remap permit may remain", 1, service.tracker().inFlight(bucket));

        oldPermit.close();
        assertEquals("the old permit may drain locally but must not create a pushed grant", 0, service.tracker().inFlight(bucket));
        assertEquals(0, admits.get());
    }

    public void testOwnerChangeDuringPushReservationReclaimsPermitBeforeGrant() {
        final int sharedLimit = 1;
        final DiscoveryNode remote = new DiscoveryNode(
            "remote-race",
            "remote-race",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            org.opensearch.Version.CURRENT
        );
        final DiscoveryNodes bothNodes = DiscoveryNodes.builder().add(localNode).add(remote).localNodeId(localNode.getId()).build();
        final ThrottleOwnerSelector twoNodeRing = ThrottleOwnerSelector.fromDiscoveryNodes(bothNodes);
        String movedBucket = null;
        for (int i = 0; i < 10_000 && movedBucket == null; i++) {
            String candidate = "reservation-race-" + i;
            if (twoNodeRing.ownerFor(candidate).filter(remote::equals).isPresent()) {
                movedBucket = candidate;
            }
        }
        assertNotNull("could not find a bucket that moves to the remote node", movedBucket);
        final String bucket = movedBucket;

        AtomicReference<Runnable> afterSuccessfulAcquire = new AtomicReference<>();
        SharedThrottleTracker raceTracker = new SharedThrottleTracker() {
            @Override
            public boolean tryAcquire(String bucketKey, int limit, String permitId, long ttlNanos) {
                boolean granted = super.tryAcquire(bucketKey, limit, permitId, ttlNanos);
                Runnable callback = afterSuccessfulAcquire.getAndSet(null);
                if (granted && callback != null) {
                    callback.run();
                }
                return granted;
            }
        };
        WorkloadGroupSharedThrottleService service = new WorkloadGroupSharedThrottleService(
            clusterService,
            threadPool,
            transportService,
            raceTracker
        );
        deliverNodesChanged(service, clusterService.state().nodes());
        stubGroupFor(bucket, sharedLimit);
        AtomicInteger admits = new AtomicInteger();
        service.setGrantConsumer((bucketKey, permit) -> {
            admits.incrementAndGet();
            return true;
        });

        Releasable holder = awaitGrant(service, bucket, sharedLimit);
        AtomicReference<Exception> denial = new AtomicReference<>();
        service.acquireAsync(
            bucket,
            sharedLimit,
            ActionListener.wrap(p -> fail("must be denied while the holder is live"), denial::set),
            () -> fail("the local owner must register itself")
        );
        assertTrue(denial.get() instanceof OpenSearchRejectedExecutionException);
        assertEquals(1, service.waiterCountForTest(bucket));

        afterSuccessfulAcquire.set(() -> deliverNodesChanged(service, bothNodes));
        holder.close();

        assertEquals(
            "the callback remapped this bucket during the pushed-grant reservation",
            remote,
            service.ring().ownerFor(bucket).orElseThrow()
        );
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
            service.tracker().tryAcquire(bucket, sharedLimit, "current-owner-permit", WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS)
        );

        AtomicReference<Exception> denial = new AtomicReference<>();
        service.acquireAsync(
            bucket,
            sharedLimit,
            ActionListener.wrap(p -> fail("must be denied while the current permit is live"), denial::set),
            () -> fail("the local owner must register itself")
        );
        assertTrue(denial.get() instanceof OpenSearchRejectedExecutionException);
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
        // Regression: one coordinator (here the local owner) parks SEVERAL requests for a shared bucket, but the owner
        // waiter registry holds one Set membership per coordinator. A grant must NOT deregister the coordinator on a
        // successful admit — it may still have more queued requests — so each successive freed slot drains the next
        // parked request. The bug drained only the first and stranded the rest until queue.timeout despite free capacity.
        final int sharedLimit = 1;
        final String bucket = "b";
        WorkloadGroupSharedThrottleService service = newService();
        stubGroupFor(bucket, sharedLimit);

        // A stubbed coordinator-side consumer standing in for the queue service: it holds `parked` requests and admits
        // one per grant, capturing the reserved permit so the test can "complete" that request by closing it (which
        // re-drives owner-push, exactly like a real request finishing).
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

        // Fill the single shared slot, then issue 3 denied acquires with wantsQueue=true. Each denial registers this
        // (local) coordinator as a waiter — idempotently, so the Set holds exactly ONE membership for 3 parked requests.
        Releasable slotHolder = awaitGrant(service, bucket, sharedLimit);
        assertNotNull("first acquire takes the only shared slot", slotHolder);
        for (int i = 0; i < 3; i++) {
            AtomicReference<Exception> denial = new AtomicReference<>();
            service.acquireAsync(
                bucket,
                sharedLimit,
                ActionListener.wrap(p -> fail("must be denied while at limit"), denial::set),
                () -> fail("local owner must always be able to register itself as a waiter")
            );
            assertTrue("acquire at limit must be denied (429)", denial.get() instanceof OpenSearchRejectedExecutionException);
        }
        assertEquals("one Set membership for the coordinator regardless of parked count", 1, service.waiterCountForTest(bucket));

        // Release the in-flight slot -> owner-push admits the FIRST parked request and the coordinator stays registered.
        slotHolder.close();
        assertEquals("first freed slot drains exactly one parked request", 1, admits.get());
        assertEquals("coordinator must remain registered while it still has parked requests", 1, service.waiterCountForTest(bucket));

        // Each admitted request completing frees the slot again and must drain the NEXT parked request.
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

    public void testEmptyRingFailsOpen() {
        // Cluster with no eligible data node (coordinating/manager-only) => empty ring => fail open (null permit).
        DiscoveryNode managerOnly = new DiscoveryNode(
            "m",
            "m",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            org.opensearch.Version.CURRENT
        );
        ClusterState state = Mockito.mock(ClusterState.class);
        when(state.nodes()).thenReturn(DiscoveryNodes.builder().add(managerOnly).localNodeId("m").build());
        when(clusterService.state()).thenReturn(state);
        when(clusterService.localNode()).thenReturn(managerOnly);

        WorkloadGroupSharedThrottleService service = newService();
        AtomicReference<Releasable> permit = new AtomicReference<>();
        AtomicReference<Boolean> called = new AtomicReference<>(false);
        service.acquireAsync("b", 1, ActionListener.wrap(p -> {
            called.set(true);
            permit.set(p);
        }, e -> fail("must not fail: " + e)));
        assertTrue("listener must be invoked inline", called.get());
        assertNull("fail-open yields a null permit (admit, untracked)", permit.get());
    }

    public void testDisconnectedOwnerFailsOpenWithoutSendingRequest() {
        // Ring owner is a remote data node (not the local node), so this is NOT the local short-circuit path.
        DiscoveryNode remote = new DiscoveryNode(
            "remote",
            "remote",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            org.opensearch.Version.CURRENT
        );
        ClusterState state = Mockito.mock(ClusterState.class);
        // Only the remote node is a data node, so every bucket hashes to it; local is a coordinating-only node.
        when(state.nodes()).thenReturn(DiscoveryNodes.builder().add(remote).localNodeId("local").build());
        when(clusterService.state()).thenReturn(state);
        when(clusterService.localNode()).thenReturn(localNode);
        // The owner is known-disconnected.
        when(transportService.nodeConnected(remote)).thenReturn(false);

        WorkloadGroupSharedThrottleService service = newService();
        AtomicReference<Releasable> permit = new AtomicReference<>();
        AtomicReference<Boolean> called = new AtomicReference<>(false);
        service.acquireAsync("b", 1, ActionListener.wrap(p -> {
            called.set(true);
            permit.set(p);
        }, e -> fail("must not fail: " + e)));

        // The listener completed inline with a null permit. This proves the disconnected pre-check fired: on the
        // transport path the listener is only invoked from a sendRequest callback, but transportService is a plain
        // mock whose sendRequest is a no-op that never calls back — so an inline null response can only come from the
        // nodeConnected()==false fast-fail branch, not from a request sent into the dead socket.
        assertTrue("listener must be invoked inline (no RTT to a dead node)", called.get());
        assertNull("disconnected owner -> fail open with a null permit", permit.get());
    }

    public void testHandleAcquireIsOwnerSideAdmission() {
        WorkloadGroupSharedThrottleService service = newService();
        // Directly exercise the owner-side handler (what a remote coordinator's RPC would hit).
        assertTrue(
            service.handleAcquire(
                new WorkloadGroupSharedThrottleService.AcquirePermitRequest(
                    "b",
                    1,
                    "permit-1",
                    WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS
                )
            ).granted
        );
        assertFalse(
            service.handleAcquire(
                new WorkloadGroupSharedThrottleService.AcquirePermitRequest(
                    "b",
                    1,
                    "permit-2",
                    WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS
                )
            ).granted
        );
        assertEquals(1, service.tracker().inFlight("b"));
    }

    public void testAcquirePermitRequestSerializationRoundTrip() throws Exception {
        WorkloadGroupSharedThrottleService.AcquirePermitRequest original = new WorkloadGroupSharedThrottleService.AcquirePermitRequest(
            "grp1:username:alice",
            42,
            "permit-xyz",
            123_456_789L
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
    }

    public void testAcquirePermitResponseSerializationRoundTrip() throws Exception {
        for (boolean granted : new boolean[] { true, false }) {
            WorkloadGroupSharedThrottleService.AcquirePermitResponse original =
                new WorkloadGroupSharedThrottleService.AcquirePermitResponse(granted);
            WorkloadGroupSharedThrottleService.AcquirePermitResponse copy = copyWriteable(
                original,
                writableRegistry(),
                WorkloadGroupSharedThrottleService.AcquirePermitResponse::new
            );
            assertEquals(granted, copy.granted);
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

    // --- serde resilience -------------------------------------------------------------------------------------------
    // All RPC bodies are ONE name-keyed map, so a peer from a different commit interoperates. These write the bytes
    // by hand to stand in for another build's writer, since our own writeTo can only emit the keys it knows.

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
        // A spread of registry types, since the point of generic values is that a future field keeps its natural type.
        body.put("requesting_node", "eph-1");
        body.put("wants_queue", true);
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

    public void testAcquirePermitRequestIsStrictBecauseADecodeErrorFailsOpen() throws Exception {
        // Deliberately NOT tolerant. A decode error reaches the coordinator's handleException, which admits the request
        // untracked (fail open). Guessing a default instead would let a mis-shaped acquire reach the tracker and be
        // DENIED, turning a shape mismatch into a spurious 429 — strictly worse than not enforcing the limit once.
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
        // Same reasoning as AcquirePermitRequest: an unreadable response fails open via handleException. Defaulting granted to
        // false would 429 the search; defaulting it to true would admit while believing a permit is held.
        expectThrows(
            IllegalStateException.class,
            () -> new WorkloadGroupSharedThrottleService.AcquirePermitResponse(bodyBytes(new HashMap<>(), false))
        );
    }

    public void testReleasePermitRequestIgnoresFieldsAddedByANewerPeer() throws Exception {
        Map<String, Object> body = new HashMap<>();
        body.put(WorkloadGroupSharedThrottleService.ReleasePermitRequest.KEY_BUCKET, "grp1:group");
        body.put(WorkloadGroupSharedThrottleService.ReleasePermitRequest.KEY_PERMIT_ID, "permit-abc");
        body.put("shared_limit", 5);              // both of these are added by the queueing branch
        body.put("queue_empty_on_node", "eph-1");
        StreamInput in = bodyBytes(body, true);
        WorkloadGroupSharedThrottleService.ReleasePermitRequest parsed = new WorkloadGroupSharedThrottleService.ReleasePermitRequest(in);
        assertEquals("grp1:group", parsed.bucketKey);
        assertEquals("permit-abc", parsed.permitId);
        assertEquals(0, in.available());
    }

    public void testReleasePermitRequestIsStrictLikeTheOthers() throws Exception {
        // Baseline fields are strict on every RPC, which is what the positional format already did for a malformed
        // message: a decode error before any handler runs. Tolerating them would buy nothing here — a release that throws
        // and a release that defaults to empty both leave the permit held until its TTL — while losing the error response
        // that makes the sender log "TTL will reclaim".
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
     * PROBE 4 — LOCAL-OWNER path. Registration there does not go through the cluster-state lookup at all (the waiter IS
     * this node), so it can never report "not registered". Verified even with the local node ABSENT from its own applied
     * state, which is the condition that defeats the remote path: the ring still routes the bucket here, the local
     * short-circuit registers {@code clusterService.localNode()} directly, and the no-waiter callback must stay unused.
     */
    public void testLocalOwnerAlwaysRegistersEvenWhenAbsentFromItsOwnAppliedState() {
        final String bucket = "grp-local:group";
        final int sharedLimit = 1;
        WorkloadGroupSharedThrottleService service = newService(); // ring built while the local node IS present
        stubGroupFor(bucket, sharedLimit);

        // Now make the local node vanish from the applied state, while the ring keeps routing this bucket to it.
        ClusterState nodeless = Mockito.mock(ClusterState.class);
        when(nodeless.nodes()).thenReturn(DiscoveryNodes.EMPTY_NODES);
        when(nodeless.metadata()).thenReturn(metadata);
        when(clusterService.state()).thenReturn(nodeless);
        assertNull("precondition: the local node must not resolve from its own state", clusterService.state().nodes().get("local"));

        assertNotNull("first acquire takes the only shared slot", awaitGrant(service, bucket, sharedLimit));

        AtomicReference<Exception> denial = new AtomicReference<>();
        service.acquireAsync(
            bucket,
            sharedLimit,
            ActionListener.wrap(p -> fail("must be denied while at limit"), denial::set),
            () -> fail("the local owner must never report itself unregisterable")
        );

        assertTrue("acquire at limit must be denied", denial.get() instanceof OpenSearchRejectedExecutionException);
        assertEquals("the local waiter must still be registered", 1, service.waiterCountForTest(bucket));
    }

    /**
     * PROBE 4b — the local-owner denial must be reported through the listener (the plain 429 marker), never by diverting
     * to the no-waiter callback, for BOTH the queueing and non-queueing overloads.
     */
    public void testLocalOwnerDenialUsesTheListenerForBothOverloads() {
        final String bucket = "grp-local2:group";
        final int sharedLimit = 1;
        WorkloadGroupSharedThrottleService service = newService();
        stubGroupFor(bucket, sharedLimit);
        assertNotNull(awaitGrant(service, bucket, sharedLimit));

        // Non-queueing overload: null callback, so a divert here would NPE.
        AtomicReference<Exception> plain = new AtomicReference<>();
        service.acquireAsync(bucket, sharedLimit, ActionListener.wrap(p -> fail("must be denied"), plain::set));
        assertTrue(plain.get() instanceof OpenSearchRejectedExecutionException);
        assertNull("no NPE cause", plain.get().getCause());
        assertEquals("wantsQueue=false must not register a waiter", 0, service.waiterCountForTest(bucket));

        // Queueing overload: the callback is supplied but must remain unused.
        AtomicReference<Exception> queued = new AtomicReference<>();
        service.acquireAsync(
            bucket,
            sharedLimit,
            ActionListener.wrap(p -> fail("must be denied"), queued::set),
            () -> fail("local registration cannot fail")
        );
        assertTrue(queued.get() instanceof OpenSearchRejectedExecutionException);
        assertEquals("now a local waiter is registered", 1, service.waiterCountForTest(bucket));
    }

}
