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
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lease.Releasable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.mockito.Mockito;

import static org.mockito.Mockito.when;

public class WorkloadGroupSharedThrottleServiceTests extends OpenSearchTestCase {

    private ClusterService clusterService;
    private ThreadPool threadPool;
    private TransportService transportService;
    private DiscoveryNode localNode;

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
        when(clusterService.state()).thenReturn(state);
        when(clusterService.localNode()).thenReturn(localNode);
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

    public void testLocalOwnerGrantsThenDeniesAtLimit() {
        WorkloadGroupSharedThrottleService service = newService();
        Releasable p1 = awaitGrant(service, "b", 1);
        assertNotNull(p1);
        expectThrows(OpenSearchRejectedExecutionException.class, () -> awaitGrant(service, "b", 1));
        // release frees the shared slot
        p1.close();
        assertNotNull(awaitGrant(service, "b", 1));
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
                new WorkloadGroupSharedThrottleService.AcquireRequest("b", 1, "lease-1", WorkloadGroupSharedThrottleService.LEASE_TTL_NANOS)
            ).granted
        );
        assertFalse(
            service.handleAcquire(
                new WorkloadGroupSharedThrottleService.AcquireRequest("b", 1, "lease-2", WorkloadGroupSharedThrottleService.LEASE_TTL_NANOS)
            ).granted
        );
        assertEquals(1, service.tracker().inFlight("b"));
    }

    public void testAcquireRequestSerializationRoundTrip() throws Exception {
        WorkloadGroupSharedThrottleService.AcquireRequest original = new WorkloadGroupSharedThrottleService.AcquireRequest(
            "grp1:username:alice",
            42,
            "lease-xyz",
            123_456_789L
        );
        WorkloadGroupSharedThrottleService.AcquireRequest copy = copyWriteable(
            original,
            writableRegistry(),
            WorkloadGroupSharedThrottleService.AcquireRequest::new
        );
        assertEquals(original.bucketKey, copy.bucketKey);
        assertEquals(original.sharedLimit, copy.sharedLimit);
        assertEquals(original.leaseId, copy.leaseId);
        assertEquals(original.ttlNanos, copy.ttlNanos);
    }

    public void testAcquireResponseSerializationRoundTrip() throws Exception {
        for (boolean granted : new boolean[] { true, false }) {
            WorkloadGroupSharedThrottleService.AcquireResponse original = new WorkloadGroupSharedThrottleService.AcquireResponse(granted);
            WorkloadGroupSharedThrottleService.AcquireResponse copy = copyWriteable(
                original,
                writableRegistry(),
                WorkloadGroupSharedThrottleService.AcquireResponse::new
            );
            assertEquals(granted, copy.granted);
        }
    }

    public void testReleaseRequestSerializationRoundTrip() throws Exception {
        WorkloadGroupSharedThrottleService.ReleaseRequest original = new WorkloadGroupSharedThrottleService.ReleaseRequest(
            "grp1:group",
            "lease-abc"
        );
        WorkloadGroupSharedThrottleService.ReleaseRequest copy = copyWriteable(
            original,
            writableRegistry(),
            WorkloadGroupSharedThrottleService.ReleaseRequest::new
        );
        assertEquals(original.bucketKey, copy.bucketKey);
        assertEquals(original.leaseId, copy.leaseId);
    }
}
