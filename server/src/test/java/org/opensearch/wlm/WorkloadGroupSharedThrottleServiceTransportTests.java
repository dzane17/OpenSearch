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
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ConnectTransportException;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Exercises the <em>cross-node</em> (remote owner) acquire/release path of {@link WorkloadGroupSharedThrottleService}
 * over a real two-node {@link MockTransportService} harness. Mockito cannot be used to stub the acquire round-trip
 * because {@code TransportService#sendRequest(node, action, request, options, handler)} is {@code final}; instead we
 * stand up two genuine transports so the {@code internal:wlm/throttle/shared/acquire} and {@code .../release}
 * RPCs travel over the wire and hit the owner-side handlers for real.
 */
public class WorkloadGroupSharedThrottleServiceTransportTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private MockTransportService coordinatorTransport;
    private MockTransportService ownerTransport;

    private ClusterService coordinatorClusterService;
    private ClusterService ownerClusterService;

    private WorkloadGroupSharedThrottleService coordinatorService;
    private WorkloadGroupSharedThrottleService ownerService;

    private DiscoveryNode coordinatorNode;
    private DiscoveryNode ownerNode;

    // A bucket key whose ring owner is the remote (owner) node, forcing the coordinator down the transport path.
    private String remoteKey;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());

        // Two real transports. Default node roles include DATA at Version.CURRENT (>= MIN_OWNER_VERSION), so both
        // nodes are eligible ring owners.
        coordinatorTransport = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, NoopTracer.INSTANCE);
        ownerTransport = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, NoopTracer.INSTANCE);
        coordinatorTransport.start();
        coordinatorTransport.acceptIncomingRequests();
        ownerTransport.start();
        ownerTransport.acceptIncomingRequests();
        // The coordinator must be able to reach the owner for the acquire/release RPCs.
        coordinatorTransport.connectToNode(ownerTransport.getLocalDiscoNode());

        coordinatorNode = coordinatorTransport.getLocalDiscoNode();
        ownerNode = ownerTransport.getLocalDiscoNode();

        // Each service sees itself as the local node.
        coordinatorClusterService = mock(ClusterService.class);
        when(coordinatorClusterService.localNode()).thenReturn(coordinatorNode);
        ownerClusterService = mock(ClusterService.class);
        when(ownerClusterService.localNode()).thenReturn(ownerNode);

        // Build one service per transport so BOTH register their acquire/release handlers on their own transport.
        coordinatorService = new WorkloadGroupSharedThrottleService(coordinatorClusterService, threadPool, coordinatorTransport);
        ownerService = new WorkloadGroupSharedThrottleService(ownerClusterService, threadPool, ownerTransport);

        // Drive an identical membership (both nodes as data nodes) into both services so they build the same ring and
        // agree on a single deterministic owner per bucket.
        DiscoveryNodes bothNodes = DiscoveryNodes.builder()
            .add(coordinatorNode)
            .add(ownerNode)
            .localNodeId(coordinatorNode.getId())
            .build();
        deliverNodes(coordinatorService, bothNodes);
        deliverNodes(ownerService, bothNodes);

        // Find a bucket key that the coordinator's ring maps to the REMOTE owner node, so acquireAsync takes the
        // transport path rather than the local short-circuit. Guard the whole harness against a mis-built ring.
        for (int i = 0; i < 10000 && remoteKey == null; i++) {
            String candidate = "bucket-" + i;
            DiscoveryNode owner = coordinatorService.ring().ownerFor(candidate).orElse(null);
            if (owner != null && owner.getId().equals(ownerNode.getId())) {
                remoteKey = candidate;
            }
        }
        assertNotNull("could not find a bucket owned by the remote owner node", remoteKey);
        assertEquals(
            "chosen bucket must be owned by the remote node (transport path), not the coordinator",
            ownerNode.getId(),
            coordinatorService.ring().ownerFor(remoteKey).orElseThrow().getId()
        );
    }

    @Override
    public void tearDown() throws Exception {
        coordinatorTransport.close();
        ownerTransport.close();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    private void deliverNodes(WorkloadGroupSharedThrottleService service, DiscoveryNodes nodes) {
        ClusterState previous = mock(ClusterState.class);
        when(previous.nodes()).thenReturn(DiscoveryNodes.EMPTY_NODES);
        ClusterState current = mock(ClusterState.class);
        when(current.nodes()).thenReturn(nodes);
        service.clusterChanged(new ClusterChangedEvent("test", current, previous));
    }

    /**
     * Happy path across nodes: the coordinator asks the remote owner for a permit, the owner grants it over the wire,
     * and closing the returned {@link Releasable} sends the fire-and-forget RELEASE RPC that drains the owner's tracker.
     */
    public void testRemoteAcquireGrantedThenReleaseRoundTrips() throws Exception {
        CapturingListener listener = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, listener);
        listener.await();

        assertNull("granted acquire must not fail", listener.failure.get());
        assertTrue("listener must have been notified via onResponse", listener.responded.get());
        Releasable permit = listener.response.get();
        assertNotNull("remote owner granted a permit, so a non-null Releasable is expected", permit);
        // The grant was recorded on the OWNER node's tracker (the acquire handler ran there before responding).
        assertEquals("owner tracker must hold exactly one in-flight permit", 1, ownerService.tracker().inFlight(remoteKey));

        // Closing the permit fires the RELEASE RPC (fire-and-forget), which the owner applies asynchronously.
        permit.close();
        assertBusy(() -> assertEquals("release RPC must drain the owner's in-flight count", 0, ownerService.tracker().inFlight(remoteKey)));
    }

    /**
     * When the remote owner is already at its shared limit for the bucket, the owner denies the acquire over the wire
     * and the coordinator surfaces an {@link OpenSearchRejectedExecutionException} via {@code onFailure} (a 429).
     */
    public void testRemoteAcquireDeniedReturns429() throws Exception {
        // Pre-fill the owner's tracker to the limit directly so the next acquire is denied at the source.
        assertTrue(ownerService.tracker().tryAcquire(remoteKey, 1, "pre", WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS));
        assertEquals(1, ownerService.tracker().inFlight(remoteKey));

        CapturingListener listener = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, listener);
        listener.await();

        assertFalse("denied acquire must not invoke onResponse", listener.responded.get());
        assertNotNull("denied acquire must invoke onFailure", listener.failure.get());
        assertTrue(
            "denial must be an OpenSearchRejectedExecutionException (429), was: " + listener.failure.get(),
            listener.failure.get() instanceof OpenSearchRejectedExecutionException
        );
    }

    /**
     * If the acquire RPC to the owner fails at send time, the coordinator must FAIL OPEN — admit the request with a
     * {@code null} permit via {@code onResponse}, never {@code onFailure}. We keep the owner connected (so the
     * in-memory {@code nodeConnected} pre-check passes) and inject a send behavior that throws, forcing the failure
     * through {@code handleException} rather than the disconnected pre-check.
     */
    public void testFailOpenWhenOwnerTransportUnavailable() throws Exception {
        assertTrue(
            "precondition: owner must be connected so we exercise the send path, not the pre-check",
            coordinatorTransport.nodeConnected(ownerNode)
        );
        coordinatorTransport.addSendBehavior(ownerTransport, (connection, requestId, action, request, options) -> {
            if (WorkloadGroupSharedThrottleService.ACQUIRE_ACTION_NAME.equals(action)) {
                // Simulate the send blowing up; TransportService routes this to the handler's handleException.
                throw new ConnectTransportException(connection.getNode(), "simulated acquire send failure");
            }
            connection.sendRequest(requestId, action, request, options);
        });

        CapturingListener listener = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, listener);
        listener.await();

        assertTrue("fail-open must invoke onResponse, not onFailure", listener.responded.get());
        assertNull("fail-open on transport error must yield a null permit (admit, untracked)", listener.response.get());
        assertNull("fail-open must never propagate a failure to the listener", listener.failure.get());
    }

    /**
     * A denial with the coordinator reachable from the owner: the owner registers it as a waiter and reports
     * {@code registered=true}, so the coordinator surfaces the plain 429 marker and its parked request keeps waiting for
     * owner-push. The no-waiter callback must NOT fire.
     */
    public void testDeniedRegistersWaiterWhenCoordinatorReachableFromOwner() throws Exception {
        // The owner must both SEE the coordinator in its applied state and hold a connection to it.
        ownerSees(coordinatorNode);
        ownerTransport.connectToNode(coordinatorNode);

        fillOwnerToLimit();

        CapturingListener listener = new CapturingListener();
        AtomicBoolean noWaiter = new AtomicBoolean(false);
        coordinatorService.acquireAsync(remoteKey, 1, listener, () -> noWaiter.set(true));
        listener.await();

        assertFalse("owner could register us, so the no-waiter path must not fire", noWaiter.get());
        assertTrue(
            "denial must surface as the plain 429 marker, was: " + listener.failure.get(),
            listener.failure.get() instanceof OpenSearchRejectedExecutionException
        );
        assertEquals("owner must have registered the coordinator as a waiter", 1, ownerService.waiterCountForTest(remoteKey));
    }

    /**
     * The JOIN race: the coordinator's acquire reaches the owner before the owner has applied the cluster state that
     * contains it (an inbound channel needs no membership). The owner cannot address it, so it must report
     * {@code registered=false} rather than silently skipping registration — otherwise the coordinator's parked request
     * would wait for a grant that never comes, with no deadline to rescue it.
     */
    public void testDeniedReportsNoWaiterWhenCoordinatorAbsentFromOwnerState() throws Exception {
        ownerSees(); // owner's applied state does not contain the coordinator yet
        fillOwnerToLimit();

        CapturingListener listener = new CapturingListener();
        AtomicBoolean noWaiter = new AtomicBoolean(false);
        CountDownLatch noWaiterLatch = new CountDownLatch(1);
        coordinatorService.acquireAsync(remoteKey, 1, listener, () -> {
            noWaiter.set(true);
            noWaiterLatch.countDown();
        });

        assertTrue("the no-waiter callback must fire", noWaiterLatch.await(10, TimeUnit.SECONDS));
        assertTrue(noWaiter.get());
        assertFalse("the listener must not be completed on this path", listener.responded.get());
        assertNull("the no-waiter callback replaces onFailure, it does not add to it", listener.failure.get());
        assertEquals("no waiter can be registered for a node the owner cannot address", 0, ownerService.waiterCountForTest(remoteKey));
    }

    /**
     * The RESTART case, which a presence-only check would get WRONG: the owner's applied state still holds the PREVIOUS
     * incarnation of the coordinator — same persistent node id, different ephemeral id — so {@code nodes().get(id)}
     * returns non-null. Registering that stale node would look like success and then be silently dropped by the first
     * freed slot (it is absent from the connection map, which is keyed on ephemeral id). The reachability check must
     * catch it and report {@code registered=false}.
     */
    public void testDeniedReportsNoWaiterWhenCoordinatorPresentButStale() throws Exception {
        DiscoveryNode staleCoordinator = new DiscoveryNode(
            coordinatorNode.getName(),
            coordinatorNode.getId(),            // same PERSISTENT id ...
            "stale-ephemeral-id",               // ... but a previous incarnation's ephemeral id
            coordinatorNode.getHostName(),
            coordinatorNode.getHostAddress(),
            coordinatorNode.getAddress(),
            coordinatorNode.getAttributes(),
            coordinatorNode.getRoles(),
            coordinatorNode.getVersion()
        );
        assertEquals(
            "precondition: the stale node resolves under the same persistent id",
            coordinatorNode.getId(),
            staleCoordinator.getId()
        );
        assertNotEquals(
            "precondition: DiscoveryNode identity is the ephemeral id, so it is a DIFFERENT node",
            coordinatorNode,
            staleCoordinator
        );

        ownerSees(staleCoordinator);
        // Even a live connection to the CURRENT coordinator must not make the stale entry look reachable.
        ownerTransport.connectToNode(coordinatorNode);
        fillOwnerToLimit();

        CapturingListener listener = new CapturingListener();
        AtomicBoolean noWaiter = new AtomicBoolean(false);
        CountDownLatch noWaiterLatch = new CountDownLatch(1);
        coordinatorService.acquireAsync(remoteKey, 1, listener, () -> {
            noWaiter.set(true);
            noWaiterLatch.countDown();
        });

        assertTrue("a stale-identity resolve must be reported as not registered", noWaiterLatch.await(10, TimeUnit.SECONDS));
        assertTrue(noWaiter.get());
        assertEquals("a stale node must never be registered as a waiter", 0, ownerService.waiterCountForTest(remoteKey));
    }

    /**
     * PROBE 6 — the NON-queueing path passes a null no-waiter callback, so {@code wantsQueue} is false and the owner
     * reports {@code registered=false}. The coordinator must fall through to the plain 429 marker and must NOT
     * dereference the null Runnable.
     */
    public void testNonQueueingDenialDoesNotTouchTheNullCallback() throws Exception {
        ownerSees(); // owner cannot see the coordinator, so registered would be false if it were even consulted
        fillOwnerToLimit();

        CapturingListener listener = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, listener); // 3-arg overload => null callback, wantsQueue=false
        listener.await();

        assertFalse("must not admit", listener.responded.get());
        assertTrue(
            "non-queueing denial must surface the plain 429, not NPE on the null callback, was: " + listener.failure.get(),
            listener.failure.get() instanceof OpenSearchRejectedExecutionException
        );
        assertNull("a NullPointerException here would mean the wantsQueue guard is missing", listener.failure.get().getCause());
        assertEquals("wantsQueue=false must never register a waiter", 0, ownerService.waiterCountForTest(remoteKey));
    }

    /**
     * PROBE 7 — a waiter registered while connected, that then disconnects before a slot frees. The owner must reclaim
     * the reserved slot (no permit leak), drop the dead waiter, and terminate its bounded drain loop.
     */
    public void testDisconnectAfterRegistrationReclaimsSlotAndDropsWaiter() throws Exception {
        ownerSeesWithGroup(coordinatorNode);
        ownerTransport.connectToNode(coordinatorNode);
        fillOwnerToLimit();

        // Register the coordinator as a waiter while it is reachable.
        CapturingListener listener = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, listener, () -> fail("owner could see and reach us, so it must register"));
        listener.await();
        assertEquals("waiter registered", 1, ownerService.waiterCountForTest(remoteKey));
        assertEquals("the pre-filled permit still holds the only slot", 1, ownerService.tracker().inFlight(remoteKey));

        // The coordinator now goes away before any slot frees.
        ownerTransport.disconnectFromNode(coordinatorNode);
        assertFalse("precondition: owner must no longer see a connection", ownerTransport.nodeConnected(coordinatorNode));

        // Free the slot -> owner-push picks the dead waiter, reclaims, drops it, and the loop must terminate.
        ownerService.handleRelease(new WorkloadGroupSharedThrottleService.ReleasePermitRequest(remoteKey, "pre"));

        assertEquals("the reserved slot must be reclaimed, not leaked until TTL", 0, ownerService.tracker().inFlight(remoteKey));
        assertEquals("the disconnected waiter must be dropped", 0, ownerService.waiterCountForTest(remoteKey));
    }

    /** Takes the bucket's only shared slot on the owner so the next acquire is denied at the source. */
    private void fillOwnerToLimit() {
        assertTrue(ownerService.tracker().tryAcquire(remoteKey, 1, "pre", WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS));
        assertEquals(1, ownerService.tracker().inFlight(remoteKey));
    }

    /** Stubs the OWNER's applied cluster state to contain itself plus {@code others} (the coordinator, or nothing). */
    private void ownerSees(DiscoveryNode... others) {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder().add(ownerNode).localNodeId(ownerNode.getId());
        for (DiscoveryNode other : others) {
            builder.add(other);
        }
        ClusterState state = mock(ClusterState.class);
        when(state.nodes()).thenReturn(builder.build());
        when(ownerClusterService.state()).thenReturn(state);
    }

    /**
     * As {@link #ownerSees}, but also stubs the bucket's workload group so the owner can resolve {@code shared_limit}
     * from its own state — required by the release path, which drives owner-push only for a resolvable limit.
     */
    private void ownerSeesWithGroup(DiscoveryNode... others) {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder().add(ownerNode).localNodeId(ownerNode.getId());
        for (DiscoveryNode other : others) {
            builder.add(other);
        }
        final int idx = remoteKey.indexOf(':');
        final String groupId = idx < 0 ? remoteKey : remoteKey.substring(0, idx);
        WorkloadGroup group = new WorkloadGroup(
            groupId + "-name",
            groupId,
            new MutableWorkloadGroupFragment(
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED,
                java.util.Map.of(ResourceType.MEMORY, 0.5),
                Settings.EMPTY,
                Settings.builder().put("attribute", "group").put("shared_limit", 1).build()
            ),
            1L
        );
        Metadata metadata = mock(Metadata.class);
        when(metadata.workloadGroups()).thenReturn(java.util.Map.of(groupId, group));
        ClusterState state = mock(ClusterState.class);
        when(state.nodes()).thenReturn(builder.build());
        when(state.metadata()).thenReturn(metadata);
        when(ownerClusterService.state()).thenReturn(state);
    }

    /** Captures the outcome of an async acquire, distinguishing onResponse(null) from an unfired listener via a latch. */
    private static final class CapturingListener implements ActionListener<Releasable> {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Releasable> response = new AtomicReference<>();
        final AtomicReference<Exception> failure = new AtomicReference<>();
        final AtomicBoolean responded = new AtomicBoolean(false);

        @Override
        public void onResponse(Releasable releasable) {
            response.set(releasable);
            responded.set(true);
            latch.countDown();
        }

        @Override
        public void onFailure(Exception e) {
            failure.set(e);
            latch.countDown();
        }

        void await() throws InterruptedException {
            assertTrue("listener was not invoked within the timeout", latch.await(10, TimeUnit.SECONDS));
        }
    }
}
