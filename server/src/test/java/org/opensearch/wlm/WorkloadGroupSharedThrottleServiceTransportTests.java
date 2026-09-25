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
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ConnectTransportException;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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

    public void testOnlyOneGrantAwaitsResponsePerCoordinatorBucket() throws Exception {
        ownerSeesWithGroup(coordinatorNode);
        ownerTransport.connectToNode(coordinatorNode);
        fillOwnerToLimit();

        CapturingListener denial = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, denial, () -> fail("the reachable coordinator must be registered"));
        denial.await();
        assertTrue(denial.failure.get() instanceof OpenSearchRejectedExecutionException);
        assertEquals(1, ownerService.waiterCountForTest(remoteKey));

        AtomicInteger grantSends = new AtomicInteger();
        AtomicReference<TimeValue> grantTimeout = new AtomicReference<>();
        ownerTransport.addSendBehavior(coordinatorTransport, (connection, requestId, action, request, options) -> {
            if (WorkloadGroupSharedThrottleService.GRANT_ACTION_NAME.equals(action)) {
                grantSends.incrementAndGet();
                grantTimeout.set(options.timeout());
                return; // keep the first GRANT unacknowledged
            }
            connection.sendRequest(requestId, action, request, options);
        });

        final DiscoveryNodes nodes = ownerClusterService.state().nodes();
        final ClusterState previousState = stateWithGroup(nodes, 1);
        final ClusterState currentState = stateWithGroup(nodes, 4);
        when(ownerClusterService.state()).thenReturn(currentState);
        ownerService.clusterChanged(new ClusterChangedEvent("shared limit increased", currentState, previousState));

        assertBusy(() -> {
            assertEquals("the bulk event must stop after one unacknowledged GRANT to this coordinator", 1, grantSends.get());
            assertEquals(WorkloadGroupSharedThrottleService.GRANT_TIMEOUT, grantTimeout.get());
            assertEquals(1, ownerService.pendingGrantCountForTest(remoteKey));
            assertEquals("the original holder plus one reserved GRANT", 2, ownerService.tracker().inFlight(remoteKey));
        });

        // A fresh denial can idempotently register the same coordinator while its GRANT is pending. It proves only that
        // coordinator -> owner traffic works; it must not permit a second owner -> coordinator GRANT before the first
        // response resolves.
        WorkloadGroupSharedThrottleService.AcquirePermitResponse reRegistration = ownerService.handleAcquire(
            new WorkloadGroupSharedThrottleService.AcquirePermitRequest(
                remoteKey,
                2,
                "re-register-while-pending",
                WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS,
                coordinatorNode.getId(),
                true
            )
        );
        assertFalse(reRegistration.granted);
        assertTrue(reRegistration.registered);

        ownerService.handleRelease(new WorkloadGroupSharedThrottleService.ReleasePermitRequest(remoteKey, "pre"));
        assertEquals("another freed slot must skip the coordinator while its first GRANT is pending", 1, grantSends.get());
        assertEquals(1, ownerService.pendingGrantCountForTest(remoteKey));
        assertEquals("only the unacknowledged GRANT remains reserved", 1, ownerService.tracker().inFlight(remoteKey));

        // Closing the connection resolves the unacknowledged send immediately instead of making this test wait five
        // seconds. The failure path must remove the waiter before reclaiming and redistributing the permit.
        ownerTransport.disconnectFromNode(coordinatorNode);
        assertBusy(() -> {
            assertEquals(0, ownerService.pendingGrantCountForTest(remoteKey));
            assertEquals(
                "the failed coordinator must be removed before the slot is re-offered",
                0,
                ownerService.waiterCountForTest(remoteKey)
            );
            assertEquals("the unacknowledged reservation must be reclaimed", 0, ownerService.tracker().inFlight(remoteKey));
            assertEquals("failure must not immediately send another GRANT to the same waiter", 1, grantSends.get());
        });

        // Once the coordinator is reachable and submits another denied acquire, normal registration makes it eligible
        // again. There is no permanent quarantine or separate recovery protocol.
        ownerTransport.clearAllRules();
        ownerTransport.connectToNode(coordinatorNode);
        assertTrue(ownerService.tracker().tryAcquire(remoteKey, 1, "new-holder", WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS));
        CapturingListener secondDenial = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, secondDenial, () -> fail("the recovered coordinator must be re-registered"));
        secondDenial.await();
        assertTrue(secondDenial.failure.get() instanceof OpenSearchRejectedExecutionException);
        assertEquals("a later denied acquire re-adds the responsive coordinator", 1, ownerService.waiterCountForTest(remoteKey));
        assertTrue(ownerService.tracker().release(remoteKey, "new-holder"));
    }

    public void testFailedGrantIsRemovedBeforePermitMovesToAnotherWaiter() throws Exception {
        MockTransportService healthyTransport = MockTransportService.createNewService(
            Settings.EMPTY,
            Version.CURRENT,
            threadPool,
            NoopTracer.INSTANCE
        );
        healthyTransport.start();
        healthyTransport.acceptIncomingRequests();
        final DiscoveryNode healthyNode = healthyTransport.getLocalDiscoNode();
        final ClusterService healthyClusterService = mock(ClusterService.class);
        when(healthyClusterService.localNode()).thenReturn(healthyNode);
        final WorkloadGroupSharedThrottleService healthyService = new WorkloadGroupSharedThrottleService(
            healthyClusterService,
            threadPool,
            healthyTransport
        );

        try {
            ownerTransport.connectToNode(coordinatorNode);
            ownerTransport.connectToNode(healthyNode);
            healthyTransport.connectToNode(ownerNode);

            final DiscoveryNodes ownerNodes = DiscoveryNodes.builder()
                .add(ownerNode)
                .add(coordinatorNode)
                .add(healthyNode)
                .localNodeId(ownerNode.getId())
                .build();
            final ThrottleOwnerSelector threeNodeRing = ThrottleOwnerSelector.fromDiscoveryNodes(ownerNodes);
            String redistributionKey = null;
            for (int i = 0; i < 10_000 && redistributionKey == null; i++) {
                final String candidate = "redistribution-bucket-" + i;
                if (threeNodeRing.ownerFor(candidate).filter(ownerNode::equals).isPresent()) {
                    redistributionKey = candidate;
                }
            }
            assertNotNull("could not find a three-node bucket owned by the grant owner", redistributionKey);
            remoteKey = redistributionKey;

            final DiscoveryNodes coordinatorNodes = DiscoveryNodes.builder(ownerNodes).localNodeId(coordinatorNode.getId()).build();
            final DiscoveryNodes healthyNodes = DiscoveryNodes.builder(ownerNodes).localNodeId(healthyNode.getId()).build();
            deliverNodes(ownerService, ownerNodes);
            deliverNodes(coordinatorService, coordinatorNodes);
            deliverNodes(healthyService, healthyNodes);
            final ClusterState ownerState = stateWithGroup(ownerNodes);
            when(ownerClusterService.state()).thenReturn(ownerState);

            fillOwnerToLimit();
            CapturingListener failedWaiterDenial = new CapturingListener();
            coordinatorService.acquireAsync(remoteKey, 1, failedWaiterDenial, () -> fail("the first coordinator must be registered"));
            failedWaiterDenial.await();
            assertTrue(failedWaiterDenial.failure.get() instanceof OpenSearchRejectedExecutionException);

            CapturingListener healthyWaiterDenial = new CapturingListener();
            healthyService.acquireAsync(remoteKey, 1, healthyWaiterDenial, () -> fail("the healthy coordinator must be registered"));
            healthyWaiterDenial.await();
            assertTrue(healthyWaiterDenial.failure.get() instanceof OpenSearchRejectedExecutionException);
            assertEquals(2, ownerService.waiterCountForTest(remoteKey));

            AtomicInteger failedGrantAttempts = new AtomicInteger();
            ownerTransport.addSendBehavior(coordinatorTransport, (connection, requestId, action, request, options) -> {
                if (WorkloadGroupSharedThrottleService.GRANT_ACTION_NAME.equals(action)) {
                    failedGrantAttempts.incrementAndGet();
                    throw new ConnectTransportException(connection.getNode(), "simulated unresponsive waiter");
                }
                connection.sendRequest(requestId, action, request, options);
            });

            AtomicInteger healthyAdmissions = new AtomicInteger();
            AtomicReference<Releasable> healthyPermit = new AtomicReference<>();
            healthyService.setGrantConsumer((bucketKey, permit) -> {
                if (healthyAdmissions.incrementAndGet() == 1) {
                    healthyPermit.set(permit);
                    return true;
                }
                return false;
            });

            ownerService.handleRelease(new WorkloadGroupSharedThrottleService.ReleasePermitRequest(remoteKey, "pre"));
            assertBusy(() -> {
                assertEquals("the failed waiter must not be retried", 1, failedGrantAttempts.get());
                assertEquals("the reclaimed slot must move to the healthy waiter", 1, healthyAdmissions.get());
                assertNotNull(healthyPermit.get());
                assertEquals("only the healthy waiter remains registered", 1, ownerService.waiterCountForTest(remoteKey));
                assertEquals(0, ownerService.pendingGrantCountForTest(remoteKey));
                assertEquals(1, ownerService.tracker().inFlight(remoteKey));
            });

            healthyPermit.get().close();
            assertBusy(() -> {
                assertTrue(healthyAdmissions.get() >= 2 && healthyAdmissions.get() <= 3);
                assertEquals(0, ownerService.waiterCountForTest(remoteKey));
                assertEquals(0, ownerService.tracker().inFlight(remoteKey));
            });
        } finally {
            healthyTransport.close();
        }
    }

    public void testFreshRegistrationSurvivesFailedGrantAndCanReceiveNextGrant() throws Exception {
        ownerSeesWithGroup(coordinatorNode);
        ownerTransport.connectToNode(coordinatorNode);
        fillOwnerToLimit();

        CapturingListener denial = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, denial, () -> fail("the reachable coordinator must be registered"));
        denial.await();
        assertTrue(denial.failure.get() instanceof OpenSearchRejectedExecutionException);

        CountDownLatch firstGrantStarted = new CountDownLatch(1);
        CountDownLatch failFirstGrant = new CountDownLatch(1);
        AtomicInteger grantSends = new AtomicInteger();
        ownerTransport.addSendBehavior(coordinatorTransport, (connection, requestId, action, request, options) -> {
            if (WorkloadGroupSharedThrottleService.GRANT_ACTION_NAME.equals(action) && grantSends.incrementAndGet() == 1) {
                firstGrantStarted.countDown();
                try {
                    if (failFirstGrant.await(10, TimeUnit.SECONDS) == false) {
                        throw new IOException("timed out waiting to fail the first grant");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException(e);
                }
                throw new ConnectTransportException(connection.getNode(), "simulated first grant failure");
            }
            connection.sendRequest(requestId, action, request, options);
        });

        AtomicInteger grantAdmissions = new AtomicInteger();
        AtomicReference<Releasable> admittedPermit = new AtomicReference<>();
        coordinatorService.setGrantConsumer((bucketKey, permit) -> {
            if (grantAdmissions.incrementAndGet() == 1) {
                admittedPermit.set(permit);
                return true;
            }
            return false;
        });

        threadPool.generic()
            .execute(() -> ownerService.handleRelease(new WorkloadGroupSharedThrottleService.ReleasePermitRequest(remoteKey, "pre")));
        assertTrue("the first grant send did not start", firstGrantStarted.await(10, TimeUnit.SECONDS));

        WorkloadGroupSharedThrottleService.AcquirePermitResponse freshRegistration = ownerService.handleAcquire(
            new WorkloadGroupSharedThrottleService.AcquirePermitRequest(
                remoteKey,
                1,
                "fresh-registration",
                WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS,
                coordinatorNode.getId(),
                true
            )
        );
        assertFalse(freshRegistration.granted);
        assertTrue(freshRegistration.registered);

        failFirstGrant.countDown();
        assertBusy(() -> {
            assertEquals("fresh registration allows exactly one replacement hand-off", 2, grantSends.get());
            assertEquals(1, grantAdmissions.get());
            assertNotNull(admittedPermit.get());
            assertEquals("the replacement grant response clears its transient marker", 0, ownerService.pendingGrantCountForTest(remoteKey));
            assertEquals(
                "the fresh waiter registration must survive the earlier grant failure",
                1,
                ownerService.waiterCountForTest(remoteKey)
            );
            assertEquals(1, ownerService.tracker().inFlight(remoteKey));
        });

        admittedPermit.get().close();
        assertBusy(() -> {
            assertTrue(
                "the now-empty queue returns the next grant unused; its release can race its ACK and allow one final "
                    + "speculative hand-off, admissions="
                    + grantAdmissions.get(),
                grantAdmissions.get() >= 2 && grantAdmissions.get() <= 3
            );
            assertEquals(0, ownerService.waiterCountForTest(remoteKey));
            assertEquals(0, ownerService.tracker().inFlight(remoteKey));
        });
    }

    public void testStaleGrantResponseAfterOwnerChurnDoesNotClearNewPendingGrant() throws Exception {
        ownerSeesWithGroup(coordinatorNode);
        ownerTransport.connectToNode(coordinatorNode);
        fillOwnerToLimit();

        CapturingListener denial = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, denial, () -> fail("the reachable coordinator must be registered"));
        denial.await();
        assertTrue(denial.failure.get() instanceof OpenSearchRejectedExecutionException);

        AtomicInteger grantSends = new AtomicInteger();
        AtomicReference<Runnable> deliverFirstGrant = new AtomicReference<>();
        AtomicReference<WorkloadGroupSharedThrottleService.GrantPermitRequest> firstGrant = new AtomicReference<>();
        ownerTransport.addSendBehavior(coordinatorTransport, (connection, requestId, action, request, options) -> {
            if (WorkloadGroupSharedThrottleService.GRANT_ACTION_NAME.equals(action)) {
                if (grantSends.incrementAndGet() == 1) {
                    firstGrant.set((WorkloadGroupSharedThrottleService.GrantPermitRequest) request);
                    deliverFirstGrant.set(() -> {
                        try {
                            connection.sendRequest(requestId, action, request, options);
                        } catch (Exception e) {
                            throw new AssertionError(e);
                        }
                    });
                }
                return; // hold both the old and replacement grants
            }
            connection.sendRequest(requestId, action, request, options);
        });

        AtomicReference<Releasable> latePermit = new AtomicReference<>();
        coordinatorService.setGrantConsumer((bucketKey, permit) -> {
            latePermit.set(permit);
            return true;
        });

        ownerService.handleRelease(new WorkloadGroupSharedThrottleService.ReleasePermitRequest(remoteKey, "pre"));
        assertEquals(1, grantSends.get());
        assertEquals(1, ownerService.pendingGrantCountForTest(remoteKey));

        final DiscoveryNodes originalNodes = ownerClusterService.state().nodes();
        final DiscoveryNode replacementOwner = new DiscoveryNode(
            ownerNode.getName(),
            ownerNode.getId(),
            ownerNode.getEphemeralId() + "-replacement",
            ownerNode.getHostName(),
            ownerNode.getHostAddress(),
            ownerNode.getAddress(),
            ownerNode.getAttributes(),
            ownerNode.getRoles(),
            ownerNode.getVersion()
        );
        final DiscoveryNodes replacementNodes = DiscoveryNodes.builder()
            .add(coordinatorNode)
            .add(replacementOwner)
            .localNodeId(replacementOwner.getId())
            .build();
        final ClusterState originalState = stateWithGroup(originalNodes);
        final ClusterState replacementState = stateWithGroup(replacementNodes);

        when(ownerClusterService.state()).thenReturn(replacementState);
        ownerService.clusterChanged(new ClusterChangedEvent("owner incarnation replaced", replacementState, originalState));
        assertEquals(0, ownerService.pendingGrantCountForTest(remoteKey));
        assertEquals(0, ownerService.waiterCountForTest(remoteKey));

        final ClusterState restoredState = stateWithGroup(originalNodes);
        when(ownerClusterService.state()).thenReturn(restoredState);
        ownerService.clusterChanged(new ClusterChangedEvent("original owner restored", restoredState, replacementState));

        WorkloadGroupSharedThrottleService.AcquirePermitResponse freshRegistration = ownerService.handleAcquire(
            new WorkloadGroupSharedThrottleService.AcquirePermitRequest(
                remoteKey,
                1,
                "post-churn-registration",
                WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS,
                coordinatorNode.getId(),
                true
            )
        );
        assertFalse(freshRegistration.granted);
        assertTrue(freshRegistration.registered);

        ownerService.handleRelease(new WorkloadGroupSharedThrottleService.ReleasePermitRequest(remoteKey, firstGrant.get().permitId));
        assertEquals(2, grantSends.get());
        assertEquals(1, ownerService.pendingGrantCountForTest(remoteKey));
        assertEquals(1, ownerService.tracker().inFlight(remoteKey));

        deliverFirstGrant.get().run();
        assertBusy(() -> {
            assertNotNull("the delayed pre-churn grant must reach the coordinator", latePermit.get());
            assertEquals(
                "the old response callback must not clear the replacement grant's marker",
                1,
                ownerService.pendingGrantCountForTest(remoteKey)
            );
            assertEquals("the stale callback must not send a third grant", 2, grantSends.get());
        });

        ownerTransport.disconnectFromNode(coordinatorNode);
        assertBusy(() -> {
            assertEquals(0, ownerService.pendingGrantCountForTest(remoteKey));
            assertEquals(0, ownerService.waiterCountForTest(remoteKey));
            assertEquals(0, ownerService.tracker().inFlight(remoteKey));
        });
        latePermit.get().close();
        ownerTransport.clearAllRules();
    }

    public void testGrantTimeoutReclaimsPermitAndLateGrantMayRunOnce() throws Exception {
        ownerSeesWithGroup(coordinatorNode);
        ownerTransport.connectToNode(coordinatorNode);
        fillOwnerToLimit();

        CapturingListener denial = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, denial, () -> fail("the reachable coordinator must be registered"));
        denial.await();
        assertTrue(denial.failure.get() instanceof OpenSearchRejectedExecutionException);
        assertEquals(1, ownerService.waiterCountForTest(remoteKey));

        AtomicInteger lateAdmissions = new AtomicInteger();
        AtomicReference<Releasable> latePermit = new AtomicReference<>();
        coordinatorService.setGrantConsumer((bucketKey, permit) -> {
            latePermit.set(permit);
            lateAdmissions.incrementAndGet();
            return true;
        });

        // Delay the actual request beyond GRANT_TIMEOUT. MockTransportService still delivers it afterward, modeling the
        // ambiguous long-GC case: the owner has timed out and reclaimed the permit, but the coordinator eventually sees
        // the original request. There is deliberately no application-level GRANT retry.
        ownerTransport.addUnresponsiveRule(
            coordinatorTransport,
            TimeValue.timeValueMillis(WorkloadGroupSharedThrottleService.GRANT_TIMEOUT.millis() + 1_000)
        );
        ownerService.handleRelease(new WorkloadGroupSharedThrottleService.ReleasePermitRequest(remoteKey, "pre"));

        assertBusy(() -> {
            assertEquals(1, ownerService.pendingGrantCountForTest(remoteKey));
            assertEquals(1, ownerService.waiterCountForTest(remoteKey));
            assertEquals(1, ownerService.tracker().inFlight(remoteKey));
        });

        assertBusy(() -> {
            assertEquals("the timeout must clear the pending marker", 0, ownerService.pendingGrantCountForTest(remoteKey));
            assertEquals("the unchanged waiter registration must be removed", 0, ownerService.waiterCountForTest(remoteKey));
            assertEquals("the owner must reclaim the ambiguous reservation", 0, ownerService.tracker().inFlight(remoteKey));
        }, 20, TimeUnit.SECONDS);

        assertBusy(
            () -> assertEquals("the delayed original GRANT may produce one accepted fail-open admission", 1, lateAdmissions.get()),
            20,
            TimeUnit.SECONDS
        );
        latePermit.get().close(); // owner already reclaimed it; the idempotent late RELEASE is a no-op
        ownerTransport.clearAllRules();
    }

    public void testRemoteOwnerChangeReRegistrationRoundTripsAndImmediatelyDrives() throws Exception {
        ownerSeesWithGroup(coordinatorNode);
        ownerTransport.connectToNode(coordinatorNode);
        AtomicInteger grantsObserved = new AtomicInteger();
        coordinatorService.setGrantConsumer((bucketKey, permit) -> {
            grantsObserved.incrementAndGet();
            return false;
        });

        coordinatorService.reRegisterWaiterAfterOwnerChange(remoteKey, ownerNode);

        assertBusy(() -> {
            assertEquals("the new owner must immediately offer its already-free capacity", 1, grantsObserved.get());
            assertEquals(
                "the unused grant must reconcile the coordinator out of the waiter set",
                0,
                ownerService.waiterCountForTest(remoteKey)
            );
            assertEquals("the unused reserved permit must be returned to the owner", 0, ownerService.tracker().inFlight(remoteKey));
        });
    }

    public void testSharedLimitIncreaseBulkOffersAddedSlotsAndUnusedGrantsReturn() throws Exception {
        ownerSeesWithGroup(coordinatorNode);
        ownerTransport.connectToNode(coordinatorNode);
        fillOwnerToLimit();

        final AtomicInteger grantAttempts = new AtomicInteger();
        final AtomicReference<Releasable> admittedPermit = new AtomicReference<>();
        coordinatorService.setGrantConsumer((bucketKey, permit) -> {
            grantAttempts.incrementAndGet();
            if (admittedPermit.compareAndSet(null, permit)) {
                return true; // model one queued request
            }
            return false; // the remaining speculative grants are returned unused
        });

        CapturingListener denial = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, denial, () -> fail("the reachable coordinator must be registered"));
        denial.await();
        assertTrue(denial.failure.get() instanceof OpenSearchRejectedExecutionException);
        assertEquals(1, ownerService.waiterCountForTest(remoteKey));

        final DiscoveryNodes nodes = ownerClusterService.state().nodes();
        final ClusterState previousState = stateWithGroup(nodes, 1);
        final ClusterState currentState = stateWithGroup(nodes, 4);
        when(ownerClusterService.state()).thenReturn(currentState);
        ownerService.clusterChanged(new ClusterChangedEvent("shared limit increased", currentState, previousState));

        assertBusy(() -> {
            assertTrue(
                "ACK pacing may stop once the unused-grant release removes the shallow waiter, but must never exceed the "
                    + "three newly-added slots; attempts="
                    + grantAttempts.get(),
                grantAttempts.get() >= 2 && grantAttempts.get() <= 3
            );
            assertNotNull("one real queued request must consume one of the grants", admittedPermit.get());
            assertEquals("all unused speculative grants must return to the owner", 2, ownerService.tracker().inFlight(remoteKey));
            assertEquals("an unused grant reconciles the shallow coordinator queue", 0, ownerService.waiterCountForTest(remoteKey));
        });

        admittedPermit.get().close();
        ownerService.handleRelease(new WorkloadGroupSharedThrottleService.ReleasePermitRequest(remoteKey, "pre"));
        assertBusy(() -> assertEquals(0, ownerService.tracker().inFlight(remoteKey)));
    }

    public void testClusterChangeHookReRegistersRetainedBucketWithNewLocalOwner() throws Exception {
        AtomicInteger grantsObserved = new AtomicInteger();
        coordinatorService.setGrantConsumer((bucketKey, permit) -> {
            grantsObserved.incrementAndGet();
            return false;
        });
        coordinatorService.setRetainedBucketKeysSupplier(() -> Set.of(remoteKey));

        DiscoveryNodes coordinatorOnly = DiscoveryNodes.builder().add(coordinatorNode).localNodeId(coordinatorNode.getId()).build();
        ClusterState coordinatorOnlyState = stateWithGroup(coordinatorOnly);
        when(coordinatorClusterService.state()).thenReturn(coordinatorOnlyState);
        deliverNodes(coordinatorService, coordinatorOnly);

        assertBusy(() -> {
            assertEquals(coordinatorNode, coordinatorService.ring().ownerFor(remoteKey).orElseThrow());
            assertEquals("the ring-change hook must re-register and drive this retained bucket", 1, grantsObserved.get());
            assertEquals(
                "the synthetic empty queue returns the grant and removes the local waiter",
                0,
                coordinatorService.waiterCountForTest(remoteKey)
            );
            assertEquals(0, coordinatorService.tracker().inFlight(remoteKey));
        });
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
        ClusterState state = stateWithGroup(builder.build());
        when(ownerClusterService.state()).thenReturn(state);
    }

    private ClusterState stateWithGroup(DiscoveryNodes nodes) {
        return stateWithGroup(nodes, 1);
    }

    private ClusterState stateWithGroup(DiscoveryNodes nodes, int sharedLimit) {
        final int idx = remoteKey.indexOf(':');
        final String groupId = idx < 0 ? remoteKey : remoteKey.substring(0, idx);
        WorkloadGroup group = new WorkloadGroup(
            groupId + "-name",
            groupId,
            new MutableWorkloadGroupFragment(
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.EMPTY,
                Settings.builder().put("attribute", "group").put("shared_limit", sharedLimit).build()
            ),
            1L
        );
        Metadata metadata = mock(Metadata.class);
        when(metadata.workloadGroups()).thenReturn(Map.of(groupId, group));
        ClusterState state = mock(ClusterState.class);
        when(state.nodes()).thenReturn(nodes);
        when(state.metadata()).thenReturn(metadata);
        return state;
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
