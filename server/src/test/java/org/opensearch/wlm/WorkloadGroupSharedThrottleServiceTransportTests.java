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
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ConnectTransportException;
import org.opensearch.transport.RequestHandlerRegistry;
import org.opensearch.transport.TestTransportChannel;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Exercises the cross-node (remote owner) acquire/release path of {@link WorkloadGroupSharedThrottleService} over two
 * real {@link MockTransportService}s, since {@code TransportService#sendRequest} is final and cannot be mocked.
 */
public class WorkloadGroupSharedThrottleServiceTransportTests extends OpenSearchTestCase {

    // Far above any CI stall, so the grant/deny assertions never race the production 200ms acquire timeout.
    private static final TimeValue GENEROUS_ACQUIRE_TIMEOUT = TimeValue.timeValueSeconds(30);

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
        coordinatorService = newService(coordinatorClusterService, coordinatorTransport, GENEROUS_ACQUIRE_TIMEOUT);
        ownerService = newService(ownerClusterService, ownerTransport, GENEROUS_ACQUIRE_TIMEOUT);

        // Drive an identical membership (both nodes as data nodes) into both services so they build the same ring and
        // agree on a single deterministic owner per bucket.
        DiscoveryNodes bothNodes = DiscoveryNodes.builder()
            .add(coordinatorNode)
            .add(ownerNode)
            .localNodeId(coordinatorNode.getId())
            .build();
        deliverNodes(coordinatorService, bothNodes);
        deliverNodes(ownerService, bothNodes);

        remoteKey = findKeyOwnedBy(ownerNode, coordinatorService, ownerService);
    }

    private WorkloadGroupSharedThrottleService newService(
        ClusterService clusterService,
        MockTransportService transport,
        TimeValue timeout
    ) {
        return new WorkloadGroupSharedThrottleService(clusterService, threadPool, transport, timeout);
    }

    // Finds a bucket key that every given service's ring maps to the remote owner, so acquireAsync takes the transport
    // path (and the owner does not refuse it as not_owner).
    private static String findKeyOwnedBy(DiscoveryNode owner, WorkloadGroupSharedThrottleService... services) {
        for (int i = 0; i < 10000; i++) {
            String candidate = "bucket-" + i;
            boolean ownedByAll = true;
            for (WorkloadGroupSharedThrottleService service : services) {
                DiscoveryNode candidateOwner = service.ring().ownerFor(candidate).orElse(null);
                ownedByAll &= candidateOwner != null && candidateOwner.getId().equals(owner.getId());
            }
            if (ownedByAll) {
                return candidate;
            }
        }
        throw new AssertionError("could not find a bucket owned by the remote owner node");
    }

    @Override
    public void tearDown() throws Exception {
        coordinatorTransport.close();
        ownerTransport.close();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    // Like a real applier, the previous state holds the nodes the service's ring was built from, so nodesChanged() is
    // true exactly when the membership differs (including a change to an empty node set).
    private void deliverNodes(WorkloadGroupSharedThrottleService service, DiscoveryNodes nodes) {
        DiscoveryNodes.Builder previousNodes = DiscoveryNodes.builder();
        service.ring().eligibleNodeSet().forEach(previousNodes::add);
        deliverNodes(service, previousNodes.build(), nodes);
    }

    private void deliverNodes(WorkloadGroupSharedThrottleService service, DiscoveryNodes previousNodes, DiscoveryNodes nodes) {
        ClusterState previous = mock(ClusterState.class);
        when(previous.nodes()).thenReturn(previousNodes);
        ClusterState current = mock(ClusterState.class);
        when(current.nodes()).thenReturn(nodes);
        service.clusterChanged(new ClusterChangedEvent("test", current, previous));
    }

    /**
     * Neither handler may trip the in-flight breaker: one owner's heap pressure would otherwise fail every bucket it owns
     * closed, and a breaker-rejected RELEASE would hold its permit until the TTL.
     */
    public void testAcquireAndReleaseHandlersCannotTripCircuitBreaker() {
        for (String action : List.of(
            WorkloadGroupSharedThrottleService.ACQUIRE_ACTION_NAME,
            WorkloadGroupSharedThrottleService.RELEASE_ACTION_NAME
        )) {
            RequestHandlerRegistry<? extends TransportRequest> handler = ownerTransport.getRequestHandler(action);
            assertNotNull("handler must be registered for " + action, handler);
            assertFalse(action + " must not trip the in-flight circuit breaker", handler.canTripCircuitBreaker());
        }
    }

    /**
     * Happy path across nodes: the coordinator asks the remote owner for a permit, the owner grants it over the wire,
     * and closing the returned {@link Releasable} sends the fire-and-forget RELEASE RPC that drains the owner's tracker.
     */
    public void testRemoteAcquireGrantedThenReleaseRoundTrips() throws Exception {
        CapturingListener listener = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, false, listener);
        listener.await();

        assertNull("granted acquire must not fail", listener.failure.get());
        assertTrue("listener must have been notified via onResponse", listener.responded.get());
        Releasable permit = listener.response.get();
        assertNotNull("remote owner granted a permit, so a non-null Releasable is expected", permit);
        assertOnSearchThread(listener);
        // The grant was recorded on the OWNER node's tracker (the acquire handler ran there before responding).
        assertEquals("owner tracker must hold exactly one in-flight permit", 1, ownerService.tracker().inFlight(remoteKey));

        // Closing the permit fires the RELEASE RPC (fire-and-forget), which the owner applies asynchronously.
        permit.close();
        assertBusy(() -> assertEquals("release RPC must drain the owner's in-flight count", 0, ownerService.tracker().inFlight(remoteKey)));
    }

    /**
     * A coordinator that leaves the cluster never releases what it holds; the owner purges its permits on the node
     * removal instead of pinning the slots until the TTL.
     */
    public void testOwnerPurgesPermitsOfDepartedCoordinator() throws Exception {
        CapturingListener listener = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, false, listener);
        listener.await();
        assertNotNull("the remote grant must be delivered, failure: " + listener.failure.get(), listener.response.get());
        assertEquals(1, ownerService.tracker().inFlight(remoteKey));

        DiscoveryNodes both = DiscoveryNodes.builder()
            .add(coordinatorTransport.getLocalDiscoNode())
            .add(ownerNode)
            .localNodeId(ownerNode.getId())
            .build();
        deliverNodes(ownerService, both, DiscoveryNodes.builder().add(ownerNode).localNodeId(ownerNode.getId()).build());
        assertEquals("the departed coordinator's permit must be purged", 0, ownerService.tracker().inFlight(remoteKey));
    }

    /**
     * An owner at its shared limit denies over the wire, and a caller that rejects on denial gets the denial inline on the
     * response thread (see {@code acquireAsync}).
     */
    public void testRemoteAcquireDeniedReturns429() throws Exception {
        // Pre-fill the owner's tracker to the limit directly so the next acquire is denied at the source.
        assertTrue(
            ownerService.tracker()
                .tryAcquire(
                    remoteKey,
                    1,
                    "pre",
                    WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS,
                    SharedThrottleTracker.UNKNOWN_COORDINATOR
                )
        );
        assertEquals(1, ownerService.tracker().inFlight(remoteKey));

        CapturingListener listener = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, false, listener);
        listener.await();

        assertFalse("denied acquire must not invoke onResponse", listener.responded.get());
        assertTrue("must be the shared-limit denial marker", WorkloadGroupSharedThrottleService.isDenial(listener.failure.get()));
        assertNotOnSearchThread(listener);
        assertTrue(
            "denial must be an OpenSearchRejectedExecutionException (429), was: " + listener.failure.get(),
            listener.failure.get() instanceof OpenSearchRejectedExecutionException
        );
    }

    /** A caller that proceeds on denial (MONITOR) continues the search from the callback, so it gets it on the search pool. */
    public void testRemoteDenialForCallerThatProceedsIsDeliveredOnSearch() throws Exception {
        assertTrue(
            ownerService.tracker()
                .tryAcquire(
                    remoteKey,
                    1,
                    "pre",
                    WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS,
                    SharedThrottleTracker.UNKNOWN_COORDINATOR
                )
        );

        CapturingListener listener = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, true, listener);
        listener.await();

        assertTrue("must be the shared-limit denial marker", WorkloadGroupSharedThrottleService.isDenial(listener.failure.get()));
        assertOnSearchThread(listener);
    }

    /**
     * Mid-rebalance the coordinator's ring can still name a node that no longer owns the bucket. That node refuses with
     * not_owner, and the coordinator must treat it as the shared tier being unavailable (fail closed), not as a denial.
     */
    public void testNotOwnerReplyIsUnavailable() throws Exception {
        // The owner node's own ring now says the coordinator owns every bucket, so it is a former owner for remoteKey.
        deliverNodes(
            ownerService,
            DiscoveryNodes.builder().add(coordinatorTransport.getLocalDiscoNode()).localNodeId(ownerNode.getId()).build()
        );

        CapturingListener listener = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, false, listener);
        listener.await();

        assertFalse("a not_owner reply must not admit", listener.responded.get());
        assertTrue(
            "a not_owner reply must report the shared tier unavailable, was: " + listener.failure.get(),
            WorkloadGroupSharedThrottleService.isUnavailable(listener.failure.get())
        );
        assertEquals("the former owner must not have recorded a permit", 0, ownerService.tracker().inFlight(remoteKey));
        assertNotOnSearchThread(listener);
    }

    /**
     * An acquire that fails at send time fails closed through {@code handleException} (the owner stays connected, so the
     * {@code nodeConnected} pre-check passes) and sends no release.
     */
    public void testUnavailableWhenOwnerTransportFails() throws Exception {
        assertTrue(
            "precondition: owner must be connected so we exercise the send path, not the pre-check",
            coordinatorTransport.nodeConnected(ownerNode)
        );
        AtomicInteger releasesSent = new AtomicInteger();
        coordinatorTransport.addSendBehavior(ownerTransport, (connection, requestId, action, request, options) -> {
            if (WorkloadGroupSharedThrottleService.ACQUIRE_ACTION_NAME.equals(action)) {
                // Simulate the send blowing up; TransportService routes this to the handler's handleException.
                throw new ConnectTransportException(connection.getNode(), "simulated acquire send failure");
            }
            if (WorkloadGroupSharedThrottleService.RELEASE_ACTION_NAME.equals(action)) {
                releasesSent.incrementAndGet();
            }
            connection.sendRequest(requestId, action, request, options);
        });

        CapturingListener listener = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, false, listener);
        listener.await();

        assertFalse("a transport error must not admit the request", listener.responded.get());
        assertTrue(
            "a transport error must report the shared tier unavailable, was: " + listener.failure.get(),
            WorkloadGroupSharedThrottleService.isUnavailable(listener.failure.get())
        );
        assertEquals("listener must be notified exactly once", 1, listener.invocations.get());
        // handleException sends any release before it notifies the listener, so this is already final.
        assertEquals("a failure that never reached the owner must not send a release", 0, releasesSent.get());
        assertEquals(0, ownerService.tracker().inFlight(remoteKey));
    }

    /**
     * The owner records the grant but answers with an error, so the coordinator gets a {@code RemoteTransportException}.
     * The owner provably processed the acquire, so the coordinator reports the shared tier unavailable once and releases
     * the permit (ordered after the grant) instead of leaving it to the TTL.
     */
    public void testRemoteErrorAfterOwnerGrantedReleasesThePermit() throws Exception {
        AtomicReference<TransportResponse> recordedReply = new AtomicReference<>();
        ownerTransport.addRequestHandlingBehavior(
            WorkloadGroupSharedThrottleService.ACQUIRE_ACTION_NAME,
            (handler, request, channel, task) -> {
                handler.messageReceived(
                    request,
                    new TestTransportChannel(ActionListener.wrap(recordedReply::set, e -> fail("the owner handler must not fail: " + e))),
                    task
                );
                channel.sendResponse(new IllegalStateException("simulated failure after the grant was recorded"));
            }
        );

        CapturingListener listener = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, false, listener);
        listener.await();

        assertTrue(
            "the owner must have granted (and recorded) the acquire",
            ((WorkloadGroupSharedThrottleService.AcquirePermitResponse) recordedReply.get()).granted
        );
        assertFalse("a remote error must not admit the request", listener.responded.get());
        assertTrue(
            "a remote error must report the shared tier unavailable, was: " + listener.failure.get(),
            WorkloadGroupSharedThrottleService.isUnavailable(listener.failure.get())
        );
        assertBusy(() -> assertEquals("the recorded grant must be released", 0, ownerService.tracker().inFlight(remoteKey)));
        assertEquals("listener must be notified exactly once", 1, listener.invocations.get());
    }

    /**
     * An acquire the owner never answers must fail closed once the acquire timeout elapses: an unavailable
     * {@code onFailure}, exactly once. The coordinator's timer decides the outcome, so no RELEASE is sent at the timeout:
     * the acquire may still be in flight, and a release could overtake it.
     */
    public void testUnavailableWhenAcquireTimesOut() throws Exception {
        withShortTimeoutCoordinator((transport, service, key) -> {
            AtomicBoolean acquireSwallowed = new AtomicBoolean();
            AtomicInteger releasesSent = new AtomicInteger();
            transport.addSendBehavior(ownerTransport, (connection, requestId, action, request, options) -> {
                if (WorkloadGroupSharedThrottleService.ACQUIRE_ACTION_NAME.equals(action)) {
                    acquireSwallowed.set(true); // never delivered, never answered
                    return;
                }
                if (WorkloadGroupSharedThrottleService.RELEASE_ACTION_NAME.equals(action)) {
                    releasesSent.incrementAndGet();
                }
                connection.sendRequest(requestId, action, request, options);
            });

            CapturingListener listener = new CapturingListener();
            service.acquireAsync(key, 1, false, listener);
            listener.await();

            assertTrue("the ACQUIRE must have been sent (and swallowed)", acquireSwallowed.get());
            assertFalse("a timeout must not admit the request", listener.responded.get());
            assertTrue(
                "a timeout must report the shared tier unavailable, was: " + listener.failure.get(),
                WorkloadGroupSharedThrottleService.isUnavailable(listener.failure.get())
            );
            assertFalse(
                "listener must be notified exactly once",
                waitUntil(() -> listener.invocations.get() > 1, 500, TimeUnit.MILLISECONDS)
            );
            assertEquals("a timeout must not send a release", 0, releasesSent.get());
            assertEquals("the owner never saw the acquire, so it holds nothing", 0, ownerService.tracker().inFlight(key));
        });
    }

    /**
     * The acquire reaches the owner only after the coordinator's timer reported it unavailable (a stalled owner). No
     * release may be sent at the timeout, since it would arrive first and strand the later grant; the late grant reply
     * triggers the release instead.
     */
    public void testLateGrantAfterTimeoutIsReleasedAfterTheGrant() throws Exception {
        withShortTimeoutCoordinator((transport, service, key) -> {
            // Hold the whole owner-side handling (acquire + reply) until the coordinator has given up on it.
            CountDownLatch acquireArrived = new CountDownLatch(1);
            AtomicReference<CheckedRunnable<Exception>> heldAcquire = new AtomicReference<>();
            AtomicBoolean acquireProcessed = new AtomicBoolean();
            AtomicInteger releasesBeforeAcquire = new AtomicInteger();
            ownerTransport.addRequestHandlingBehavior(
                WorkloadGroupSharedThrottleService.ACQUIRE_ACTION_NAME,
                (handler, request, channel, task) -> {
                    heldAcquire.set(() -> {
                        handler.messageReceived(request, channel, task);
                        acquireProcessed.set(true);
                    });
                    acquireArrived.countDown();
                }
            );
            ownerTransport.addRequestHandlingBehavior(
                WorkloadGroupSharedThrottleService.RELEASE_ACTION_NAME,
                (handler, request, channel, task) -> {
                    if (acquireProcessed.get() == false) {
                        releasesBeforeAcquire.incrementAndGet();
                    }
                    handler.messageReceived(request, channel, task);
                }
            );

            AtomicReference<WorkloadGroupSharedThrottleService.RemoteAcquire> sent = new AtomicReference<>();
            service.beforeRemoteAcquireSent = sent::set;

            CapturingListener listener = new CapturingListener();
            service.acquireAsync(key, 1, false, listener);
            listener.await();
            assertTrue(
                "the timer must report the shared tier unavailable, was: " + listener.failure.get(),
                WorkloadGroupSharedThrottleService.isUnavailable(listener.failure.get())
            );
            assertTrue("the acquire must have reached the owner", acquireArrived.await(10, TimeUnit.SECONDS));
            // The handler stays registered for the late reply, but must no longer pin the answered request's listener.
            assertFalse("a decided acquire must drop the caller's listener", sent.get().holdsListener());
            assertFalse(
                "no release may reach the owner ahead of the acquire it would reclaim",
                waitUntil(() -> releasesBeforeAcquire.get() > 0, 500, TimeUnit.MILLISECONDS)
            );

            // The owner now grants, and its reply reaches the coordinator after the timeout.
            heldAcquire.get().run();
            assertTrue(acquireProcessed.get());
            assertBusy(() -> assertEquals("the late grant must be released", 0, ownerService.tracker().inFlight(key)));
            assertEquals("listener must be notified exactly once", 1, listener.invocations.get());
            assertFalse(listener.responded.get());
        });
    }

    /**
     * A reply that beats the timer decides the outcome: a normal grant, the timer is cancelled, and the timer firing
     * anyway afterwards must neither notify again nor release the permit the caller now holds.
     */
    public void testReplyBeforeTimeoutIsDeliveredOnce() throws Exception {
        AtomicReference<WorkloadGroupSharedThrottleService.RemoteAcquire> sent = new AtomicReference<>();
        coordinatorService.beforeRemoteAcquireSent = sent::set;

        CapturingListener listener = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, false, listener);
        listener.await();
        assertNotNull("a prompt grant must be delivered, failure: " + listener.failure.get(), listener.response.get());

        WorkloadGroupSharedThrottleService.RemoteAcquire acquire = sent.get();
        assertTrue("the reply must cancel the acquire timer", acquire.timer().isCancelled());
        assertFalse("a decided acquire must drop the caller's listener", acquire.holdsListener());
        // Simulate the timer having already fired when the reply decided: it must be a no-op.
        acquire.onTimeout();
        assertEquals("a grant must not be followed by a second notification", 1, listener.invocations.get());
        assertEquals("the delivered permit still holds its slot", 1, ownerService.tracker().inFlight(remoteKey));
        listener.response.get().close();
        assertBusy(() -> assertEquals(0, ownerService.tracker().inFlight(remoteKey)));
    }

    private interface ShortTimeoutBody {
        void run(MockTransportService transport, WorkloadGroupSharedThrottleService service, String key) throws Exception;
    }

    // Runs body against a dedicated coordinator with a short acquire timeout; key is owned by the remote owner node.
    private void withShortTimeoutCoordinator(ShortTimeoutBody body) throws Exception {
        final TimeValue acquireTimeout = TimeValue.timeValueMillis(50);
        try (
            MockTransportService transport = MockTransportService.createNewService(
                Settings.EMPTY,
                Version.CURRENT,
                threadPool,
                NoopTracer.INSTANCE
            )
        ) {
            transport.start();
            transport.acceptIncomingRequests();
            transport.connectToNode(ownerNode);
            DiscoveryNode node = transport.getLocalDiscoNode();
            ClusterService clusterService = mock(ClusterService.class);
            when(clusterService.localNode()).thenReturn(node);
            WorkloadGroupSharedThrottleService service = newService(clusterService, transport, acquireTimeout);
            DiscoveryNodes nodes = DiscoveryNodes.builder().add(node).add(ownerNode).localNodeId(node.getId()).build();
            deliverNodes(service, nodes);
            // The owner must know this coordinator too, both to agree on the ring and so its removal can be observed.
            deliverNodes(ownerService, nodes);
            String key = findKeyOwnedBy(ownerNode, service, ownerService);
            body.run(transport, service, key);
        }
    }

    /**
     * If the search pool rejects the hand-off of a granted acquire, the coordinator must release the shared permit it was
     * just granted and surface the pool's own rejection (a 429), never the shared-limit denial marker.
     */
    public void testSearchPoolRejectionReleasesGrantedPermit() throws Exception {
        withSaturatedSearchPool((service, key) -> {
            CapturingListener listener = new CapturingListener();
            service.acquireAsync(key, 1, false, listener);
            listener.await();

            assertFalse("a rejected hand-off must not admit the request", listener.responded.get());
            assertTrue(
                "the pool's rejection must surface, was: " + listener.failure.get(),
                listener.failure.get() instanceof OpenSearchRejectedExecutionException
            );
            assertFalse(
                "a pool rejection is not a shared-limit denial",
                WorkloadGroupSharedThrottleService.isDenial(listener.failure.get())
            );
            assertBusy(() -> assertEquals("the granted permit must be released", 0, ownerService.tracker().inFlight(key)));
        });
    }

    /**
     * A caller that proceeds on every outcome (MONITOR) must never be rejected by the shared tier: when the pool rejects
     * the hand-off of a grant, the grant is delivered inline instead.
     */
    public void testSearchPoolRejectionDeliversGrantInlineForCallerThatProceeds() throws Exception {
        withSaturatedSearchPool((service, key) -> {
            CapturingListener listener = new CapturingListener();
            service.acquireAsync(key, 1, true, listener);
            listener.await();

            assertNull("the grant must not be turned into a failure, was: " + listener.failure.get(), listener.failure.get());
            assertNotNull("the granted permit must be delivered", listener.response.get());
            assertEquals("the delivered permit still holds its slot", 1, ownerService.tracker().inFlight(key));
            listener.response.get().close();
            assertBusy(() -> assertEquals(0, ownerService.tracker().inFlight(key)));
        });
    }

    /** With the search pool saturated, a denial for a caller that proceeds (MONITOR) is delivered inline, not lost to the pool. */
    public void testSearchPoolRejectionDeliversDenialInlineForCallerThatProceeds() throws Exception {
        withSaturatedSearchPool((service, key) -> {
            assertTrue(
                ownerService.tracker()
                    .tryAcquire(
                        key,
                        1,
                        "pre",
                        WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS,
                        SharedThrottleTracker.UNKNOWN_COORDINATOR
                    )
            );
            CapturingListener listener = new CapturingListener();
            service.acquireAsync(key, 1, true, listener);
            listener.await();

            assertTrue(
                "the denial must survive the pool rejection, was: " + listener.failure.get(),
                WorkloadGroupSharedThrottleService.isDenial(listener.failure.get())
            );
            assertNotOnSearchThread(listener);
        });
    }

    /** With the search pool saturated, a denial for a caller that rejects on denial is still the denial marker. */
    public void testSaturatedSearchPoolDoesNotMaskDenial() throws Exception {
        withSaturatedSearchPool((service, key) -> {
            assertTrue(
                ownerService.tracker()
                    .tryAcquire(
                        key,
                        1,
                        "pre",
                        WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS,
                        SharedThrottleTracker.UNKNOWN_COORDINATOR
                    )
            );
            CapturingListener listener = new CapturingListener();
            service.acquireAsync(key, 1, false, listener);
            listener.await();

            assertTrue(
                "a denial must not become a pool rejection, was: " + listener.failure.get(),
                WorkloadGroupSharedThrottleService.isDenial(listener.failure.get())
            );
        });
    }

    private interface CoordinatorBody {
        void run(WorkloadGroupSharedThrottleService service, String key) throws Exception;
    }

    // Runs body against a fresh coordinator whose search pool has its only thread and only queue slot occupied, so any
    // hand-off to it is rejected. key is owned by the remote owner node.
    private void withSaturatedSearchPool(CoordinatorBody body) throws Exception {
        Settings oneSlotSearchPool = Settings.builder().put("thread_pool.search.size", 1).put("thread_pool.search.queue_size", 1).build();
        ThreadPool saturatedPool = new TestThreadPool("saturated-search", oneSlotSearchPool);
        CountDownLatch unblock = new CountDownLatch(1);
        try (
            MockTransportService transport = MockTransportService.createNewService(
                Settings.EMPTY,
                Version.CURRENT,
                threadPool,
                NoopTracer.INSTANCE
            )
        ) {
            transport.start();
            transport.acceptIncomingRequests();
            transport.connectToNode(ownerNode);
            DiscoveryNode node = transport.getLocalDiscoNode();
            ClusterService clusterService = mock(ClusterService.class);
            when(clusterService.localNode()).thenReturn(node);
            WorkloadGroupSharedThrottleService service = new WorkloadGroupSharedThrottleService(
                clusterService,
                saturatedPool,
                transport,
                GENEROUS_ACQUIRE_TIMEOUT
            );
            deliverNodes(service, DiscoveryNodes.builder().add(node).add(ownerNode).localNodeId(node.getId()).build());
            String key = findKeyOwnedBy(ownerNode, service, ownerService);

            // Occupy the only search thread, then the only queue slot, so the next hand-off is rejected.
            CountDownLatch running = new CountDownLatch(1);
            saturatedPool.executor(ThreadPool.Names.SEARCH).execute(() -> {
                running.countDown();
                try {
                    unblock.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            assertTrue(running.await(10, TimeUnit.SECONDS));
            saturatedPool.executor(ThreadPool.Names.SEARCH).execute(() -> {});

            body.run(service, key);
        } finally {
            unblock.countDown();
            ThreadPool.terminate(saturatedPool, 10, TimeUnit.SECONDS);
        }
    }

    /**
     * A denial with the coordinator reachable from the owner: the owner registers it as a waiter and reports
     * {@code registered=true}, so the coordinator surfaces the denial marker (its retained request then waits for
     * owner-push). The no-waiter callback must NOT fire.
     */
    public void testDeniedRegistersWaiterWhenCoordinatorReachableFromOwner() throws Exception {
        ownerSees(coordinatorNode);
        ownerTransport.connectToNode(coordinatorNode);
        fillOwnerToLimit();

        CapturingListener listener = new CapturingListener();
        AtomicBoolean noWaiter = new AtomicBoolean(false);
        coordinatorService.acquireAsync(remoteKey, 1, false, listener, () -> noWaiter.set(true));
        listener.await();

        assertFalse("owner could register us, so the no-waiter path must not fire", noWaiter.get());
        assertTrue("denial must surface as the denial marker, was: " + listener.failure.get(), isDenial(listener));
        // A queueing caller never proceeds on denial, so the (registered) denial is delivered inline: it only moves the
        // retained entry to WAITING.
        assertNotOnSearchThread(listener);
        assertEquals("owner must have registered the coordinator as a waiter", 1, ownerService.waiterCountForTest(remoteKey));
    }

    /**
     * The JOIN race: the coordinator's acquire reaches the owner before the owner has applied the cluster state that
     * contains it. The owner cannot address it, so it must report {@code registered=false} — otherwise the coordinator's
     * retained request would wait for a grant that never comes, with no deadline to rescue it.
     */
    public void testDeniedReportsNoWaiterWhenCoordinatorAbsentFromOwnerState() throws Exception {
        ownerSees(); // owner's applied state does not contain the coordinator yet
        fillOwnerToLimit();

        CapturingListener listener = new CapturingListener();
        AtomicReference<String> callbackThread = new AtomicReference<>();
        CountDownLatch noWaiterLatch = new CountDownLatch(1);
        coordinatorService.acquireAsync(remoteKey, 1, false, listener, () -> {
            callbackThread.set(Thread.currentThread().getName());
            noWaiterLatch.countDown();
        });

        assertTrue("the no-waiter callback must fire", noWaiterLatch.await(10, TimeUnit.SECONDS));
        assertFalse(
            "the no-waiter outcome only rejects the retained request, so it is delivered inline, was [" + callbackThread.get() + "]",
            callbackThread.get().contains("[" + ThreadPool.Names.SEARCH + "]")
        );
        assertFalse("the listener must not be completed on this path", listener.responded.get());
        assertNull("the no-waiter callback replaces the denial, it does not add to it", listener.failure.get());
        assertEquals("no waiter can be registered for a node the owner cannot address", 0, ownerService.waiterCountForTest(remoteKey));
    }

    /**
     * The RESTART case, which a presence-only check would get WRONG: the owner's applied state still holds the PREVIOUS
     * incarnation of the coordinator — same persistent node id, different ephemeral id — so {@code nodes().get(id)}
     * returns non-null. The reachability check must catch it and report {@code registered=false}.
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
        assertNotEquals("precondition: DiscoveryNode identity is the ephemeral id", coordinatorNode, staleCoordinator);

        ownerSees(staleCoordinator);
        // Even a live connection to the CURRENT coordinator must not make the stale entry look reachable.
        ownerTransport.connectToNode(coordinatorNode);
        fillOwnerToLimit();

        CapturingListener listener = new CapturingListener();
        CountDownLatch noWaiterLatch = new CountDownLatch(1);
        coordinatorService.acquireAsync(remoteKey, 1, false, listener, noWaiterLatch::countDown);

        assertTrue("a stale-identity resolve must be reported as not registered", noWaiterLatch.await(10, TimeUnit.SECONDS));
        assertEquals("a stale node must never be registered as a waiter", 0, ownerService.waiterCountForTest(remoteKey));
    }

    /**
     * The NON-queueing path passes a null no-waiter callback, so {@code wantsQueue} is false. The coordinator must fall
     * through to the plain denial marker and must NOT dereference the null Runnable.
     */
    public void testNonQueueingDenialDoesNotTouchTheNullCallback() throws Exception {
        ownerSees(); // owner cannot see the coordinator, so registered would be false if it were even consulted
        fillOwnerToLimit();

        CapturingListener listener = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, false, listener); // 4-arg overload => null callback, wantsQueue=false
        listener.await();

        assertFalse("must not admit", listener.responded.get());
        assertTrue("non-queueing denial must surface the denial marker, was: " + listener.failure.get(), isDenial(listener));
        assertEquals("wantsQueue=false must never register a waiter", 0, ownerService.waiterCountForTest(remoteKey));
    }

    /**
     * Fail-closed for the queueing overload: a transport failure reports the shared tier unavailable through the listener.
     * It must never be mistaken for an unregistered denial (the no-waiter callback) and never register a waiter.
     */
    public void testQueueingAcquireTransportFailureIsUnavailable() throws Exception {
        ownerSees(coordinatorNode);
        coordinatorTransport.addSendBehavior(ownerTransport, (connection, requestId, action, request, options) -> {
            if (WorkloadGroupSharedThrottleService.ACQUIRE_ACTION_NAME.equals(action)) {
                throw new ConnectTransportException(connection.getNode(), "simulated acquire send failure");
            }
            connection.sendRequest(requestId, action, request, options);
        });

        CapturingListener listener = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, false, listener, () -> fail("an unavailable owner is not a denial"));
        listener.await();

        assertTrue(WorkloadGroupSharedThrottleService.isUnavailable(listener.failure.get()));
        assertEquals(0, ownerService.waiterCountForTest(remoteKey));
    }

    /**
     * A queueing acquire decided by the coordinator's timer is unavailable (its PENDING_ACQUIRE entry is rejected, never
     * parked), and a grant arriving after the timer is released rather than admitted.
     */
    public void testQueueingAcquireTimeoutIsUnavailableAndLateGrantIsReleased() throws Exception {
        withShortTimeoutCoordinator((transport, service, key) -> {
            CountDownLatch acquireArrived = new CountDownLatch(1);
            AtomicReference<CheckedRunnable<Exception>> heldAcquire = new AtomicReference<>();
            ownerTransport.addRequestHandlingBehavior(
                WorkloadGroupSharedThrottleService.ACQUIRE_ACTION_NAME,
                (handler, request, channel, task) -> {
                    heldAcquire.set(() -> handler.messageReceived(request, channel, task));
                    acquireArrived.countDown();
                }
            );

            CapturingListener listener = new CapturingListener();
            service.acquireAsync(key, 1, false, listener, () -> fail("a timed-out acquire is not an unregistered denial"));
            listener.await();
            assertTrue(
                "the timer must report the shared tier unavailable, was: " + listener.failure.get(),
                WorkloadGroupSharedThrottleService.isUnavailable(listener.failure.get())
            );
            assertTrue("the acquire must have reached the owner", acquireArrived.await(10, TimeUnit.SECONDS));

            heldAcquire.get().run();
            assertBusy(() -> assertEquals("the late grant must be released", 0, ownerService.tracker().inFlight(key)));
            assertEquals("listener must be notified exactly once", 1, listener.invocations.get());
            assertFalse("a late grant must never be admitted", listener.responded.get());
        });
    }

    /**
     * An unregistered denial must still reach a terminal outcome when the coordinator's search pool is saturated: like
     * any refusal of a caller that does not proceed on denial, it is delivered inline (never handed to the pool), so the
     * no-waiter callback runs and a queueing caller's retained request cannot strand. The listener is untouched.
     */
    public void testSaturatedSearchPoolDoesNotMaskUnregisteredDenial() throws Exception {
        withSaturatedSearchPool((service, key) -> {
            assertTrue(
                ownerService.tracker()
                    .tryAcquire(
                        key,
                        1,
                        "pre",
                        WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS,
                        SharedThrottleTracker.UNKNOWN_COORDINATOR
                    )
            );
            ownerSees(); // the owner cannot address this coordinator, so the denial is unregistered

            CapturingListener listener = new CapturingListener();
            CountDownLatch noWaiter = new CountDownLatch(1);
            AtomicReference<String> callbackThread = new AtomicReference<>();
            service.acquireAsync(key, 1, false, listener, () -> {
                callbackThread.set(Thread.currentThread().getName());
                noWaiter.countDown();
            });

            assertTrue("the no-waiter callback must run despite the saturated pool", noWaiter.await(10, TimeUnit.SECONDS));
            assertFalse(
                "the no-waiter callback must be delivered inline, not on the search pool, was [" + callbackThread.get() + "]",
                callbackThread.get().contains("[" + ThreadPool.Names.SEARCH + "]")
            );
            assertEquals("the callback replaces the listener outcome", 1, listener.latch.getCount());
            assertEquals(0, ownerService.waiterCountForTest(key));
        });
    }

    /**
     * A waiter registered while connected, that then disconnects before a slot frees. The owner must reclaim the reserved
     * slot (no permit leak), drop the dead waiter, and terminate its bounded drain loop.
     */
    public void testDisconnectAfterRegistrationReclaimsSlotAndDropsWaiter() throws Exception {
        ownerSeesWithGroup(coordinatorNode);
        ownerTransport.connectToNode(coordinatorNode);
        fillOwnerToLimit();

        CapturingListener listener = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, false, listener, () -> fail("owner could see and reach us, so it must register"));
        listener.await();
        assertEquals("waiter registered", 1, ownerService.waiterCountForTest(remoteKey));
        assertEquals("the pre-filled permit still holds the only slot", 1, ownerService.tracker().inFlight(remoteKey));

        ownerTransport.disconnectFromNode(coordinatorNode);
        assertFalse("precondition: owner must no longer see a connection", ownerTransport.nodeConnected(coordinatorNode));

        ownerService.handleRelease(new WorkloadGroupSharedThrottleService.ReleasePermitRequest(remoteKey, "pre"));

        assertEquals("the reserved slot must be reclaimed, not leaked until TTL", 0, ownerService.tracker().inFlight(remoteKey));
        assertEquals("the disconnected waiter must be dropped", 0, ownerService.waiterCountForTest(remoteKey));
    }

    public void testOnlyOneGrantAwaitsResponsePerCoordinatorBucket() throws Exception {
        ownerSeesWithGroup(coordinatorNode);
        ownerTransport.connectToNode(coordinatorNode);
        fillOwnerToLimit();

        CapturingListener denial = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, false, denial, () -> fail("the reachable coordinator must be registered"));
        denial.await();
        assertTrue(isDenial(denial));
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

        // A fresh denial can idempotently register the same coordinator while its GRANT is pending. It must not permit a
        // second owner -> coordinator GRANT before the first response resolves.
        WorkloadGroupSharedThrottleService.AcquirePermitResponse reRegistration = ownerService.handleAcquire(
            new WorkloadGroupSharedThrottleService.AcquirePermitRequest(
                remoteKey,
                2,
                "re-register-while-pending",
                WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS,
                coordinatorNode.getEphemeralId(),
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

        // Closing the connection resolves the unacknowledged send immediately. The failure path must remove the waiter
        // before reclaiming and redistributing the permit.
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
        assertTrue(
            ownerService.tracker()
                .tryAcquire(
                    remoteKey,
                    1,
                    "new-holder",
                    WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS,
                    SharedThrottleTracker.UNKNOWN_COORDINATOR
                )
        );
        CapturingListener secondDenial = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, false, secondDenial, () -> fail("the recovered coordinator must be re-registered"));
        secondDenial.await();
        assertTrue(isDenial(secondDenial));
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
        final WorkloadGroupSharedThrottleService healthyService = newService(
            healthyClusterService,
            healthyTransport,
            GENEROUS_ACQUIRE_TIMEOUT
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
            coordinatorService.acquireAsync(
                remoteKey,
                1,
                false,
                failedWaiterDenial,
                () -> fail("the first coordinator must be registered")
            );
            failedWaiterDenial.await();
            assertTrue(isDenial(failedWaiterDenial));

            CapturingListener healthyWaiterDenial = new CapturingListener();
            healthyService.acquireAsync(remoteKey, 1, false, healthyWaiterDenial, () -> fail("the healthy coordinator must be registered"));
            healthyWaiterDenial.await();
            assertTrue(isDenial(healthyWaiterDenial));
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
        coordinatorService.acquireAsync(remoteKey, 1, false, denial, () -> fail("the reachable coordinator must be registered"));
        denial.await();
        assertTrue(isDenial(denial));

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
                coordinatorNode.getEphemeralId(),
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
        coordinatorService.acquireAsync(remoteKey, 1, false, denial, () -> fail("the reachable coordinator must be registered"));
        denial.await();
        assertTrue(isDenial(denial));

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
                coordinatorNode.getEphemeralId(),
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
        coordinatorService.acquireAsync(remoteKey, 1, false, denial, () -> fail("the reachable coordinator must be registered"));
        denial.await();
        assertTrue(isDenial(denial));
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

        // The accepted cost of an ambiguous timeout: the late original GRANT can admit one request whose permit the owner
        // has already reclaimed (a bounded, one-request over-admission, not an unavailable-tier admission).
        assertBusy(() -> assertEquals("the delayed original GRANT may admit exactly once", 1, lateAdmissions.get()), 20, TimeUnit.SECONDS);
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
        coordinatorService.acquireAsync(remoteKey, 1, false, denial, () -> fail("the reachable coordinator must be registered"));
        denial.await();
        assertTrue(isDenial(denial));
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

    public void testRefusedReRegistrationRejectsRetainedWaiters() throws Exception {
        // The owner does not see the coordinator in its applied state, so it cannot register (or ever push to) it.
        ownerSeesWithGroup();
        Set<String> unavailable = ConcurrentHashMap.newKeySet();
        coordinatorService.setSharedTierUnavailableHandler(unavailable::add);

        coordinatorService.reconcileRetainedWaiters(remoteKey);

        assertBusy(() -> assertEquals("a refused registration leaves no wake-up, so fail closed", Set.of(remoteKey), unavailable));
        assertEquals(0, ownerService.waiterCountForTest(remoteKey));
    }

    public void testFailedReRegistrationRpcRejectsRetainedWaiters() throws Exception {
        ownerSeesWithGroup(coordinatorNode);
        coordinatorTransport.addSendBehavior(ownerTransport, (connection, requestId, action, request, options) -> {
            if (WorkloadGroupSharedThrottleService.RE_REGISTER_WAITER_AFTER_OWNER_CHANGE_ACTION_NAME.equals(action)) {
                throw new ConnectTransportException(connection.getNode(), "simulated re-register send failure");
            }
            connection.sendRequest(requestId, action, request, options);
        });
        Set<String> unavailable = ConcurrentHashMap.newKeySet();
        coordinatorService.setSharedTierUnavailableHandler(unavailable::add);

        coordinatorService.reconcileRetainedWaiters(remoteKey);

        assertBusy(() -> assertEquals(Set.of(remoteKey), unavailable));
    }

    public void testConfirmedReRegistrationRejectsNothingAndRestoresTheWaiter() throws Exception {
        ownerSeesWithGroup(coordinatorNode);
        ownerTransport.connectToNode(coordinatorNode);
        fillOwnerToLimit(); // no free capacity, so the restored registration is not immediately consumed
        Set<String> unavailable = ConcurrentHashMap.newKeySet();
        coordinatorService.setSharedTierUnavailableHandler(unavailable::add);

        coordinatorService.reconcileRetainedWaiters(remoteKey);

        assertBusy(() -> assertEquals("the backstop restores a lost membership", 1, ownerService.waiterCountForTest(remoteKey)));
        assertTrue(unavailable.isEmpty());
    }

    public void testReleaseBetweenRemoteDenialAndRegistrationPushesAGrant() throws Exception {
        // The holder's release lands after the owner's failed tryAcquire but before it registers the coordinator, so the
        // release finds no waiter. The owner must re-check after registering and push the freed slot.
        ownerSeesWithGroup(coordinatorNode);
        ownerTransport.connectToNode(coordinatorNode);
        fillOwnerToLimit();
        AtomicInteger grants = new AtomicInteger();
        AtomicReference<Releasable> granted = new AtomicReference<>();
        coordinatorService.setGrantConsumer((bucketKey, permit) -> {
            grants.incrementAndGet();
            return granted.compareAndSet(null, permit); // one queued request; a later grant is returned unused
        });
        ownerService.afterTrackerDenial = () -> ownerService.handleRelease(
            new WorkloadGroupSharedThrottleService.ReleasePermitRequest(remoteKey, "pre")
        );

        CapturingListener denial = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, false, denial, () -> fail("the reachable coordinator must be registered"));
        denial.await();

        assertBusy(() -> assertEquals("the slot freed inside the window must be pushed to the new waiter", 1, grants.get()));
        assertEquals(1, ownerService.tracker().inFlight(remoteKey));
        granted.get().close();
        assertBusy(() -> {
            assertEquals(0, ownerService.tracker().inFlight(remoteKey));
            assertEquals("the unused follow-up grant deregisters the drained coordinator", 0, ownerService.waiterCountForTest(remoteKey));
        });
    }

    public void testRingEmptyingRejectsRetainedWaiters() throws Exception {
        Set<String> unavailable = ConcurrentHashMap.newKeySet();
        coordinatorService.setSharedTierUnavailableHandler(unavailable::add);
        coordinatorService.setRetainedBucketKeysSupplier(() -> Set.of(remoteKey));
        ClusterState noOwners = stateWithGroup(DiscoveryNodes.EMPTY_NODES);
        when(coordinatorClusterService.state()).thenReturn(noOwners);

        deliverNodes(coordinatorService, DiscoveryNodes.EMPTY_NODES);

        assertBusy(() -> assertEquals("no owner can push a grant, so fail closed", Set.of(remoteKey), unavailable));
    }

    private static boolean isDenial(CapturingListener listener) {
        return WorkloadGroupSharedThrottleService.isDenial(listener.failure.get());
    }

    /** Takes the bucket's only shared slot on the owner so the next acquire is denied at the source. */
    private void fillOwnerToLimit() {
        assertTrue(
            ownerService.tracker()
                .tryAcquire(
                    remoteKey,
                    1,
                    "pre",
                    WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS,
                    SharedThrottleTracker.UNKNOWN_COORDINATOR
                )
        );
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
                Settings.builder().put("shared_limit", sharedLimit).build()
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

    private static void assertOnSearchThread(CapturingListener listener) {
        assertTrue(
            "remote outcome must be delivered on the search pool, was [" + listener.thread.get() + "]",
            listener.thread.get().contains("[" + ThreadPool.Names.SEARCH + "]")
        );
    }

    private static void assertNotOnSearchThread(CapturingListener listener) {
        assertFalse(
            "outcome must be delivered inline, not on the search pool, was [" + listener.thread.get() + "]",
            listener.thread.get().contains("[" + ThreadPool.Names.SEARCH + "]")
        );
    }

    /** Captures the outcome of an async acquire, distinguishing onResponse(null) from an unfired listener via a latch. */
    private static class CapturingListener implements ActionListener<Releasable> {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Releasable> response = new AtomicReference<>();
        final AtomicReference<Exception> failure = new AtomicReference<>();
        final AtomicBoolean responded = new AtomicBoolean(false);
        final AtomicReference<String> thread = new AtomicReference<>();
        final AtomicInteger invocations = new AtomicInteger();

        @Override
        public void onResponse(Releasable releasable) {
            invocations.incrementAndGet();
            thread.set(Thread.currentThread().getName());
            response.set(releasable);
            responded.set(true);
            latch.countDown();
        }

        @Override
        public void onFailure(Exception e) {
            invocations.incrementAndGet();
            thread.set(Thread.currentThread().getName());
            failure.set(e);
            latch.countDown();
        }

        void await() throws InterruptedException {
            assertTrue("listener was not invoked within the timeout", latch.await(10, TimeUnit.SECONDS));
        }
    }
}
