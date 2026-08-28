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
