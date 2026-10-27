/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.Version;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchTask;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.search.backpressure.trackers.NodeDuressTrackers;
import org.opensearch.tasks.Task;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ConnectTransportException;
import org.opensearch.transport.RequestHandlerRegistry;
import org.opensearch.transport.TestTransportChannel;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;
import org.opensearch.wlm.WorkloadGroupSharedThrottleService.TaskCheckRequest;
import org.opensearch.wlm.cancellation.WorkloadGroupTaskCancellationService;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.wlm.SharedThrottleTestPermits.ADMITTED;
import static org.opensearch.wlm.SharedThrottleTestPermits.NOT_ADMITTED;
import static org.opensearch.wlm.SharedThrottleTestPermits.tryAcquire;
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
    private AtomicLong ownerClock;

    // A bucket key whose ring owner is the remote (owner) node, forcing the coordinator down the transport path.
    private String remoteKey;

    // The last ACQUIRE response handler a coordinator sent, captured by its transport interceptor.
    private final AtomicReference<WorkloadGroupSharedThrottleService.RemoteAcquire> sentAcquire = new AtomicReference<>();
    // Counted down once the owner has applied a CHECK_TASKS reply or failure; see expectTaskChecks.
    private final AtomicReference<CountDownLatch> checkRepliesHandled = new AtomicReference<>(new CountDownLatch(0));
    // Counted down once the owner has applied a GRANT reply or failure; see expectGrantReplies.
    private final AtomicReference<CountDownLatch> grantRepliesHandled = new AtomicReference<>(new CountDownLatch(0));
    // Counted down once the coordinator has applied a REGISTER_WAITER reply or failure; see expectRegisterReplies.
    private final AtomicReference<CountDownLatch> registerRepliesHandled = new AtomicReference<>(new CountDownLatch(0));

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());

        // Two real transports. Default node roles include DATA at Version.CURRENT (>= WorkloadGroupThrottleSettings.MIN_SUPPORTED_VERSION),
        // so both nodes are eligible ring owners.
        coordinatorTransport = newTransport(chain(captureAcquires(), countCoordinatorReplies()));
        ownerTransport = newTransport(countOwnerReplies());
        coordinatorTransport.start();
        coordinatorTransport.acceptIncomingRequests();
        ownerTransport.start();
        ownerTransport.acceptIncomingRequests();
        // The coordinator must be able to reach the owner for the acquire/release RPCs.
        coordinatorTransport.connectToNode(ownerTransport.getLocalDiscoNode());
        ownerTransport.connectToNode(coordinatorTransport.getLocalDiscoNode());

        coordinatorNode = coordinatorTransport.getLocalDiscoNode();
        ownerNode = ownerTransport.getLocalDiscoNode();

        // Each service sees itself as the local node.
        coordinatorClusterService = mock(ClusterService.class);
        when(coordinatorClusterService.localNode()).thenReturn(coordinatorNode);
        ownerClusterService = mock(ClusterService.class);
        when(ownerClusterService.localNode()).thenReturn(ownerNode);

        // Build one service per transport so BOTH register their acquire/release handlers on their own transport.
        coordinatorService = newService(coordinatorClusterService, coordinatorTransport, GENEROUS_ACQUIRE_TIMEOUT);
        ownerClock = new AtomicLong();
        ownerService = new WorkloadGroupSharedThrottleService(
            ownerClusterService,
            threadPool,
            ownerTransport,
            GENEROUS_ACQUIRE_TIMEOUT,
            ownerClock::get
        );

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

    private MockTransportService newTransport(TransportInterceptor interceptor) {
        return MockTransportService.createNewService(
            Settings.EMPTY,
            MockTransportService.newMockTransport(Settings.EMPTY, Version.CURRENT, threadPool),
            Version.CURRENT,
            threadPool,
            null,
            Collections.emptySet(),
            interceptor,
            NoopTracer.INSTANCE
        );
    }

    // Records each ACQUIRE's response handler (the service's RemoteAcquire, passed through unwrapped) as it is sent.
    private TransportInterceptor captureAcquires() {
        return new TransportInterceptor() {
            @Override
            public AsyncSender interceptSender(AsyncSender sender) {
                return new AsyncSender() {
                    @Override
                    public <T extends TransportResponse> void sendRequest(
                        Transport.Connection connection,
                        String action,
                        TransportRequest request,
                        TransportRequestOptions options,
                        TransportResponseHandler<T> handler
                    ) {
                        if (WorkloadGroupSharedThrottleService.ACQUIRE_ACTION_NAME.equals(action)
                            && handler instanceof WorkloadGroupSharedThrottleService.RemoteAcquire acquire) {
                            sentAcquire.set(acquire);
                        }
                        sender.sendRequest(connection, action, request, options, handler);
                    }
                };
            }
        };
    }

    // Counts down checkRepliesHandled (grantRepliesHandled) after the owner's CHECK_TASKS (GRANT) handler has applied a
    // reply or a failure.
    private TransportInterceptor countOwnerReplies() {
        return afterRepliesTo(
            Map.of(
                WorkloadGroupSharedThrottleService.CHECK_TASKS_ACTION_NAME,
                () -> checkRepliesHandled.get().countDown(),
                WorkloadGroupSharedThrottleService.GRANT_ACTION_NAME,
                () -> grantRepliesHandled.get().countDown()
            )
        );
    }

    // Counts down registerRepliesHandled after the coordinator's REGISTER_WAITER handler has applied a reply or a failure.
    private TransportInterceptor countCoordinatorReplies() {
        return afterRepliesTo(
            Map.of(WorkloadGroupSharedThrottleService.REGISTER_WAITER_ACTION_NAME, () -> registerRepliesHandled.get().countDown())
        );
    }

    // Sends through outer, then inner, then the transport, so outer sees each handler exactly as the service passed it.
    private static TransportInterceptor chain(TransportInterceptor outer, TransportInterceptor inner) {
        return new TransportInterceptor() {
            @Override
            public AsyncSender interceptSender(AsyncSender sender) {
                return outer.interceptSender(inner.interceptSender(sender));
            }
        };
    }

    // Runs an action's callback after the sender has applied a reply or a failure to a request it sent for that action.
    private static TransportInterceptor afterRepliesTo(Map<String, Runnable> onHandledByAction) {
        return new TransportInterceptor() {
            @Override
            public AsyncSender interceptSender(AsyncSender sender) {
                return new AsyncSender() {
                    @Override
                    public <T extends TransportResponse> void sendRequest(
                        Transport.Connection connection,
                        String action,
                        TransportRequest request,
                        TransportRequestOptions options,
                        TransportResponseHandler<T> handler
                    ) {
                        final Runnable onHandled = onHandledByAction.get(action);
                        if (onHandled == null) {
                            sender.sendRequest(connection, action, request, options, handler);
                            return;
                        }
                        sender.sendRequest(connection, action, request, options, new TransportResponseHandler<T>() {
                            @Override
                            public T read(StreamInput in) throws IOException {
                                return handler.read(in);
                            }

                            @Override
                            public void handleResponse(T response) {
                                try {
                                    handler.handleResponse(response);
                                } finally {
                                    onHandled.run();
                                }
                            }

                            @Override
                            public void handleException(TransportException exp) {
                                try {
                                    handler.handleException(exp);
                                } finally {
                                    onHandled.run();
                                }
                            }

                            @Override
                            public void handleRejection(Exception exp) {
                                handler.handleRejection(exp);
                            }

                            @Override
                            public String executor() {
                                return handler.executor();
                            }
                        });
                    }
                };
            }
        };
    }

    // Finds a bucket key that every given service's ring maps to the remote owner, so acquireAsync takes the transport
    // path (and the owner does not refuse it as not_owner).
    private static String findKeyOwnedBy(DiscoveryNode owner, WorkloadGroupSharedThrottleService... services) {
        for (int i = 0; i < 10000; i++) {
            String candidate = "bucket-" + i;
            if (isOwnedByAll(candidate, owner, services)) {
                return candidate;
            }
        }
        throw new AssertionError("could not find a bucket owned by the remote owner node");
    }

    // As findKeyOwnedBy, for a workload group whose whole-group bucket (see groupBucketKey) every service maps to owner.
    private static String findGroupOwnedBy(DiscoveryNode owner, WorkloadGroupSharedThrottleService... services) {
        for (int i = 0; i < 10000; i++) {
            String groupId = "queued-group-" + i;
            if (isOwnedByAll(groupBucketKey(groupId), owner, services)) {
                return groupId;
            }
        }
        throw new AssertionError("could not find a workload group whose bucket is owned by the remote owner node");
    }

    private static boolean isOwnedByAll(String bucketKey, DiscoveryNode owner, WorkloadGroupSharedThrottleService... services) {
        for (WorkloadGroupSharedThrottleService service : services) {
            DiscoveryNode candidateOwner = service.ring().ownerFor(bucketKey).orElse(null);
            if (candidateOwner == null || candidateOwner.getId().equals(owner.getId()) == false) {
                return false;
            }
        }
        return true;
    }

    // The bucket key WorkloadGroupService builds for a group throttled as a whole (no "by").
    private static String groupBucketKey(String groupId) {
        return groupId + WorkloadGroupService.BUCKET_KEY_DELIMITER + WorkloadGroupThrottleSettings.GROUP_SCOPE
            + WorkloadGroupService.BUCKET_KEY_DELIMITER + WorkloadGroupThrottleSettings.GROUP_SCOPE;
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
            WorkloadGroupSharedThrottleService.RELEASE_ACTION_NAME,
            WorkloadGroupSharedThrottleService.CHECK_TASKS_ACTION_NAME,
            WorkloadGroupSharedThrottleService.GRANT_ACTION_NAME,
            WorkloadGroupSharedThrottleService.REGISTER_WAITER_ACTION_NAME
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
        coordinatorService.acquireAsync(remoteKey, 1, SharedThrottleTestPermits.NO_SUCH_TASK, false, listener);
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

    public void testShortRequestsNeverSendTaskChecks() throws Exception {
        AtomicInteger checks = countTaskChecks();
        Task task = registerSearchTask();
        Releasable permit = null;
        try {
            permit = acquireTrackedPermit(task);
            ownerClock.set(WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS - 1);
            ownerService.tracker().sweepExpired();
            CapturingListener overflow = new CapturingListener();
            coordinatorService.acquireAsync(remoteKey, 1, task.getId(), false, overflow);
            overflow.await();
            assertTrue(WorkloadGroupSharedThrottleService.isDenial(overflow.failure.get()));
            assertEquals("fresh permits require no liveness RPC", 0, checks.get());
            permit.close();
            assertBusy(() -> assertEquals(0, ownerService.tracker().inFlight(remoteKey)));
            ownerClock.set(WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS);
            ownerService.tracker().sweepExpired();
            assertEquals("completed short searches also require no liveness RPC", 0, checks.get());
        } finally {
            if (permit != null) {
                permit.close();
            }
            coordinatorTransport.getTaskManager().unregister(task);
        }
    }

    public void testExpiredRemotePermitRenewsOnlyWhileItsTaskIsRegistered() throws Exception {
        AtomicInteger checks = countTaskChecks();
        Task task = registerSearchTask();
        Releasable permit = null;
        try {
            permit = acquireTrackedPermit(task);
            CountDownLatch applied = expectTaskChecks(1);
            ownerClock.set(WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS);
            ownerService.tracker().sweepExpired();
            awaitChecks(applied);
            assertEquals(1, checks.get());
            assertEquals(1, ownerService.tracker().inFlight(remoteKey));
            CapturingListener overflow = new CapturingListener();
            coordinatorService.acquireAsync(remoteKey, 1, task.getId(), false, overflow);
            overflow.await();
            assertTrue(
                "the active expired search still occupies its slot",
                WorkloadGroupSharedThrottleService.isDenial(overflow.failure.get())
            );

            ownerClock.set(2 * WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS - 1);
            ownerService.tracker().sweepExpired();
            assertEquals("renewal uses the original five-minute TTL", 1, checks.get());
            coordinatorTransport.getTaskManager().unregister(task);
            applied = expectTaskChecks(1);
            ownerClock.set(2 * WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS);
            ownerService.tracker().sweepExpired();
            awaitChecks(applied);
            assertEquals(2, checks.get());
            assertEquals("a lost release is reclaimed once the task is absent", 0, ownerService.tracker().inFlight(remoteKey));
        } finally {
            if (permit != null) {
                permit.close();
            }
            if (coordinatorTransport.getTaskManager().getTask(task.getId()) != null) {
                coordinatorTransport.getTaskManager().unregister(task);
            }
        }
    }

    public void testExpiredPermitsAreBatchedAcrossBuckets() throws Exception {
        List<Integer> sizes = new CopyOnWriteArrayList<>();
        ownerTransport.addSendBehavior(coordinatorTransport, (connection, requestId, action, request, options) -> {
            if (WorkloadGroupSharedThrottleService.CHECK_TASKS_ACTION_NAME.equals(action)) {
                sizes.add(((TaskCheckRequest) request).taskIds.length);
            }
            connection.sendRequest(requestId, action, request, options);
        });
        Task task = registerSearchTask();
        try {
            for (int i = 0; i < 3; i++) {
                assertTrue(
                    tryAcquire(ownerService.tracker(), "batch-" + i, 1, "permit-" + i, 100, coordinatorNode.getEphemeralId(), task.getId())
                );
            }
            CountDownLatch applied = expectTaskChecks(1);
            ownerClock.set(100);
            ownerService.tracker().sweepExpired();
            awaitChecks(applied);
            assertEquals(List.of(3), sizes);
            for (int i = 0; i < 3; i++) {
                assertEquals(1, ownerService.tracker().inFlight("batch-" + i));
                ownerService.tracker().release("batch-" + i, "permit-" + i);
            }
        } finally {
            coordinatorTransport.getTaskManager().unregister(task);
        }
    }

    public void testExpiredChecksSplitAtTheBatchLimit() throws Exception {
        List<Integer> sizes = new CopyOnWriteArrayList<>();
        ownerTransport.addSendBehavior(coordinatorTransport, (connection, requestId, action, request, options) -> {
            if (WorkloadGroupSharedThrottleService.CHECK_TASKS_ACTION_NAME.equals(action)) {
                sizes.add(((TaskCheckRequest) request).taskIds.length);
            }
            connection.sendRequest(requestId, action, request, options);
        });
        Task task = registerSearchTask();
        int permits = WorkloadGroupSharedThrottleService.MAX_TASKS_PER_CHECK + 1;
        try {
            for (int i = 0; i < permits; i++) {
                assertTrue(
                    tryAcquire(
                        ownerService.tracker(),
                        remoteKey,
                        permits,
                        "permit-" + i,
                        100,
                        coordinatorNode.getEphemeralId(),
                        task.getId()
                    )
                );
            }
            CountDownLatch applied = expectTaskChecks(2);
            ownerClock.set(100);
            ownerService.tracker().sweepExpired();
            awaitChecks(applied);
            assertEquals(List.of(1, WorkloadGroupSharedThrottleService.MAX_TASKS_PER_CHECK), sizes.stream().sorted().toList());
            assertEquals(permits, ownerService.tracker().inFlight(remoteKey));
        } finally {
            ownerService.tracker().releaseAllFrom(Set.of(coordinatorNode.getEphemeralId()));
            coordinatorTransport.getTaskManager().unregister(task);
        }
    }

    public void testFailedCheckUsesShortTtlAndRecoveryRestoresFullTtl() throws Exception {
        AtomicBoolean failChecks = new AtomicBoolean(true);
        AtomicInteger checks = new AtomicInteger();
        ownerTransport.addSendBehavior(coordinatorTransport, (connection, requestId, action, request, options) -> {
            if (WorkloadGroupSharedThrottleService.CHECK_TASKS_ACTION_NAME.equals(action)) {
                checks.incrementAndGet();
                if (failChecks.get()) {
                    throw new ConnectTransportException(connection.getNode(), "simulated task check failure");
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });
        Task task = registerSearchTask();
        Releasable permit = null;
        try {
            permit = acquireTrackedPermit(task);
            CountDownLatch applied = expectTaskChecks(1);
            long expiry = WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS;
            ownerClock.set(expiry);
            ownerService.tracker().sweepExpired();
            awaitChecks(applied);
            assertEquals(1, ownerService.tracker().inFlight(remoteKey));
            ownerClock.set(expiry + WorkloadGroupSharedThrottleService.FAILED_CHECK_TTL.nanos() - 1);
            ownerService.tracker().sweepExpired();
            assertEquals("failed checks do not trigger continuous retries", 1, checks.get());
            failChecks.set(false);
            applied = expectTaskChecks(1);
            ownerClock.set(expiry + WorkloadGroupSharedThrottleService.FAILED_CHECK_TTL.nanos());
            ownerService.tracker().sweepExpired();
            awaitChecks(applied);
            assertEquals("retry occurs after thirty seconds, not another five minutes", 2, checks.get());
            ownerClock.addAndGet(WorkloadGroupSharedThrottleService.FAILED_CHECK_TTL.nanos());
            ownerService.tracker().sweepExpired();
            assertEquals("an active reply restores the full five-minute TTL", 2, checks.get());
        } finally {
            if (permit != null) {
                permit.close();
            }
            coordinatorTransport.getTaskManager().unregister(task);
        }
    }

    public void testUnreachableCoordinatorGetsShortExtension() throws Exception {
        Task task = registerSearchTask();
        Releasable permit = null;
        try {
            permit = acquireTrackedPermit(task);
            ownerTransport.disconnectFromNode(coordinatorNode);
            long scansBefore = ownerService.tracker().pruneScanCount();
            ownerClock.set(WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS);
            ownerService.tracker().sweepExpired();
            assertTrue("the sweep must collect the expired permit", ownerService.tracker().pruneScanCount() > scansBefore);
            // A disconnected coordinator gets no RPC; the drain extends the permit directly, clearing its pending mark.
            assertBusy(() -> assertEquals("task checks were not applied", 0, ownerService.tracker().pendingChecks()));
            assertEquals(1, ownerService.tracker().inFlight(remoteKey));
            ownerTransport.connectToNode(coordinatorNode);
            CountDownLatch applied = expectTaskChecks(1);
            ownerClock.addAndGet(WorkloadGroupSharedThrottleService.FAILED_CHECK_TTL.nanos());
            ownerService.tracker().sweepExpired();
            awaitChecks(applied);
            assertEquals(1, ownerService.tracker().inFlight(remoteKey));
        } finally {
            if (permit != null) {
                permit.close();
            }
            coordinatorTransport.getTaskManager().unregister(task);
        }
    }

    public void testLateTaskCheckCannotRestoreReleasedPermit() throws Exception {
        CountDownLatch received = new CountDownLatch(1);
        AtomicReference<CheckedRunnable<Exception>> heldCheck = new AtomicReference<>();
        coordinatorTransport.addRequestHandlingBehavior(
            WorkloadGroupSharedThrottleService.CHECK_TASKS_ACTION_NAME,
            (handler, request, channel, transportTask) -> {
                heldCheck.set(() -> handler.messageReceived(request, channel, transportTask));
                received.countDown();
            }
        );
        Task task = registerSearchTask();
        Releasable permit = null;
        try {
            permit = acquireTrackedPermit(task);
            CountDownLatch applied = expectTaskChecks(1);
            ownerClock.set(WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS);
            ownerService.tracker().sweepExpired();
            assertTrue(received.await(10, TimeUnit.SECONDS));
            permit.close();
            assertBusy(() -> assertEquals(0, ownerService.tracker().inFlight(remoteKey)));
            heldCheck.getAndSet(null).run();
            awaitChecks(applied);
            assertEquals("a delayed active reply must never recreate a released permit", 0, ownerService.tracker().inFlight(remoteKey));
        } finally {
            CheckedRunnable<Exception> pending = heldCheck.getAndSet(null);
            if (pending != null) {
                pending.run();
            }
            coordinatorTransport.clearAllRules();
            if (permit != null) {
                permit.close();
            }
            coordinatorTransport.getTaskManager().unregister(task);
        }
    }

    public void testFormerOwnerCanVerifyItsRetainedPermits() throws Exception {
        DiscoveryNode third = new DiscoveryNode(
            "third",
            "third",
            buildNewFakeTransportAddress(),
            Map.of(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
        DiscoveryNodes before = DiscoveryNodes.builder().add(ownerNode).add(coordinatorNode).localNodeId(ownerNode.getId()).build();
        DiscoveryNodes after = DiscoveryNodes.builder()
            .add(ownerNode)
            .add(coordinatorNode)
            .add(third)
            .localNodeId(ownerNode.getId())
            .build();
        ThrottleOwnerSelector nextRing = ThrottleOwnerSelector.fromDiscoveryNodes(after);
        String moved = null;
        for (int i = 0; i < 10000; i++) {
            String candidate = "moving-" + i;
            if (ownerService.ring().ownerFor(candidate).orElseThrow().equals(ownerNode)
                && nextRing.ownerFor(candidate).orElseThrow().equals(ownerNode) == false) {
                moved = candidate;
                break;
            }
        }
        assertNotNull(moved);
        Task task = registerSearchTask();
        Releasable permit = null;
        try {
            CapturingListener listener = new CapturingListener();
            coordinatorService.acquireAsync(moved, 1, task.getId(), false, listener);
            listener.await();
            permit = listener.response.get();
            assertNotNull(permit);
            deliverNodes(ownerService, before, after);
            CountDownLatch applied = expectTaskChecks(1);
            ownerClock.set(WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS);
            ownerService.tracker().sweepExpired();
            awaitChecks(applied);
            assertEquals(1, ownerService.tracker().inFlight(moved));
        } finally {
            if (permit != null) {
                permit.close();
            }
            coordinatorTransport.getTaskManager().unregister(task);
        }
    }

    private Task registerSearchTask() {
        return coordinatorTransport.getTaskManager().register("transport", "indices:data/read/search", new SearchRequest("test"));
    }

    private Releasable acquireTrackedPermit(Task task) throws Exception {
        CapturingListener listener = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, task.getId(), false, listener);
        listener.await();
        assertNull(listener.failure.get());
        assertNotNull(listener.response.get());
        return listener.response.get();
    }

    private AtomicInteger countTaskChecks() {
        AtomicInteger checks = new AtomicInteger();
        ownerTransport.addSendBehavior(coordinatorTransport, (connection, requestId, action, request, options) -> {
            if (WorkloadGroupSharedThrottleService.CHECK_TASKS_ACTION_NAME.equals(action)) {
                checks.incrementAndGet();
            }
            connection.sendRequest(requestId, action, request, options);
        });
        return checks;
    }

    // Expects count CHECK_TASKS round trips (reply or failure) to be applied by the owner.
    private CountDownLatch expectTaskChecks(int count) {
        CountDownLatch applied = new CountDownLatch(count);
        checkRepliesHandled.set(applied);
        return applied;
    }

    private static void awaitChecks(CountDownLatch applied) throws InterruptedException {
        assertTrue("task checks were not applied", applied.await(10, TimeUnit.SECONDS));
    }

    // Expects count GRANT round trips (reply or failure) to be applied by the owner.
    private CountDownLatch expectGrantReplies(int count) {
        CountDownLatch applied = new CountDownLatch(count);
        grantRepliesHandled.set(applied);
        return applied;
    }

    private static void awaitGrantReplies(CountDownLatch applied) throws InterruptedException {
        assertTrue("grant replies were not applied", applied.await(10, TimeUnit.SECONDS));
    }

    // Expects count REGISTER_WAITER round trips (reply or failure) to be applied by the setUp coordinator.
    private CountDownLatch expectRegisterReplies(int count) {
        CountDownLatch applied = new CountDownLatch(count);
        registerRepliesHandled.set(applied);
        return applied;
    }

    private static void awaitRegisterReplies(CountDownLatch applied) throws InterruptedException {
        assertTrue("waiter registration replies were not applied", applied.await(10, TimeUnit.SECONDS));
    }

    /**
     * A coordinator that leaves the cluster never releases what it holds; the owner purges its permits on the node
     * removal instead of pinning the slots until the TTL.
     */
    public void testOwnerPurgesPermitsOfDepartedCoordinator() throws Exception {
        CapturingListener listener = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, SharedThrottleTestPermits.NO_SUCH_TASK, false, listener);
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
            tryAcquire(
                ownerService.tracker(),
                remoteKey,
                1,
                "pre",
                WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS,
                "filler-coordinator"
            )
        );
        assertEquals(1, ownerService.tracker().inFlight(remoteKey));

        CapturingListener listener = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, SharedThrottleTestPermits.NO_SUCH_TASK, false, listener);
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
            tryAcquire(
                ownerService.tracker(),
                remoteKey,
                1,
                "pre",
                WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS,
                "filler-coordinator"
            )
        );

        CapturingListener listener = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, SharedThrottleTestPermits.NO_SUCH_TASK, true, listener);
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
        coordinatorService.acquireAsync(remoteKey, 1, SharedThrottleTestPermits.NO_SUCH_TASK, false, listener);
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
        coordinatorService.acquireAsync(remoteKey, 1, SharedThrottleTestPermits.NO_SUCH_TASK, false, listener);
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
        coordinatorService.acquireAsync(remoteKey, 1, SharedThrottleTestPermits.NO_SUCH_TASK, false, listener);
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
            service.acquireAsync(key, 1, SharedThrottleTestPermits.NO_SUCH_TASK, false, listener);
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

            CapturingListener listener = new CapturingListener();
            service.acquireAsync(key, 1, SharedThrottleTestPermits.NO_SUCH_TASK, false, listener);
            listener.await();
            assertTrue(
                "the timer must report the shared tier unavailable, was: " + listener.failure.get(),
                WorkloadGroupSharedThrottleService.isUnavailable(listener.failure.get())
            );
            assertTrue("the acquire must have reached the owner", acquireArrived.await(10, TimeUnit.SECONDS));
            // The handler stays registered for the late reply, but must no longer pin the answered request's listener.
            WorkloadGroupSharedThrottleService.RemoteAcquire acquire = sentAcquire.get();
            assertNotNull("the ACQUIRE handler must reach the interceptor unwrapped", acquire);
            assertFalse("a decided acquire must drop the caller's listener", acquire.holdsListener());
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
        CapturingListener listener = new CapturingListener();
        coordinatorService.acquireAsync(remoteKey, 1, SharedThrottleTestPermits.NO_SUCH_TASK, false, listener);
        listener.await();
        assertNotNull("a prompt grant must be delivered, failure: " + listener.failure.get(), listener.response.get());

        WorkloadGroupSharedThrottleService.RemoteAcquire acquire = sentAcquire.get();
        assertNotNull("the ACQUIRE handler must reach the interceptor unwrapped", acquire);
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
        try (MockTransportService transport = newTransport(captureAcquires())) {
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
            service.acquireAsync(key, 1, SharedThrottleTestPermits.NO_SUCH_TASK, false, listener);
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
            service.acquireAsync(key, 1, SharedThrottleTestPermits.NO_SUCH_TASK, true, listener);
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
                tryAcquire(ownerService.tracker(), key, 1, "pre", WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS, "filler-coordinator")
            );
            CapturingListener listener = new CapturingListener();
            service.acquireAsync(key, 1, SharedThrottleTestPermits.NO_SUCH_TASK, true, listener);
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
                tryAcquire(ownerService.tracker(), key, 1, "pre", WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS, "filler-coordinator")
            );
            CapturingListener listener = new CapturingListener();
            service.acquireAsync(key, 1, SharedThrottleTestPermits.NO_SUCH_TASK, false, listener);
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

    private static final String TEST_HEADER = "x-wlm-test-header";
    private static final String TEST_TRANSIENT = "x-wlm-test-transient";

    /**
     * A remote grant resumes the search on the coordinator's SEARCH pool, not on the thread that called acquireAsync. The
     * caller's ThreadContext (request headers and transients, e.g. the security user) must be restored for that
     * continuation, not the network thread's.
     */
    public void testRemoteGrantResumesOnSearchWithCallersThreadContext() throws Exception {
        ContextCapturingListener listener = acquireWithCallerContext(coordinatorService, remoteKey, false);
        assertNotNull("remote owner granted a permit", listener.response.get());
        assertOnSearchThread(listener);
        listener.assertSawCallerContext();
        listener.response.get().close();
    }

    /** A MONITOR denial also continues the search, on the SEARCH pool, with the caller's context. */
    public void testRemoteDenialForCallerThatProceedsKeepsCallersThreadContext() throws Exception {
        fillOwner(remoteKey);
        ContextCapturingListener listener = acquireWithCallerContext(coordinatorService, remoteKey, true);
        assertTrue(WorkloadGroupSharedThrottleService.isDenial(listener.failure.get()));
        assertOnSearchThread(listener);
        listener.assertSawCallerContext();
    }

    /** An ENFORCED denial is delivered inline on the response (network) thread, still with the caller's context. */
    public void testInlineDenialKeepsCallersThreadContext() throws Exception {
        fillOwner(remoteKey);
        ContextCapturingListener listener = acquireWithCallerContext(coordinatorService, remoteKey, false);
        assertTrue(WorkloadGroupSharedThrottleService.isDenial(listener.failure.get()));
        assertNotOnSearchThread(listener);
        listener.assertSawCallerContext();
    }

    /** A timed-out acquire is reported from the GENERIC timer thread, still with the caller's context. */
    public void testTimeoutKeepsCallersThreadContext() throws Exception {
        withShortTimeoutCoordinator((transport, service, key) -> {
            transport.addSendBehavior(ownerTransport, (connection, requestId, action, request, options) -> {
                if (WorkloadGroupSharedThrottleService.ACQUIRE_ACTION_NAME.equals(action) == false) {
                    connection.sendRequest(requestId, action, request, options);
                } // the ACQUIRE is swallowed so the timer decides
            });
            ContextCapturingListener listener = acquireWithCallerContext(service, key, false);
            assertTrue(WorkloadGroupSharedThrottleService.isUnavailable(listener.failure.get()));
            listener.assertSawCallerContext();
        });
    }

    private void fillOwner(String key) {
        assertTrue(
            tryAcquire(ownerService.tracker(), key, 1, "pre", WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS, "filler-coordinator")
        );
    }

    // Calls acquireAsync under a caller context carrying a header and a transient, then restores the test thread's own
    // context before waiting, so anything the listener sees must have come from the captured caller context.
    private ContextCapturingListener acquireWithCallerContext(WorkloadGroupSharedThrottleService service, String key, boolean proceeds)
        throws InterruptedException {
        ContextCapturingListener listener = new ContextCapturingListener(threadPool.getThreadContext());
        try (ThreadContext.StoredContext ignored = threadPool.getThreadContext().stashContext()) {
            threadPool.getThreadContext().putHeader(TEST_HEADER, "caller");
            threadPool.getThreadContext().putTransient(TEST_TRANSIENT, "caller-transient");
            service.acquireAsync(key, 1, SharedThrottleTestPermits.NO_SUCH_TASK, proceeds, listener);
        }
        assertNull("the caller context must not leak into the test thread", threadPool.getThreadContext().getHeader(TEST_HEADER));
        listener.await();
        return listener;
    }

    /** Records the header and transient visible in the ThreadContext when the outcome is delivered. */
    private static final class ContextCapturingListener extends CapturingListener {
        private final ThreadContext threadContext;
        final AtomicReference<String> header = new AtomicReference<>();
        final AtomicReference<Object> transientValue = new AtomicReference<>();

        ContextCapturingListener(ThreadContext threadContext) {
            this.threadContext = threadContext;
        }

        @Override
        public void onResponse(Releasable releasable) {
            capture();
            super.onResponse(releasable);
        }

        @Override
        public void onFailure(Exception e) {
            capture();
            super.onFailure(e);
        }

        private void capture() {
            header.set(threadContext.getHeader(TEST_HEADER));
            transientValue.set(threadContext.getTransient(TEST_TRANSIENT));
        }

        void assertSawCallerContext() {
            assertEquals("caller header must survive the hop to [" + thread.get() + "]", "caller", header.get());
            assertEquals("caller transient must survive the hop to [" + thread.get() + "]", "caller-transient", transientValue.get());
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
        coordinatorService.acquireAsync(remoteKey, 1, SharedThrottleTestPermits.NO_SUCH_TASK, false, listener, () -> noWaiter.set(true));
        listener.await();

        assertFalse("owner could register us, so the no-waiter path must not fire", noWaiter.get());
        assertTrue("denial must surface as the denial marker, was: " + listener.failure.get(), isDenial(listener));
        // A queueing caller never proceeds on denial, so the (registered) denial is delivered inline: it only moves the
        // retained entry to WAITING.
        assertNotOnSearchThread(listener);
        assertEquals("owner must have registered the coordinator as a waiter", 1, ownerService.waiterCount(remoteKey));
    }

    /**
     * The JOIN race: the coordinator's acquire reaches the owner before the owner has applied the cluster state that
     * contains it. The owner cannot address it, so it must not register it and reports {@code registered=false}; the
     * coordinator then parks its retained request anyway and registers through its backstop pass once the owner can.
     */
    public void testDeniedReportsNoWaiterWhenCoordinatorAbsentFromOwnerState() throws Exception {
        ownerSees(); // owner's applied state does not contain the coordinator yet
        fillOwnerToLimit();

        CapturingListener listener = new CapturingListener();
        AtomicReference<String> callbackThread = new AtomicReference<>();
        CountDownLatch noWaiterLatch = new CountDownLatch(1);
        coordinatorService.acquireAsync(remoteKey, 1, SharedThrottleTestPermits.NO_SUCH_TASK, false, listener, () -> {
            callbackThread.set(Thread.currentThread().getName());
            noWaiterLatch.countDown();
        });

        assertTrue("the no-waiter callback must fire", noWaiterLatch.await(10, TimeUnit.SECONDS));
        assertFalse(
            "the no-waiter outcome only moves the retained request, so it is delivered inline, was [" + callbackThread.get() + "]",
            callbackThread.get().contains("[" + ThreadPool.Names.SEARCH + "]")
        );
        assertFalse("the listener must not be completed on this path", listener.responded.get());
        assertNull("the no-waiter callback replaces the denial, it does not add to it", listener.failure.get());
        assertEquals("no waiter can be registered for a node the owner cannot address", 0, ownerService.waiterCount(remoteKey));
    }

    /**
     * The RESTART case: this coordinator restarted (same persistent node id, new ephemeral id) and the owner has not
     * applied its rejoin, so the owner's membership still holds the PREVIOUS incarnation. The owner looks the waiter up by
     * the ephemeral id the acquire carries, finds no such process, and reports {@code registered=false} rather than
     * registering the previous one; a backstop pass before the rejoin is refused the same way. Once the owner applies the
     * rejoin, the next pass registers this coordinator.
     */
    public void testRestartedCoordinatorIsRefusedUntilTheOwnerAppliesItsRejoin() throws Exception {
        DiscoveryNode previousIncarnation = new DiscoveryNode(
            coordinatorNode.getName(),
            coordinatorNode.getId(),            // same PERSISTENT id ...
            "previous-ephemeral-id",            // ... but the previous incarnation's ephemeral id
            coordinatorNode.getHostName(),
            coordinatorNode.getHostAddress(),
            coordinatorNode.getAddress(),
            coordinatorNode.getAttributes(),
            coordinatorNode.getRoles(),
            coordinatorNode.getVersion()
        );
        assertNotEquals("precondition: DiscoveryNode identity is the ephemeral id", coordinatorNode, previousIncarnation);

        ownerSeesWithGroup(previousIncarnation);
        // Even a live connection to this coordinator must not make the owner register a process it has not applied.
        ownerTransport.connectToNode(coordinatorNode);
        fillOwnerToLimit(); // no free capacity, so a registration is not consumed by an immediate grant

        CapturingListener listener = new CapturingListener();
        CountDownLatch noWaiterLatch = new CountDownLatch(1);
        coordinatorService.acquireAsync(remoteKey, 1, SharedThrottleTestPermits.NO_SUCH_TASK, false, listener, noWaiterLatch::countDown);
        assertTrue("a process the owner has not applied must be reported as not registered", noWaiterLatch.await(10, TimeUnit.SECONDS));
        assertEquals("no incarnation may be registered in its place", 0, ownerService.waiterCount(remoteKey));

        final CountDownLatch refused = expectRegisterReplies(1);
        coordinatorService.reconcileRetainedWaiters(remoteKey);
        awaitRegisterReplies(refused);
        assertEquals("a pass before the owner applies the rejoin is refused too", 0, ownerService.waiterCount(remoteKey));

        ownerSeesWithGroup(coordinatorNode);
        final CountDownLatch registered = expectRegisterReplies(1);
        coordinatorService.reconcileRetainedWaiters(remoteKey);
        awaitRegisterReplies(registered);
        assertEquals("the next pass after the rejoin registers the restarted coordinator", 1, ownerService.waiterCount(remoteKey));
    }

    /**
     * The NON-queueing path passes a null no-waiter callback, so {@code wantsQueue} is false. The coordinator must fall
     * through to the plain denial marker and must NOT dereference the null Runnable.
     */
    public void testNonQueueingDenialDoesNotTouchTheNullCallback() throws Exception {
        ownerSees(); // owner cannot see the coordinator, so registered would be false if it were even consulted
        fillOwnerToLimit();

        CapturingListener listener = new CapturingListener();
        // The non-queueing overload => null callback, wantsQueue=false.
        coordinatorService.acquireAsync(remoteKey, 1, SharedThrottleTestPermits.NO_SUCH_TASK, false, listener);
        listener.await();

        assertFalse("must not admit", listener.responded.get());
        assertTrue("non-queueing denial must surface the denial marker, was: " + listener.failure.get(), isDenial(listener));
        assertEquals("wantsQueue=false must never register a waiter", 0, ownerService.waiterCount(remoteKey));
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
        coordinatorService.acquireAsync(
            remoteKey,
            1,
            SharedThrottleTestPermits.NO_SUCH_TASK,
            false,
            listener,
            () -> fail("an unavailable owner is not a denial")
        );
        listener.await();

        assertTrue(WorkloadGroupSharedThrottleService.isUnavailable(listener.failure.get()));
        assertEquals(0, ownerService.waiterCount(remoteKey));
    }

    /**
     * A queueing acquire decided by the coordinator's timer is unavailable (its caller parks the PENDING_ACQUIRE entry),
     * and a grant arriving after the timer is released rather than admitted.
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
            service.acquireAsync(
                key,
                1,
                SharedThrottleTestPermits.NO_SUCH_TASK,
                false,
                listener,
                () -> fail("a timed-out acquire is not an unregistered denial")
            );
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
                SharedThrottleTestPermits.tryAcquire(
                    ownerService.tracker(),
                    key,
                    1,
                    "pre",
                    WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS,
                    "other-coordinator"
                )
            );
            ownerSees(); // the owner cannot address this coordinator, so the denial is unregistered

            CapturingListener listener = new CapturingListener();
            CountDownLatch noWaiter = new CountDownLatch(1);
            AtomicReference<String> callbackThread = new AtomicReference<>();
            service.acquireAsync(key, 1, SharedThrottleTestPermits.NO_SUCH_TASK, false, listener, () -> {
                callbackThread.set(Thread.currentThread().getName());
                noWaiter.countDown();
            });

            assertTrue("the no-waiter callback must run despite the saturated pool", noWaiter.await(10, TimeUnit.SECONDS));
            assertFalse(
                "the no-waiter callback must be delivered inline, not on the search pool, was [" + callbackThread.get() + "]",
                callbackThread.get().contains("[" + ThreadPool.Names.SEARCH + "]")
            );
            assertEquals("the callback replaces the listener outcome", 1, listener.latch.getCount());
            assertEquals(0, ownerService.waiterCount(key));
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
        coordinatorService.acquireAsync(
            remoteKey,
            1,
            SharedThrottleTestPermits.NO_SUCH_TASK,
            false,
            listener,
            () -> fail("owner could see and reach us, so it must register")
        );
        listener.await();
        assertEquals("waiter registered", 1, ownerService.waiterCount(remoteKey));
        assertEquals("the pre-filled permit still holds the only slot", 1, ownerService.tracker().inFlight(remoteKey));

        ownerTransport.disconnectFromNode(coordinatorNode);
        assertFalse("precondition: owner must no longer see a connection", ownerTransport.nodeConnected(coordinatorNode));

        ownerService.handleRelease(new WorkloadGroupSharedThrottleService.ReleasePermitRequest(remoteKey, "pre"));

        assertEquals("the reserved slot must be reclaimed, not leaked until TTL", 0, ownerService.tracker().inFlight(remoteKey));
        assertEquals("the disconnected waiter must be dropped", 0, ownerService.waiterCount(remoteKey));
    }

    public void testOnlyOneGrantAwaitsResponsePerCoordinatorBucket() throws Exception {
        ownerSeesWithGroup(coordinatorNode);
        ownerTransport.connectToNode(coordinatorNode);
        fillOwnerToLimit();

        CapturingListener denial = new CapturingListener();
        coordinatorService.acquireAsync(
            remoteKey,
            1,
            SharedThrottleTestPermits.NO_SUCH_TASK,
            false,
            denial,
            () -> fail("the reachable coordinator must be registered")
        );
        denial.await();
        assertTrue(isDenial(denial));
        assertEquals(1, ownerService.waiterCount(remoteKey));

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
            assertEquals(1, ownerService.pendingGrantCount(remoteKey));
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
                SharedThrottleTestPermits.NO_SUCH_TASK,
                true
            )
        );
        assertFalse(reRegistration.granted);
        assertTrue(reRegistration.registered);

        ownerService.handleRelease(new WorkloadGroupSharedThrottleService.ReleasePermitRequest(remoteKey, "pre"));
        assertEquals("another freed slot must skip the coordinator while its first GRANT is pending", 1, grantSends.get());
        assertEquals(1, ownerService.pendingGrantCount(remoteKey));
        assertEquals("only the unacknowledged GRANT remains reserved", 1, ownerService.tracker().inFlight(remoteKey));

        // Closing the connection resolves the unacknowledged send immediately. The failure path must remove the waiter
        // before reclaiming and redistributing the permit.
        ownerTransport.disconnectFromNode(coordinatorNode);
        assertBusy(() -> {
            assertEquals(0, ownerService.pendingGrantCount(remoteKey));
            assertEquals("the failed coordinator must be removed before the slot is re-offered", 0, ownerService.waiterCount(remoteKey));
            assertEquals("the unacknowledged reservation must be reclaimed", 0, ownerService.tracker().inFlight(remoteKey));
            assertEquals("failure must not immediately send another GRANT to the same waiter", 1, grantSends.get());
        });

        // Once the coordinator is reachable and submits another denied acquire, normal registration makes it eligible
        // again. There is no permanent quarantine or separate recovery protocol.
        ownerTransport.clearAllRules();
        ownerTransport.connectToNode(coordinatorNode);
        assertTrue(
            SharedThrottleTestPermits.tryAcquire(
                ownerService.tracker(),
                remoteKey,
                1,
                "new-holder",
                WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS,
                "other-coordinator"
            )
        );
        CapturingListener secondDenial = new CapturingListener();
        coordinatorService.acquireAsync(
            remoteKey,
            1,
            SharedThrottleTestPermits.NO_SUCH_TASK,
            false,
            secondDenial,
            () -> fail("the recovered coordinator must be re-registered")
        );
        secondDenial.await();
        assertTrue(isDenial(secondDenial));
        assertEquals("a later denied acquire re-adds the responsive coordinator", 1, ownerService.waiterCount(remoteKey));
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
                SharedThrottleTestPermits.NO_SUCH_TASK,
                false,
                failedWaiterDenial,
                () -> fail("the first coordinator must be registered")
            );
            failedWaiterDenial.await();
            assertTrue(isDenial(failedWaiterDenial));

            CapturingListener healthyWaiterDenial = new CapturingListener();
            healthyService.acquireAsync(
                remoteKey,
                1,
                SharedThrottleTestPermits.NO_SUCH_TASK,
                false,
                healthyWaiterDenial,
                () -> fail("the healthy coordinator must be registered")
            );
            healthyWaiterDenial.await();
            assertTrue(isDenial(healthyWaiterDenial));
            assertEquals(2, ownerService.waiterCount(remoteKey));

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
                    return ADMITTED;
                }
                return NOT_ADMITTED;
            });

            ownerService.handleRelease(new WorkloadGroupSharedThrottleService.ReleasePermitRequest(remoteKey, "pre"));
            assertBusy(() -> {
                assertEquals("the failed waiter must not be retried", 1, failedGrantAttempts.get());
                assertEquals("the reclaimed slot must move to the healthy waiter", 1, healthyAdmissions.get());
                assertNotNull(healthyPermit.get());
                assertEquals("only the healthy waiter remains registered", 1, ownerService.waiterCount(remoteKey));
                assertEquals(0, ownerService.pendingGrantCount(remoteKey));
                assertEquals(1, ownerService.tracker().inFlight(remoteKey));
            });

            healthyPermit.get().close();
            assertBusy(() -> {
                assertEquals("one more grant finds the drained queue; its own reply deregisters the waiter", 2, healthyAdmissions.get());
                assertEquals(0, ownerService.waiterCount(remoteKey));
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
        coordinatorService.acquireAsync(
            remoteKey,
            1,
            SharedThrottleTestPermits.NO_SUCH_TASK,
            false,
            denial,
            () -> fail("the reachable coordinator must be registered")
        );
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
                return ADMITTED;
            }
            return NOT_ADMITTED;
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
                SharedThrottleTestPermits.NO_SUCH_TASK,
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
            assertEquals("the replacement grant response clears its transient marker", 0, ownerService.pendingGrantCount(remoteKey));
            assertEquals("the fresh waiter registration must survive the earlier grant failure", 1, ownerService.waiterCount(remoteKey));
            assertEquals(1, ownerService.tracker().inFlight(remoteKey));
        });

        admittedPermit.get().close();
        assertBusy(() -> {
            assertEquals("the now-empty queue's reply returns the next grant and deregisters the waiter", 2, grantAdmissions.get());
            assertEquals(0, ownerService.waiterCount(remoteKey));
            assertEquals(0, ownerService.tracker().inFlight(remoteKey));
        });
    }

    public void testStaleGrantResponseAfterOwnerChurnDoesNotClearNewPendingGrant() throws Exception {
        ownerSeesWithGroup(coordinatorNode);
        ownerTransport.connectToNode(coordinatorNode);
        fillOwnerToLimit();

        CapturingListener denial = new CapturingListener();
        coordinatorService.acquireAsync(
            remoteKey,
            1,
            SharedThrottleTestPermits.NO_SUCH_TASK,
            false,
            denial,
            () -> fail("the reachable coordinator must be registered")
        );
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
            return ADMITTED;
        });

        ownerService.handleRelease(new WorkloadGroupSharedThrottleService.ReleasePermitRequest(remoteKey, "pre"));
        assertEquals(1, grantSends.get());
        assertEquals(1, ownerService.pendingGrantCount(remoteKey));

        mapBucketAwayAndBack(1);

        WorkloadGroupSharedThrottleService.AcquirePermitResponse freshRegistration = ownerService.handleAcquire(
            new WorkloadGroupSharedThrottleService.AcquirePermitRequest(
                remoteKey,
                1,
                "post-churn-registration",
                WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS,
                coordinatorNode.getEphemeralId(),
                SharedThrottleTestPermits.NO_SUCH_TASK,
                true
            )
        );
        assertFalse(freshRegistration.granted);
        assertTrue(freshRegistration.registered);

        ownerService.handleRelease(new WorkloadGroupSharedThrottleService.ReleasePermitRequest(remoteKey, firstGrant.get().permitId));
        assertEquals(2, grantSends.get());
        assertEquals(1, ownerService.pendingGrantCount(remoteKey));
        assertEquals(1, ownerService.tracker().inFlight(remoteKey));

        deliverFirstGrant.get().run();
        assertBusy(() -> {
            assertNotNull("the delayed pre-churn grant must reach the coordinator", latePermit.get());
            assertEquals(
                "the old response callback must not clear the replacement grant's marker",
                1,
                ownerService.pendingGrantCount(remoteKey)
            );
            assertEquals("the stale callback must not send a third grant", 2, grantSends.get());
        });

        ownerTransport.disconnectFromNode(coordinatorNode);
        assertBusy(() -> {
            assertEquals(0, ownerService.pendingGrantCount(remoteKey));
            assertEquals(0, ownerService.waiterCount(remoteKey));
            assertEquals(0, ownerService.tracker().inFlight(remoteKey));
        });
        latePermit.get().close();
        ownerTransport.clearAllRules();
    }

    /**
     * As {@link #testStaleGrantResponseAfterOwnerChurnDoesNotClearNewPendingGrant}, but the pre-churn reservation is still
     * recorded when the delayed GRANT's admitted reply arrives. The ownership change cleared that hand-off's marker, so
     * the reply leaves the replacement grant's marker in place and sends no GRANT, yet it still re-keys the reservation:
     * the search it admitted stays counted past PERMIT_TTL.
     */
    public void testStaleAdmittedGrantReplyAfterOwnerChurnStillReKeysItsReservation() throws Exception {
        // Two slots, so the pre-churn reservation and the replacement grant's reservation can be recorded together.
        final int sharedLimit = 2;
        final DiscoveryNodes nodes = DiscoveryNodes.builder().add(ownerNode).add(coordinatorNode).localNodeId(ownerNode.getId()).build();
        final ClusterState state = stateWithGroup(nodes, sharedLimit);
        when(ownerClusterService.state()).thenReturn(state);
        ownerTransport.connectToNode(coordinatorNode);
        for (String filler : List.of("pre", "pre2")) {
            assertTrue(
                tryAcquire(
                    ownerService.tracker(),
                    remoteKey,
                    sharedLimit,
                    filler,
                    WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS,
                    "other"
                )
            );
        }

        // Every GRANT is held until the test delivers it; every task check is recorded.
        List<CheckedRunnable<Exception>> heldGrants = new CopyOnWriteArrayList<>();
        List<Long> checkedTaskIds = new CopyOnWriteArrayList<>();
        ownerTransport.addSendBehavior(coordinatorTransport, (connection, requestId, action, request, options) -> {
            if (WorkloadGroupSharedThrottleService.GRANT_ACTION_NAME.equals(action)) {
                heldGrants.add(() -> connection.sendRequest(requestId, action, request, options));
                return;
            }
            if (WorkloadGroupSharedThrottleService.CHECK_TASKS_ACTION_NAME.equals(action)) {
                for (long taskId : ((TaskCheckRequest) request).taskIds) {
                    checkedTaskIds.add(taskId);
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });
        Task task = registerSearchTask();
        AtomicReference<Releasable> admittedPermit = new AtomicReference<>();
        coordinatorService.setGrantConsumer(
            (bucketKey, permit) -> admittedPermit.compareAndSet(null, permit) ? OptionalLong.of(task.getId()) : NOT_ADMITTED
        );
        try {
            CapturingListener denial = new CapturingListener();
            coordinatorService.acquireAsync(
                remoteKey,
                sharedLimit,
                SharedThrottleTestPermits.NO_SUCH_TASK,
                false,
                denial,
                () -> fail("the reachable coordinator must be registered")
            );
            denial.await();
            assertTrue(isDenial(denial));

            // The first tenure's hand-off: its GRANT is held while the bucket maps away and back, clearing its marker.
            ownerService.handleRelease(new WorkloadGroupSharedThrottleService.ReleasePermitRequest(remoteKey, "pre"));
            assertEquals(1, heldGrants.size());
            mapBucketAwayAndBack(sharedLimit);

            // In the restored tenure a new denial registers the coordinator again, and the other filler's release sends it
            // the replacement grant, held too, while the first reservation is still recorded.
            WorkloadGroupSharedThrottleService.AcquirePermitResponse registeredAgain = ownerService.handleAcquire(
                new WorkloadGroupSharedThrottleService.AcquirePermitRequest(
                    remoteKey,
                    sharedLimit,
                    "post-churn-registration",
                    WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS,
                    coordinatorNode.getEphemeralId(),
                    SharedThrottleTestPermits.NO_SUCH_TASK,
                    true
                )
            );
            assertFalse(registeredAgain.granted);
            assertTrue(registeredAgain.registered);
            ownerService.handleRelease(new WorkloadGroupSharedThrottleService.ReleasePermitRequest(remoteKey, "pre2"));
            assertEquals(2, heldGrants.size());
            assertEquals(1, ownerService.pendingGrantCount(remoteKey));
            assertEquals(2, ownerService.tracker().inFlight(remoteKey));

            // The delayed first GRANT admits the search.
            CountDownLatch replied = expectGrantReplies(1);
            heldGrants.get(0).run();
            awaitGrantReplies(replied);
            assertNotNull(admittedPermit.get());
            assertEquals("the stale reply leaves the replacement grant's marker in place", 1, ownerService.pendingGrantCount(remoteKey));
            assertEquals("the stale reply sends no GRANT", 2, heldGrants.size());

            // The replacement grant finds the queue drained: its reply returns that reservation and drops the waiter.
            replied = expectGrantReplies(1);
            heldGrants.get(1).run();
            awaitGrantReplies(replied);
            assertEquals(0, ownerService.pendingGrantCount(remoteKey));
            assertEquals(0, ownerService.waiterCount(remoteKey));
            assertEquals("only the admitted search's reservation remains", 1, ownerService.tracker().inFlight(remoteKey));

            // The stale reply re-keyed that reservation, so past PERMIT_TTL its check names the admitted search and renews it.
            CountDownLatch applied = expectTaskChecks(1);
            ownerClock.set(WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS);
            ownerService.tracker().sweepExpired();
            awaitChecks(applied);
            assertEquals("the check names the admitted search, not the placeholder", List.of(task.getId()), checkedTaskIds);
            assertEquals("the running search keeps its slot", 1, ownerService.tracker().inFlight(remoteKey));
        } finally {
            coordinatorTransport.getTaskManager().unregister(task);
        }
        admittedPermit.get().close();
        assertBusy(() -> assertEquals(0, ownerService.tracker().inFlight(remoteKey)));
        ownerTransport.clearAllRules();
    }

    // Replaces the owner's node incarnation and then restores it, so the bucket maps away from this owner and back. The
    // ownership change drops the bucket's waiters and the marker of any GRANT still awaiting its reply.
    private void mapBucketAwayAndBack(int sharedLimit) {
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
        final ClusterState originalState = stateWithGroup(originalNodes, sharedLimit);
        final ClusterState replacementState = stateWithGroup(replacementNodes, sharedLimit);

        when(ownerClusterService.state()).thenReturn(replacementState);
        ownerService.clusterChanged(new ClusterChangedEvent("owner incarnation replaced", replacementState, originalState));
        assertEquals(0, ownerService.pendingGrantCount(remoteKey));
        assertEquals(0, ownerService.waiterCount(remoteKey));

        final ClusterState restoredState = stateWithGroup(originalNodes, sharedLimit);
        when(ownerClusterService.state()).thenReturn(restoredState);
        ownerService.clusterChanged(new ClusterChangedEvent("original owner restored", restoredState, replacementState));
    }

    public void testGrantTimeoutReclaimsPermitAndLateGrantMayRunOnce() throws Exception {
        ownerSeesWithGroup(coordinatorNode);
        ownerTransport.connectToNode(coordinatorNode);
        fillOwnerToLimit();

        CapturingListener denial = new CapturingListener();
        coordinatorService.acquireAsync(
            remoteKey,
            1,
            SharedThrottleTestPermits.NO_SUCH_TASK,
            false,
            denial,
            () -> fail("the reachable coordinator must be registered")
        );
        denial.await();
        assertTrue(isDenial(denial));
        assertEquals(1, ownerService.waiterCount(remoteKey));

        AtomicInteger lateAdmissions = new AtomicInteger();
        AtomicReference<Releasable> latePermit = new AtomicReference<>();
        coordinatorService.setGrantConsumer((bucketKey, permit) -> {
            latePermit.set(permit);
            lateAdmissions.incrementAndGet();
            return ADMITTED;
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
            assertEquals(1, ownerService.pendingGrantCount(remoteKey));
            assertEquals(1, ownerService.waiterCount(remoteKey));
            assertEquals(1, ownerService.tracker().inFlight(remoteKey));
        });

        assertBusy(() -> {
            assertEquals("the timeout must clear the pending marker", 0, ownerService.pendingGrantCount(remoteKey));
            assertEquals("the unchanged waiter registration must be removed", 0, ownerService.waiterCount(remoteKey));
            assertEquals("the owner must reclaim the ambiguous reservation", 0, ownerService.tracker().inFlight(remoteKey));
        }, 20, TimeUnit.SECONDS);

        // The accepted cost of an ambiguous timeout: the late original GRANT can admit one request whose permit the owner
        // has already reclaimed (a bounded, one-request over-admission, not an unavailable-tier admission).
        assertBusy(() -> assertEquals("the delayed original GRANT may admit exactly once", 1, lateAdmissions.get()), 20, TimeUnit.SECONDS);
        latePermit.get().close(); // owner already reclaimed it; the idempotent late RELEASE is a no-op
        ownerTransport.clearAllRules();
    }

    /**
     * A queued search admitted by a pushed GRANT that runs past PERMIT_TTL stays counted: the coordinator's reply names
     * the admitted task, the owner re-keys the reservation's placeholder to it, and the expiry check then renews the
     * permit through CHECK_TASKS instead of freeing the slot for the next waiter while the search still runs.
     */
    public void testGrantReplyReKeysTheReservationSoALongQueuedSearchStaysCounted() throws Exception {
        ownerSeesWithGroup(coordinatorNode);
        ownerTransport.connectToNode(coordinatorNode);
        fillOwnerToLimit();
        List<Long> checkedTaskIds = new CopyOnWriteArrayList<>();
        AtomicInteger grantSends = new AtomicInteger();
        ownerTransport.addSendBehavior(coordinatorTransport, (connection, requestId, action, request, options) -> {
            if (WorkloadGroupSharedThrottleService.CHECK_TASKS_ACTION_NAME.equals(action)) {
                for (long taskId : ((TaskCheckRequest) request).taskIds) {
                    checkedTaskIds.add(taskId);
                }
            } else if (WorkloadGroupSharedThrottleService.GRANT_ACTION_NAME.equals(action)) {
                grantSends.incrementAndGet();
            }
            connection.sendRequest(requestId, action, request, options);
        });
        Task task = registerSearchTask();
        AtomicReference<Releasable> admittedPermit = new AtomicReference<>();
        coordinatorService.setGrantConsumer(
            (bucketKey, permit) -> admittedPermit.compareAndSet(null, permit) ? OptionalLong.of(task.getId()) : NOT_ADMITTED
        );
        try {
            CapturingListener denial = new CapturingListener();
            coordinatorService.acquireAsync(
                remoteKey,
                1,
                SharedThrottleTestPermits.NO_SUCH_TASK,
                false,
                denial,
                () -> fail("the reachable coordinator must be registered")
            );
            denial.await();
            assertTrue(isDenial(denial));

            ownerService.handleRelease(new WorkloadGroupSharedThrottleService.ReleasePermitRequest(remoteKey, "pre"));
            assertBusy(() -> {
                assertNotNull("the pushed grant admitted the queued search", admittedPermit.get());
                assertEquals("the owner applied the grant's reply", 0, ownerService.pendingGrantCount(remoteKey));
            });
            assertEquals(1, ownerService.tracker().inFlight(remoteKey));

            // Still running past its TTL: the check asks about the admitted task, which is registered, so it renews.
            CountDownLatch applied = expectTaskChecks(1);
            ownerClock.set(WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS);
            ownerService.tracker().sweepExpired();
            awaitChecks(applied);
            assertEquals("the running search's slot must not be re-granted", 1, grantSends.get());
            assertEquals("the check names the admitted search, not the placeholder", List.of(task.getId()), checkedTaskIds);
            assertEquals("the running search keeps its slot", 1, ownerService.tracker().inFlight(remoteKey));
            CapturingListener overflow = new CapturingListener();
            coordinatorService.acquireAsync(remoteKey, 1, SharedThrottleTestPermits.NO_SUCH_TASK, false, overflow);
            overflow.await();
            assertTrue("shared_limit still holds", isDenial(overflow));

            ownerClock.set(2 * WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS - 1);
            ownerService.tracker().sweepExpired();
            assertEquals("the renewal lasts a full TTL", 1, checkedTaskIds.size());

            // The search ends without its release arriving: the next check frees the slot, offered to the still-registered
            // coordinator, whose drained queue's reply then returns it.
            coordinatorTransport.getTaskManager().unregister(task);
            applied = expectTaskChecks(1);
            ownerClock.set(2 * WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS);
            ownerService.tracker().sweepExpired();
            awaitChecks(applied);
            assertEquals(List.of(task.getId(), task.getId()), checkedTaskIds);
            assertBusy(() -> {
                assertEquals(2, grantSends.get());
                assertEquals(0, ownerService.pendingGrantCount(remoteKey));
                assertEquals(0, ownerService.waiterCount(remoteKey));
                assertEquals(0, ownerService.tracker().inFlight(remoteKey));
            });
        } finally {
            if (coordinatorTransport.getTaskManager().getTask(task.getId()) != null) {
                coordinatorTransport.getTaskManager().unregister(task);
            }
        }
    }

    /**
     * A grant whose coordinator no longer has a queued request is returned by its reply alone, with no RELEASE: the owner
     * frees the reservation, deregisters that coordinator, and offers the slot to the next waiter.
     */
    public void testNotAdmittedGrantReplyReturnsTheReservationToTheNextWaiter() throws Exception {
        ownerSeesWithGroup(coordinatorNode);
        ownerTransport.connectToNode(coordinatorNode);
        fillOwnerToLimit();
        AtomicInteger releasesSent = new AtomicInteger();
        coordinatorTransport.addSendBehavior(ownerTransport, (connection, requestId, action, request, options) -> {
            if (WorkloadGroupSharedThrottleService.RELEASE_ACTION_NAME.equals(action)) {
                releasesSent.incrementAndGet();
            }
            connection.sendRequest(requestId, action, request, options);
        });
        AtomicInteger remoteGrants = new AtomicInteger();
        coordinatorService.setGrantConsumer((bucketKey, permit) -> {
            remoteGrants.incrementAndGet();
            return NOT_ADMITTED; // its retained request left the queue before the grant arrived
        });
        AtomicReference<Releasable> localPermit = new AtomicReference<>();
        ownerService.setGrantConsumer((bucketKey, permit) -> localPermit.compareAndSet(null, permit) ? ADMITTED : NOT_ADMITTED);

        // The remote coordinator registers first, then the owner's own waiter: waiters are [coordinator, owner].
        CapturingListener remoteDenial = new CapturingListener();
        coordinatorService.acquireAsync(
            remoteKey,
            1,
            SharedThrottleTestPermits.NO_SUCH_TASK,
            false,
            remoteDenial,
            () -> fail("the reachable coordinator must be registered")
        );
        remoteDenial.await();
        assertTrue(isDenial(remoteDenial));
        CapturingListener localDenial = new CapturingListener();
        ownerService.acquireAsync(
            remoteKey,
            1,
            SharedThrottleTestPermits.NO_SUCH_TASK,
            false,
            localDenial,
            () -> fail("the local owner always registers itself")
        );
        assertTrue(isDenial(localDenial));
        assertEquals(2, ownerService.waiterCount(remoteKey));

        ownerService.handleRelease(new WorkloadGroupSharedThrottleService.ReleasePermitRequest(remoteKey, "pre"));
        assertBusy(() -> {
            assertEquals("the head waiter is offered the freed slot first", 1, remoteGrants.get());
            assertNotNull("the returned reservation is offered to the next waiter", localPermit.get());
            assertEquals(0, ownerService.pendingGrantCount(remoteKey));
        });
        assertEquals("the slot moved, it was not duplicated", 1, ownerService.tracker().inFlight(remoteKey));
        assertEquals("the waiter that admitted nothing is deregistered, the admitted one stays", 1, ownerService.waiterCount(remoteKey));
        assertEquals("the reply alone returned the unused grant", 0, releasesSent.get());

        localPermit.get().close();
        assertBusy(() -> {
            assertEquals(0, ownerService.tracker().inFlight(remoteKey));
            assertEquals(0, ownerService.waiterCount(remoteKey));
        });
        assertEquals("the deregistered coordinator draws no further grant", 1, remoteGrants.get());
    }

    /**
     * The admitted search can finish, and its RELEASE reach the owner, before the GRANT's reply does. The reply's late
     * re-key must find the reservation gone and leave it gone, rather than re-create a permit nothing would ever release.
     */
    public void testReleaseThatOvertakesTheGrantReplyIsNotResurrected() throws Exception {
        ownerSeesWithGroup(coordinatorNode);
        ownerTransport.connectToNode(coordinatorNode);
        fillOwnerToLimit();
        // Admit on the coordinator as usual, but hold the GRANT's reply.
        CountDownLatch admitted = new CountDownLatch(1);
        AtomicReference<TransportResponse> heldReply = new AtomicReference<>();
        AtomicReference<TransportChannel> heldChannel = new AtomicReference<>();
        coordinatorTransport.addRequestHandlingBehavior(
            WorkloadGroupSharedThrottleService.GRANT_ACTION_NAME,
            (handler, request, channel, task) -> {
                handler.messageReceived(
                    request,
                    new TestTransportChannel(ActionListener.wrap(heldReply::set, e -> fail("the grant handler must not fail: " + e))),
                    task
                );
                heldChannel.set(channel);
                admitted.countDown();
            }
        );
        AtomicReference<Releasable> admittedPermit = new AtomicReference<>();
        coordinatorService.setGrantConsumer((bucketKey, permit) -> {
            admittedPermit.set(permit);
            return OptionalLong.of(42L);
        });
        CapturingListener denial = new CapturingListener();
        coordinatorService.acquireAsync(
            remoteKey,
            1,
            SharedThrottleTestPermits.NO_SUCH_TASK,
            false,
            denial,
            () -> fail("the reachable coordinator must be registered")
        );
        denial.await();
        assertTrue(isDenial(denial));

        ownerService.handleRelease(new WorkloadGroupSharedThrottleService.ReleasePermitRequest(remoteKey, "pre"));
        assertTrue("the grant must reach the coordinator", admitted.await(10, TimeUnit.SECONDS));
        assertEquals(1, ownerService.pendingGrantCount(remoteKey));

        // The admitted search completes at once: its RELEASE overtakes the held reply.
        admittedPermit.get().close();
        assertBusy(() -> assertEquals("the release freed the reservation", 0, ownerService.tracker().inFlight(remoteKey)));
        // Refill the bucket, so the reply's follow-up offer cannot spend the slot and hide a re-created record.
        fillOwnerToLimit();

        heldChannel.get().sendResponse(heldReply.get());
        assertBusy(() -> assertEquals("the reply was applied", 0, ownerService.pendingGrantCount(remoteKey)));
        assertEquals("the late re-key must not re-create the released reservation", 1, ownerService.tracker().inFlight(remoteKey));
        assertEquals("an admitted coordinator stays registered", 1, ownerService.waiterCount(remoteKey));
        assertTrue(ownerService.tracker().release(remoteKey, "pre"));
    }

    /**
     * A coordinator that registers again (a new request's denial) while a grant to it is in flight has demand that grant
     * did not see. If the grant's reply reports nothing admitted, the registration survives and the returned slot is
     * offered to it again, instead of leaving the new request parked until the coordinator's next reconcile.
     */
    public void testNotAdmittedGrantReplyKeepsAWaiterThatRegisteredAgain() throws Exception {
        ownerSeesWithGroup(coordinatorNode);
        ownerTransport.connectToNode(coordinatorNode);
        fillOwnerToLimit();
        CapturingListener denial = new CapturingListener();
        coordinatorService.acquireAsync(
            remoteKey,
            1,
            SharedThrottleTestPermits.NO_SUCH_TASK,
            false,
            denial,
            () -> fail("the reachable coordinator must be registered")
        );
        denial.await();
        assertTrue(isDenial(denial));

        // Hold the first GRANT on the coordinator until the coordinator has registered again.
        AtomicBoolean holdNext = new AtomicBoolean(true);
        CountDownLatch firstGrantArrived = new CountDownLatch(1);
        AtomicReference<CheckedRunnable<Exception>> heldGrant = new AtomicReference<>();
        coordinatorTransport.addRequestHandlingBehavior(
            WorkloadGroupSharedThrottleService.GRANT_ACTION_NAME,
            (handler, request, channel, task) -> {
                if (holdNext.compareAndSet(true, false)) {
                    heldGrant.set(() -> handler.messageReceived(request, channel, task));
                    firstGrantArrived.countDown();
                } else {
                    handler.messageReceived(request, channel, task);
                }
            }
        );
        AtomicInteger grants = new AtomicInteger();
        AtomicReference<Releasable> admittedPermit = new AtomicReference<>();
        coordinatorService.setGrantConsumer((bucketKey, permit) -> {
            final int grant = grants.incrementAndGet();
            if (grant == 2) {
                admittedPermit.set(permit); // the request whose denial registered the coordinator again
                return ADMITTED;
            }
            return NOT_ADMITTED; // the first grant's request already left the queue; the third finds it drained
        });

        ownerService.handleRelease(new WorkloadGroupSharedThrottleService.ReleasePermitRequest(remoteKey, "pre"));
        assertTrue("the first grant must reach the coordinator", firstGrantArrived.await(10, TimeUnit.SECONDS));
        WorkloadGroupSharedThrottleService.AcquirePermitResponse registeredAgain = ownerService.handleAcquire(
            new WorkloadGroupSharedThrottleService.AcquirePermitRequest(
                remoteKey,
                1,
                "registered-while-granting",
                WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS,
                coordinatorNode.getEphemeralId(),
                SharedThrottleTestPermits.NO_SUCH_TASK,
                true
            )
        );
        assertFalse(registeredAgain.granted);
        assertTrue(registeredAgain.registered);

        heldGrant.get().run();
        assertBusy(() -> {
            assertEquals("the returned slot is offered to the re-registered coordinator", 2, grants.get());
            assertNotNull(admittedPermit.get());
            assertEquals(0, ownerService.pendingGrantCount(remoteKey));
        });
        assertEquals("a registration made while the grant was in flight survives its reply", 1, ownerService.waiterCount(remoteKey));
        assertEquals(1, ownerService.tracker().inFlight(remoteKey));

        admittedPermit.get().close();
        assertBusy(() -> {
            assertEquals("the drained queue's reply deregisters the waiter", 3, grants.get());
            assertEquals(0, ownerService.waiterCount(remoteKey));
            assertEquals(0, ownerService.tracker().inFlight(remoteKey));
        });
    }

    public void testRemoteOwnerChangeReRegistrationRoundTripsAndImmediatelyDrives() throws Exception {
        ownerSeesWithGroup(coordinatorNode);
        ownerTransport.connectToNode(coordinatorNode);
        AtomicInteger grantsObserved = new AtomicInteger();
        coordinatorService.setGrantConsumer((bucketKey, permit) -> {
            grantsObserved.incrementAndGet();
            return NOT_ADMITTED;
        });

        coordinatorService.registerWaiterWithOwner(remoteKey, ownerNode);

        assertBusy(() -> {
            assertEquals("the new owner must immediately offer its already-free capacity", 1, grantsObserved.get());
            assertEquals("the unused grant must reconcile the coordinator out of the waiter set", 0, ownerService.waiterCount(remoteKey));
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
                return ADMITTED; // model one queued request
            }
            return NOT_ADMITTED; // the remaining speculative grants are returned unused
        });

        CapturingListener denial = new CapturingListener();
        coordinatorService.acquireAsync(
            remoteKey,
            1,
            SharedThrottleTestPermits.NO_SUCH_TASK,
            false,
            denial,
            () -> fail("the reachable coordinator must be registered")
        );
        denial.await();
        assertTrue(isDenial(denial));
        assertEquals(1, ownerService.waiterCount(remoteKey));

        final DiscoveryNodes nodes = ownerClusterService.state().nodes();
        final ClusterState previousState = stateWithGroup(nodes, 1);
        final ClusterState currentState = stateWithGroup(nodes, 4);
        when(ownerClusterService.state()).thenReturn(currentState);
        ownerService.clusterChanged(new ClusterChangedEvent("shared limit increased", currentState, previousState));

        assertBusy(() -> {
            assertEquals(
                "ACK pacing sends one grant at a time: the second one's reply reports nothing admitted and removes the shallow "
                    + "waiter, so the third added slot is never offered",
                2,
                grantAttempts.get()
            );
            assertNotNull("one real queued request must consume one of the grants", admittedPermit.get());
            assertEquals("all unused speculative grants must return to the owner", 2, ownerService.tracker().inFlight(remoteKey));
            assertEquals("an unused grant reconciles the shallow coordinator queue", 0, ownerService.waiterCount(remoteKey));
        });

        admittedPermit.get().close();
        ownerService.handleRelease(new WorkloadGroupSharedThrottleService.ReleasePermitRequest(remoteKey, "pre"));
        assertBusy(() -> assertEquals(0, ownerService.tracker().inFlight(remoteKey)));
    }

    public void testClusterChangeHookReRegistersRetainedBucketWithNewLocalOwner() throws Exception {
        AtomicInteger grantsObserved = new AtomicInteger();
        coordinatorService.setGrantConsumer((bucketKey, permit) -> {
            grantsObserved.incrementAndGet();
            return NOT_ADMITTED;
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
                coordinatorService.waiterCount(remoteKey)
            );
            assertEquals(0, coordinatorService.tracker().inFlight(remoteKey));
        });
    }

    /**
     * An owner that has not applied this coordinator's join refuses the registration (it cannot resolve the node). The
     * parked request stays WAITING, and once the owner knows this coordinator, the next backstop pass registers it and
     * the owner's free slot admits the request.
     */
    public void testParkedRequestStaysWaitingWhileItsOwnerDoesNotKnowTheCoordinator() throws Exception {
        final CoordinatorStack stack = coordinatorStackOnTheRemoteOwner();
        final CapturingListener parked = parkWithoutAnOwnerRegistration(stack);
        final ClusterState ownerKnowsCoordinator = ownerClusterService.state();
        ownerApplies(stateWithGroup(ownerView(), stack.bucketKey, 1));

        final CountDownLatch refused = expectRegisterReplies(1);
        stack.backstopPass();
        awaitRegisterReplies(refused);
        assertParked(stack, parked);
        assertEquals("the owner registered nobody", 0, ownerService.waiterCount(stack.bucketKey));

        ownerApplies(ownerKnowsCoordinator);
        stack.backstopPass();
        assertAdmittedByTheOwner(stack, parked);
    }

    /** A registration whose RPC fails leaves the parked request WAITING; the next pass that gets through admits it. */
    public void testParkedRequestStaysWaitingWhenItsRegistrationFails() throws Exception {
        final CoordinatorStack stack = coordinatorStackOnTheRemoteOwner();
        final CapturingListener parked = parkWithoutAnOwnerRegistration(stack);
        coordinatorTransport.addSendBehavior(ownerTransport, (connection, requestId, action, request, options) -> {
            if (WorkloadGroupSharedThrottleService.REGISTER_WAITER_ACTION_NAME.equals(action)) {
                throw new ConnectTransportException(connection.getNode(), "simulated registration send failure");
            }
            connection.sendRequest(requestId, action, request, options);
        });

        final CountDownLatch failed = expectRegisterReplies(1);
        stack.backstopPass();
        awaitRegisterReplies(failed);
        assertParked(stack, parked);
        assertEquals(0, ownerService.waiterCount(stack.bucketKey));

        coordinatorTransport.clearAllRules();
        stack.backstopPass();
        assertAdmittedByTheOwner(stack, parked);
    }

    /**
     * A registration the owner does not answer within GRANT_TIMEOUT leaves the parked request WAITING; the next pass that
     * gets an answer admits it. Waits out the real 5s timeout.
     */
    public void testParkedRequestStaysWaitingWhenItsRegistrationTimesOut() throws Exception {
        final CoordinatorStack stack = coordinatorStackOnTheRemoteOwner();
        final CapturingListener parked = parkWithoutAnOwnerRegistration(stack);
        final AtomicReference<TransportChannel> unanswered = new AtomicReference<>();
        ownerTransport.addRequestHandlingBehavior(
            WorkloadGroupSharedThrottleService.REGISTER_WAITER_ACTION_NAME,
            (handler, request, channel, task) -> unanswered.set(channel)
        );

        final CountDownLatch timedOut = expectRegisterReplies(1);
        stack.backstopPass();
        assertTrue(
            "the registration must time out",
            timedOut.await(WorkloadGroupSharedThrottleService.GRANT_TIMEOUT.seconds() + 20, TimeUnit.SECONDS)
        );
        assertParked(stack, parked);
        assertNotNull("the owner received the registration and never answered it", unanswered.get());
        assertEquals(0, ownerService.waiterCount(stack.bucketKey));

        // Free the owner's channel; the coordinator already dropped its handler, so this only completes the owner's task.
        unanswered.get().sendResponse(new IllegalStateException("answered after the coordinator gave up"));
        ownerTransport.clearAllRules();
        stack.backstopPass();
        assertAdmittedByTheOwner(stack, parked);
    }

    /** While this coordinator is not connected to the owner, a pass sends nothing and the parked request stays WAITING. */
    public void testParkedRequestStaysWaitingWhileItsOwnerIsNotConnected() throws Exception {
        final CoordinatorStack stack = coordinatorStackOnTheRemoteOwner();
        final CapturingListener parked = parkWithoutAnOwnerRegistration(stack);
        coordinatorTransport.disconnectFromNode(ownerNode);
        assertFalse(coordinatorTransport.nodeConnected(ownerNode));

        stack.backstopPass();
        assertParked(stack, parked);
        assertEquals(0, ownerService.waiterCount(stack.bucketKey));

        coordinatorTransport.connectToNode(ownerNode);
        stack.backstopPass();
        assertAdmittedByTheOwner(stack, parked);
    }

    /**
     * The bucket's owner restarts: the same node id (so the same buckets) under a new ephemeral id. When this coordinator
     * applies the rejoin it re-registers its parked request with the new incarnation, which has not applied its first
     * cluster state and refuses. The request stays WAITING; once that owner has applied its state, the next pass registers
     * this coordinator there and the restarted owner's free slot admits the request.
     */
    public void testParkedRequestStaysWaitingWhenARestartedOwnerRefusesItsRegistration() throws Exception {
        final CoordinatorStack stack = coordinatorStackOnTheRemoteOwner();
        fillOwnerToLimit(stack.bucketKey);
        final CapturingListener parked = stack.admit();
        awaitAcquireOutcome(stack, parked);
        assertParked(stack, parked);

        try (MockTransportService restartedTransport = newRestartedOwnerTransport()) {
            restartedTransport.start();
            restartedTransport.acceptIncomingRequests();
            final DiscoveryNode restartedNode = restartedTransport.getLocalDiscoNode();
            assertEquals("a restart keeps the persistent node id", ownerNode.getId(), restartedNode.getId());
            assertNotEquals("but is a new incarnation", ownerNode, restartedNode);
            final ClusterService restartedClusterService = mock(ClusterService.class);
            when(restartedClusterService.localNode()).thenReturn(restartedNode);
            // Delivered no cluster-state event yet, so its ring is empty: it has not applied its first state.
            final WorkloadGroupSharedThrottleService restartedService = newService(
                restartedClusterService,
                restartedTransport,
                GENEROUS_ACQUIRE_TIMEOUT
            );
            coordinatorTransport.connectToNode(restartedNode);

            final DiscoveryNodes rejoined = DiscoveryNodes.builder()
                .add(coordinatorNode)
                .add(restartedNode)
                .localNodeId(coordinatorNode.getId())
                .build();
            applyState(coordinatorClusterService, stateWithQueueingGroup(rejoined, stack.groupId));
            final CountDownLatch refused = expectRegisterReplies(1);
            deliverNodes(coordinatorService, rejoined);
            awaitRegisterReplies(refused);
            assertEquals(restartedNode, coordinatorService.ring().ownerFor(stack.bucketKey).orElseThrow());
            assertEquals("an owner without its first cluster state registers nobody", 0, restartedService.waiterCount(stack.bucketKey));
            assertParked(stack, parked);

            applyState(restartedClusterService, stateWithGroup(rejoined, stack.bucketKey, 1));
            deliverNodes(restartedService, rejoined);
            restartedTransport.connectToNode(coordinatorNode);
            stack.backstopPass();
            parked.await();
            assertNotNull("the restarted owner's grant must admit the request: " + parked.failure.get(), parked.response.get());
            assertEquals(1, parked.invocations.get());
            assertEquals(0, stack.waiting());
            assertEquals(1, restartedService.tracker().inFlight(stack.bucketKey));
            parked.response.get().close();
            assertBusy(() -> {
                assertEquals(0, restartedService.tracker().inFlight(stack.bucketKey));
                assertEquals(
                    "the unused follow-up grant deregisters the drained coordinator",
                    0,
                    restartedService.waiterCount(stack.bucketKey)
                );
            });
        }
    }

    /**
     * A new request whose owner is not connected gets no answer, so it parks instead of getting the unavailable 429, and
     * the first pass after the connection is back registers this coordinator and admits it.
     */
    public void testNewRequestParksWhileItsOwnerIsNotConnected() throws Exception {
        final CoordinatorStack stack = coordinatorStackOnTheRemoteOwner();
        coordinatorTransport.disconnectFromNode(ownerNode);

        final CapturingListener parked = stack.admit(); // refused inline: there is no connection to ask the owner on
        assertParked(stack, parked);
        assertEquals("nothing reached the owner", 0, ownerService.tracker().inFlight(stack.bucketKey));

        coordinatorTransport.connectToNode(ownerNode);
        stack.backstopPass();
        assertAdmittedByTheOwner(stack, parked);
    }

    /**
     * A new request whose acquire times out parks. The grant the owner then sends late is released and admits nothing, so
     * the request is admitted exactly once: by the grant that follows the next pass's registration.
     */
    public void testNewRequestParksWhenItsAcquireTimesOutAndTheLateGrantAdmitsNothing() throws Exception {
        try (MockTransportService transport = newTransport(captureAcquires())) {
            transport.start();
            transport.acceptIncomingRequests();
            transport.connectToNode(ownerNode);
            final DiscoveryNode node = transport.getLocalDiscoNode();
            final ClusterService clusterService = mock(ClusterService.class);
            when(clusterService.localNode()).thenReturn(node);
            final WorkloadGroupSharedThrottleService service = newService(clusterService, transport, TimeValue.timeValueMillis(50));
            final DiscoveryNodes nodes = DiscoveryNodes.builder().add(node).add(ownerNode).localNodeId(node.getId()).build();
            deliverNodes(service, nodes);
            deliverNodes(ownerService, nodes);
            final CoordinatorStack stack = new CoordinatorStack(
                service,
                clusterService,
                nodes,
                findGroupOwnedBy(ownerNode, service, ownerService)
            );
            applyState(ownerClusterService, stateWithGroup(ownerView(node), stack.bucketKey, 1));
            ownerTransport.connectToNode(node);
            // Hold the owner's handling of the acquire until the coordinator has given up on it.
            final CountDownLatch acquireArrived = new CountDownLatch(1);
            final AtomicReference<CheckedRunnable<Exception>> heldAcquire = new AtomicReference<>();
            ownerTransport.addRequestHandlingBehavior(
                WorkloadGroupSharedThrottleService.ACQUIRE_ACTION_NAME,
                (handler, request, channel, task) -> {
                    heldAcquire.set(() -> handler.messageReceived(request, channel, task));
                    acquireArrived.countDown();
                }
            );
            final AtomicInteger releases = new AtomicInteger();
            ownerTransport.addRequestHandlingBehavior(
                WorkloadGroupSharedThrottleService.RELEASE_ACTION_NAME,
                (handler, request, channel, task) -> {
                    handler.messageReceived(request, channel, task);
                    releases.incrementAndGet();
                }
            );

            final CapturingListener parked = stack.admit();
            awaitAcquireOutcome(stack, parked);
            assertParked(stack, parked);
            assertTrue("the acquire must have reached the owner", acquireArrived.await(10, TimeUnit.SECONDS));

            heldAcquire.get().run(); // the owner grants after all
            assertBusy(() -> {
                assertEquals("the late grant must be released", 1, releases.get());
                assertEquals(0, ownerService.tracker().inFlight(stack.bucketKey));
            });
            assertParked(stack, parked);

            ownerTransport.clearAllRules();
            stack.backstopPass();
            assertAdmittedByTheOwner(stack, parked);
        }
    }

    /**
     * A new request whose owner answers not_owner (the rings disagree mid-rebalance) parks. Once the rings agree again,
     * the next pass registers this coordinator with the owner and its free slot admits the request.
     */
    public void testNewRequestParksOnANotOwnerReply() throws Exception {
        final CoordinatorStack stack = coordinatorStackOnTheRemoteOwner();
        // The owner's ring already maps every bucket to this coordinator; this coordinator's ring has not caught up.
        deliverNodes(ownerService, DiscoveryNodes.builder().add(coordinatorNode).localNodeId(ownerNode.getId()).build());

        final CapturingListener parked = stack.admit();
        awaitAcquireOutcome(stack, parked);
        assertParked(stack, parked);
        assertEquals("a former owner records nothing", 0, ownerService.tracker().inFlight(stack.bucketKey));

        deliverNodes(ownerService, DiscoveryNodes.builder().add(coordinatorNode).add(ownerNode).localNodeId(ownerNode.getId()).build());
        stack.backstopPass();
        assertAdmittedByTheOwner(stack, parked);
    }

    /**
     * A new request denied by an owner that cannot register this coordinator yet (it has not applied the join) parks like
     * a registered denial. Once the owner knows this coordinator, the next pass registers it and a freed slot admits it.
     */
    public void testNewRequestParksOnADenialItsOwnerCouldNotRegister() throws Exception {
        final CoordinatorStack stack = coordinatorStackOnTheRemoteOwner();
        final ClusterState ownerKnowsCoordinator = ownerClusterService.state();
        ownerApplies(stateWithGroup(ownerView(), stack.bucketKey, 1));
        fillOwnerToLimit(stack.bucketKey);

        final CapturingListener parked = stack.admit();
        awaitAcquireOutcome(stack, parked);
        assertParked(stack, parked);
        assertEquals("the owner registered nobody", 0, ownerService.waiterCount(stack.bucketKey));

        // The owner applies the join and its holder finishes, with no waiter registered to push the slot to.
        ownerApplies(ownerKnowsCoordinator);
        ownerService.handleRelease(new WorkloadGroupSharedThrottleService.ReleasePermitRequest(stack.bucketKey, "pre"));
        assertEquals(0, ownerService.tracker().inFlight(stack.bucketKey));
        assertParked(stack, parked);

        stack.backstopPass();
        assertAdmittedByTheOwner(stack, parked);
    }

    /**
     * A request must not keep its own lost grant alive. The owner grants the request's ACQUIRE, but the connection drops
     * before the reply arrives, a transport failure after which no release is sent: the request parks as unavailable while
     * the owner counts that grant in the bucket's only slot. The request's task is registered, so were the grant's expiry
     * check to report it active, the grant would be renewed for as long as the request waits, for a slot only it holds.
     * A task WAITING in its coordinator's queue is reported inactive instead: the orphan is freed and its slot pushed to
     * the request, which the backstop registered with the owner. Once admitted, the running task is reported active.
     */
    public void testParkedRequestDoesNotKeepItsLostGrantAlive() throws Exception {
        final CoordinatorStack stack = coordinatorStackOnTheRemoteOwner();
        final CountDownLatch grantedUnanswered = new CountDownLatch(1);
        final AtomicReference<TransportChannel> unanswered = new AtomicReference<>();
        final AtomicReference<TransportResponse> grant = new AtomicReference<>();
        ownerTransport.addRequestHandlingBehavior(
            WorkloadGroupSharedThrottleService.ACQUIRE_ACTION_NAME,
            (handler, request, channel, transportTask) -> {
                // The owner records its verdict, but the reply is held back until the coordinator has lost it.
                handler.messageReceived(
                    request,
                    new TestTransportChannel(ActionListener.wrap(grant::set, e -> fail(e.toString()))),
                    transportTask
                );
                unanswered.set(channel);
                grantedUnanswered.countDown();
            }
        );
        final WorkloadGroupTask task = (WorkloadGroupTask) registerSearchTask();
        Releasable admitted = null;
        try {
            final CapturingListener parked = stack.admit(task);
            assertTrue("the acquire must reach the owner", grantedUnanswered.await(10, TimeUnit.SECONDS));
            assertTrue("the owner granted it", ((WorkloadGroupSharedThrottleService.AcquirePermitResponse) grant.get()).granted);
            assertEquals("and counts it", 1, ownerService.tracker().inFlight(stack.bucketKey));

            // The connection drops before the reply: the acquire fails with a transport error, which sends no release.
            coordinatorTransport.disconnectFromNode(ownerNode);
            awaitAcquireOutcome(stack, parked);
            assertParked(stack, parked);
            unanswered.get().sendResponse(grant.get()); // lost: the coordinator no longer waits for it
            ownerTransport.clearAllRules();
            coordinatorTransport.connectToNode(ownerNode);

            // The backstop registers this coordinator with the owner, whose only slot the lost grant still holds.
            final CountDownLatch registered = expectRegisterReplies(1);
            stack.backstopPass();
            awaitRegisterReplies(registered);
            assertEquals(1, ownerService.waiterCount(stack.bucketKey));
            assertParked(stack, parked);
            assertEquals(1, ownerService.tracker().inFlight(stack.bucketKey));

            // The lost grant's expiry check finds its task WAITING, so it is freed, and the freed slot admits the request.
            final CountDownLatch checked = expectTaskChecks(1);
            final CountDownLatch grantReplied = expectGrantReplies(1);
            ownerClock.set(WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS);
            ownerService.tracker().sweepExpired();
            awaitChecks(checked);
            parked.await();
            admitted = parked.response.get();
            assertNotNull("the freed slot must admit the parked request, failure: " + parked.failure.get(), admitted);
            assertEquals("admitted exactly once", 1, parked.invocations.get());
            assertEquals(0, stack.waiting());
            assertEquals(0, stack.throttled());
            awaitGrantReplies(grantReplied);
            assertEquals("the grant that admitted it holds the only slot", 1, ownerService.tracker().inFlight(stack.bucketKey));

            // Admitted, the task is running: it is reported active, so its permit's next expiry check renews the permit and
            // frees no slot for another grant.
            final TaskCheckRequest runningCheck = new TaskCheckRequest(coordinatorNode.getEphemeralId(), new long[] { task.getId() });
            assertArrayEquals(
                "a running task is reported active",
                new boolean[] { true },
                coordinatorService.checkTasks(runningCheck).active
            );
            final AtomicInteger grantsSent = new AtomicInteger();
            ownerTransport.addSendBehavior(coordinatorTransport, (connection, requestId, action, request, options) -> {
                if (WorkloadGroupSharedThrottleService.GRANT_ACTION_NAME.equals(action)) {
                    grantsSent.incrementAndGet();
                }
                connection.sendRequest(requestId, action, request, options);
            });
            final CountDownLatch checkedAgain = expectTaskChecks(1);
            ownerClock.addAndGet(WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS);
            ownerService.tracker().sweepExpired();
            awaitChecks(checkedAgain);
            assertEquals("the check freed no slot", 0, grantsSent.get());
            assertEquals("a running task keeps its permit", 1, ownerService.tracker().inFlight(stack.bucketKey));
            assertEquals(0, ownerService.tracker().pendingChecks());

            admitted.close();
            admitted = null;
            assertBusy(() -> {
                assertEquals(0, ownerService.tracker().inFlight(stack.bucketKey));
                assertEquals(
                    "the drained coordinator's unused follow-up grant deregisters it",
                    0,
                    ownerService.waiterCount(stack.bucketKey)
                );
            });
        } finally {
            if (admitted != null) {
                admitted.close();
            }
            coordinatorTransport.getTaskManager().unregister(task);
        }
    }

    public void testConfirmedReRegistrationRestoresTheWaiter() throws Exception {
        ownerSeesWithGroup(coordinatorNode);
        ownerTransport.connectToNode(coordinatorNode);
        fillOwnerToLimit(); // no free capacity, so the restored registration is not immediately consumed

        coordinatorService.reconcileRetainedWaiters(remoteKey);

        assertBusy(() -> assertEquals("the backstop restores a lost membership", 1, ownerService.waiterCount(remoteKey)));
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
            return granted.compareAndSet(null, permit) ? ADMITTED : NOT_ADMITTED; // one queued request; a later grant goes unused
        });
        // On the acquire path the owner first checks a connection right after the tracker denies: that the coordinator it
        // is about to register is connected; nothing on that path checks one earlier. Releasing the holder on that check
        // lands the release inside the window between the denial and the registration.
        final AtomicReference<Runnable> onNextConnectionCheck = new AtomicReference<>(
            () -> ownerService.handleRelease(new WorkloadGroupSharedThrottleService.ReleasePermitRequest(remoteKey, "pre"))
        );
        ownerTransport.addNodeConnectedBehavior((connectionManager, node) -> {
            Runnable action = onNextConnectionCheck.getAndSet(null);
            if (action != null) {
                action.run();
            }
            return connectionManager.nodeConnected(node);
        });

        CapturingListener denial = new CapturingListener();
        coordinatorService.acquireAsync(
            remoteKey,
            1,
            SharedThrottleTestPermits.NO_SUCH_TASK,
            false,
            denial,
            () -> fail("the reachable coordinator must be registered")
        );
        denial.await();
        assertNull("the release must have run inside the window", onNextConnectionCheck.get());
        assertTrue("the acquire was denied, so the release landed after the tracker's verdict", isDenial(denial));

        assertBusy(() -> assertEquals("the slot freed inside the window must be pushed to the new waiter", 1, grants.get()));
        assertEquals(1, ownerService.tracker().inFlight(remoteKey));
        granted.get().close();
        assertBusy(() -> {
            assertEquals(0, ownerService.tracker().inFlight(remoteKey));
            assertEquals("the unused follow-up grant deregisters the drained coordinator", 0, ownerService.waiterCount(remoteKey));
        });
    }

    public void testRingEmptyingLeavesRetainedWaitersToTheNodeTier() throws Exception {
        Set<String> noOwner = ConcurrentHashMap.newKeySet();
        coordinatorService.setNoEligibleOwnerHandler(noOwner::add);
        coordinatorService.setRetainedBucketKeysSupplier(() -> Set.of(remoteKey));
        ClusterState noOwners = stateWithGroup(DiscoveryNodes.EMPTY_NODES);
        when(coordinatorClusterService.state()).thenReturn(noOwners);

        deliverNodes(coordinatorService, DiscoveryNodes.EMPTY_NODES);

        assertBusy(() -> assertEquals("no node counts shared_limit, so the node tier decides", Set.of(remoteKey), noOwner));
    }

    public void testOwnerReturningReRegistersRetainedWaitersWithTheNewOwner() throws Exception {
        ownerSeesWithGroup(coordinatorNode);
        ownerTransport.connectToNode(coordinatorNode);
        fillOwnerToLimit(); // no free capacity, so the restored registration is not consumed by an immediate grant
        Set<String> noOwner = ConcurrentHashMap.newKeySet();
        coordinatorService.setNoEligibleOwnerHandler(noOwner::add);
        coordinatorService.setRetainedBucketKeysSupplier(() -> Set.of(remoteKey));
        final DiscoveryNodes bothNodes = DiscoveryNodes.builder()
            .add(coordinatorNode)
            .add(ownerNode)
            .localNodeId(coordinatorNode.getId())
            .build();
        final ClusterState noOwners = stateWithGroup(DiscoveryNodes.EMPTY_NODES);
        final ClusterState ownerBack = stateWithGroup(bothNodes);
        when(coordinatorClusterService.state()).thenReturn(noOwners);
        deliverNodes(coordinatorService, DiscoveryNodes.EMPTY_NODES);
        assertBusy(() -> assertEquals(Set.of(remoteKey), noOwner));

        when(coordinatorClusterService.state()).thenReturn(ownerBack);
        deliverNodes(coordinatorService, bothNodes);

        assertBusy(() -> assertEquals("the returning owner registers this coordinator again", 1, ownerService.waiterCount(remoteKey)));
    }

    public void testRegisterWaiterUsesTheGrantTimeoutNotTheAcquireBudget() throws Exception {
        ownerSeesWithGroup(coordinatorNode);
        ownerTransport.connectToNode(coordinatorNode);
        fillOwnerToLimit();
        AtomicReference<TimeValue> registerTimeout = new AtomicReference<>();
        coordinatorTransport.addSendBehavior(ownerTransport, (connection, requestId, action, request, options) -> {
            if (WorkloadGroupSharedThrottleService.REGISTER_WAITER_ACTION_NAME.equals(action)) {
                registerTimeout.set(options.timeout());
            }
            connection.sendRequest(requestId, action, request, options);
        });

        coordinatorService.reconcileRetainedWaiters(remoteKey);

        assertBusy(() -> assertEquals(1, ownerService.waiterCount(remoteKey)));
        assertEquals(WorkloadGroupSharedThrottleService.GRANT_TIMEOUT, registerTimeout.get());
    }

    /**
     * An offer that reserves nothing (here the owner's answer to a coordinator's periodic re-registration on a full
     * bucket) must not cost the head waiter its turn: the next freed slot still goes to it.
     */
    public void testFailedOfferDoesNotCostTheHeadWaiterItsTurn() throws Exception {
        ownerSeesWithGroup(coordinatorNode);
        ownerTransport.connectToNode(coordinatorNode);
        fillOwnerToLimit();
        AtomicInteger localAdmissions = new AtomicInteger();
        AtomicInteger remoteAdmissions = new AtomicInteger();
        AtomicReference<Releasable> localPermit = new AtomicReference<>();
        AtomicReference<Releasable> remotePermit = new AtomicReference<>();
        ownerService.setGrantConsumer((bucketKey, permit) -> {
            if (localPermit.compareAndSet(null, permit)) {
                localAdmissions.incrementAndGet();
                return ADMITTED;
            }
            return NOT_ADMITTED;
        });
        coordinatorService.setGrantConsumer((bucketKey, permit) -> {
            if (remotePermit.compareAndSet(null, permit)) {
                remoteAdmissions.incrementAndGet();
                return ADMITTED;
            }
            return NOT_ADMITTED;
        });

        // The owner's own waiter registers first, then the remote coordinator: waiters are [owner, coordinator].
        CapturingListener localDenial = new CapturingListener();
        ownerService.acquireAsync(
            remoteKey,
            1,
            SharedThrottleTestPermits.NO_SUCH_TASK,
            false,
            localDenial,
            () -> fail("the local owner always registers itself")
        );
        assertTrue("the local owner answers inline", isDenial(localDenial));
        CapturingListener remoteDenial = new CapturingListener();
        coordinatorService.acquireAsync(
            remoteKey,
            1,
            SharedThrottleTestPermits.NO_SUCH_TASK,
            false,
            remoteDenial,
            () -> fail("the reachable coordinator must be registered")
        );
        remoteDenial.await();
        assertTrue(isDenial(remoteDenial));
        assertEquals(2, ownerService.waiterCount(remoteKey));

        // An offer against the still-full bucket, as the coordinator's 5s reconcile drives it, reserves nothing.
        assertTrue(
            ownerService.handleRegisterWaiter(
                new WorkloadGroupSharedThrottleService.RegisterWaiterRequest(remoteKey, coordinatorNode.getEphemeralId())
            ).registered
        );
        assertEquals(1, ownerService.tracker().inFlight(remoteKey));
        assertEquals(0, localAdmissions.get() + remoteAdmissions.get());

        ownerService.handleRelease(new WorkloadGroupSharedThrottleService.ReleasePermitRequest(remoteKey, "pre"));
        assertEquals("the failed offer must not have rotated the head waiter away from the freed slot", 1, localAdmissions.get());

        localPermit.get().close();
        assertBusy(() -> assertEquals("the next freed slot goes to the other waiter", 1, remoteAdmissions.get()));

        remotePermit.get().close();
        assertBusy(() -> {
            assertEquals(0, ownerService.tracker().inFlight(remoteKey));
            assertEquals("unused grants deregister both drained waiters", 0, ownerService.waiterCount(remoteKey));
        });
    }

    /** Successive freed slots round-robin across registered coordinators instead of favouring one. */
    public void testOwnerPushAlternatesFreedSlotsAcrossCoordinators() throws Exception {
        ownerSeesWithGroup(coordinatorNode);
        ownerTransport.connectToNode(coordinatorNode);
        fillOwnerToLimit();
        List<String> admissions = new CopyOnWriteArrayList<>();
        AtomicReference<Releasable> latest = new AtomicReference<>();
        AtomicBoolean drained = new AtomicBoolean();
        coordinatorService.setGrantConsumer((bucketKey, permit) -> {
            if (drained.get()) {
                return NOT_ADMITTED;
            }
            latest.set(permit);
            admissions.add("remote");
            return ADMITTED;
        });
        ownerService.setGrantConsumer((bucketKey, permit) -> {
            if (drained.get()) {
                return NOT_ADMITTED;
            }
            latest.set(permit);
            admissions.add("local");
            return ADMITTED;
        });

        CapturingListener remoteDenial = new CapturingListener();
        coordinatorService.acquireAsync(
            remoteKey,
            1,
            SharedThrottleTestPermits.NO_SUCH_TASK,
            false,
            remoteDenial,
            () -> fail("the reachable coordinator must be registered")
        );
        remoteDenial.await();
        assertTrue(isDenial(remoteDenial));
        CapturingListener localDenial = new CapturingListener();
        ownerService.acquireAsync(
            remoteKey,
            1,
            SharedThrottleTestPermits.NO_SUCH_TASK,
            false,
            localDenial,
            () -> fail("the local owner always registers itself")
        );
        assertTrue(isDenial(localDenial));
        assertEquals(2, ownerService.waiterCount(remoteKey));

        ownerService.handleRelease(new WorkloadGroupSharedThrottleService.ReleasePermitRequest(remoteKey, "pre"));
        for (int i = 1; i <= 4; i++) {
            final int admitted = i;
            // Wait for the remote GRANT's acknowledgement too, so its follow-up offer has been made.
            assertBusy(() -> {
                assertEquals(admitted, admissions.size());
                assertEquals(0, ownerService.pendingGrantCount(remoteKey));
            });
            if (i < 4) {
                latest.get().close();
            }
        }
        assertEquals(List.of("remote", "local", "remote", "local"), admissions);

        drained.set(true);
        latest.get().close();
        assertBusy(() -> {
            assertEquals(0, ownerService.waiterCount(remoteKey));
            assertEquals(0, ownerService.tracker().inFlight(remoteKey));
        });
    }

    // --- a coordinator's admission stack over the shared tier ----------------------------------------------------------

    /**
     * A coordinator's WLM admission wired as in Node: a real WorkloadGroupService and WorkloadGroupQueueService over one
     * coordinator's shared throttle service, for one shared-only group ({@code shared_limit=1}) that queues. Their
     * executor hops run inline, so a request is parked, admitted or rejected on the thread that decides it, and the
     * backstop clock moves only when a test runs a pass, so every {@link #backstopPass} is due.
     */
    private final class CoordinatorStack {
        final String groupId;
        final String bucketKey;
        final WorkloadGroupService workloadGroupService;
        final WorkloadGroupQueueService queueService;
        final WorkloadGroupsStateAccessor stateAccessor = new WorkloadGroupsStateAccessor();
        private final AtomicLong backstopClock = new AtomicLong();

        CoordinatorStack(
            WorkloadGroupSharedThrottleService sharedService,
            ClusterService clusterService,
            DiscoveryNodes nodes,
            String groupId
        ) {
            this.groupId = groupId;
            this.bucketKey = groupBucketKey(groupId);
            applyState(clusterService, stateWithQueueingGroup(nodes, groupId));
            final ThreadPool inlinePool = mock(ThreadPool.class);
            when(inlinePool.getThreadContext()).thenReturn(threadPool.getThreadContext());
            when(inlinePool.executor(ThreadPool.Names.GENERIC)).thenReturn(OpenSearchExecutors.newDirectExecutorService());
            when(inlinePool.relativeTimeInNanos()).thenAnswer(invocation -> backstopClock.get());
            when(inlinePool.preciseRelativeTimeInNanos()).thenAnswer(invocation -> System.nanoTime());
            final WorkloadManagementSettings settings = mock(WorkloadManagementSettings.class);
            when(settings.getWlmMode()).thenReturn(WlmMode.ENABLED);
            stateAccessor.addNewWorkloadGroup(groupId);
            workloadGroupService = new WorkloadGroupService(
                mock(WorkloadGroupTaskCancellationService.class),
                clusterService,
                inlinePool,
                settings,
                mock(NodeDuressTrackers.class),
                stateAccessor,
                new HashSet<>(),
                new HashSet<>()
            );
            queueService = new WorkloadGroupQueueService(inlinePool, stateAccessor);
            workloadGroupService.setSharedThrottleService(sharedService);
            workloadGroupService.setQueueService(queueService);
            sharedService.setQueueService(queueService);
        }

        // Submits one search of the group for admission. A parked one is neither admitted nor failed yet.
        CapturingListener admit() {
            return admit(new SearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME, () -> "", TaskId.EMPTY_TASK_ID, Map.of()));
        }

        // As admit(), for a given search task, e.g. one registered with the coordinator's task manager.
        CapturingListener admit(WorkloadGroupTask task) {
            final ThreadContext context = new ThreadContext(Settings.EMPTY);
            context.putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, groupId);
            task.setWorkloadGroupId(context);
            final CapturingListener listener = new CapturingListener();
            workloadGroupService.acquireThrottlePermit(task, () -> false, listener);
            return listener;
        }

        // One queue backstop pass (WorkloadGroupService#doRun): re-registers for every bucket holding WAITING requests.
        void backstopPass() {
            backstopClock.addAndGet(WorkloadGroupService.QUEUE_BACKSTOP_SWEEP_INTERVAL_NANOS);
            workloadGroupService.doRun();
        }

        int waiting() {
            return queueService.waitingDepth(groupId);
        }

        long throttled() {
            return stateAccessor.getWorkloadGroupState(groupId).getTotalThrottled();
        }
    }

    // The setUp coordinator's stack, for a group whose bucket the remote owner owns; the owner's applied state holds both
    // nodes and the group.
    private CoordinatorStack coordinatorStackOnTheRemoteOwner() {
        final DiscoveryNodes bothNodes = DiscoveryNodes.builder()
            .add(coordinatorNode)
            .add(ownerNode)
            .localNodeId(coordinatorNode.getId())
            .build();
        final CoordinatorStack stack = new CoordinatorStack(
            coordinatorService,
            coordinatorClusterService,
            bothNodes,
            findGroupOwnedBy(ownerNode, coordinatorService, ownerService)
        );
        applyState(ownerClusterService, stateWithGroup(ownerView(coordinatorNode), stack.bucketKey, 1));
        return stack;
    }

    /**
     * Parks one request on a registered denial at the owner, whose bucket is full, then has the owner drop that
     * registration the way it drops any waiter it cannot reach: the holder's release offers the freed slot while the
     * owner is disconnected from this coordinator. The request is left WAITING with no registration at the owner, as
     * after a lost wake-up, and the owner's slot is free.
     */
    private CapturingListener parkWithoutAnOwnerRegistration(CoordinatorStack stack) throws Exception {
        fillOwnerToLimit(stack.bucketKey);
        final CapturingListener parked = stack.admit();
        awaitAcquireOutcome(stack, parked);
        assertParked(stack, parked);
        assertEquals("the denial registered this coordinator", 1, ownerService.waiterCount(stack.bucketKey));

        ownerTransport.disconnectFromNode(coordinatorNode);
        ownerService.handleRelease(new WorkloadGroupSharedThrottleService.ReleasePermitRequest(stack.bucketKey, "pre"));
        assertEquals("the owner dropped the waiter it could not reach", 0, ownerService.waiterCount(stack.bucketKey));
        assertEquals("and kept the freed slot", 0, ownerService.tracker().inFlight(stack.bucketKey));
        ownerTransport.connectToNode(coordinatorNode);
        assertParked(stack, parked);
        return parked;
    }

    // Waits until the request's acquire outcome was applied on the coordinator: it parked, or it completed.
    private static void awaitAcquireOutcome(CoordinatorStack stack, CapturingListener request) throws Exception {
        assertBusy(() -> assertTrue("no acquire outcome applied yet", request.invocations.get() > 0 || stack.waiting() > 0));
    }

    // The request is parked: neither admitted nor rejected, and counted in its group's WAITING depth.
    private static void assertParked(CoordinatorStack stack, CapturingListener request) {
        assertEquals("the request must be neither admitted nor rejected, failure: " + request.failure.get(), 0, request.invocations.get());
        assertEquals("the request must be WAITING", 1, stack.waiting());
        assertEquals("nothing may be throttled", 0, stack.throttled());
    }

    // The parked request is admitted, once, by the grant the remote owner pushed after registering this coordinator, and
    // closing it frees the owner's slot.
    private void assertAdmittedByTheOwner(CoordinatorStack stack, CapturingListener parked) throws Exception {
        parked.await();
        assertNotNull("the owner's grant must admit the request, failure: " + parked.failure.get(), parked.response.get());
        assertEquals("admitted exactly once", 1, parked.invocations.get());
        assertEquals(0, stack.waiting());
        assertEquals(0, stack.throttled());
        assertEquals("the admitted request holds the owner's only slot", 1, ownerService.tracker().inFlight(stack.bucketKey));
        parked.response.get().close();
        assertBusy(() -> {
            assertEquals(0, ownerService.tracker().inFlight(stack.bucketKey));
            assertEquals("the drained coordinator's unused follow-up grant deregisters it", 0, ownerService.waiterCount(stack.bucketKey));
        });
    }

    // A transport for the owner node after a restart: the same persistent node id, so the ring gives it the same buckets,
    // under a new ephemeral id, so it is a new incarnation.
    private MockTransportService newRestartedOwnerTransport() {
        return new MockTransportService(
            Settings.EMPTY,
            MockTransportService.newMockTransport(Settings.EMPTY, Version.CURRENT, threadPool),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> new DiscoveryNode(
                "restarted-owner",
                ownerNode.getId(),
                boundAddress.publishAddress(),
                Collections.emptyMap(),
                ownerNode.getRoles(),
                Version.CURRENT
            ),
            null,
            Collections.emptySet(),
            NoopTracer.INSTANCE
        );
    }

    // Stubs clusterService's applied state. The state is an argument, so its own mocks are stubbed before this stubbing
    // starts (Mockito rejects stubbing nested inside another).
    private static void applyState(ClusterService clusterService, ClusterState state) {
        when(clusterService.state()).thenReturn(state);
    }

    // The owner's applied node set: itself plus others.
    private DiscoveryNodes ownerView(DiscoveryNode... others) {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder().add(ownerNode).localNodeId(ownerNode.getId());
        for (DiscoveryNode other : others) {
            builder.add(other);
        }
        return builder.build();
    }

    // A coordinator's applied state: nodes, and groupId as a shared-only group (shared_limit=1) that queues 5 per bucket.
    private static ClusterState stateWithQueueingGroup(DiscoveryNodes nodes, String groupId) {
        WorkloadGroup group = new WorkloadGroup(
            groupId + "-name",
            groupId,
            new MutableWorkloadGroupFragment(
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.EMPTY,
                Settings.builder().put("shared_limit", 1).build(),
                Settings.builder().put("size_per_bucket", 5).build()
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

    private static boolean isDenial(CapturingListener listener) {
        return WorkloadGroupSharedThrottleService.isDenial(listener.failure.get());
    }

    /** Takes the bucket's only shared slot on the owner so the next acquire is denied at the source. */
    private void fillOwnerToLimit() {
        fillOwnerToLimit(remoteKey);
    }

    private void fillOwnerToLimit(String bucketKey) {
        assertTrue(
            SharedThrottleTestPermits.tryAcquire(
                ownerService.tracker(),
                bucketKey,
                1,
                "pre",
                WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS,
                "other-coordinator"
            )
        );
        assertEquals(1, ownerService.tracker().inFlight(bucketKey));
    }

    /** The OWNER applies a cluster state holding itself plus {@code others} (the coordinator, or nothing). */
    private void ownerSees(DiscoveryNode... others) {
        ClusterState state = mock(ClusterState.class);
        when(state.nodes()).thenReturn(ownerView(others));
        ownerApplies(state);
    }

    /**
     * As {@link #ownerSees}, but the state also holds the bucket's workload group so the owner can resolve
     * {@code shared_limit} from its own state — required by the release path, which drives owner-push only for a
     * resolvable limit.
     */
    private void ownerSeesWithGroup(DiscoveryNode... others) {
        ownerApplies(stateWithGroup(ownerView(others)));
    }

    // The OWNER applies state as the cluster applier does: it becomes the applied state, and then the owner's listener sees
    // its membership, which is where the owner looks up the coordinator processes it registers as waiters.
    private void ownerApplies(ClusterState state) {
        applyState(ownerClusterService, state);
        deliverNodes(ownerService, state.nodes());
    }

    private ClusterState stateWithGroup(DiscoveryNodes nodes) {
        return stateWithGroup(nodes, 1);
    }

    private ClusterState stateWithGroup(DiscoveryNodes nodes, int sharedLimit) {
        return stateWithGroup(nodes, remoteKey, sharedLimit);
    }

    // A state with nodes and the workload group of bucketKey at the given shared_limit.
    private static ClusterState stateWithGroup(DiscoveryNodes nodes, String bucketKey, int sharedLimit) {
        final int idx = bucketKey.indexOf(':');
        final String groupId = idx < 0 ? bucketKey : bucketKey.substring(0, idx);
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
