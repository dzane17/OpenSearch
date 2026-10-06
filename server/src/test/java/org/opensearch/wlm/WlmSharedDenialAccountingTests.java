/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.action.search.SearchTask;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.search.backpressure.trackers.NodeDuressTrackers;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.cancellation.WorkloadGroupTaskCancellationService;
import org.opensearch.wlm.stats.WorkloadGroupState;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

/**
 * End-to-end accounting probes for the SHARED-tier queueing paths in {@link WorkloadGroupService}, driven by a mocked
 * {@link WorkloadGroupSharedThrottleService} so each owner verdict (granted / unavailable / denied-registered /
 * denied-unregistered / search-pool rejection) can be forced deterministically without transport.
 */
public class WlmSharedDenialAccountingTests extends OpenSearchTestCase {

    private static final String GROUP = "wg-shared";
    private static final String BUCKET = GROUP + ":group:group";

    private ClusterService mockClusterService;
    private ThreadPool mockThreadPool;
    private WorkloadManagementSettings mockSettings;
    private WorkloadGroupsStateAccessor stateAccessor;
    private WorkloadGroupService service;
    private WorkloadGroupQueueService queueService;
    private WorkloadGroupSharedThrottleService mockShared;
    private Metadata metadata;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockClusterService = Mockito.mock(ClusterService.class);
        mockThreadPool = Mockito.mock(ThreadPool.class);
        // The queue wraps retained listeners to restore the caller's thread context, so the mock needs a real one.
        when(mockThreadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        mockSettings = Mockito.mock(WorkloadManagementSettings.class);
        NodeDuressTrackers duress = Mockito.mock(NodeDuressTrackers.class);
        when(duress.isNodeInDuress()).thenReturn(false);
        when(mockSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        // Direct executor so the queue's admit/reject dispatch completes synchronously and assertions stay simple.
        when(mockThreadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(OpenSearchExecutors.newDirectExecutorService());

        stateAccessor = new WorkloadGroupsStateAccessor();
        stateAccessor.addNewWorkloadGroup(GROUP);

        service = new WorkloadGroupService(
            Mockito.mock(WorkloadGroupTaskCancellationService.class),
            mockClusterService,
            mockThreadPool,
            mockSettings,
            duress,
            stateAccessor,
            new HashSet<>(),
            new HashSet<>()
        );

        // shared_limit only (no node tier) so every request goes straight to the shared/queueing path.
        ClusterState clusterState = Mockito.mock(ClusterState.class);
        metadata = Mockito.mock(Metadata.class);
        when(mockClusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        stubGroup(MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED);

        queueService = new WorkloadGroupQueueService(mockThreadPool, stateAccessor);
        service.setQueueService(queueService);

        mockShared = Mockito.mock(WorkloadGroupSharedThrottleService.class);
        service.setSharedThrottleService(mockShared);
    }

    private void stubGroup(MutableWorkloadGroupFragment.ResiliencyMode mode) {
        WorkloadGroup wg = new WorkloadGroup(
            GROUP + "-name",
            GROUP,
            new MutableWorkloadGroupFragment(
                mode,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.EMPTY,
                Settings.builder().put("shared_limit", 1).build(),
                Settings.builder().put("size_per_bucket", 1).build()
            ),
            1L
        );
        when(metadata.workloadGroups()).thenReturn(Map.of(GROUP, wg));
    }

    private WorkloadGroupState state() {
        return stateAccessor.getWorkloadGroupState(GROUP);
    }

    /** Issues one request through the full acquire path, returning what the listener saw. */
    private Outcome request() {
        return request(true);
    }

    private Outcome request(boolean mayQueue) {
        WorkloadGroupTask task = new SearchTask(randomNonNegativeLong(), "", "", () -> "", null, null);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, GROUP);
        task.setWorkloadGroupId(threadContext);
        Outcome outcome = new Outcome();
        outcome.task = task;
        ActionListener<Releasable> listener = ActionListener.wrap(p -> {
            outcome.permit.set(p);
            outcome.admitted.incrementAndGet();
        }, e -> {
            outcome.failure.set(e);
            outcome.failed.incrementAndGet();
        });
        if (mayQueue) {
            service.acquireThrottlePermit(task, () -> false, listener);
        } else {
            service.acquireThrottlePermitWithoutQueueing(task, listener);
        }
        return outcome;
    }

    private static final class Outcome {
        final AtomicReference<Releasable> permit = new AtomicReference<>();
        final AtomicReference<Exception> failure = new AtomicReference<>();
        final AtomicInteger admitted = new AtomicInteger();
        final AtomicInteger failed = new AtomicInteger();
        WorkloadGroupTask task;
    }

    /** Stubs the shared service's queue-aware acquire with a caller-supplied verdict. */
    private void stubAcquire(Verdict verdict) {
        doAnswer(inv -> {
            @SuppressWarnings("unchecked")
            ActionListener<Releasable> listener = (ActionListener<Releasable>) inv.getArgument(3);
            Runnable onNoWaiterRegistered = inv.getArgument(4);
            verdict.apply(listener, onNoWaiterRegistered);
            return null;
        }).when(mockShared).acquireAsync(anyString(), anyInt(), anyBoolean(), any(), any());
    }

    /** Stubs the shared service's non-queueing acquire with a caller-supplied outcome. */
    private void stubUnqueuedAcquire(Consumer<ActionListener<Releasable>> outcome) {
        doAnswer(inv -> {
            @SuppressWarnings("unchecked")
            ActionListener<Releasable> listener = (ActionListener<Releasable>) inv.getArgument(3);
            outcome.accept(listener);
            return null;
        }).when(mockShared).acquireAsync(anyString(), anyInt(), anyBoolean(), any());
    }

    private static WorkloadGroupTask newTask() {
        return new SearchTask(randomNonNegativeLong(), "", "", () -> "", null, null);
    }

    // The handler WorkloadGroupService registers to reject WAITING requests whose shared tier became unavailable.
    @SuppressWarnings("unchecked")
    private Consumer<String> sharedTierUnavailableHandler() {
        ArgumentCaptor<Consumer<String>> captor = ArgumentCaptor.forClass(Consumer.class);
        Mockito.verify(mockShared).setSharedTierUnavailableHandler(captor.capture());
        return captor.getValue();
    }

    private interface Verdict {
        void apply(ActionListener<Releasable> listener, Runnable onNoWaiterRegistered);
    }

    // ---------------------------------------------------------------------------------------------------------------

    /**
     * BASELINE: owner denied but DID register the coordinator. The request stays parked awaiting owner-push, and nothing
     * is thrown away. It is NOT yet counted in total_queued — a denial means "will wait", not "has waited", and the count
     * is taken on admission so it can never disagree with the wait sum. Until then it shows up as live depth.
     */
    public void testDeniedAndRegisteredParksWithoutCountingYet() {
        stubAcquire((listener, noWaiter) -> listener.onFailure(WorkloadGroupSharedThrottleService.deniedMarker()));

        Outcome o = request();
        assertEquals("the request must stay parked, not completed", 0, o.admitted.get() + o.failed.get());
        assertEquals("it is parked", 1, queueService.currentDepth(GROUP));
        assertEquals("a denial alone does not count towards total_queued", 0, state().getTotalQueued());
        assertEquals("nor does it record a wait yet", 0, state().getTotalQueueWaitMillis());
        assertEquals("nothing was rejected", 0, state().getTotalThrottled());

        // Now finish the wait the way owner-push would: the count and the wait sample appear together.
        assertTrue(queueService.admitWithPermit(BUCKET, () -> {}));
        assertEquals("the completed wait is counted exactly once", 1, state().getTotalQueued());
        assertEquals(0, queueService.currentDepth(GROUP));
    }

    /**
     * BASELINE: owner denied and could NOT register. The parked request must be failed with a 429 rather than left to
     * hang, and must NOT be counted as queued.
     */
    public void testDeniedAndUnregisteredRejectsWith429() {
        stubAcquire((listener, noWaiter) -> noWaiter.run());

        Outcome o = request();
        assertEquals("the refused request must be failed, not parked", 1, o.failed.get());
        assertTrue("must be the throttle 429, was: " + o.failure.get(), o.failure.get() instanceof OpenSearchRejectedExecutionException);
        assertEquals("queue must be empty again", 0, queueService.currentDepth(GROUP));
        assertEquals("a request refused outright never waited", 0, state().getTotalQueued());
        assertEquals("it is a throttle rejection", 1, state().getTotalThrottled());
    }

    /** BASELINE: a granted acquire admits the request and counts neither a queue wait nor a throttle. */
    public void testGrantedAdmitsWithoutCountingQueuedOrThrottled() {
        Releasable permit = () -> {};
        stubAcquire((listener, noWaiter) -> listener.onResponse(permit));

        Outcome o = request();
        assertEquals("granted request must be admitted", 1, o.admitted.get());
        assertSame(permit, o.permit.get());
        assertTrue("a request admitted against a shared permit carries the throttle charge", o.task.isThrottleCounted());
        assertEquals(0, queueService.currentDepth(GROUP));
        assertEquals("an enqueue-first pass-through is not a wait", 0, state().getTotalQueued());
        assertEquals(0, state().getTotalThrottled());
        assertEquals("no queue wait recorded for a self-supplied admission", 0, state().getTotalQueueWaitMillis());
    }

    /**
     * FAIL-CLOSED (replaces the target's fail-open "admit untracked" baseline): when the shared tier cannot answer, the
     * provisional request is taken back and rejected with the unavailable 429. It is never admitted untracked and never
     * parked as WAITING, since no owner registered it and no grant can come.
     */
    public void testUnavailableSharedTierRejectsWith429AndNeverParks() {
        stubAcquire((listener, noWaiter) -> listener.onFailure(WorkloadGroupSharedThrottleService.unavailableMarker()));

        Outcome o = request();
        assertEquals("an unavailable shared tier must not admit", 0, o.admitted.get());
        assertEquals(1, o.failed.get());
        assertTrue(o.failure.get() instanceof OpenSearchRejectedExecutionException);
        assertTrue(o.failure.get().getMessage(), o.failure.get().getMessage().contains("cluster-wide throttle unavailable"));
        assertEquals("never parked", 0, queueService.retainedDepth(GROUP));
        assertEquals(0, state().getTotalQueued());
        assertEquals(0, state().getTotalQueueRejections());
        assertEquals("an unavailable-tier rejection is a throttle rejection", 1, state().getTotalThrottled());
        assertFalse(o.task.isThrottleCounted());
    }

    /** An unexpected shared-acquire failure fails closed exactly like an unavailable tier. */
    public void testUnexpectedSharedFailureFailsClosed() {
        stubAcquire((listener, noWaiter) -> listener.onFailure(new IllegalStateException("boom")));

        Outcome o = request();
        assertEquals(0, o.admitted.get());
        assertTrue(o.failure.get().getMessage(), o.failure.get().getMessage().contains("cluster-wide throttle unavailable"));
        assertEquals(0, queueService.retainedDepth(GROUP));
        assertEquals(1, state().getTotalThrottled());
    }

    /**
     * A search-pool rejection of the acquire hand-off (any granted permit already released) is passed through as the
     * pool's own 429: it is not a shared-limit denial, so it must not park the request as WAITING or count a throttle.
     */
    public void testSearchPoolRejectionIsPassedThroughAndNeverParks() {
        OpenSearchRejectedExecutionException poolRejection = new OpenSearchRejectedExecutionException("search pool full");
        stubAcquire((listener, noWaiter) -> listener.onFailure(poolRejection));

        Outcome o = request();
        assertSame("the pool's rejection reaches the client unchanged", poolRejection, o.failure.get());
        assertEquals(0, queueService.retainedDepth(GROUP));
        assertEquals(0, state().getTotalQueueRejections());
        assertEquals("not a throttle breach", 0, state().getTotalThrottled());
    }

    /** A stale unavailable outcome whose entry a racing grant already served is a no-op: no 429, no throttle count. */
    public void testUnavailableAfterRacingDrainIsNoOp() {
        stubAcquire((listener, noWaiter) -> {
            assertTrue(queueService.admitWithPermit(BUCKET, () -> {}));
            listener.onFailure(WorkloadGroupSharedThrottleService.unavailableMarker());
        });

        Outcome o = request();
        assertEquals(1, o.admitted.get());
        assertEquals(0, o.failed.get());
        assertEquals(0, state().getTotalThrottled());
    }

    /**
     * MONITOR never parks, even with a queue configured: it takes the non-queueing shared path, and an unavailable tier
     * there is observed (would_throttle) and admitted, as before queueing.
     */
    public void testMonitorModeNeverParksAndObservesAnUnavailableTier() {
        stubGroup(MutableWorkloadGroupFragment.ResiliencyMode.MONITOR);
        doAnswer(inv -> {
            @SuppressWarnings("unchecked")
            ActionListener<Releasable> listener = (ActionListener<Releasable>) inv.getArgument(3);
            listener.onFailure(WorkloadGroupSharedThrottleService.unavailableMarker());
            return null;
        }).when(mockShared).acquireAsync(anyString(), anyInt(), anyBoolean(), any());

        Outcome o = request();
        assertEquals("MONITOR admits", 1, o.admitted.get());
        assertNull("untracked", o.permit.get());
        assertEquals(0, queueService.retainedDepth(GROUP));
        assertEquals(1, state().getTotalWouldThrottle());
        assertEquals(0, state().getTotalThrottled());
        Mockito.verify(mockShared, Mockito.never()).acquireAsync(anyString(), anyInt(), anyBoolean(), any(), any());
    }

    /**
     * A concurrent drain admits the provisional request before the denied-and-unregistered reply lands. The exact
     * pending token is therefore already gone when the reply tries to reject it. The request was served, so no
     * rejection occurred and {@code total_throttled} must not increase.
     */
    public void testUnregisteredDenialAfterRacingDrainDoesNotCountAThrottle() {
        stubAcquire((listener, noWaiter) -> {
            // A racing owner-push grant / node-tier drain serves the parked request first...
            assertTrue("precondition: the racing drain must find the parked request", queueService.admitWithPermit(BUCKET, () -> {}));
            // ...and only then does our denied-and-unregistered reply arrive.
            noWaiter.run();
        });

        Outcome o = request();
        assertEquals("the request WAS served by the racing drain", 1, o.admitted.get());
        assertEquals("and it was not failed", 0, o.failed.get());
        assertEquals(0, queueService.currentDepth(GROUP));
        assertEquals("the stale denial must not count a rejection after the request succeeded", 0, state().getTotalThrottled());
    }

    /**
     * The motivating Option-A regression: pending owner RPCs do not consume queue.size_per_bucket. With a configured
     * waiting depth of one, several requests can all be retained while their owner acquires are in flight and then run
     * on free permits without receiving false queue-full 429s.
     */
    public void testPendingBurstWithFreePermitsDoesNotFillWaitingQueue() {
        List<ActionListener<Releasable>> ownerReplies = new ArrayList<>();
        doAnswer(inv -> {
            @SuppressWarnings("unchecked")
            ActionListener<Releasable> listener = (ActionListener<Releasable>) inv.getArgument(3);
            ownerReplies.add(listener);
            return null;
        }).when(mockShared).acquireAsync(anyString(), anyInt(), anyBoolean(), any(), any());

        List<Outcome> outcomes = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            outcomes.add(request());
        }
        assertEquals(10, ownerReplies.size());
        assertEquals("PENDING_ACQUIRE is not queued_current", 0, queueService.currentDepth(GROUP));
        assertEquals(10, queueService.retainedDepth(GROUP));
        assertEquals(0, state().getTotalQueueRejections());
        assertEquals(0, state().getTotalThrottled());

        ownerReplies.forEach(reply -> reply.onResponse(() -> {}));

        for (Outcome outcome : outcomes) {
            assertEquals(1, outcome.admitted.get());
            assertEquals(0, outcome.failed.get());
        }
        assertEquals(0, queueService.retainedDepth(GROUP));
        assertEquals(0, state().getTotalQueueRejections());
        assertEquals(0, state().getTotalThrottled());
    }

    /** An early pushed grant followed by the original registered denial response completes the request exactly once. */
    public void testPushedGrantBeforeRegisteredDenialResponseMakesDenialNoOp() {
        stubAcquire((listener, noWaiter) -> {
            assertTrue(queueService.admitWithPermit(BUCKET, () -> {}));
            listener.onFailure(WorkloadGroupSharedThrottleService.deniedMarker());
        });

        Outcome outcome = request();
        assertEquals(1, outcome.admitted.get());
        assertEquals(0, outcome.failed.get());
        assertEquals(0, queueService.retainedDepth(GROUP));
        assertEquals(0, state().getTotalQueued());
        assertEquals(0, state().getTotalThrottled());
    }

    /** Only a real denial transition consumes the configured one-slot waiting queue. */
    public void testSecondRegisteredDenialIsRejectedWhenWaitingQueueIsFull() {
        List<ActionListener<Releasable>> ownerReplies = new ArrayList<>();
        doAnswer(inv -> {
            @SuppressWarnings("unchecked")
            ActionListener<Releasable> listener = (ActionListener<Releasable>) inv.getArgument(3);
            ownerReplies.add(listener);
            return null;
        }).when(mockShared).acquireAsync(anyString(), anyInt(), anyBoolean(), any(), any());

        Outcome first = request();
        Outcome second = request();
        assertEquals(0, queueService.currentDepth(GROUP));
        assertEquals(2, queueService.retainedDepth(GROUP));

        ownerReplies.get(0).onFailure(WorkloadGroupSharedThrottleService.deniedMarker());
        ownerReplies.get(1).onFailure(WorkloadGroupSharedThrottleService.deniedMarker());

        assertEquals(0, first.admitted.get() + first.failed.get());
        assertEquals(1, second.failed.get());
        assertEquals(1, queueService.currentDepth(GROUP));
        assertEquals(1, queueService.retainedDepth(GROUP));
        assertEquals(1, state().getTotalQueueRejections());
        assertEquals(1, state().getTotalThrottled());
    }

    /**
     * Repeated unregistered denials must each reject exactly one request and never double-count or leak parked
     * requests — the shared-only strand this whole mechanism exists to prevent.
     */
    public void testRepeatedUnregisteredDenialsRejectOneEach() {
        stubAcquire((listener, noWaiter) -> noWaiter.run());

        for (int i = 1; i <= 5; i++) {
            Outcome o = request();
            assertEquals("request " + i + " must be failed", 1, o.failed.get());
            assertEquals("no request may be left parked", 0, queueService.currentDepth(GROUP));
            assertEquals("one throttle per refused request", i, state().getTotalThrottled());
            assertEquals("none of them waited", 0, state().getTotalQueued());
        }
    }

    /** A null Runnable never reaches the coordinator on the queueing path: the callback must always be supplied. */
    public void testQueueingPathAlwaysSuppliesTheNoWaiterCallback() {
        AtomicReference<Object> captured = new AtomicReference<>();
        doAnswer(inv -> {
            captured.set(inv.getArgument(4));
            return null;
        }).when(mockShared).acquireAsync(anyString(), anyInt(), anyBoolean(), any(), any());

        request();
        assertNotNull("the enqueue-first path must always pass a no-waiter callback", captured.get());
        assertTrue(captured.get() instanceof Runnable);
    }

    /**
     * Scroll continuations are throttled but never retained: a page parked behind a backlog could outlive its scroll's
     * keep_alive, so a shared denial is an immediate 429 and the queueing acquire is never used.
     */
    public void testWithoutQueueingSharedDenialRejectsAtOnceAndNeverParks() {
        stubUnqueuedAcquire(listener -> listener.onFailure(WorkloadGroupSharedThrottleService.deniedMarker()));

        Outcome o = request(false);
        assertEquals(1, o.failed.get());
        assertTrue(o.failure.get() instanceof OpenSearchRejectedExecutionException);
        assertTrue(o.failure.get().getMessage(), o.failure.get().getMessage().contains("shared limit of 1"));
        assertEquals("never retained", 0, queueService.retainedDepth(GROUP));
        assertEquals("not a queue rejection: queueing was never offered", 0, state().getTotalQueueRejections());
        assertEquals(1, state().getTotalThrottled());
        Mockito.verify(mockShared, Mockito.never()).acquireAsync(anyString(), anyInt(), anyBoolean(), any(), any());
    }

    /**
     * A full per-group retained-request ceiling must not reject a request whose own bucket has shared capacity: it still
     * asks the owner (without retaining the request), and a grant admits it.
     */
    public void testFullRetainedCeilingStillAdmitsOnAGrant() {
        fillRetainedCeilingWithOtherBuckets();
        Releasable permit = () -> {};
        stubUnqueuedAcquire(listener -> listener.onResponse(permit));

        Outcome o = request();
        assertEquals("a full queue must not reject a request the owner admits", 1, o.admitted.get());
        assertSame(permit, o.permit.get());
        assertTrue(o.task.isThrottleCounted());
        assertEquals(0, state().getTotalQueueRejections());
        assertEquals(0, state().getTotalThrottled());
    }

    /** With the ceiling full, only a real shared denial rejects, counted as both a throttle and a queue rejection. */
    public void testFullRetainedCeilingRejectsOnlyARealDenial() {
        fillRetainedCeilingWithOtherBuckets();
        stubUnqueuedAcquire(listener -> listener.onFailure(WorkloadGroupSharedThrottleService.deniedMarker()));

        Outcome o = request();
        assertEquals(1, o.failed.get());
        assertTrue(o.failure.get().getMessage(), o.failure.get().getMessage().contains("shared limit of 1"));
        assertEquals(1, state().getTotalQueueRejections());
        assertEquals(1, state().getTotalThrottled());
    }

    /** With the ceiling full, an unavailable shared tier still fails closed, with the unavailable 429. */
    public void testFullRetainedCeilingStillFailsClosed() {
        fillRetainedCeilingWithOtherBuckets();
        stubUnqueuedAcquire(listener -> listener.onFailure(WorkloadGroupSharedThrottleService.unavailableMarker()));

        Outcome o = request();
        assertEquals(0, o.admitted.get());
        assertTrue(o.failure.get().getMessage(), o.failure.get().getMessage().contains("cluster-wide throttle unavailable"));
        assertEquals(0, state().getTotalQueueRejections());
        assertEquals(1, state().getTotalThrottled());
    }

    private void fillRetainedCeilingWithOtherBuckets() {
        for (int i = 0; i < WorkloadGroupQueueSettings.MAX_GROUP_QUEUE_DEPTH; i++) {
            assertNotNull(
                queueService.tryRegisterPendingAcquire(
                    GROUP,
                    GROUP + ":username:noisy-" + (i % 10),
                    newTask(),
                    ActionListener.wrap(p -> {}, e -> {})
                )
            );
        }
    }

    /**
     * FAIL-CLOSED for requests that are already WAITING: once their registration can no longer be confirmed with an
     * owner, no grant can come, so they are rejected with the unavailable 429 instead of staying parked. A still-pending
     * request in the same bucket is left for its own acquire outcome.
     */
    public void testSharedTierUnavailableRejectsAlreadyWaitingRequests() {
        stubAcquire((listener, noWaiter) -> listener.onFailure(WorkloadGroupSharedThrottleService.deniedMarker()));
        Outcome waiting = request();
        assertEquals(1, queueService.currentDepth(GROUP));
        stubAcquire((listener, noWaiter) -> {}); // the owner never answers this one: it stays PENDING_ACQUIRE
        Outcome pending = request();
        assertEquals(2, queueService.retainedDepth(GROUP));

        sharedTierUnavailableHandler().accept(BUCKET);

        assertEquals(1, waiting.failed.get());
        assertTrue(waiting.failure.get().getMessage(), waiting.failure.get().getMessage().contains("cluster-wide throttle unavailable"));
        assertEquals(0, waiting.admitted.get());
        assertEquals("the pending request is settled by its own acquire, not here", 0, pending.admitted.get() + pending.failed.get());
        assertEquals(0, queueService.currentDepth(GROUP));
        assertEquals(1, queueService.retainedDepth(GROUP));
        assertEquals(1, state().getTotalThrottled());
        assertEquals("the ended WAITING interval is still counted", 1, state().getTotalQueued());
    }

    /** In MONITOR the same event observes (would_throttle) and admits the waiting request untracked, never rejects it. */
    public void testSharedTierUnavailableObservesAndAdmitsWaitingRequestsInMonitor() {
        stubAcquire((listener, noWaiter) -> listener.onFailure(WorkloadGroupSharedThrottleService.deniedMarker()));
        Outcome waiting = request();
        assertEquals(1, queueService.currentDepth(GROUP));
        stubGroup(MutableWorkloadGroupFragment.ResiliencyMode.MONITOR);

        sharedTierUnavailableHandler().accept(BUCKET);

        assertEquals(1, waiting.admitted.get());
        assertNull(waiting.permit.get());
        assertEquals(1, state().getTotalWouldThrottle());
        assertEquals(0, state().getTotalThrottled());
        assertEquals(0, queueService.retainedDepth(GROUP));
    }

    /** The periodic queue sweep re-confirms the waiter registration of every bucket holding WAITING shared requests. */
    public void testQueueSweepReconcilesBucketsWithWaitingSharedRequests() {
        stubAcquire((listener, noWaiter) -> listener.onFailure(WorkloadGroupSharedThrottleService.deniedMarker()));
        request();
        assertEquals(1, queueService.currentDepth(GROUP));

        service.doRun();

        Mockito.verify(mockShared).reconcileRetainedWaiters(BUCKET);
    }
}
