/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.action.search.SearchScrollAction;
import org.opensearch.action.search.SearchScrollRequest;
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
import org.opensearch.tasks.TaskManager;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.cancellation.WorkloadGroupTaskCancellationService;
import org.opensearch.wlm.stats.WorkloadGroupState;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
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
    private static final String QUEUE_FULL_DENIAL = "Request throttled: workload group ["
        + GROUP
        + "-name] reached its shared limit of 1 concurrent requests, and its request queue is full.";
    private static final String QUEUE_FULL_UNAVAILABLE = "Request throttled: workload group ["
        + GROUP
        + "-name] could not check its shared limit of 1 concurrent requests (cluster-wide throttle unavailable), "
        + "and its request queue is full.";

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
        // The ring has an owner, so shared_limit applies.
        when(mockShared.hasEligibleOwner()).thenReturn(true);
        service.setSharedThrottleService(mockShared);
    }

    private void stubGroup(MutableWorkloadGroupFragment.ResiliencyMode mode) {
        stubGroup(mode, Settings.builder().put("shared_limit", 1).build(), 1);
    }

    // node_limit=nodeLimit and shared_limit=1, so a request the node tier denies goes on to the shared tier.
    private void stubNodeAndSharedGroup(int nodeLimit) {
        stubGroup(
            MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED,
            Settings.builder().put("node_limit", nodeLimit).put("shared_limit", 1).build(),
            5
        );
    }

    private void stubGroup(MutableWorkloadGroupFragment.ResiliencyMode mode, Settings throttling, int sizePerBucket) {
        WorkloadGroup wg = new WorkloadGroup(
            GROUP + "-name",
            GROUP,
            new MutableWorkloadGroupFragment(
                mode,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.EMPTY,
                throttling,
                Settings.builder().put("size_per_bucket", sizePerBucket).build()
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
        return request(false);
    }

    // scrollPage: admit a scroll page through the search admission, as the scroll action's search gate does.
    private Outcome request(boolean scrollPage) {
        WorkloadGroupTask task = new SearchTask(
            randomNonNegativeLong(),
            "",
            scrollPage ? SearchScrollAction.NAME : "",
            () -> "",
            null,
            null
        );
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
        if (scrollPage) {
            new WorkloadGroupSearchAdmission(service, mockClusterService, Mockito.mock(TaskManager.class)).admit(
                task,
                new SearchScrollRequest("scroll-id"),
                listener
            );
        } else {
            service.acquireThrottlePermit(task, () -> false, listener);
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
            ActionListener<Releasable> listener = (ActionListener<Releasable>) inv.getArgument(4);
            Runnable onNoWaiterRegistered = inv.getArgument(5);
            verdict.apply(listener, onNoWaiterRegistered);
            return null;
        }).when(mockShared).acquireAsync(anyString(), anyInt(), anyLong(), anyBoolean(), any(), any());
    }

    /** Stubs the shared service's non-queueing acquire with a caller-supplied outcome. */
    private void stubUnqueuedAcquire(Consumer<ActionListener<Releasable>> outcome) {
        doAnswer(inv -> {
            @SuppressWarnings("unchecked")
            ActionListener<Releasable> listener = (ActionListener<Releasable>) inv.getArgument(4);
            outcome.accept(listener);
            return null;
        }).when(mockShared).acquireAsync(anyString(), anyInt(), anyLong(), anyBoolean(), any());
    }

    private static WorkloadGroupTask newTask() {
        return new SearchTask(randomNonNegativeLong(), "", "", () -> "", null, null);
    }

    // The handler WorkloadGroupService registers for a bucket whose ring has no eligible owner.
    @SuppressWarnings("unchecked")
    private Consumer<String> noEligibleOwnerHandler() {
        ArgumentCaptor<Consumer<String>> captor = ArgumentCaptor.forClass(Consumer.class);
        Mockito.verify(mockShared).setNoEligibleOwnerHandler(captor.capture());
        return captor.getValue();
    }

    private void stubDenialRegistered() {
        stubAcquire((listener, noWaiter) -> listener.onFailure(WorkloadGroupSharedThrottleService.deniedMarker()));
    }

    private interface Verdict {
        void apply(ActionListener<Releasable> listener, Runnable onNoWaiterRegistered);
    }

    // ---------------------------------------------------------------------------------------------------------------

    /**
     * BASELINE: owner denied but DID register the coordinator. The request stays parked awaiting owner-push, and nothing
     * is thrown away. The move to WAITING counts it in total_queued at once, so it shows up there and as live depth; its
     * wait is recorded only when the wait ends, and the admission that ends it does not count it again.
     */
    public void testDeniedAndRegisteredParksAndIsCountedAtEntry() {
        stubAcquire((listener, noWaiter) -> listener.onFailure(WorkloadGroupSharedThrottleService.deniedMarker()));

        Outcome o = request();
        assertEquals("the request must stay parked, not completed", 0, o.admitted.get() + o.failed.get());
        assertEquals("it is parked", 1, queueService.waitingDepth(GROUP));
        assertEquals("entering WAITING counts it in total_queued", 1, state().getTotalQueued());
        assertEquals("but records no wait yet", 0, state().getTotalQueueWaitMillis());
        assertEquals("nothing was rejected", 0, state().getTotalThrottled());

        // Now finish the wait the way owner-push would: it records the wait without counting the request again.
        assertTrue(queueService.admitWithPermit(BUCKET, () -> {}).isPresent());
        assertEquals("the request is counted exactly once", 1, state().getTotalQueued());
        assertEquals(0, queueService.waitingDepth(GROUP));
    }

    /**
     * Owner denied and could NOT register this coordinator, so no grant follows from that denial. The request parks anyway,
     * consuming the bucket's waiting budget like a registered denial; the next backstop pass reconciles its bucket, which
     * registers this coordinator with the owner, and the grant that follows admits it.
     */
    public void testDeniedAndUnregisteredParksUntilTheBackstopRegistersIt() {
        stubAcquire((listener, noWaiter) -> noWaiter.run());

        Outcome o = request();
        assertEquals("the request must stay parked, not be rejected", 0, o.admitted.get() + o.failed.get());
        assertEquals("it consumes the bucket's waiting budget", 1, queueService.waitingDepth(GROUP));
        assertEquals("and is counted when it enters WAITING", 1, state().getTotalQueued());
        assertEquals("nothing was rejected", 0, state().getTotalThrottled());
        assertEquals(0, state().getTotalQueueRejections());

        service.doRun();
        Mockito.verify(mockShared).reconcileRetainedWaiters(BUCKET);

        // The grant the owner pushes once that pass registered this coordinator.
        assertTrue(queueService.admitWithPermit(BUCKET, () -> {}).isPresent());
        assertEquals(1, o.admitted.get());
        assertTrue(o.task.isThrottleCounted());
        assertEquals("the admission does not count it again", 1, state().getTotalQueued());
    }

    /** BASELINE: a granted acquire admits the request and counts neither a queue wait nor a throttle. */
    public void testGrantedAdmitsWithoutCountingQueuedOrThrottled() {
        Releasable permit = () -> {};
        stubAcquire((listener, noWaiter) -> listener.onResponse(permit));

        Outcome o = request();
        assertEquals("granted request must be admitted", 1, o.admitted.get());
        assertSame(permit, o.permit.get());
        assertTrue("a request admitted against a shared permit carries the throttle charge", o.task.isThrottleCounted());
        assertEquals(0, queueService.waitingDepth(GROUP));
        assertEquals("an enqueue-first pass-through is not a wait", 0, state().getTotalQueued());
        assertEquals(0, state().getTotalThrottled());
        assertEquals("no queue wait recorded for a self-supplied admission", 0, state().getTotalQueueWaitMillis());
    }

    /**
     * When the shared tier cannot answer, the provisional request parks as WAITING (consuming the bucket's waiting budget)
     * instead of getting a 429. It is never admitted untracked: no owner registered this coordinator, so the next backstop
     * pass reconciles the bucket, and the grant that follows admits it.
     */
    public void testUnavailableSharedTierParksUntilTheBackstopRegistersIt() {
        stubAcquire((listener, noWaiter) -> listener.onFailure(WorkloadGroupSharedThrottleService.unavailableMarker()));

        Outcome o = request();
        assertEquals("an unavailable shared tier must neither admit nor reject", 0, o.admitted.get() + o.failed.get());
        assertEquals("it parks as WAITING", 1, queueService.waitingDepth(GROUP));
        assertEquals("and is counted when it enters WAITING", 1, state().getTotalQueued());
        assertFalse("not admitted, so not counted against a throttle", o.task.isThrottleCounted());
        assertEquals(0, state().getTotalThrottled());
        assertEquals(0, state().getTotalQueueRejections());

        service.doRun();
        Mockito.verify(mockShared).reconcileRetainedWaiters(BUCKET);

        assertTrue(queueService.admitWithPermit(BUCKET, () -> {}).isPresent());
        assertEquals(1, o.admitted.get());
        assertEquals(0, o.failed.get());
        assertTrue(o.task.isThrottleCounted());
        assertEquals("the admission does not count it again", 1, state().getTotalQueued());
    }

    /** An unexpected shared-acquire failure is treated exactly like an unavailable tier: the request parks. */
    public void testUnexpectedSharedFailureParksLikeAnUnavailableTier() {
        stubAcquire((listener, noWaiter) -> listener.onFailure(new IllegalStateException("boom")));

        Outcome o = request();
        assertEquals(0, o.admitted.get() + o.failed.get());
        assertEquals(1, queueService.waitingDepth(GROUP));
        assertEquals(0, state().getTotalThrottled());
    }

    /**
     * An unavailable outcome that finds the bucket's waiting budget full gets the unavailable 429 it would get without a
     * queue, which also says the queue is full, counted as a queue rejection, and leaves the request already waiting
     * there untouched.
     */
    public void testUnavailableSharedTierWithAFullBucketRejectsWithTheUnavailable429() {
        stubAcquire((listener, noWaiter) -> listener.onFailure(WorkloadGroupSharedThrottleService.unavailableMarker()));
        Outcome waiting = request(); // queue.size_per_bucket is 1
        assertEquals(1, queueService.waitingDepth(GROUP));

        Outcome rejected = request();
        assertEquals(1, rejected.failed.get());
        assertTrue(rejected.failure.get() instanceof OpenSearchRejectedExecutionException);
        assertEquals(QUEUE_FULL_UNAVAILABLE, rejected.failure.get().getMessage());
        assertEquals(1, state().getTotalQueueRejections());
        assertEquals(1, state().getTotalThrottled());
        assertEquals("only the request that entered WAITING is counted, not the refusal", 1, state().getTotalQueued());
        assertEquals("the request already waiting is untouched", 0, waiting.admitted.get() + waiting.failed.get());
        assertEquals(1, queueService.waitingDepth(GROUP));
        assertEquals(1, queueService.retainedDepth(GROUP));
    }

    /**
     * With node_limit set too, a request the node tier denied and the shared tier could not answer parks in its bucket,
     * and the node permit a completing request hands to that bucket admits it.
     */
    public void testUnavailableSharedTierParksAndAFreedNodePermitAdmitsIt() {
        stubNodeAndSharedGroup(1);
        Outcome running = request();
        assertNotNull("the first request takes the only node permit", running.permit.get());
        stubAcquire((listener, noWaiter) -> listener.onFailure(WorkloadGroupSharedThrottleService.unavailableMarker()));

        Outcome parked = request();
        assertEquals(0, parked.admitted.get() + parked.failed.get());
        assertEquals(1, queueService.waitingDepth(GROUP));

        running.permit.get().close();
        assertEquals("the freed node permit admits the parked request", 1, parked.admitted.get());
        assertTrue(parked.task.isThrottleCounted());
        assertEquals(0, state().getTotalThrottled());
        parked.permit.get().close();
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
            assertTrue(queueService.admitWithPermit(BUCKET, () -> {}).isPresent());
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
            ActionListener<Releasable> listener = (ActionListener<Releasable>) inv.getArgument(4);
            listener.onFailure(WorkloadGroupSharedThrottleService.unavailableMarker());
            return null;
        }).when(mockShared).acquireAsync(anyString(), anyInt(), anyLong(), anyBoolean(), any());

        Outcome o = request();
        assertEquals("MONITOR admits", 1, o.admitted.get());
        assertNull("untracked", o.permit.get());
        assertEquals(0, queueService.retainedDepth(GROUP));
        assertEquals(1, state().getTotalWouldThrottle());
        assertEquals(0, state().getTotalThrottled());
        Mockito.verify(mockShared, Mockito.never()).acquireAsync(anyString(), anyInt(), anyLong(), anyBoolean(), any(), any());
    }

    /**
     * A concurrent drain admits the provisional request before the denied-and-unregistered reply lands. The exact
     * pending token is therefore already gone when the reply tries to park it. The request was served, so it is neither
     * parked again nor rejected, and {@code total_throttled} must not increase.
     */
    public void testUnregisteredDenialAfterRacingDrainDoesNotCountAThrottle() {
        stubAcquire((listener, noWaiter) -> {
            // A racing owner-push grant / node-tier drain serves the parked request first...
            assertTrue(
                "precondition: the racing drain must find the parked request",
                queueService.admitWithPermit(BUCKET, () -> {}).isPresent()
            );
            // ...and only then does our denied-and-unregistered reply arrive.
            noWaiter.run();
        });

        Outcome o = request();
        assertEquals("the request WAS served by the racing drain", 1, o.admitted.get());
        assertEquals("and it was not failed", 0, o.failed.get());
        assertEquals(0, queueService.waitingDepth(GROUP));
        assertEquals("the stale denial must not count a rejection after the request succeeded", 0, state().getTotalThrottled());
    }

    /**
     * Pending owner RPCs do not consume queue.size_per_bucket. With a configured waiting depth of one, several requests
     * can all be retained while their owner acquires are in flight and then run on free permits without receiving false
     * queue-full 429s.
     */
    public void testPendingBurstWithFreePermitsDoesNotFillWaitingQueue() {
        List<ActionListener<Releasable>> ownerReplies = new ArrayList<>();
        doAnswer(inv -> {
            @SuppressWarnings("unchecked")
            ActionListener<Releasable> listener = (ActionListener<Releasable>) inv.getArgument(4);
            ownerReplies.add(listener);
            return null;
        }).when(mockShared).acquireAsync(anyString(), anyInt(), anyLong(), anyBoolean(), any(), any());

        List<Outcome> outcomes = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            outcomes.add(request());
        }
        assertEquals(10, ownerReplies.size());
        assertEquals("PENDING_ACQUIRE is not queued_current", 0, queueService.waitingDepth(GROUP));
        assertEquals(10, queueService.retainedDepth(GROUP));
        assertEquals(
            "the stats report WAITING depth only",
            0L,
            service.nodeStats(Set.of(GROUP), null).getStats().get(GROUP).getQueuedCurrent()
        );
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

    /** Only a WAITING request is queued: one still awaiting the owner's first answer (PENDING_ACQUIRE) is not. */
    public void testIsQueuedIsFalseWhilePendingAcquireAndTrueOnceADenialMovesItToWaiting() {
        List<ActionListener<Releasable>> ownerReplies = new ArrayList<>();
        doAnswer(inv -> {
            @SuppressWarnings("unchecked")
            ActionListener<Releasable> listener = (ActionListener<Releasable>) inv.getArgument(4);
            ownerReplies.add(listener);
            return null;
        }).when(mockShared).acquireAsync(anyString(), anyInt(), anyLong(), anyBoolean(), any(), any());

        Outcome o = request();
        assertEquals(1, ownerReplies.size());
        assertEquals("the request is retained as PENDING_ACQUIRE", 1, queueService.retainedDepth(GROUP));
        assertEquals(0, queueService.waitingDepth(GROUP));
        assertFalse("a PENDING_ACQUIRE request is not queued", service.isQueued(GROUP, o.task.getId()));

        ownerReplies.get(0).onFailure(WorkloadGroupSharedThrottleService.deniedMarker());
        assertEquals("the request must stay parked, not completed", 0, o.admitted.get() + o.failed.get());
        assertEquals(1, queueService.waitingDepth(GROUP));
        assertTrue("a denial moves it to WAITING, so it is queued", service.isQueued(GROUP, o.task.getId()));

        assertTrue(queueService.admitWithPermit(BUCKET, () -> {}).isPresent());
        assertEquals(1, o.admitted.get());
        assertFalse("an admitted request is no longer queued", service.isQueued(GROUP, o.task.getId()));
    }

    /** An early pushed grant followed by the original registered denial response completes the request exactly once. */
    public void testPushedGrantBeforeRegisteredDenialResponseMakesDenialNoOp() {
        stubAcquire((listener, noWaiter) -> {
            assertTrue(queueService.admitWithPermit(BUCKET, () -> {}).isPresent());
            listener.onFailure(WorkloadGroupSharedThrottleService.deniedMarker());
        });

        Outcome outcome = request();
        assertEquals(1, outcome.admitted.get());
        assertEquals(0, outcome.failed.get());
        assertEquals(0, queueService.retainedDepth(GROUP));
        assertEquals(0, state().getTotalQueued());
        assertEquals(0, state().getTotalThrottled());
    }

    /** Only a refusal's move to WAITING, not PENDING_ACQUIRE, consumes the configured one-slot waiting queue. */
    public void testSecondRegisteredDenialIsRejectedWhenWaitingQueueIsFull() {
        List<ActionListener<Releasable>> ownerReplies = new ArrayList<>();
        doAnswer(inv -> {
            @SuppressWarnings("unchecked")
            ActionListener<Releasable> listener = (ActionListener<Releasable>) inv.getArgument(4);
            ownerReplies.add(listener);
            return null;
        }).when(mockShared).acquireAsync(anyString(), anyInt(), anyLong(), anyBoolean(), any(), any());

        Outcome first = request();
        Outcome second = request();
        assertEquals(0, queueService.waitingDepth(GROUP));
        assertEquals(2, queueService.retainedDepth(GROUP));
        assertEquals("PENDING_ACQUIRE is not counted", 0, state().getTotalQueued());

        ownerReplies.get(0).onFailure(WorkloadGroupSharedThrottleService.deniedMarker());
        ownerReplies.get(1).onFailure(WorkloadGroupSharedThrottleService.deniedMarker());

        assertEquals(0, first.admitted.get() + first.failed.get());
        assertEquals(1, second.failed.get());
        assertTrue(second.failure.get() instanceof OpenSearchRejectedExecutionException);
        assertEquals(QUEUE_FULL_DENIAL, second.failure.get().getMessage());
        assertEquals(1, queueService.waitingDepth(GROUP));
        assertEquals(1, queueService.retainedDepth(GROUP));
        assertEquals(1, state().getTotalQueueRejections());
        assertEquals(1, state().getTotalThrottled());
        assertEquals("only the denial that moved to WAITING is counted; the bucket-full one is not", 1, state().getTotalQueued());
    }

    /**
     * Unregistered denials consume the bucket's waiting budget exactly like registered ones: the first parks, and once
     * the bucket is full each further one gets the queue-full shared-limit 429, counted once as a throttle and a queue
     * rejection, with no double count and no extra parked request.
     */
    public void testUnregisteredDenialsParkUpToTheBucketCapThenGetTheBreach429() {
        stubAcquire((listener, noWaiter) -> noWaiter.run());
        Outcome parked = request(); // queue.size_per_bucket is 1
        assertEquals(0, parked.admitted.get() + parked.failed.get());

        for (int i = 1; i <= 4; i++) {
            Outcome o = request();
            assertEquals("request " + i + " must be failed", 1, o.failed.get());
            assertEquals(QUEUE_FULL_DENIAL, o.failure.get().getMessage());
            assertEquals("only the first request is parked", 1, queueService.waitingDepth(GROUP));
            assertEquals("one throttle per refused request", i, state().getTotalThrottled());
            assertEquals("one queue rejection per refused request", i, state().getTotalQueueRejections());
            assertEquals(
                "only the parked request entered WAITING: a refusal that finds the bucket full is not",
                1,
                state().getTotalQueued()
            );
        }
        assertEquals(0, parked.admitted.get() + parked.failed.get());
    }

    /** A null Runnable never reaches the coordinator on the queueing path: the callback must always be supplied. */
    public void testQueueingPathAlwaysSuppliesTheNoWaiterCallback() {
        AtomicReference<Object> captured = new AtomicReference<>();
        doAnswer(inv -> {
            captured.set(inv.getArgument(5));
            return null;
        }).when(mockShared).acquireAsync(anyString(), anyInt(), anyLong(), anyBoolean(), any(), any());

        request();
        assertNotNull("the enqueue-first path must always pass a no-waiter callback", captured.get());
        assertTrue(captured.get() instanceof Runnable);
    }

    /** A scroll page queues like any other search: a registered shared denial parks it, and owner-push admits it. */
    public void testScrollPageSharedDenialParksUntilOwnerPushAdmitsIt() {
        stubAcquire((listener, noWaiter) -> listener.onFailure(WorkloadGroupSharedThrottleService.deniedMarker()));

        Outcome o = request(true);
        assertEquals("a scroll page at the shared limit must wait, not be rejected", 0, o.admitted.get() + o.failed.get());
        assertEquals("it is parked", 1, queueService.waitingDepth(GROUP));
        assertEquals("and counted as queued", 1, state().getTotalQueued());
        assertEquals(0, state().getTotalThrottled());

        Releasable pushed = () -> {};
        queueService.admitWithPermit(BUCKET, pushed);
        assertEquals("the pushed grant admits the parked page", 1, o.admitted.get());
        assertSame(pushed, o.permit.get());
        assertTrue(o.task.isThrottleCounted());
        assertEquals("the admission does not count it again", 1, state().getTotalQueued());
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

    /**
     * With the ceiling full, a real shared denial rejects with the queue-full 429, counted as both a throttle and a queue
     * rejection.
     */
    public void testFullRetainedCeilingRejectsARealDenial() {
        fillRetainedCeilingWithOtherBuckets();
        stubUnqueuedAcquire(listener -> listener.onFailure(WorkloadGroupSharedThrottleService.deniedMarker()));

        Outcome o = request();
        assertEquals(1, o.failed.get());
        assertEquals(QUEUE_FULL_DENIAL, o.failure.get().getMessage());
        assertEquals(1, state().getTotalQueueRejections());
        assertEquals(1, state().getTotalThrottled());
    }

    /**
     * With the ceiling full, an unavailable shared tier cannot park the request, so it still gets the unavailable 429
     * (which also says the queue is full), counted as a queue rejection like a denial that could not be queued.
     */
    public void testFullRetainedCeilingRejectsAnUnavailableOutcomeWithTheUnavailable429() {
        fillRetainedCeilingWithOtherBuckets();
        stubUnqueuedAcquire(listener -> listener.onFailure(WorkloadGroupSharedThrottleService.unavailableMarker()));

        Outcome o = request();
        assertEquals(0, o.admitted.get());
        assertEquals(1, o.failed.get());
        assertEquals(QUEUE_FULL_UNAVAILABLE, o.failure.get().getMessage());
        assertEquals("it would have parked had it fit", 1, state().getTotalQueueRejections());
        assertEquals(1, state().getTotalThrottled());
    }

    /** A group without a queue has nowhere to park an unavailable outcome: it gets the unavailable 429, as before queueing. */
    public void testGroupWithoutAQueueStillRejectsAnUnavailableOutcome() {
        stubGroup(MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED, Settings.builder().put("shared_limit", 1).build(), 0);
        stubUnqueuedAcquire(listener -> listener.onFailure(WorkloadGroupSharedThrottleService.unavailableMarker()));

        Outcome o = request();
        assertEquals(0, o.admitted.get());
        assertEquals(1, o.failed.get());
        assertEquals(
            "Request throttled: workload group ["
                + GROUP
                + "-name] could not check its shared limit of 1 concurrent requests (cluster-wide throttle unavailable).",
            o.failure.get().getMessage()
        );
        assertEquals(0, queueService.retainedDepth(GROUP));
        assertEquals("queueing is off, so this is no queue rejection", 0, state().getTotalQueueRejections());
        assertEquals(1, state().getTotalThrottled());
        Mockito.verify(mockShared, Mockito.never()).acquireAsync(anyString(), anyInt(), anyLong(), anyBoolean(), any(), any());
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

    /** The periodic queue sweep re-confirms the waiter registration of every bucket holding WAITING shared requests. */
    public void testQueueSweepReconcilesBucketsWithWaitingSharedRequests() {
        stubAcquire((listener, noWaiter) -> listener.onFailure(WorkloadGroupSharedThrottleService.deniedMarker()));
        request();
        assertEquals(1, queueService.waitingDepth(GROUP));

        service.doRun();

        Mockito.verify(mockShared).reconcileRetainedWaiters(BUCKET);
    }

    /**
     * No eligible owner: no node counts shared_limit, so it is treated as unset. A shared-only group then has no tier that
     * applies, and its WAITING requests run untracked, as a new arrival would, instead of being rejected.
     */
    public void testNoEligibleOwnerAdmitsASharedOnlyGroupsWaitingRequestsUntracked() {
        stubDenialRegistered();
        Outcome waiting = request();
        assertEquals(1, queueService.waitingDepth(GROUP));
        assertEquals(1, state().getTotalQueued());

        when(mockShared.hasEligibleOwner()).thenReturn(false);
        noEligibleOwnerHandler().accept(BUCKET);

        assertEquals(1, waiting.admitted.get());
        assertEquals(0, waiting.failed.get());
        assertFalse("an untracked admission carries no throttle charge", waiting.task.isThrottleCounted());
        assertEquals(0, queueService.retainedDepth(GROUP));
        assertEquals(0, state().getTotalThrottled());
        assertEquals("counted once, when it entered WAITING; the untracked release does not count it again", 1, state().getTotalQueued());
    }

    /**
     * No eligible owner with node_limit set: node_limit alone decides. The WAITING requests are not rejected and stay
     * parked while the node permit is held, and each completing request hands it to the oldest.
     */
    public void testNoEligibleOwnerLeavesWaitingRequestsToTheNodeTier() {
        stubNodeAndSharedGroup(1);
        Outcome running = request();
        assertNotNull("the first request takes the only node permit", running.permit.get());
        stubDenialRegistered();
        Outcome first = request();
        Outcome second = request();
        assertEquals(2, queueService.waitingDepth(GROUP));
        assertEquals("both are counted when they enter WAITING", 2, state().getTotalQueued());

        when(mockShared.hasEligibleOwner()).thenReturn(false);
        noEligibleOwnerHandler().accept(BUCKET);
        assertEquals("the node permit is held, so both stay parked", 2, queueService.waitingDepth(GROUP));
        assertEquals(0, first.admitted.get() + first.failed.get() + second.admitted.get() + second.failed.get());

        running.permit.get().close();
        assertEquals("the freed node permit goes to the oldest request", 1, first.admitted.get());
        assertTrue(first.task.isThrottleCounted());
        assertEquals(0, second.admitted.get() + second.failed.get());
        first.permit.get().close();
        assertEquals(1, second.admitted.get());
        assertTrue(second.task.isThrottleCounted());
        second.permit.get().close();
        assertEquals(0, state().getTotalThrottled());
        assertEquals("the node-tier admissions do not count them again", 2, state().getTotalQueued());
    }

    /**
     * No eligible owner when the request arrives: shared_limit counts as unset, so node_limit alone decides. A request the
     * node tier denies parks at the node tier without asking any owner, and the freed node permit admits it.
     */
    public void testNoEligibleOwnerParksANewArrivalAtTheNodeTier() {
        stubNodeAndSharedGroup(1);
        when(mockShared.hasEligibleOwner()).thenReturn(false);
        Outcome running = request();
        assertNotNull("the first request takes the only node permit", running.permit.get());

        Outcome waiting = request();
        assertEquals("parked, not rejected", 0, waiting.admitted.get() + waiting.failed.get());
        assertEquals("parked at the node tier", 1, queueService.waitingDepth(GROUP));
        assertEquals("and counted when it parked", 1, state().getTotalQueued());
        Mockito.verify(mockShared, Mockito.never()).acquireAsync(anyString(), anyInt(), anyLong(), anyBoolean(), any(), any());
        Mockito.verify(mockShared, Mockito.never()).acquireAsync(anyString(), anyInt(), anyLong(), anyBoolean(), any());

        running.permit.get().close();
        assertEquals("the freed node permit admits it", 1, waiting.admitted.get());
        assertTrue(waiting.task.isThrottleCounted());
        assertEquals(0, state().getTotalThrottled());
        assertEquals("the admission does not count it again", 1, state().getTotalQueued());
        waiting.permit.get().close();
    }

    /** No eligible owner: node permits that are already free admit the WAITING requests at once, oldest first. */
    public void testNoEligibleOwnerAdmitsWaitingRequestsOnFreeNodePermitsAtOnce() {
        stubNodeAndSharedGroup(1);
        Outcome running = request();
        stubDenialRegistered();
        Outcome first = request();
        Outcome second = request();
        // A node_limit increase adds node permits that no completing request hands to the queue.
        stubNodeAndSharedGroup(3);

        when(mockShared.hasEligibleOwner()).thenReturn(false);
        noEligibleOwnerHandler().accept(BUCKET);

        assertEquals(1, first.admitted.get());
        assertEquals(1, second.admitted.get());
        assertTrue("each runs on a node permit", first.task.isThrottleCounted() && second.task.isThrottleCounted());
        assertEquals(0, queueService.retainedDepth(GROUP));
        assertEquals(0, state().getTotalThrottled());
        running.permit.get().close();
        first.permit.get().close();
        second.permit.get().close();
    }

    /** The backstop pass treats a shared_limit no node can own as unset too: a shared-only group's backlog runs untracked. */
    public void testBackstopSweepAdmitsASharedOnlyGroupUntrackedWithoutAnEligibleOwner() {
        stubDenialRegistered();
        Outcome waiting = request();
        assertEquals(1, queueService.waitingDepth(GROUP));

        when(mockShared.hasEligibleOwner()).thenReturn(false);
        service.doRun();

        assertEquals(1, waiting.admitted.get());
        assertFalse(waiting.task.isThrottleCounted());
        assertEquals(0, queueService.retainedDepth(GROUP));
        assertEquals(0, state().getTotalThrottled());
    }
}
