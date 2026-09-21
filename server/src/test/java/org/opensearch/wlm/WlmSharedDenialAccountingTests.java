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

import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

/**
 * End-to-end accounting probes for the SHARED-tier denial paths in {@link WorkloadGroupService}, driven by a mocked
 * {@link WorkloadGroupSharedThrottleService} so each owner verdict (granted / fail-open / denied-registered /
 * denied-unregistered) can be forced deterministically without transport.
 */
public class WlmSharedDenialAccountingTests extends OpenSearchTestCase {

    private static final String GROUP = "wg-shared";
    private static final String BUCKET = GROUP + ":group";

    private ClusterService mockClusterService;
    private ThreadPool mockThreadPool;
    private WorkloadManagementSettings mockSettings;
    private WorkloadGroupsStateAccessor stateAccessor;
    private WorkloadGroupService service;
    private WorkloadGroupQueueService queueService;
    private WorkloadGroupSharedThrottleService mockShared;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockClusterService = Mockito.mock(ClusterService.class);
        mockThreadPool = Mockito.mock(ThreadPool.class);
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
        Settings throttling = Settings.builder().put("attribute", "group").put("shared_limit", 1).build();
        Settings queue = Settings.builder().put("size_per_bucket", 5).build();
        WorkloadGroup wg = new WorkloadGroup(
            GROUP + "-name",
            GROUP,
            new MutableWorkloadGroupFragment(
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.EMPTY,
                throttling,
                queue
            ),
            1L
        );
        ClusterState clusterState = Mockito.mock(ClusterState.class);
        Metadata metadata = Mockito.mock(Metadata.class);
        when(mockClusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.workloadGroups()).thenReturn(Map.of(GROUP, wg));

        queueService = new WorkloadGroupQueueService(mockThreadPool, stateAccessor);
        service.setQueueService(queueService);

        mockShared = Mockito.mock(WorkloadGroupSharedThrottleService.class);
        service.setSharedThrottleService(mockShared);
    }

    private WorkloadGroupState state() {
        return stateAccessor.getWorkloadGroupState(GROUP);
    }

    /** Issues one request through the full acquire path, returning what the listener saw. */
    private Outcome request() {
        WorkloadGroupTask task = new SearchTask(randomNonNegativeLong(), "", "", () -> "", null, null);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, GROUP);
        task.setWorkloadGroupId(threadContext);
        Outcome outcome = new Outcome();
        service.acquireThrottlePermit(task, null, ActionListener.wrap(p -> {
            outcome.permit.set(p);
            outcome.admitted.incrementAndGet();
        }, e -> {
            outcome.failure.set(e);
            outcome.failed.incrementAndGet();
        }));
        return outcome;
    }

    private static final class Outcome {
        final AtomicReference<Releasable> permit = new AtomicReference<>();
        final AtomicReference<Exception> failure = new AtomicReference<>();
        final AtomicInteger admitted = new AtomicInteger();
        final AtomicInteger failed = new AtomicInteger();
    }

    /** Stubs the shared service's queue-aware acquire with a caller-supplied verdict. */
    private void stubAcquire(Verdict verdict) {
        doAnswer(inv -> {
            @SuppressWarnings("unchecked")
            ActionListener<Releasable> listener = (ActionListener<Releasable>) inv.getArgument(2);
            Runnable onNoWaiterRegistered = inv.getArgument(3);
            verdict.apply(listener, onNoWaiterRegistered);
            return null;
        }).when(mockShared).acquireAsync(anyString(), anyInt(), any(), any());
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
        stubAcquire((listener, noWaiter) -> listener.onFailure(new OpenSearchRejectedExecutionException()));

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
        assertEquals(0, queueService.currentDepth(GROUP));
        assertEquals("an enqueue-first pass-through is not a wait", 0, state().getTotalQueued());
        assertEquals(0, state().getTotalThrottled());
        assertEquals("no queue wait recorded for a self-supplied admission", 0, state().getTotalQueueWaitMillis());
    }

    /** BASELINE: fail-open admits with a null (untracked) permit and counts nothing. */
    public void testFailOpenAdmitsUntracked() {
        stubAcquire((listener, noWaiter) -> listener.onResponse(null));

        Outcome o = request();
        assertEquals("fail-open must admit", 1, o.admitted.get());
        assertEquals(0, queueService.currentDepth(GROUP));
        assertEquals(0, state().getTotalQueued());
        assertEquals(0, state().getTotalThrottled());
    }

    /**
     * THE RACE: a concurrent drain admits the parked request BEFORE the denied-and-unregistered reply lands, so
     * rejectOldest finds an empty bucket and returns false. The request was served, so no rejection happened — but
     * incrementThrottled is called unconditionally ahead of rejectOldest, so total_throttled is incremented anyway.
     */
    public void testUnregisteredDenialAfterRacingDrainCountsASpuriousThrottle() {
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
        assertEquals("BUG: total_throttled counts a rejection that never happened — the request succeeded", 0, state().getTotalThrottled());
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
            captured.set(inv.getArgument(3));
            return null;
        }).when(mockShared).acquireAsync(anyString(), anyInt(), any(), any());

        request();
        assertNotNull("the enqueue-first path must always pass a no-waiter callback", captured.get());
        assertTrue(captured.get() instanceof Runnable);
    }
}
