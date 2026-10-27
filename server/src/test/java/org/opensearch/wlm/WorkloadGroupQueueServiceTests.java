/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.action.search.SearchTask;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.stats.WorkloadGroupState;

import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.mockito.Mockito;

import static org.mockito.Mockito.when;

public class WorkloadGroupQueueServiceTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private WorkloadGroupsStateAccessor stateAccessor;
    private WorkloadGroupQueueService service;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        stateAccessor = new WorkloadGroupsStateAccessor();
        stateAccessor.addNewWorkloadGroup("g1");
        service = new WorkloadGroupQueueService(threadPool, stateAccessor);
    }

    @Override
    public void tearDown() throws Exception {
        threadPool.shutdown();
        super.tearDown();
    }

    private static SearchTask task() {
        return new SearchTask(randomNonNegativeLong(), "", "", () -> "", null, null);
    }

    public void testTryEnqueueRejectsWhenDisabled() {
        boolean parked = service.tryEnqueue("g1", "g1:group", task(), 0, ActionListener.wrap(r -> {}, e -> {}));
        assertFalse(parked);
        assertEquals(0, service.waitingDepth("g1"));
    }

    public void testTryEnqueueParksAndCountsDepth() {
        boolean parked = service.tryEnqueue("g1", "g1:group", task(), 5, ActionListener.wrap(r -> {}, e -> {}));
        assertTrue(parked);
        assertEquals(1, service.waitingDepth("g1"));
        assertEquals(1, service.retainedDepth("g1"));
    }

    public void testTryEnqueueCapacityIsPerBucket() {
        // The size_per_bucket cap is threaded through tryEnqueue per bucket, not as one budget for the whole group:
        // saturating alice's bucket must not deny bob.
        assertTrue(service.tryEnqueue("g1", "g1:username:alice", task(), 1, ActionListener.wrap(r -> {}, e -> {})));
        assertFalse(service.tryEnqueue("g1", "g1:username:alice", task(), 1, ActionListener.wrap(r -> {}, e -> {})));
        assertTrue(service.tryEnqueue("g1", "g1:username:bob", task(), 1, ActionListener.wrap(r -> {}, e -> {})));
        assertEquals(2, service.waitingDepth("g1"));
    }

    public void testPendingAcquiresDoNotConsumeBucketWaitingCapacity() throws Exception {
        AtomicInteger admitted = new AtomicInteger();
        WorkloadGroupQueue.QueuedRequest first = service.tryRegisterPendingAcquire(
            "g1",
            "g1:group",
            task(),
            ActionListener.wrap(p -> admitted.incrementAndGet(), e -> fail(e.getMessage()))
        );
        WorkloadGroupQueue.QueuedRequest second = service.tryRegisterPendingAcquire(
            "g1",
            "g1:group",
            task(),
            ActionListener.wrap(p -> admitted.incrementAndGet(), e -> fail(e.getMessage()))
        );

        assertNotNull(first);
        assertNotNull(second);
        assertEquals("pending owner RPCs are not WAITING backlog", 0, service.waitingDepth("g1"));
        assertEquals(2, service.retainedDepth("g1"));
        assertEquals(0, stateAccessor.getWorkloadGroupState("g1").getTotalQueueRejections());

        // Both owner acquires can be granted even if queue.size_per_bucket would be 1: that limit is consulted only on
        // an actual denial transition.
        assertTrue(service.admitPendingAcquire(first, () -> {}));
        assertTrue(service.admitPendingAcquire(second, () -> {}));
        assertBusy(() -> assertEquals(2, admitted.get()));
        assertEquals(0, service.retainedDepth("g1"));
    }

    public void testRetainedBucketKeysIncludesPendingAndWaitingAndPrunesEmptyBuckets() throws Exception {
        final String waitingBucket = "g1:username:alice";
        final String pendingBucket = "g1:username:bob";
        AtomicInteger admitted = new AtomicInteger();

        assertTrue(
            service.tryEnqueue(
                "g1",
                waitingBucket,
                task(),
                1,
                ActionListener.wrap(p -> admitted.incrementAndGet(), e -> fail(e.getMessage()))
            )
        );
        WorkloadGroupQueue.QueuedRequest pending = service.tryRegisterPendingAcquire(
            "g1",
            pendingBucket,
            task(),
            ActionListener.wrap(p -> admitted.incrementAndGet(), e -> fail(e.getMessage()))
        );
        assertNotNull(pending);
        assertEquals(Set.of(waitingBucket, pendingBucket), service.retainedBucketKeys());

        assertTrue(service.admitPendingAcquire(pending, () -> {}));
        assertBusy(() -> assertEquals(1, admitted.get()));
        assertEquals(Set.of(waitingBucket), service.retainedBucketKeys());

        assertTrue(service.admitWithPermit(waitingBucket, () -> {}).isPresent());
        assertBusy(() -> assertEquals(2, admitted.get()));
        assertTrue(service.retainedBucketKeys().isEmpty());
    }

    public void testAdmitWithPermitReportsTheAdmittedTaskId() throws Exception {
        // A pushed grant's reply names the task it admitted, so the owner can keep that search's permit counted while it
        // runs; with nothing to admit there is no task to name.
        assertEquals("no queue for the group yet", OptionalLong.empty(), service.admitWithPermit("g1:group", () -> {}));
        SearchTask older = task();
        SearchTask newer = task();
        AtomicInteger admitted = new AtomicInteger();
        assertTrue(
            service.tryEnqueue("g1", "g1:group", older, 5, ActionListener.wrap(p -> admitted.incrementAndGet(), e -> fail(e.getMessage())))
        );
        assertNotNull(
            service.tryRegisterPendingAcquire(
                "g1",
                "g1:group",
                newer,
                ActionListener.wrap(p -> admitted.incrementAndGet(), e -> fail(e.getMessage()))
            )
        );

        assertEquals("the oldest request is admitted", OptionalLong.of(older.getId()), service.admitWithPermit("g1:group", () -> {}));
        assertEquals("a PENDING_ACQUIRE request too", OptionalLong.of(newer.getId()), service.admitWithPermit("g1:group", () -> {}));
        assertEquals("a drained bucket admits nothing", OptionalLong.empty(), service.admitWithPermit("g1:group", () -> {}));
        assertBusy(() -> assertEquals(2, admitted.get()));
    }

    public void testDenialTransitionEnforcesBucketCapacityAndRejectsExactRequest() throws Exception {
        AtomicInteger firstAdmitted = new AtomicInteger();
        AtomicInteger firstFailed = new AtomicInteger();
        AtomicInteger secondAdmitted = new AtomicInteger();
        AtomicInteger secondFailed = new AtomicInteger();
        WorkloadGroupQueue.QueuedRequest first = service.tryRegisterPendingAcquire(
            "g1",
            "g1:group",
            task(),
            ActionListener.wrap(p -> firstAdmitted.incrementAndGet(), e -> firstFailed.incrementAndGet())
        );
        WorkloadGroupQueue.QueuedRequest second = service.tryRegisterPendingAcquire(
            "g1",
            "g1:group",
            task(),
            ActionListener.wrap(p -> secondAdmitted.incrementAndGet(), e -> secondFailed.incrementAndGet())
        );
        assertNotNull(first);
        assertNotNull(second);

        assertEquals(WorkloadGroupQueue.WaitingTransition.WAITING, service.transitionPendingAcquireToWaiting(first, 1));
        assertEquals(WorkloadGroupQueue.WaitingTransition.BUCKET_FULL, service.transitionPendingAcquireToWaiting(second, 1));
        // QUEUE_FULL hands the exact request back to the caller, which composes and delivers the 429; the queue never
        // completes it itself.
        assertEquals(0, secondFailed.get());
        assertEquals(0, secondAdmitted.get());
        assertEquals(0, firstFailed.get());
        assertEquals("the existing waiter remains", 1, service.waitingDepth("g1"));
        assertEquals(1, service.retainedDepth("g1"));
        assertEquals(1, stateAccessor.getWorkloadGroupState("g1").getTotalQueueRejections());

        assertTrue(service.admitWithPermit("g1:group", () -> {}).isPresent());
        assertBusy(() -> assertEquals(1, firstAdmitted.get()));
        assertEquals(0, service.retainedDepth("g1"));
    }

    public void testEarlyPushedGrantClaimsPendingBeforeDenialResponse() throws Exception {
        AtomicInteger admitted = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();
        WorkloadGroupQueue.QueuedRequest pending = service.tryRegisterPendingAcquire(
            "g1",
            "g1:group",
            task(),
            ActionListener.wrap(p -> admitted.incrementAndGet(), e -> failed.incrementAndGet())
        );
        assertNotNull(pending);

        assertTrue(service.admitWithPermit("g1:group", () -> {}).isPresent());
        assertEquals(
            "the delayed denial must be an exact-entry no-op",
            WorkloadGroupQueue.WaitingTransition.REQUEST_GONE,
            service.transitionPendingAcquireToWaiting(pending, 1)
        );
        assertBusy(() -> assertEquals(1, admitted.get()));
        assertEquals(0, failed.get());
        assertEquals(0, service.retainedDepth("g1"));
    }

    public void testStaleAcquireResultCannotRejectReplacement() throws Exception {
        AtomicInteger firstFailed = new AtomicInteger();
        AtomicInteger secondAdmitted = new AtomicInteger();
        AtomicInteger secondFailed = new AtomicInteger();
        WorkloadGroupQueue.QueuedRequest first = service.tryRegisterPendingAcquire(
            "g1",
            "g1:group",
            task(),
            ActionListener.wrap(p -> {}, e -> firstFailed.incrementAndGet())
        );
        assertNotNull(first);
        assertTrue("the first terminal outcome claims its exact entry", service.claimPendingAcquire(first));
        assertEquals("a claim hands the listener back without completing it", 0, firstFailed.get());
        assertEquals(0, service.retainedDepth("g1"));

        WorkloadGroupQueue.QueuedRequest replacement = service.tryRegisterPendingAcquire(
            "g1",
            "g1:group",
            task(),
            ActionListener.wrap(p -> secondAdmitted.incrementAndGet(), e -> secondFailed.incrementAndGet())
        );
        assertNotNull(replacement);
        assertFalse("a stale result for the old token must not touch the replacement", service.claimPendingAcquire(first));
        assertEquals(1, service.retainedDepth("g1"));
        assertTrue(service.admitPendingAcquire(replacement, () -> {}));
        assertBusy(() -> assertEquals(1, secondAdmitted.get()));
        assertEquals(0, secondFailed.get());
    }

    public void testDirectGrantAdmitsItsExactPendingRequest() throws Exception {
        AtomicInteger olderAdmitted = new AtomicInteger();
        AtomicInteger pendingAdmitted = new AtomicInteger();
        assertTrue(
            service.tryEnqueue(
                "g1",
                "g1:group",
                task(),
                1,
                ActionListener.wrap(p -> olderAdmitted.incrementAndGet(), e -> fail(e.getMessage()))
            )
        );
        WorkloadGroupQueue.QueuedRequest pending = service.tryRegisterPendingAcquire(
            "g1",
            "g1:group",
            task(),
            ActionListener.wrap(p -> pendingAdmitted.incrementAndGet(), e -> fail(e.getMessage()))
        );
        assertNotNull(pending);

        assertTrue(service.admitPendingAcquire(pending, () -> {}));
        assertBusy(() -> assertEquals(1, pendingAdmitted.get()));
        assertEquals("the older denied waiter remains queued", 1, service.waitingDepth("g1"));
        assertEquals(0, olderAdmitted.get());

        assertTrue(service.admitWithPermit("g1:group", () -> {}).isPresent());
        assertBusy(() -> assertEquals(1, olderAdmitted.get()));
    }

    public void testDrainNodeAdmitsOneWaiterWithPermit() throws Exception {
        AtomicReference<Releasable> admittedPermit = new AtomicReference<>();
        AtomicInteger admitted = new AtomicInteger();
        assertTrue(service.tryEnqueue("g1", "g1:group", task(), 5, ActionListener.wrap(p -> {
            admittedPermit.set(p);
            admitted.incrementAndGet();
        }, e -> {})));

        Releasable permit = () -> {};
        service.drainNode("g1", "g1:group", key -> permit);

        assertBusy(() -> assertEquals(1, admitted.get()));
        assertSame(permit, admittedPermit.get());
        assertEquals(0, service.waitingDepth("g1")); // drained
    }

    public void testHandoffNodePermitAdmitsOldestRetainedRequest() throws Exception {
        AtomicReference<Releasable> admittedPermit = new AtomicReference<>();
        assertTrue(
            service.tryEnqueue(
                "g1",
                "g1:group",
                task(),
                5,
                ActionListener.wrap(admittedPermit::set, e -> fail("handoff must admit, not fail: " + e))
            )
        );

        Releasable stillHeldPermit = () -> {};
        assertTrue(service.handoffNodePermit("g1", "g1:group", stillHeldPermit));

        assertBusy(() -> assertSame(stillHeldPermit, admittedPermit.get()));
        assertEquals(0, service.retainedDepth("g1"));
    }

    public void testHandoffNodePermitReturnsFalseWithoutDemand() {
        assertFalse(service.handoffNodePermit("g1", "g1:group", () -> {}));
    }

    public void testDrainNodeAdmitsAtMostOnePerFreedPermit() throws Exception {
        AtomicInteger admitted = new AtomicInteger();
        for (int i = 0; i < 3; i++) {
            assertTrue(service.tryEnqueue("g1", "g1:group", task(), 5, ActionListener.wrap(p -> admitted.incrementAndGet(), e -> {})));
        }
        assertEquals(3, service.waitingDepth("g1"));

        // A single freed node permit admits exactly one waiter.
        service.drainNode("g1", "g1:group", key -> (Releasable) () -> {});
        assertBusy(() -> assertEquals(1, admitted.get()));
        assertEquals(2, service.waitingDepth("g1")); // two still parked
    }

    public void testHasRetainedIsPerBucket() {
        assertFalse(service.hasRetained("g1", "g1:username:alice"));
        assertTrue(service.tryEnqueue("g1", "g1:username:alice", task(), 1, ActionListener.wrap(p -> {}, e -> {})));
        assertNotNull(service.tryRegisterPendingAcquire("g1", "g1:username:carol", task(), ActionListener.wrap(p -> {}, e -> {})));

        assertTrue(service.hasRetained("g1", "g1:username:alice"));
        assertTrue("a PENDING_ACQUIRE counts: a node-permit handoff may admit it", service.hasRetained("g1", "g1:username:carol"));
        assertFalse("another bucket of the same group", service.hasRetained("g1", "g1:username:bob"));
        assertFalse("another group", service.hasRetained("g2", "g2:group:group"));

        assertTrue(service.drainNode("g1", "g1:username:alice", key -> () -> {}));
        assertFalse("an emptied bucket is pruned at once", service.hasRetained("g1", "g1:username:alice"));
    }

    // A queue service whose relative clocks are stubbed: the cached clock is frozen, as it is between two ticks.
    private WorkloadGroupQueueService serviceWithStubbedClocks(ThreadPool pool) {
        when(pool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(pool.executor(ThreadPool.Names.GENERIC)).thenReturn(OpenSearchExecutors.newDirectExecutorService());
        when(pool.relativeTimeInNanos()).thenReturn(0L);
        return new WorkloadGroupQueueService(pool, stateAccessor);
    }

    public void testNodeTierQueueWaitUsesThePreciseClock() {
        ThreadPool pool = Mockito.mock(ThreadPool.class);
        WorkloadGroupQueueService svc = serviceWithStubbedClocks(pool);
        when(pool.preciseRelativeTimeInNanos()).thenReturn(0L, TimeUnit.MILLISECONDS.toNanos(7));

        assertTrue(svc.tryEnqueue("g1", "g1:group", task(), 1, ActionListener.wrap(p -> {}, e -> fail(e.toString()))));
        assertTrue(svc.admitWithPermit("g1:group", () -> {}).isPresent());

        WorkloadGroupState state = stateAccessor.getWorkloadGroupState("g1");
        assertEquals("a wait shorter than the cached clock's tick must still be measured", 7, state.getTotalQueueWaitMillis());
    }

    public void testSharedTierQueueWaitUsesThePreciseClockFromTheWaitingTransition() {
        ThreadPool pool = Mockito.mock(ThreadPool.class);
        WorkloadGroupQueueService svc = serviceWithStubbedClocks(pool);
        when(pool.preciseRelativeTimeInNanos()).thenReturn(TimeUnit.MILLISECONDS.toNanos(2), TimeUnit.MILLISECONDS.toNanos(9));

        WorkloadGroupQueue.QueuedRequest pending = svc.tryRegisterPendingAcquire(
            "g1",
            "g1:group",
            task(),
            ActionListener.wrap(p -> {}, e -> fail(e.toString()))
        );
        assertNotNull(pending);
        assertEquals(WorkloadGroupQueue.WaitingTransition.WAITING, svc.transitionPendingAcquireToWaiting(pending, 1));
        assertTrue(svc.admitWithPermit("g1:group", () -> {}).isPresent());

        assertEquals("the wait starts at the WAITING transition", 7, stateAccessor.getWorkloadGroupState("g1").getTotalQueueWaitMillis());
    }

    // As serviceWithStubbedClocks, with the precise clock reading nowNanos, so a test advances time explicitly. Listener
    // completion runs inline on the direct GENERIC executor.
    private WorkloadGroupQueueService serviceWithControlledClock(AtomicLong nowNanos) {
        ThreadPool pool = Mockito.mock(ThreadPool.class);
        WorkloadGroupQueueService svc = serviceWithStubbedClocks(pool);
        when(pool.preciseRelativeTimeInNanos()).thenAnswer(invocation -> nowNanos.get());
        return svc;
    }

    private static long millisToNanos(long millis) {
        return TimeUnit.MILLISECONDS.toNanos(millis);
    }

    public void testNodeTierParkCountsTotalQueuedAtEntryAndAdmissionDoesNotCountItAgain() {
        AtomicLong nowNanos = new AtomicLong();
        WorkloadGroupQueueService svc = serviceWithControlledClock(nowNanos);
        WorkloadGroupState state = stateAccessor.getWorkloadGroupState("g1");

        assertTrue(svc.tryEnqueue("g1", "g1:group", task(), 1, ActionListener.wrap(p -> {}, e -> fail(e.toString()))));
        assertEquals("a node-tier park is counted when it enters WAITING", 1, state.getTotalQueued());
        assertEquals("its wait has not ended yet", 0, state.getTotalQueueWaitMillis());

        nowNanos.set(millisToNanos(7));
        assertTrue(svc.admitWithPermit("g1:group", () -> {}).isPresent());
        assertEquals("admission does not count it again", 1, state.getTotalQueued());
        assertEquals("admission records the wait", 7, state.getTotalQueueWaitMillis());
    }

    public void testEveryAdmissionPathRecordsTheWaitOnceWithoutCountingTheRequestAgain() {
        AtomicLong nowNanos = new AtomicLong();
        WorkloadGroupQueueService svc = serviceWithControlledClock(nowNanos);
        WorkloadGroupState state = stateAccessor.getWorkloadGroupState("g1");
        AtomicInteger admitted = new AtomicInteger();
        // One WAITING request per bucket, all parked at t=0, each admitted by a different path.
        for (String bucketKey : new String[] { "g1:pushed", "g1:handoff", "g1:drain", "g1:policy" }) {
            assertTrue(
                svc.tryEnqueue("g1", bucketKey, task(), 1, ActionListener.wrap(p -> admitted.incrementAndGet(), e -> fail(e.toString())))
            );
        }
        assertEquals("each park is counted at entry", 4, state.getTotalQueued());

        // Waits of 1, 2, 4 and 8 ms: the sum shows which waits were recorded, and that none was recorded twice.
        nowNanos.set(millisToNanos(1));
        assertTrue(svc.admitWithPermit("g1:pushed", () -> {}).isPresent());
        nowNanos.set(millisToNanos(2));
        assertTrue(svc.handoffNodePermit("g1", "g1:handoff", () -> {}));
        nowNanos.set(millisToNanos(4));
        assertTrue(svc.drainNode("g1", "g1:drain", key -> () -> {}));
        nowNanos.set(millisToNanos(8));
        assertEquals("the policy-change drain admits the last waiter", 1, svc.admitAllUntracked("g1"));

        assertEquals(4, admitted.get());
        assertEquals(0, svc.retainedDepth("g1"));
        assertEquals("no admission counts a request again", 4, state.getTotalQueued());
        assertEquals("every admission records its wait exactly once", 1 + 2 + 4 + 8, state.getTotalQueueWaitMillis());
    }

    public void testSharedTierCountsTheMoveToWaitingButNotADirectGrantOrABucketFullRefusal() {
        WorkloadGroupQueueService svc = serviceWithControlledClock(new AtomicLong());
        WorkloadGroupState state = stateAccessor.getWorkloadGroupState("g1");
        WorkloadGroupQueue.QueuedRequest granted = svc.tryRegisterPendingAcquire(
            "g1",
            "g1:group",
            task(),
            ActionListener.wrap(p -> {}, e -> {})
        );
        WorkloadGroupQueue.QueuedRequest denied = svc.tryRegisterPendingAcquire(
            "g1",
            "g1:group",
            task(),
            ActionListener.wrap(p -> {}, e -> {})
        );
        WorkloadGroupQueue.QueuedRequest refused = svc.tryRegisterPendingAcquire(
            "g1",
            "g1:group",
            task(),
            ActionListener.wrap(p -> {}, e -> {})
        );
        assertNotNull(granted);
        assertNotNull(denied);
        assertNotNull(refused);
        assertEquals("PENDING_ACQUIRE is not counted", 0, state.getTotalQueued());

        assertTrue(svc.admitPendingAcquire(granted, () -> {}));
        assertEquals("a direct grant never waited", 0, state.getTotalQueued());

        assertEquals(WorkloadGroupQueue.WaitingTransition.WAITING, svc.transitionPendingAcquireToWaiting(denied, 1));
        assertEquals("the move to WAITING is counted at once", 1, state.getTotalQueued());
        assertEquals(WorkloadGroupQueue.WaitingTransition.WAITING, svc.transitionPendingAcquireToWaiting(denied, 1));
        assertEquals("a request already WAITING is not counted again", 1, state.getTotalQueued());

        assertEquals(WorkloadGroupQueue.WaitingTransition.BUCKET_FULL, svc.transitionPendingAcquireToWaiting(refused, 1));
        assertEquals("a refusal that finds the bucket full is not counted", 1, state.getTotalQueued());
        assertEquals("it is a queue rejection instead", 1, state.getTotalQueueRejections());

        assertTrue(svc.admitWithPermit("g1:group", () -> {}).isPresent());
        assertEquals("admission does not count it again", 1, state.getTotalQueued());
    }

    public void testCancellingANodeTierWaiterRecordsItsWaitWithoutCountingItAgain() {
        AtomicLong nowNanos = new AtomicLong();
        WorkloadGroupQueueService svc = serviceWithControlledClock(nowNanos);
        WorkloadGroupState state = stateAccessor.getWorkloadGroupState("g1");
        SearchTask t = task();
        AtomicInteger admitted = new AtomicInteger();
        AtomicReference<Exception> failure = new AtomicReference<>();
        assertTrue(svc.tryEnqueue("g1", "g1:group", t, 1, ActionListener.wrap(p -> admitted.incrementAndGet(), failure::set)));
        assertEquals(1, state.getTotalQueued());

        nowNanos.set(millisToNanos(7));
        t.cancel("client disconnected");

        assertEquals(0, admitted.get());
        assertTrue(String.valueOf(failure.get()), failure.get() instanceof TaskCancelledException);
        assertEquals(0, svc.waitingDepth("g1"));
        assertEquals("the cancellation does not count the request again", 1, state.getTotalQueued());
        assertEquals("the evicted wait is added to the sum", 7, state.getTotalQueueWaitMillis());
    }

    public void testCancellationRecordsAWaitForAWaitingRequestButNothingForAPendingAcquire() {
        AtomicLong nowNanos = new AtomicLong();
        WorkloadGroupQueueService svc = serviceWithControlledClock(nowNanos);
        WorkloadGroupState state = stateAccessor.getWorkloadGroupState("g1");
        SearchTask pendingTask = task();
        SearchTask waitingTask = task();
        AtomicInteger pendingFailures = new AtomicInteger();
        AtomicInteger waitingFailures = new AtomicInteger();
        WorkloadGroupQueue.QueuedRequest pending = svc.tryRegisterPendingAcquire(
            "g1",
            "g1:group",
            pendingTask,
            ActionListener.wrap(p -> fail("a cancelled request must not be admitted"), e -> pendingFailures.incrementAndGet())
        );
        WorkloadGroupQueue.QueuedRequest waiting = svc.tryRegisterPendingAcquire(
            "g1",
            "g1:group",
            waitingTask,
            ActionListener.wrap(p -> fail("a cancelled request must not be admitted"), e -> waitingFailures.incrementAndGet())
        );
        assertNotNull(pending);
        assertNotNull(waiting);
        nowNanos.set(millisToNanos(2));
        assertEquals(WorkloadGroupQueue.WaitingTransition.WAITING, svc.transitionPendingAcquireToWaiting(waiting, 1));
        assertEquals("only the request that entered WAITING is counted", 1, state.getTotalQueued());

        // Both were registered at t=0; the owner never answered the first, so it is still PENDING_ACQUIRE.
        nowNanos.set(millisToNanos(9));
        pendingTask.cancel("client disconnected");
        assertEquals(1, pendingFailures.get());
        assertEquals("a cancelled PENDING_ACQUIRE never waited: it records no wait", 0, state.getTotalQueueWaitMillis());
        assertEquals("nor is it counted", 1, state.getTotalQueued());

        waitingTask.cancel("client disconnected");
        assertEquals(1, waitingFailures.get());
        assertEquals(0, svc.retainedDepth("g1"));
        assertEquals("the cancellation does not count the WAITING request again", 1, state.getTotalQueued());
        assertEquals("its wait is recorded, measured from the WAITING transition", 7, state.getTotalQueueWaitMillis());
    }

    public void testCancellationThatWinsThePostDequeueCheckRecordsItsWaitOnce() {
        AtomicLong nowNanos = new AtomicLong();
        WorkloadGroupQueueService svc = serviceWithControlledClock(nowNanos);
        WorkloadGroupState state = stateAccessor.getWorkloadGroupState("g1");
        // Typed as WorkloadGroupTask: addOnCancelledCallback is package-private there, so SearchTask does not inherit it.
        WorkloadGroupTask t = task();
        AtomicInteger permitCloses = new AtomicInteger();
        // Registered before the queue's own cancellation callback, so it runs first: the cancelled request is dequeued for
        // admission before the queue's eviction callback runs (which then finds nothing to remove), and the admission's
        // final cancellation check fails it.
        t.addOnCancelledCallback(() -> assertTrue(svc.admitWithPermit("g1:group", permitCloses::incrementAndGet).isPresent()));
        AtomicInteger admitted = new AtomicInteger();
        AtomicReference<Exception> failure = new AtomicReference<>();
        assertTrue(svc.tryEnqueue("g1", "g1:group", t, 1, ActionListener.wrap(p -> admitted.incrementAndGet(), failure::set)));
        assertEquals("counted when it parked", 1, state.getTotalQueued());

        nowNanos.set(millisToNanos(7));
        t.cancel("client disconnected");

        assertEquals(0, admitted.get());
        assertTrue(String.valueOf(failure.get()), failure.get() instanceof TaskCancelledException);
        assertEquals("the unused permit is released", 1, permitCloses.get());
        assertEquals(0, svc.retainedDepth("g1"));
        assertEquals(1, state.getTotalQueued());
        assertEquals("the wait is recorded once, at dequeue, and not again on eviction", 7, state.getTotalQueueWaitMillis());
    }

    // An already-cancelled task is evicted inline while it is parked, under the enqueue's own (reentrant) bucket lock, so
    // holding the lock does not order its exit after its entry: the count must come first. The listener's failure runs
    // inline on the direct GENERIC executor right after the eviction recorded the wait, so it sees the stats at the exit.
    public void testAlreadyCancelledParkIsCountedBeforeItsInlineEvictionRecordsItsWait() {
        ThreadPool pool = Mockito.mock(ThreadPool.class);
        WorkloadGroupQueueService svc = serviceWithStubbedClocks(pool);
        // Two precise-clock reads: the park's wait start, then the inline eviction's wait end.
        when(pool.preciseRelativeTimeInNanos()).thenReturn(0L, millisToNanos(7));
        WorkloadGroupState state = stateAccessor.getWorkloadGroupState("g1");
        SearchTask t = task();
        t.cancel("already gone");
        AtomicReference<Exception> failure = new AtomicReference<>();
        AtomicLong queuedAtExit = new AtomicLong(-1);
        AtomicLong waitSumAtExit = new AtomicLong(-1);
        svc.tryEnqueue("g1", "g1:group", t, 1, ActionListener.wrap(p -> fail("a cancelled request must not be admitted"), e -> {
            failure.set(e);
            queuedAtExit.set(state.getTotalQueued());
            waitSumAtExit.set(state.getTotalQueueWaitMillis());
        }));

        assertTrue(String.valueOf(failure.get()), failure.get() instanceof TaskCancelledException);
        assertEquals("the eviction recorded the wait before failing the request", 7, waitSumAtExit.get());
        assertEquals("and the entry was already counted: an exit is never recorded before its entry", 1, queuedAtExit.get());
        assertEquals("counted once", 1, state.getTotalQueued());
        assertEquals(7, state.getTotalQueueWaitMillis());
        assertEquals(0, svc.retainedDepth("g1"));
    }

    public void testQueuedAdmissionCompletesOnGenericPool() throws Exception {
        AtomicReference<String> thread = new AtomicReference<>();
        assertTrue(
            service.tryEnqueue(
                "g1",
                "g1:group",
                task(),
                1,
                ActionListener.wrap(p -> thread.set(Thread.currentThread().getName()), e -> fail(e.getMessage()))
            )
        );
        assertTrue(service.handoffNodePermit("g1", "g1:group", () -> {}));
        assertBusy(() -> {
            assertNotNull(thread.get());
            assertTrue(thread.get(), thread.get().contains("[" + ThreadPool.Names.GENERIC + "]"));
        });
    }

    public void testDrainNodeLeavesWaiterQueuedWhenNoPermit() {
        AtomicInteger admitted = new AtomicInteger();
        assertTrue(service.tryEnqueue("g1", "g1:group", task(), 5, ActionListener.wrap(p -> admitted.incrementAndGet(), e -> {})));
        // nodeAcquire returns null (limit reached) -> nothing admitted, request stays queued.
        service.drainNode("g1", "g1:group", key -> null);
        assertEquals(0, admitted.get());
        assertEquals(1, service.waitingDepth("g1"));
    }

    // admit() must complete the parked listener on the executor, never inline on the caller's (drain/completion) thread.
    // That breaks the node-tier close()->drainNode->admit->close() chain into separate executor tasks; an inline
    // completion would recurse and could overflow the stack under a cancel storm. We assert the common admit path hands
    // off to a different thread than the caller — the same dispatch that also governs the cancelled branch's
    // permit.close().
    public void testAdmitCompletesOffCallerThread() throws Exception {
        Thread callerThread = Thread.currentThread();
        AtomicReference<Thread> respondedOn = new AtomicReference<>();
        assertTrue(
            service.tryEnqueue("g1", "g1:group", task(), 5, ActionListener.wrap(p -> respondedOn.set(Thread.currentThread()), e -> {}))
        );
        service.drainNode("g1", "g1:group", key -> (Releasable) () -> {});
        assertBusy(() -> assertNotNull(respondedOn.get()));
        assertNotSame("admit must complete the listener off the caller thread (recursion-safety)", callerThread, respondedOn.get());
    }

    public void testPendingDirectGrantCompletesInline() {
        Thread callerThread = Thread.currentThread();
        AtomicReference<Thread> respondedOn = new AtomicReference<>();
        WorkloadGroupQueue.QueuedRequest pending = service.tryRegisterPendingAcquire(
            "g1",
            "g1:group",
            task(),
            ActionListener.wrap(p -> respondedOn.set(Thread.currentThread()), e -> fail(e.getMessage()))
        );
        assertNotNull(pending);
        assertTrue(service.admitPendingAcquire(pending, () -> {}));
        assertSame(callerThread, respondedOn.get());
        assertEquals(0, service.retainedDepth("g1"));
    }

    public void testPendingAcquireThatReachedWaitingCompletesOffCallerThread() throws Exception {
        Thread callerThread = Thread.currentThread();
        AtomicReference<Thread> respondedOn = new AtomicReference<>();
        WorkloadGroupQueue.QueuedRequest pending = service.tryRegisterPendingAcquire(
            "g1",
            "g1:group",
            task(),
            ActionListener.wrap(p -> respondedOn.set(Thread.currentThread()), e -> fail(e.getMessage()))
        );
        assertNotNull(pending);
        assertEquals(WorkloadGroupQueue.WaitingTransition.WAITING, service.transitionPendingAcquireToWaiting(pending, 5));
        assertTrue(service.admitPendingAcquire(pending, () -> {}));
        assertBusy(() -> assertNotNull(respondedOn.get()));
        assertNotSame(callerThread, respondedOn.get());
        assertEquals(0, service.retainedDepth("g1"));
    }

    // Regression for the exactly-once contract on cancellation-during-enqueue: an already-cancelled task must not be
    // left parked, and its listener is failed exactly once.
    public void testEnqueueOfAlreadyCancelledTaskFailsExactlyOnce() throws Exception {
        SearchTask t = task();
        t.cancel("already gone");
        AtomicInteger failures = new AtomicInteger();
        AtomicBoolean admittedFlag = new AtomicBoolean(false);
        // tryEnqueue may report parked=true, but the immediate cancellation callback evicts + fails it exactly once.
        service.tryEnqueue("g1", "g1:group", t, 5, ActionListener.wrap(p -> admittedFlag.set(true), e -> failures.incrementAndGet()));
        assertBusy(() -> assertEquals(1, failures.get()));
        assertFalse(admittedFlag.get());
        assertEquals(0, service.waitingDepth("g1"));
    }

    public void testTrackedAdmissionMarksTaskCountedButUntrackedDrainDoesNot() throws Exception {
        SearchTask trackedTask = task();
        SearchTask pendingTask = task();
        SearchTask untrackedTask = task();
        AtomicInteger admitted = new AtomicInteger();
        assertTrue(service.tryEnqueue("g1", "g1:a", trackedTask, 5, ActionListener.wrap(p -> admitted.incrementAndGet(), e -> {})));
        WorkloadGroupQueue.QueuedRequest pending = service.tryRegisterPendingAcquire(
            "g1",
            "g1:b",
            pendingTask,
            ActionListener.wrap(p -> admitted.incrementAndGet(), e -> {})
        );
        assertNotNull(pending);
        assertTrue(service.tryEnqueue("g1", "g1:c", untrackedTask, 5, ActionListener.wrap(p -> admitted.incrementAndGet(), e -> {})));

        assertTrue(service.admitWithPermit("g1:a", () -> {}).isPresent());
        assertTrue(service.admitPendingAcquire(pending, () -> {}));
        assertEquals(1, service.admitAllUntracked("g1"));
        assertBusy(() -> assertEquals(3, admitted.get()));

        assertTrue("an admission holding a permit inherits the throttle charge", trackedTask.isThrottleCounted());
        assertTrue("a direct grant holds a shared permit", pendingTask.isThrottleCounted());
        assertFalse("a policy-change drain holds no permit, so it is not charged", untrackedTask.isThrottleCounted());
    }

    public void testCancellationHookRemovesQueuedRequestWithoutSweep() throws Exception {
        SearchTask t = task();
        AtomicInteger failures = new AtomicInteger();
        assertTrue(
            service.tryEnqueue(
                "g1",
                "g1:group",
                t,
                5,
                ActionListener.wrap(p -> fail("cancelled request must not be admitted"), e -> failures.incrementAndGet())
            )
        );
        assertEquals(1, service.retainedDepth("g1"));

        t.cancel("client disconnected");

        assertBusy(() -> assertEquals(1, failures.get()));
        assertEquals("the per-request cancellation callback must remove it immediately", 0, service.retainedDepth("g1"));
        AtomicInteger sweepDrains = new AtomicInteger();
        service.sweep((groupId, bucketKey) -> sweepDrains.incrementAndGet());
        assertEquals("the sweep is not needed for cancellation cleanup", 0, sweepDrains.get());
    }

    // A SearchTask subclass can override onCancelled() without calling super, as the asynchronous search plugin's task
    // does. Cancelling it must still evict its parked request rather than leave it holding a queue slot.
    public void testCancellationEvictsAQueuedRequestWhoseTaskOverridesOnCancelledWithoutSuper() throws Exception {
        AtomicInteger subclassHookRuns = new AtomicInteger();
        SearchTask t = new SearchTask(randomNonNegativeLong(), "", "", () -> "", null, null) {
            @Override
            protected void onCancelled() {
                subclassHookRuns.incrementAndGet();
            }
        };
        AtomicReference<Exception> failure = new AtomicReference<>();
        assertTrue(
            service.tryEnqueue(
                "g1",
                "g1:group",
                t,
                5,
                ActionListener.wrap(p -> fail("a cancelled request must not be admitted"), failure::set)
            )
        );

        t.cancel("asynchronous search deleted");

        assertBusy(() -> assertTrue(String.valueOf(failure.get()), failure.get() instanceof TaskCancelledException));
        assertEquals("the cancellation callback must remove it", 0, service.retainedDepth("g1"));
        assertEquals("the subclass's own hook still runs", 1, subclassHookRuns.get());
    }

    // A retained request is admitted on whatever thread frees its slot. It must resume in its OWN thread context (security
    // transients, headers), never in the context of the request or transport message that happened to admit it.
    private ActionListener<Releasable> headerCapturingListener(AtomicReference<String> seen, CountDownLatch done) {
        return ActionListener.wrap(permit -> {
            seen.set(threadPool.getThreadContext().getHeader("X"));
            done.countDown();
        }, e -> {
            seen.set("failed: " + e);
            done.countDown();
        });
    }

    public void testNodeHandoffAdmitsInTheParkedRequestsOwnContext() throws Exception {
        final ThreadContext threadContext = threadPool.getThreadContext();
        final AtomicReference<String> seen = new AtomicReference<>();
        final CountDownLatch done = new CountDownLatch(1);
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader("X", "1");
            assertTrue(service.tryEnqueue("g1", "g1:group", task(), 5, headerCapturingListener(seen, done)));
        }
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader("X", "2");
            assertTrue(service.handoffNodePermit("g1", "g1:group", () -> {}));
        }
        assertTrue(done.await(10, TimeUnit.SECONDS));
        assertEquals("the handed-off request must run in its own context", "1", seen.get());
    }

    public void testPushedGrantAdmitsWaitingAndPendingRequestsInTheirOwnContext() throws Exception {
        final ThreadContext threadContext = threadPool.getThreadContext();
        final AtomicReference<String> waitingSeen = new AtomicReference<>();
        final AtomicReference<String> pendingSeen = new AtomicReference<>();
        final CountDownLatch done = new CountDownLatch(2);
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader("X", "1");
            assertTrue(service.tryEnqueue("g1", "g1:group", task(), 5, headerCapturingListener(waitingSeen, done)));
        }
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader("X", "3");
            assertNotNull(service.tryRegisterPendingAcquire("g1", "g1:group", task(), headerCapturingListener(pendingSeen, done)));
        }
        // A pushed grant is handled on the transport thread, inside the owner's inbound GRANT context.
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader("X", "2");
            assertTrue(service.admitWithPermit("g1:group", () -> {}).isPresent());
            assertTrue(service.admitWithPermit("g1:group", () -> {}).isPresent());
        }
        assertTrue(done.await(10, TimeUnit.SECONDS));
        assertEquals("a WAITING request admitted by a grant runs in its own context", "1", waitingSeen.get());
        assertEquals("a PENDING_ACQUIRE request admitted by a grant runs in its own context", "3", pendingSeen.get());
    }

    public void testBucketKeysWithWaitingListsOnlyBucketsHoldingWaitingRequests() {
        assertTrue(service.tryEnqueue("g1", "g1:group", task(), 5, ActionListener.wrap(r -> {}, e -> {})));
        assertTrue(service.tryEnqueue("g1", "g1:username:other", task(), 5, ActionListener.wrap(r -> {}, e -> {})));
        assertNotNull(service.tryRegisterPendingAcquire("g1", "g1:username:pending", task(), ActionListener.wrap(r -> {}, e -> {})));
        assertEquals(
            "a bucket with only a PENDING_ACQUIRE request is left to that request's own acquire",
            Set.of("g1:group", "g1:username:other"),
            service.bucketKeysWithWaiting()
        );

        assertTrue(service.admitWithPermit("g1:group", () -> {}).isPresent());
        assertEquals(Set.of("g1:username:other"), service.bucketKeysWithWaiting());
    }

    public void testFullRetainedCeilingIsNotCountedAsAQueueRejectionByRegistration() {
        for (int i = 0; i < WorkloadGroupQueueSettings.MAX_GROUP_QUEUE_DEPTH; i++) {
            assertNotNull(service.tryRegisterPendingAcquire("g1", "g1:group", task(), ActionListener.wrap(r -> {}, e -> {})));
        }
        assertNull(service.tryRegisterPendingAcquire("g1", "g1:group", task(), ActionListener.wrap(r -> {}, e -> {})));
        assertEquals(
            "the caller still asks the owner, so only a later refusal (a denial, or no answer) is a queue rejection",
            0,
            stateAccessor.getWorkloadGroupState("g1").getTotalQueueRejections()
        );
    }

    public void testIsWaitingFollowsATaskThroughTheQueue() throws Exception {
        // Only WAITING counts: a task the shared tier still asks about (PENDING_ACQUIRE) may be holding a grant in flight.
        SearchTask sharedTierTask = task();
        SearchTask nodeTierTask = task();
        SearchTask otherGroupTask = task();
        WorkloadGroupQueue.QueuedRequest pending = service.tryRegisterPendingAcquire(
            "g1",
            "g1:group",
            sharedTierTask,
            ActionListener.wrap(p -> {}, e -> {})
        );
        assertNotNull(pending);
        assertFalse("a PENDING_ACQUIRE request is not waiting", service.isWaiting(sharedTierTask.getId()));
        assertEquals(WorkloadGroupQueue.WaitingTransition.WAITING, service.transitionPendingAcquireToWaiting(pending, 5));
        assertTrue(service.isWaiting(sharedTierTask.getId()));
        AtomicReference<Exception> evicted = new AtomicReference<>();
        assertTrue(service.tryEnqueue("g1", "g1:group", nodeTierTask, 5, ActionListener.wrap(p -> {}, evicted::set)));
        assertTrue(service.tryEnqueue("g2", "g2:group", otherGroupTask, 5, ActionListener.wrap(p -> {}, e -> {})));
        assertTrue(service.isWaiting(nodeTierTask.getId()));
        assertTrue("every group is asked", service.isWaiting(otherGroupTask.getId()));
        assertFalse("a task that never queued is not waiting", service.isWaiting(task().getId()));

        assertEquals("a grant admits the oldest", OptionalLong.of(sharedTierTask.getId()), service.admitWithPermit("g1:group", () -> {}));
        assertFalse("an admitted request no longer waits", service.isWaiting(sharedTierTask.getId()));
        assertTrue(service.isWaiting(nodeTierTask.getId()));

        nodeTierTask.cancel("client disconnected");
        assertBusy(() -> assertTrue(evicted.get() instanceof TaskCancelledException));
        assertFalse("an evicted request no longer waits", service.isWaiting(nodeTierTask.getId()));
        assertEquals(1, service.admitAllUntracked("g2"));
        assertFalse("a released request no longer waits", service.isWaiting(otherGroupTask.getId()));
    }

    public void testHasRetainedRequestsCoversEveryGroupAndEveryExit() {
        assertFalse("nothing retained yet", service.hasRetainedRequests());
        SearchTask cancelled = task();
        WorkloadGroupQueue.QueuedRequest claimed = service.tryRegisterPendingAcquire(
            "g1",
            "g1:group",
            task(),
            ActionListener.wrap(p -> {}, e -> {})
        );
        WorkloadGroupQueue.QueuedRequest refused = service.tryRegisterPendingAcquire(
            "g1",
            "g1:group",
            task(),
            ActionListener.wrap(p -> {}, e -> {})
        );
        assertTrue(service.hasRetainedRequests());
        assertTrue(service.tryEnqueue("g2", "g2:username:alice", cancelled, 5, ActionListener.wrap(p -> {}, e -> {})));
        assertTrue(service.tryEnqueue("g2", "g2:username:bob", task(), 5, ActionListener.wrap(p -> {}, e -> {})));

        assertTrue(service.claimPendingAcquire(claimed));
        assertEquals(WorkloadGroupQueue.WaitingTransition.BUCKET_FULL, service.transitionPendingAcquireToWaiting(refused, 0));
        assertTrue("the other group still retains requests", service.hasRetainedRequests());
        cancelled.cancel("client disconnected");
        assertTrue(service.hasRetainedRequests());
        assertTrue(service.admitWithPermit("g2:username:bob", () -> {}).isPresent());

        assertEquals("both groups keep their queue objects", Set.of("g1", "g2"), service.queuedGroupIds());
        assertFalse("but no retained request", service.hasRetainedRequests());
    }
}
