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
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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
        assertEquals(0, service.currentDepth("g1"));
    }

    public void testTryEnqueueParksAndCountsDepth() {
        boolean parked = service.tryEnqueue("g1", "g1:group", task(), 5, ActionListener.wrap(r -> {}, e -> {}));
        assertTrue(parked);
        assertEquals(1, service.currentDepth("g1"));
        assertEquals(1, service.totalDepth());
    }

    public void testTryEnqueueCapacityIsPerBucket() {
        // The size_per_bucket cap is threaded through tryEnqueue per bucket, not as one budget for the whole group:
        // saturating alice's bucket must not deny bob.
        assertTrue(service.tryEnqueue("g1", "g1:username:alice", task(), 1, ActionListener.wrap(r -> {}, e -> {})));
        assertFalse(service.tryEnqueue("g1", "g1:username:alice", task(), 1, ActionListener.wrap(r -> {}, e -> {})));
        assertTrue(service.tryEnqueue("g1", "g1:username:bob", task(), 1, ActionListener.wrap(r -> {}, e -> {})));
        assertEquals(2, service.currentDepth("g1"));
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
        assertEquals("pending owner RPCs are not WAITING backlog", 0, service.currentDepth("g1"));
        assertEquals(2, service.retainedDepth("g1"));
        assertEquals(2, service.totalDepth());
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

        assertTrue(service.admitWithPermit(waitingBucket, () -> {}));
        assertBusy(() -> assertEquals(2, admitted.get()));
        assertTrue(service.retainedBucketKeys().isEmpty());
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

        assertEquals(
            WorkloadGroupQueueService.PendingAcquireTransition.WAITING,
            service.transitionPendingAcquireToWaiting(first, 1, new RuntimeException("full"))
        );
        assertEquals(
            WorkloadGroupQueueService.PendingAcquireTransition.QUEUE_FULL,
            service.transitionPendingAcquireToWaiting(second, 1, new RuntimeException("full"))
        );
        assertBusy(() -> assertEquals(1, secondFailed.get()));
        assertEquals(0, secondAdmitted.get());
        assertEquals(0, firstFailed.get());
        assertEquals("the existing waiter remains", 1, service.currentDepth("g1"));
        assertEquals(1, service.retainedDepth("g1"));
        assertEquals(1, stateAccessor.getWorkloadGroupState("g1").getTotalQueueRejections());

        assertTrue(service.admitWithPermit("g1:group", () -> {}));
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

        assertTrue(service.admitWithPermit("g1:group", () -> {}));
        assertEquals(
            "the delayed denial must be an exact-entry no-op",
            WorkloadGroupQueueService.PendingAcquireTransition.REQUEST_GONE,
            service.transitionPendingAcquireToWaiting(pending, 1, new RuntimeException("full"))
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
        assertTrue(service.rejectPendingAcquire(first, new RuntimeException("first")));
        assertBusy(() -> assertEquals(1, firstFailed.get()));

        WorkloadGroupQueue.QueuedRequest replacement = service.tryRegisterPendingAcquire(
            "g1",
            "g1:group",
            task(),
            ActionListener.wrap(p -> secondAdmitted.incrementAndGet(), e -> secondFailed.incrementAndGet())
        );
        assertNotNull(replacement);
        assertFalse(
            "a stale result for the old token must not touch the replacement",
            service.rejectPendingAcquire(first, new RuntimeException())
        );
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
        assertEquals("the older denied waiter remains queued", 1, service.currentDepth("g1"));
        assertEquals(0, olderAdmitted.get());

        assertTrue(service.admitWithPermit("g1:group", () -> {}));
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
        assertEquals(0, service.currentDepth("g1")); // drained
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
        assertEquals(3, service.currentDepth("g1"));

        // A single freed node permit admits exactly one waiter.
        service.drainNode("g1", "g1:group", key -> (Releasable) () -> {});
        assertBusy(() -> assertEquals(1, admitted.get()));
        assertEquals(2, service.currentDepth("g1")); // two still parked
    }

    public void testDrainNodeLeavesWaiterQueuedWhenNoPermit() {
        AtomicInteger admitted = new AtomicInteger();
        assertTrue(service.tryEnqueue("g1", "g1:group", task(), 5, ActionListener.wrap(p -> admitted.incrementAndGet(), e -> {})));
        // nodeAcquire returns null (limit reached) -> nothing admitted, request stays queued.
        service.drainNode("g1", "g1:group", key -> null);
        assertEquals(0, admitted.get());
        assertEquals(1, service.currentDepth("g1"));
    }

    // Regression for the recursion fix: admit() must complete the parked listener on the executor, never inline on the
    // caller's (drain/completion) thread. This is what breaks the node-tier close()->drainNode->admit->close() chain
    // into separate executor tasks (a synchronous inline completion allowed StackOverflow under a cancel storm). We
    // assert the common admit path hands off to a different thread than the caller — the same dispatch that also
    // governs the cancelled branch's permit.close().
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
        assertEquals(0, service.currentDepth("g1"));
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
        assertEquals("the sweep is no longer needed for cancellation cleanup", 0, sweepDrains.get());
    }
}
