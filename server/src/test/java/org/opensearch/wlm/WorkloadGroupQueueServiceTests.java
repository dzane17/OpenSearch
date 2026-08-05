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
        boolean parked = service.tryEnqueue("g1", "g1:group", task(), null, 0, 0L, ActionListener.wrap(r -> {}, e -> {}));
        assertFalse(parked);
        assertEquals(0, service.currentDepth("g1"));
    }

    public void testTryEnqueueParksAndCountsDepth() {
        boolean parked = service.tryEnqueue("g1", "g1:group", task(), null, 5, 0L, ActionListener.wrap(r -> {}, e -> {}));
        assertTrue(parked);
        assertEquals(1, service.currentDepth("g1"));
        assertEquals(1, service.totalDepth());
    }

    public void testDrainNodeAdmitsOneWaiterWithPermit() throws Exception {
        AtomicReference<Releasable> admittedPermit = new AtomicReference<>();
        AtomicInteger admitted = new AtomicInteger();
        assertTrue(service.tryEnqueue("g1", "g1:group", task(), null, 5, 0L, ActionListener.wrap(p -> {
            admittedPermit.set(p);
            admitted.incrementAndGet();
        }, e -> {})));

        Releasable permit = () -> {};
        service.drainNode("g1", "g1:group", key -> permit);

        assertBusy(() -> assertEquals(1, admitted.get()));
        assertSame(permit, admittedPermit.get());
        assertEquals(0, service.currentDepth("g1")); // drained
    }

    public void testDrainNodeAdmitsAtMostOnePerFreedPermit() throws Exception {
        AtomicInteger admitted = new AtomicInteger();
        for (int i = 0; i < 3; i++) {
            assertTrue(
                service.tryEnqueue("g1", "g1:group", task(), null, 5, 0L, ActionListener.wrap(p -> admitted.incrementAndGet(), e -> {}))
            );
        }
        assertEquals(3, service.currentDepth("g1"));

        // A single freed node permit admits exactly one waiter.
        service.drainNode("g1", "g1:group", key -> (Releasable) () -> {});
        assertBusy(() -> assertEquals(1, admitted.get()));
        assertEquals(2, service.currentDepth("g1")); // two still parked
    }

    public void testDrainNodeLeavesWaiterQueuedWhenNoPermit() {
        AtomicInteger admitted = new AtomicInteger();
        assertTrue(
            service.tryEnqueue("g1", "g1:group", task(), null, 5, 0L, ActionListener.wrap(p -> admitted.incrementAndGet(), e -> {}))
        );
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
            service.tryEnqueue(
                "g1",
                "g1:group",
                task(),
                null,
                5,
                0L,
                ActionListener.wrap(p -> respondedOn.set(Thread.currentThread()), e -> {})
            )
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
        service.tryEnqueue(
            "g1",
            "g1:group",
            t,
            null,
            5,
            0L,
            ActionListener.wrap(p -> admittedFlag.set(true), e -> failures.incrementAndGet())
        );
        assertBusy(() -> assertEquals(1, failures.get()));
        assertFalse(admittedFlag.get());
        assertEquals(0, service.currentDepth("g1"));
    }
}
