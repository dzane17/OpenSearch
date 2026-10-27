/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.common.lease.Releasable;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.wlm.WorkloadGroupTask.DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER;
import static org.opensearch.wlm.WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER;

public class WorkloadGroupTaskTests extends OpenSearchTestCase {
    private ThreadPool threadPool;
    private WorkloadGroupTask sut;

    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        sut = new WorkloadGroupTask(123, "transport", "Search", "test task", null, Collections.emptyMap());
    }

    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
    }

    public void testSuccessfulSetWorkloadGroupId() {
        sut.setWorkloadGroupId(threadPool.getThreadContext());
        assertEquals(DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get(), sut.getWorkloadGroupId());

        threadPool.getThreadContext().putHeader(WORKLOAD_GROUP_ID_HEADER, "akfanglkaglknag2332");

        sut.setWorkloadGroupId(threadPool.getThreadContext());
        assertEquals("akfanglkaglknag2332", sut.getWorkloadGroupId());
    }

    public void testThrottlePrincipalIsPerTaskAndNotAHeader() {
        assertNull("an absent principal must read as null so username/role throttling fails open", sut.getThrottlePrincipal());

        sut.setThrottlePrincipal("username|alice");
        assertEquals("username|alice", sut.getThrottlePrincipal());

        // _msearch sub-requests share a thread context, so the principal is per task to avoid cross-billing.
        WorkloadGroupTask other = new WorkloadGroupTask(124, "transport", "Search", "test task", null, Collections.emptyMap());
        assertNull(other.getThrottlePrincipal());

        // Not in the header maps, which are serialized to every shard and remote cluster.
        assertNull(sut.getHeader("workloadGroupPrincipal"));
        assertNull(threadPool.getThreadContext().getHeader("workloadGroupPrincipal"));
    }

    public void testOnCancelledCallbackFiresOnceOnCancel() {
        AtomicInteger fired = new AtomicInteger(0);
        sut.addOnCancelledCallback(fired::incrementAndGet);
        assertEquals(0, fired.get());
        sut.cancel("test");
        assertEquals(1, fired.get());
        // A second cancel must not re-fire the callback.
        sut.cancel("again");
        assertEquals(1, fired.get());
    }

    public void testOnCancelledCallbackRunsImmediatelyIfAlreadyCancelled() {
        sut.cancel("test");
        AtomicInteger fired = new AtomicInteger(0);
        // Registering after cancellation runs the callback immediately (no lost-cancellation race).
        sut.addOnCancelledCallback(fired::incrementAndGet);
        assertEquals(1, fired.get());
    }

    public void testDeregisteredCallbackDoesNotFire() {
        AtomicInteger fired = new AtomicInteger(0);
        Releasable handle = sut.addOnCancelledCallback(fired::incrementAndGet);
        handle.close(); // deregister (request admitted/drained before any cancellation)
        sut.cancel("test");
        assertEquals(0, fired.get());
    }

    public void testCancelWithoutCallbackThenLateRegisterRunsImmediately() {
        // A task that never queued has no callback list; cancelling it must not fail, and a late register still runs.
        sut.cancel("test");
        AtomicInteger fired = new AtomicInteger(0);
        sut.addOnCancelledCallback(fired::incrementAndGet);
        assertEquals(1, fired.get());
    }

    public void testDeregisterAfterCancellationIsNoop() {
        AtomicInteger fired = new AtomicInteger(0);
        Releasable handle = sut.addOnCancelledCallback(fired::incrementAndGet);
        sut.cancel("test");
        assertEquals(1, fired.get());
        handle.close();
        assertEquals(1, fired.get());
    }

    public void testOnlyStillRegisteredCallbacksFire() {
        AtomicInteger firedA = new AtomicInteger(0);
        AtomicInteger firedB = new AtomicInteger(0);
        Releasable handleA = sut.addOnCancelledCallback(firedA::incrementAndGet);
        sut.addOnCancelledCallback(firedB::incrementAndGet);
        handleA.close();
        sut.cancel("test");
        assertEquals(0, firedA.get());
        assertEquals(1, firedB.get());
    }

    public void testAlreadyCancelledCallbackRunsOutsideTaskMonitor() {
        // The callback (the queue's eviction) takes a bucket lock and dispatches to an executor, so it must not run while
        // holding the task's monitor.
        sut.cancel("test");
        AtomicBoolean heldMonitor = new AtomicBoolean(true);
        sut.addOnCancelledCallback(() -> heldMonitor.set(Thread.holdsLock(sut)));
        assertFalse(heldMonitor.get());
    }

    public void testCallbackFiresWhenASubclassOverridesOnCancelledWithoutSuper() {
        // The asynchronous search plugin's task overrides onCancelled() without calling super.
        AtomicInteger subclassHookRuns = new AtomicInteger(0);
        WorkloadGroupTask task = new WorkloadGroupTask(125, "transport", "Search", "test task", null, Collections.emptyMap()) {
            @Override
            protected void onCancelled() {
                subclassHookRuns.incrementAndGet();
            }
        };
        AtomicInteger fired = new AtomicInteger(0);
        task.addOnCancelledCallback(fired::incrementAndGet);
        task.cancel("test");
        task.cancel("again");
        assertEquals("the registered callback fires once", 1, fired.get());
        assertEquals("the subclass's own hook still runs once", 1, subclassHookRuns.get());
    }

    public void testCallbackFiresWhenASubclassOnCancelledThrows() {
        WorkloadGroupTask task = new WorkloadGroupTask(126, "transport", "Search", "test task", null, Collections.emptyMap()) {
            @Override
            protected void onCancelled() {
                throw new IllegalStateException("subclass hook failed");
            }
        };
        AtomicInteger fired = new AtomicInteger(0);
        task.addOnCancelledCallback(fired::incrementAndGet);
        expectThrows(IllegalStateException.class, () -> task.cancel("test"));
        assertTrue(task.isCancelled());
        assertEquals(1, fired.get());
    }
}
