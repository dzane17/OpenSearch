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
}
