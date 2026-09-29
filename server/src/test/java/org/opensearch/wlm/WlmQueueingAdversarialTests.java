/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.action.search.SearchTask;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** Adversarial probes of the PENDING_ACQUIRE -> WAITING state machine and exact-entry acquire outcomes. */
public class WlmQueueingAdversarialTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private WorkloadGroupsStateAccessor stateAccessor;
    private WorkloadGroupQueueService service;

    private static final String GROUP = "g1";
    private static final String BUCKET = "g1:group";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        stateAccessor = new WorkloadGroupsStateAccessor();
        stateAccessor.addNewWorkloadGroup(GROUP);
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

    private static OpenSearchRejectedExecutionException throttle429() {
        return new OpenSearchRejectedExecutionException("Request throttled: test.");
    }

    /** A granted result raced by an unregistered-denial result for the same token must complete exactly once. */
    public void testExactAdmitAndRejectRaceCompletesExactlyOnce() throws Exception {
        for (int iter = 0; iter < 200; iter++) {
            WorkloadGroupQueueService svc = new WorkloadGroupQueueService(threadPool, stateAccessor);
            AtomicInteger admits = new AtomicInteger();
            AtomicInteger failures = new AtomicInteger();
            CountDownLatch done = new CountDownLatch(1);

            WorkloadGroupQueue.QueuedRequest pending = svc.tryRegisterPendingAcquire(GROUP, BUCKET, task(), ActionListener.wrap(p -> {
                admits.incrementAndGet();
                done.countDown();
            }, e -> {
                failures.incrementAndGet();
                done.countDown();
            }));
            assertNotNull(pending);

            CyclicBarrier barrier = new CyclicBarrier(2);
            AtomicReference<Boolean> admitResult = new AtomicReference<>();
            AtomicReference<Boolean> rejectResult = new AtomicReference<>();

            Thread admitter = new Thread(() -> {
                try {
                    barrier.await();
                } catch (Exception ignored) {}
                admitResult.set(svc.admitPendingAcquire(pending, () -> {}));
            });
            Thread rejecter = new Thread(() -> {
                try {
                    barrier.await();
                } catch (Exception ignored) {}
                rejectResult.set(svc.rejectPendingAcquire(pending, throttle429()));
            });
            admitter.start();
            rejecter.start();
            admitter.join();
            rejecter.join();

            assertTrue("listener must be completed", done.await(10, TimeUnit.SECONDS));
            // Give any erroneous second completion a chance to land.
            Thread.yield();
            assertEquals("iter " + iter + ": exactly one completion", 1, admits.get() + failures.get());
            assertTrue("iter " + iter + ": exactly one of admit/reject must claim the request", admitResult.get() ^ rejectResult.get());
            assertEquals("iter " + iter + ": retained queue must be empty", 0, svc.retainedDepth(GROUP));
        }
    }

    /**
     * Owner-push can race the original denial response. Either the grant claims PENDING_ACQUIRE first and the denial is
     * a no-op, or the denial transitions to WAITING first and the grant completes that wait. Both orders are safe.
     */
    public void testPushedGrantAndDenialTransitionRaceCompletesExactlyOnce() throws Exception {
        for (int iter = 0; iter < 200; iter++) {
            WorkloadGroupQueueService svc = new WorkloadGroupQueueService(threadPool, stateAccessor);
            AtomicInteger admits = new AtomicInteger();
            AtomicInteger failures = new AtomicInteger();
            CountDownLatch completed = new CountDownLatch(1);
            WorkloadGroupQueue.QueuedRequest pending = svc.tryRegisterPendingAcquire(GROUP, BUCKET, task(), ActionListener.wrap(p -> {
                admits.incrementAndGet();
                completed.countDown();
            }, e -> {
                failures.incrementAndGet();
                completed.countDown();
            }));
            assertNotNull(pending);

            CyclicBarrier barrier = new CyclicBarrier(2);
            AtomicReference<Boolean> grantClaimed = new AtomicReference<>();
            AtomicReference<WorkloadGroupQueueService.PendingAcquireTransition> denialResult = new AtomicReference<>();
            Thread grant = new Thread(() -> {
                try {
                    barrier.await();
                } catch (Exception ignored) {}
                grantClaimed.set(svc.admitWithPermit(BUCKET, () -> {}));
            });
            Thread denial = new Thread(() -> {
                try {
                    barrier.await();
                } catch (Exception ignored) {}
                denialResult.set(svc.transitionPendingAcquireToWaiting(pending, 1, throttle429()));
            });
            grant.start();
            denial.start();
            grant.join();
            denial.join();

            assertTrue(completed.await(10, TimeUnit.SECONDS));
            assertTrue(grantClaimed.get());
            assertTrue(
                denialResult.get() == WorkloadGroupQueueService.PendingAcquireTransition.WAITING
                    || denialResult.get() == WorkloadGroupQueueService.PendingAcquireTransition.REQUEST_GONE
            );
            assertEquals(1, admits.get());
            assertEquals(0, failures.get());
            assertEquals(0, svc.retainedDepth(GROUP));
        }
    }

    /** A full WAITING bucket rejects the originating pending token, never the existing waiter. */
    public void testQueueFullTransitionRejectsExactPendingRequest() throws Exception {
        AtomicInteger existingAdmitted = new AtomicInteger();
        AtomicInteger existingFailed = new AtomicInteger();
        AtomicInteger pendingAdmitted = new AtomicInteger();
        AtomicInteger pendingFailed = new AtomicInteger();

        assertTrue(
            service.tryEnqueue(
                GROUP,
                BUCKET,
                task(),
                1,
                ActionListener.wrap(p -> existingAdmitted.incrementAndGet(), e -> existingFailed.incrementAndGet())
            )
        );
        WorkloadGroupQueue.QueuedRequest pending = service.tryRegisterPendingAcquire(
            GROUP,
            BUCKET,
            task(),
            ActionListener.wrap(p -> pendingAdmitted.incrementAndGet(), e -> pendingFailed.incrementAndGet())
        );
        assertNotNull(pending);

        assertEquals(
            WorkloadGroupQueueService.PendingAcquireTransition.QUEUE_FULL,
            service.transitionPendingAcquireToWaiting(pending, 1, throttle429())
        );
        assertBusy(() -> assertEquals(1, pendingFailed.get()));
        assertEquals(0, pendingAdmitted.get());
        assertEquals(0, existingFailed.get());
        assertEquals(1, service.currentDepth(GROUP));
        assertEquals(1, service.retainedDepth(GROUP));

        assertTrue(service.admitWithPermit(BUCKET, () -> {}));
        assertBusy(() -> assertEquals(1, existingAdmitted.get()));
    }

    /** Cancellation removes a pending token, and every delayed acquire outcome becomes a harmless no-op. */
    public void testCancellationBeforeAcquireResultCompletesExactlyOnce() throws Exception {
        SearchTask t = task();
        AtomicInteger completions = new AtomicInteger();
        CountDownLatch completed = new CountDownLatch(1);
        WorkloadGroupQueue.QueuedRequest pending = service.tryRegisterPendingAcquire(GROUP, BUCKET, t, ActionListener.wrap(p -> {
            completions.incrementAndGet();
            completed.countDown();
        }, e -> {
            completions.incrementAndGet();
            completed.countDown();
        }));
        assertNotNull(pending);

        t.cancel("client gone");
        assertTrue(completed.await(10, TimeUnit.SECONDS));
        assertEquals(1, completions.get());
        assertEquals(0, service.retainedDepth(GROUP));

        assertFalse(service.admitPendingAcquire(pending, () -> {}));
        assertFalse(service.rejectPendingAcquire(pending, throttle429()));
        assertEquals(
            WorkloadGroupQueueService.PendingAcquireTransition.REQUEST_GONE,
            service.transitionPendingAcquireToWaiting(pending, 1, throttle429())
        );
        Thread.yield();
        assertEquals(1, completions.get());
    }

    /** Exact-entry rejection must not complete the listener inline on the caller's transport thread. */
    public void testRejectPendingCompletesOffCallerThread() throws Exception {
        Thread caller = Thread.currentThread();
        AtomicReference<Thread> failedOn = new AtomicReference<>();
        WorkloadGroupQueue.QueuedRequest pending = service.tryRegisterPendingAcquire(
            GROUP,
            BUCKET,
            task(),
            ActionListener.wrap(p -> {}, e -> failedOn.set(Thread.currentThread()))
        );
        assertNotNull(pending);
        assertTrue(service.rejectPendingAcquire(pending, throttle429()));
        assertBusy(() -> assertNotNull(failedOn.get()));
        assertNotSame("must be dispatched, not inline (caller is a transport thread)", caller, failedOn.get());
    }

    // ---------------------------------------------------------------------------------------------------------------
    // PROBE 5 — mixed-version wire compatibility of the `registered` key
    // ---------------------------------------------------------------------------------------------------------------

    /** A pre-queueing owner sends only `granted`; the missing `registered` key must read as false. */
    public void testAcquireResponseWithoutRegisteredKeyDefaultsToFalse() throws Exception {
        for (boolean granted : new boolean[] { true, false }) {
            BytesReference bytes;
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                // Exactly what a pre-queueing owner writes: a one-key body.
                out.writeMap(
                    Map.of(WorkloadGroupSharedThrottleService.AcquirePermitResponse.KEY_GRANTED, granted),
                    StreamOutput::writeString,
                    StreamOutput::writeGenericValue
                );
                bytes = out.bytes();
            }
            try (StreamInput in = bytes.streamInput()) {
                WorkloadGroupSharedThrottleService.AcquirePermitResponse response =
                    new WorkloadGroupSharedThrottleService.AcquirePermitResponse(in);
                assertEquals("granted must round-trip", granted, response.granted);
                assertFalse("a missing registered key must mean NOT registered", response.registered);
            }
        }
    }

    /** A newer owner may add keys this build does not know; they must be ignored, not fatal. */
    public void testAcquireResponseIgnoresUnknownExtraKeys() throws Exception {
        BytesReference bytes;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            Map<String, Object> body = new HashMap<>();
            body.put(WorkloadGroupSharedThrottleService.AcquirePermitResponse.KEY_GRANTED, false);
            body.put(WorkloadGroupSharedThrottleService.AcquirePermitResponse.KEY_REGISTERED, true);
            body.put("some_future_field", 42L);
            body.put("another_future_field", "hello");
            out.writeMap(body, StreamOutput::writeString, StreamOutput::writeGenericValue);
            bytes = out.bytes();
        }
        try (StreamInput in = bytes.streamInput()) {
            WorkloadGroupSharedThrottleService.AcquirePermitResponse response =
                new WorkloadGroupSharedThrottleService.AcquirePermitResponse(in);
            assertFalse(response.granted);
            assertTrue("known keys must still be read correctly alongside unknown ones", response.registered);
        }
    }

    /** Full round-trip of the real writeTo for both values of the new field. */
    public void testAcquireResponseRegisteredRoundTrips() throws Exception {
        for (boolean granted : new boolean[] { true, false }) {
            for (boolean registered : new boolean[] { true, false }) {
                BytesReference bytes;
                try (BytesStreamOutput out = new BytesStreamOutput()) {
                    new WorkloadGroupSharedThrottleService.AcquirePermitResponse(granted, registered).writeTo(out);
                    bytes = out.bytes();
                }
                try (StreamInput in = bytes.streamInput()) {
                    WorkloadGroupSharedThrottleService.AcquirePermitResponse rt =
                        new WorkloadGroupSharedThrottleService.AcquirePermitResponse(in);
                    assertEquals(granted, rt.granted);
                    assertEquals(registered, rt.registered);
                }
            }
        }
    }

    /** The one-arg convenience constructor must default registered to false (never silently "registered"). */
    public void testAcquireResponseOneArgCtorDefaultsRegisteredFalse() {
        assertFalse(new WorkloadGroupSharedThrottleService.AcquirePermitResponse(true).registered);
        assertFalse(new WorkloadGroupSharedThrottleService.AcquirePermitResponse(false).registered);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // total_queued / queue-wait accounting
    // ---------------------------------------------------------------------------------------------------------------

    /** A provisional owner round trip is retained for safety but is not visible as denied queue backlog. */
    public void testPendingAcquireDoesNotCountOrAppearAsWaiting() {
        WorkloadGroupQueue.QueuedRequest pending = service.tryRegisterPendingAcquire(
            GROUP,
            BUCKET,
            task(),
            ActionListener.wrap(p -> {}, e -> {})
        );
        assertNotNull(pending);
        assertEquals(1, service.retainedDepth(GROUP));
        assertEquals(0, service.currentDepth(GROUP));
        assertEquals(0, service.peakDepth(GROUP));
        assertEquals(0, stateAccessor.getWorkloadGroupState(GROUP).getTotalQueued());
    }

    /** A direct grant to PENDING_ACQUIRE counts neither a queue wait nor a throttle. */
    public void testPendingDirectAdmissionDoesNotCountQueued() throws Exception {
        WorkloadGroupQueue.QueuedRequest pending = service.tryRegisterPendingAcquire(
            GROUP,
            BUCKET,
            task(),
            ActionListener.wrap(p -> {}, e -> {})
        );
        assertNotNull(pending);
        assertTrue(service.admitPendingAcquire(pending, () -> {}));
        assertEquals("a provisional pass-through must not count as queued", 0, stateAccessor.getWorkloadGroupState(GROUP).getTotalQueued());
        assertEquals("and must not record a wait", 0, stateAccessor.getWorkloadGroupState(GROUP).getTotalQueueWaitMillis());
        assertEquals(0, service.retainedDepth(GROUP));
    }

    /** Once denial transitions the entry to WAITING, a pushed grant records exactly one completed queue wait. */
    public void testPushedAdmissionAfterDenialCountsQueuedAndRecordsWait() {
        WorkloadGroupQueue.QueuedRequest pending = service.tryRegisterPendingAcquire(
            GROUP,
            BUCKET,
            task(),
            ActionListener.wrap(p -> {}, e -> {})
        );
        assertNotNull(pending);
        assertEquals(
            WorkloadGroupQueueService.PendingAcquireTransition.WAITING,
            service.transitionPendingAcquireToWaiting(pending, 1, throttle429())
        );
        assertEquals(1, service.currentDepth(GROUP));
        assertTrue(service.admitWithPermit(BUCKET, () -> {}));
        assertEquals(
            "a pushed admission counts exactly one queued request",
            1,
            stateAccessor.getWorkloadGroupState(GROUP).getTotalQueued()
        );
        assertEquals(0, service.currentDepth(GROUP));
    }

    /** An unregistered denial of a provisional request does not count as a completed queue wait. */
    public void testRejectPendingDoesNotCountQueued() {
        WorkloadGroupQueue.QueuedRequest pending = service.tryRegisterPendingAcquire(
            GROUP,
            BUCKET,
            task(),
            ActionListener.wrap(p -> {}, e -> {})
        );
        assertNotNull(pending);
        assertTrue(service.rejectPendingAcquire(pending, throttle429()));
        assertEquals(
            "a request refused outright never waited, so it must not count as queued",
            0,
            stateAccessor.getWorkloadGroupState(GROUP).getTotalQueued()
        );
        assertEquals("and it must not record a queue wait either", 0, stateAccessor.getWorkloadGroupState(GROUP).getTotalQueueWaitMillis());
    }

    /** The failure instance associated with the exact pending token must be the one its client sees. */
    public void testRejectPendingPropagatesTheGivenFailure() throws Exception {
        OpenSearchRejectedExecutionException expected = throttle429();
        AtomicReference<Exception> seen = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        WorkloadGroupQueue.QueuedRequest pending = service.tryRegisterPendingAcquire(
            GROUP,
            BUCKET,
            task(),
            ActionListener.wrap(p -> {}, e -> {
                seen.set(e);
                latch.countDown();
            })
        );
        assertNotNull(pending);
        assertTrue(service.rejectPendingAcquire(pending, expected));
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertSame("the client must receive exactly the supplied 429", expected, seen.get());
    }
}
