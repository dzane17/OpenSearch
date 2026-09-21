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

/**
 * Adversarial probes of the owner-push {@code registered} bit and the {@code rejectOldest} path added by queueing.
 * Throwaway/diagnostic suite: each test states the interleaving it forces and the invariant it is trying to break.
 */
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

    // ---------------------------------------------------------------------------------------------------------------
    // PROBE 1 — exactly-once completion
    // ---------------------------------------------------------------------------------------------------------------

    /**
     * A single parked request raced by an admit and a reject must be completed EXACTLY ONCE, never both admitted and
     * failed. Both paths poll under the same per-bucket lock, so one must lose.
     */
    public void testAdmitAndRejectRaceCompletesExactlyOnce() throws Exception {
        for (int iter = 0; iter < 200; iter++) {
            WorkloadGroupQueueService svc = new WorkloadGroupQueueService(threadPool, stateAccessor);
            AtomicInteger admits = new AtomicInteger();
            AtomicInteger failures = new AtomicInteger();
            CountDownLatch done = new CountDownLatch(1);

            assertTrue(svc.tryEnqueue(GROUP, BUCKET, task(), 5, ActionListener.wrap(p -> {
                admits.incrementAndGet();
                done.countDown();
            }, e -> {
                failures.incrementAndGet();
                done.countDown();
            })));

            CyclicBarrier barrier = new CyclicBarrier(2);
            AtomicReference<Boolean> admitResult = new AtomicReference<>();
            AtomicReference<Boolean> rejectResult = new AtomicReference<>();

            Thread admitter = new Thread(() -> {
                try {
                    barrier.await();
                } catch (Exception ignored) {}
                admitResult.set(svc.admitWithOwnPermit(BUCKET, () -> {}));
            });
            Thread rejecter = new Thread(() -> {
                try {
                    barrier.await();
                } catch (Exception ignored) {}
                rejectResult.set(svc.rejectOldest(BUCKET, throttle429()));
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
            assertEquals("iter " + iter + ": queue must be empty", 0, svc.currentDepth(GROUP));
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // PROBE 2 — reject-oldest victim selection and the dropped rejection
    // ---------------------------------------------------------------------------------------------------------------

    /** An empty bucket yields false: the rejection is dropped on the floor. Caller must cope. */
    public void testRejectOldestOnEmptyBucketReturnsFalse() {
        assertFalse("no queue object at all", service.rejectOldest(BUCKET, throttle429()));

        // Now create the queue object, park and drain, leaving an empty bucket behind a live queue.
        assertTrue(service.tryEnqueue(GROUP, BUCKET, task(), 5, ActionListener.wrap(p -> {}, e -> {})));
        assertTrue(service.admitWithOwnPermit(BUCKET, () -> {}));
        assertEquals(0, service.currentDepth(GROUP));
        assertFalse("queue exists but bucket is empty", service.rejectOldest(BUCKET, throttle429()));
    }

    /**
     * VICTIM SWAP: the request whose acquire was refused registration is NOT necessarily the one that gets the 429.
     * R1 parks, R2 parks; R2's acquire is granted and (FIFO) admits R1; R1's refused-registration callback then fires
     * and rejects the head, which is now R2. Net accounting is still one-in one-out, but the 429 lands on the request
     * whose own acquire succeeded.
     */
    public void testRejectOldestFailsTheHeadNotTheRefusedRequest() throws Exception {
        AtomicReference<String> r1Outcome = new AtomicReference<>();
        AtomicReference<String> r2Outcome = new AtomicReference<>();
        CountDownLatch both = new CountDownLatch(2);

        assertTrue(service.tryEnqueue(GROUP, BUCKET, task(), 5, ActionListener.wrap(p -> {
            r1Outcome.set("admitted");
            both.countDown();
        }, e -> {
            r1Outcome.set("failed");
            both.countDown();
        })));
        assertTrue(service.tryEnqueue(GROUP, BUCKET, task(), 5, ActionListener.wrap(p -> {
            r2Outcome.set("admitted");
            both.countDown();
        }, e -> {
            r2Outcome.set("failed");
            both.countDown();
        })));

        // R2's own acquire came back granted -> admits the OLDEST, which is R1.
        assertTrue(service.admitWithOwnPermit(BUCKET, () -> {}));
        // R1's acquire came back denied-and-unregistered -> rejects the head, which is now R2.
        assertTrue(service.rejectOldest(BUCKET, throttle429()));

        assertTrue(both.await(10, TimeUnit.SECONDS));
        assertEquals("R1 (the refused one) actually RAN", "admitted", r1Outcome.get());
        assertEquals("R2 (whose acquire was granted) took the 429", "failed", r2Outcome.get());
        assertEquals(0, service.currentDepth(GROUP));
    }

    /**
     * N unregisterable denials can never reject more than N requests, and never more than the queue holds — so
     * rejectOldest cannot over-reject a bucket.
     */
    public void testRejectOldestCannotOverRejectBeyondQueueDepth() throws Exception {
        AtomicInteger failures = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(2);
        for (int i = 0; i < 2; i++) {
            assertTrue(service.tryEnqueue(GROUP, BUCKET, task(), 5, ActionListener.wrap(p -> {}, e -> {
                failures.incrementAndGet();
                latch.countDown();
            })));
        }
        assertTrue(service.rejectOldest(BUCKET, throttle429()));
        assertTrue(service.rejectOldest(BUCKET, throttle429()));
        assertFalse("third rejection has no victim", service.rejectOldest(BUCKET, throttle429()));

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertEquals(2, failures.get());
        assertEquals(0, service.currentDepth(GROUP));
    }

    // ---------------------------------------------------------------------------------------------------------------
    // PROBE 3 — cancellation handle interaction
    // ---------------------------------------------------------------------------------------------------------------

    /**
     * A task cancelled (and hence evicted by its cancellation callback) before rejectOldest runs must be completed
     * exactly once — by the cancellation path — and rejectOldest must find nothing to reject.
     */
    public void testRejectOldestAfterCancellationEvictionDoesNotDoubleComplete() throws Exception {
        SearchTask t = task();
        AtomicInteger completions = new AtomicInteger();
        AtomicReference<Exception> failure = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        assertTrue(service.tryEnqueue(GROUP, BUCKET, t, 5, ActionListener.wrap(p -> {
            completions.incrementAndGet();
            latch.countDown();
        }, e -> {
            completions.incrementAndGet();
            failure.set(e);
            latch.countDown();
        })));

        t.cancel("client gone"); // fires the cancellation callback -> evictCancelled removes + fails it
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertEquals("cancellation must be the single completion", 1, completions.get());
        assertEquals(0, service.currentDepth(GROUP));

        assertFalse("the cancelled request is gone, so there is nothing to reject", service.rejectOldest(BUCKET, throttle429()));
        Thread.yield();
        assertEquals("rejectOldest must not add a second completion", 1, completions.get());
    }

    /**
     * Reverse order: rejectOldest claims the request first, then the task is cancelled. The cancellation callback must
     * find it already removed and not complete it again.
     */
    public void testCancellationAfterRejectOldestDoesNotDoubleComplete() throws Exception {
        SearchTask t = task();
        AtomicInteger completions = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(1);

        assertTrue(service.tryEnqueue(GROUP, BUCKET, t, 5, ActionListener.wrap(p -> {
            completions.incrementAndGet();
            latch.countDown();
        }, e -> {
            completions.incrementAndGet();
            latch.countDown();
        })));

        assertTrue(service.rejectOldest(BUCKET, throttle429()));
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        t.cancel("too late"); // handle already released; entry already removed
        Thread.yield();
        assertEquals("exactly one completion across reject + cancel", 1, completions.get());
        assertEquals(0, service.currentDepth(GROUP));
    }

    /** rejectOldest must not complete the listener inline on the caller's (transport) thread. */
    public void testRejectOldestCompletesOffCallerThread() throws Exception {
        Thread caller = Thread.currentThread();
        AtomicReference<Thread> failedOn = new AtomicReference<>();
        assertTrue(service.tryEnqueue(GROUP, BUCKET, task(), 5, ActionListener.wrap(p -> {}, e -> failedOn.set(Thread.currentThread()))));
        assertTrue(service.rejectOldest(BUCKET, throttle429()));
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

    /** Parking starts a wait; only finishing one counts. */
    public void testTryEnqueueDoesNotCountQueued() {
        assertTrue(service.tryEnqueue(GROUP, BUCKET, task(), 5, ActionListener.wrap(p -> {}, e -> {})));
        assertEquals("parking alone must not count as queued", 0, stateAccessor.getWorkloadGroupState(GROUP).getTotalQueued());
        assertEquals("but it is visible as live depth", 1, service.currentDepth(GROUP));
    }

    /**
     * A self-supplied admission (enqueue-first pass-through) counts nothing; a pushed one counts exactly one queued
     * request AND one wait sample. This is the invariant that makes total_queued a valid mean-wait denominator.
     */
    public void testOnlyPushedAdmissionsCountQueuedAndRecordWait() throws Exception {
        assertTrue(service.tryEnqueue(GROUP, BUCKET, task(), 5, ActionListener.wrap(p -> {}, e -> {})));
        assertTrue(service.admitWithOwnPermit(BUCKET, () -> {}));
        assertEquals(
            "an enqueue-first pass-through must not count as queued",
            0,
            stateAccessor.getWorkloadGroupState(GROUP).getTotalQueued()
        );
        assertEquals("and must not record a wait", 0, stateAccessor.getWorkloadGroupState(GROUP).getTotalQueueWaitMillis());

        assertTrue(service.tryEnqueue(GROUP, BUCKET, task(), 5, ActionListener.wrap(p -> {}, e -> {})));
        assertTrue(service.admitWithPermit(BUCKET, () -> {}));
        // The wait itself may legitimately be 0ms, so the COUNT is what proves a sample was taken.
        assertEquals(
            "a pushed admission counts exactly one queued request",
            1,
            stateAccessor.getWorkloadGroupState(GROUP).getTotalQueued()
        );
    }

    /** rejectOldest must NOT count the request as queued (it never waited for capacity). */
    public void testRejectOldestDoesNotCountQueued() throws Exception {
        assertTrue(service.tryEnqueue(GROUP, BUCKET, task(), 5, ActionListener.wrap(p -> {}, e -> {})));
        assertTrue(service.rejectOldest(BUCKET, throttle429()));
        assertEquals(
            "a request refused outright never waited, so it must not count as queued",
            0,
            stateAccessor.getWorkloadGroupState(GROUP).getTotalQueued()
        );
        assertEquals("and it must not record a queue wait either", 0, stateAccessor.getWorkloadGroupState(GROUP).getTotalQueueWaitMillis());
    }

    /** rejectOldest on an unknown group must be a safe no-op, not an NPE. */
    public void testRejectOldestUnknownGroupIsSafe() {
        assertFalse(service.rejectOldest("nosuchgroup:group", throttle429()));
        assertFalse(service.rejectOldest("weirdkeynocolon", throttle429()));
    }

    /** The failure instance handed to rejectOldest must be the one the client sees. */
    public void testRejectOldestPropagatesTheGivenFailure() throws Exception {
        OpenSearchRejectedExecutionException expected = throttle429();
        AtomicReference<Exception> seen = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        assertTrue(service.tryEnqueue(GROUP, BUCKET, task(), 5, ActionListener.wrap(p -> {}, e -> {
            seen.set(e);
            latch.countDown();
        })));
        assertTrue(service.rejectOldest(BUCKET, expected));
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertSame("the client must receive exactly the supplied 429", expected, seen.get());
    }
}
