/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.action.search.SearchTask;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.stats.WorkloadGroupState;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.mockito.Mockito;

import static org.mockito.Mockito.when;

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

    private WorkloadGroupQueueService directExecutorService() {
        ThreadPool pool = Mockito.mock(ThreadPool.class);
        when(pool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        // Direct GENERIC executor: any (erroneous) eviction or admission completes inline, so negative assertions are deterministic.
        when(pool.executor(ThreadPool.Names.GENERIC)).thenReturn(OpenSearchExecutors.newDirectExecutorService());
        return new WorkloadGroupQueueService(pool, stateAccessor);
    }

    private static OpenSearchRejectedExecutionException throttle429() {
        return new OpenSearchRejectedExecutionException("Request throttled: test.");
    }

    /**
     * A granted result raced by a search-pool rejection result for the same token must complete exactly once. The
     * rejection path claims the exact entry and, only if it won, completes the listener with its 429 (as
     * WorkloadGroupService does).
     */
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
                boolean claimed = svc.claimPendingAcquire(pending);
                if (claimed) {
                    pending.listener().onFailure(throttle429());
                }
                rejectResult.set(claimed);
            });
            admitter.start();
            rejecter.start();
            admitter.join();
            rejecter.join();

            assertTrue("listener must be completed", done.await(10, TimeUnit.SECONDS));
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
            AtomicReference<WorkloadGroupQueue.WaitingTransition> denialResult = new AtomicReference<>();
            Thread grant = new Thread(() -> {
                try {
                    barrier.await();
                } catch (Exception ignored) {}
                grantClaimed.set(svc.admitWithPermit(BUCKET, () -> {}).isPresent());
            });
            Thread denial = new Thread(() -> {
                try {
                    barrier.await();
                } catch (Exception ignored) {}
                denialResult.set(svc.transitionPendingAcquireToWaiting(pending, 1));
            });
            grant.start();
            denial.start();
            grant.join();
            denial.join();

            assertTrue(completed.await(10, TimeUnit.SECONDS));
            assertTrue(grantClaimed.get());
            assertTrue(
                denialResult.get() == WorkloadGroupQueue.WaitingTransition.WAITING
                    || denialResult.get() == WorkloadGroupQueue.WaitingTransition.REQUEST_GONE
            );
            assertEquals(1, admits.get());
            assertEquals(0, failures.get());
            assertEquals(0, svc.retainedDepth(GROUP));
        }
    }

    /**
     * A full WAITING bucket hands the originating pending token back to the caller (which rejects it), never the
     * existing waiter.
     */
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

        assertEquals(WorkloadGroupQueue.WaitingTransition.BUCKET_FULL, service.transitionPendingAcquireToWaiting(pending, 1));
        assertEquals("the queue leaves completing the exact request to the caller", 0, pendingFailed.get());
        assertEquals(0, pendingAdmitted.get());
        assertEquals(0, existingFailed.get());
        assertEquals(1, service.waitingDepth(GROUP));
        assertEquals(1, service.retainedDepth(GROUP));

        assertTrue(service.admitWithPermit(BUCKET, () -> {}).isPresent());
        assertBusy(() -> assertEquals(1, existingAdmitted.get()));
    }

    /** Cancellation removes a pending token, and every delayed acquire outcome becomes a harmless no-op. */
    public void testCancellationBeforeAcquireResultCompletesExactlyOnce() throws Exception {
        WorkloadGroupQueueService svc = directExecutorService();
        SearchTask t = task();
        AtomicInteger completions = new AtomicInteger();
        CountDownLatch completed = new CountDownLatch(1);
        WorkloadGroupQueue.QueuedRequest pending = svc.tryRegisterPendingAcquire(GROUP, BUCKET, t, ActionListener.wrap(p -> {
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
        assertEquals(0, svc.retainedDepth(GROUP));

        assertFalse(svc.admitPendingAcquire(pending, () -> {}));
        assertFalse(svc.claimPendingAcquire(pending));
        assertEquals(WorkloadGroupQueue.WaitingTransition.REQUEST_GONE, svc.transitionPendingAcquireToWaiting(pending, 1));
        assertEquals(1, completions.get());
    }

    /** A claimed entry no longer belongs to the queue: a later cancellation must not complete it a second time. */
    public void testClaimedPendingIsNotFailedAgainByCancellation() throws Exception {
        WorkloadGroupQueueService svc = directExecutorService();
        SearchTask t = task();
        AtomicInteger failures = new AtomicInteger();
        WorkloadGroupQueue.QueuedRequest pending = svc.tryRegisterPendingAcquire(
            GROUP,
            BUCKET,
            t,
            ActionListener.wrap(p -> {}, e -> failures.incrementAndGet())
        );
        assertNotNull(pending);
        assertTrue(svc.claimPendingAcquire(pending));
        t.cancel("client gone after the claim");
        assertEquals("a claimed entry is not failed by a later cancellation", 0, failures.get());
        assertEquals(0, svc.retainedDepth(GROUP));
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
        assertEquals(0, service.waitingDepth(GROUP));
        assertEquals(0, service.peakWaitingDepth(GROUP));
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

    /**
     * Once denial transitions the entry to WAITING it is counted in total_queued at once, and the pushed grant that later
     * admits it does not count it again.
     */
    public void testDenialTransitionCountsQueuedOnceThroughPushedAdmission() {
        WorkloadGroupQueue.QueuedRequest pending = service.tryRegisterPendingAcquire(
            GROUP,
            BUCKET,
            task(),
            ActionListener.wrap(p -> {}, e -> {})
        );
        assertNotNull(pending);
        assertEquals(WorkloadGroupQueue.WaitingTransition.WAITING, service.transitionPendingAcquireToWaiting(pending, 1));
        assertEquals(1, service.waitingDepth(GROUP));
        assertEquals("the request is counted when it enters WAITING", 1, stateAccessor.getWorkloadGroupState(GROUP).getTotalQueued());
        assertTrue(service.admitWithPermit(BUCKET, () -> {}).isPresent());
        assertEquals("the pushed admission does not count it again", 1, stateAccessor.getWorkloadGroupState(GROUP).getTotalQueued());
        assertEquals(0, service.waitingDepth(GROUP));
    }

    /** A provisional request claimed back (on a search-pool rejection) is neither counted as queued nor records a wait. */
    public void testClaimPendingDoesNotCountQueued() {
        WorkloadGroupQueue.QueuedRequest pending = service.tryRegisterPendingAcquire(
            GROUP,
            BUCKET,
            task(),
            ActionListener.wrap(p -> {}, e -> {})
        );
        assertNotNull(pending);
        assertTrue(service.claimPendingAcquire(pending));
        assertEquals(
            "a request refused outright never waited, so it must not count as queued",
            0,
            stateAccessor.getWorkloadGroupState(GROUP).getTotalQueued()
        );
        assertEquals("and it must not record a queue wait either", 0, stateAccessor.getWorkloadGroupState(GROUP).getTotalQueueWaitMillis());
    }

    // A mocked pool whose GENERIC executor runs inline and whose precise clock advances 1ms on every read. An ended wait
    // reads it exactly twice, when the request enters WAITING and when it leaves, and nothing else reads it in between, so
    // a wait recorded once sums to exactly 1ms; recording it twice would add at least another 1ms.
    private static ThreadPool advancingClockPool() {
        ThreadPool pool = Mockito.mock(ThreadPool.class);
        when(pool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(pool.executor(ThreadPool.Names.GENERIC)).thenReturn(OpenSearchExecutors.newDirectExecutorService());
        AtomicLong clock = new AtomicLong();
        when(pool.preciseRelativeTimeInNanos()).thenAnswer(invocation -> clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(1)));
        return pool;
    }

    /**
     * A cancellation racing a pushed grant for the same WAITING request ends that wait exactly once: whichever removes the
     * entry records its wait, and the request, counted in total_queued when it parked, is not counted again.
     */
    public void testCancellationRacingAdmissionRecordsTheWaitExactlyOnce() throws Exception {
        ThreadPool pool = advancingClockPool();
        for (int iter = 0; iter < 200; iter++) {
            WorkloadGroupsStateAccessor accessor = new WorkloadGroupsStateAccessor();
            accessor.addNewWorkloadGroup(GROUP);
            WorkloadGroupQueueService svc = new WorkloadGroupQueueService(pool, accessor);
            SearchTask t = task();
            AtomicInteger completions = new AtomicInteger();
            assertTrue(
                svc.tryEnqueue(
                    GROUP,
                    BUCKET,
                    t,
                    1,
                    ActionListener.wrap(p -> completions.incrementAndGet(), e -> completions.incrementAndGet())
                )
            );

            CyclicBarrier barrier = new CyclicBarrier(2);
            Thread admitter = new Thread(() -> {
                try {
                    barrier.await();
                } catch (Exception ignored) {}
                svc.admitWithPermit(BUCKET, () -> {});
            });
            Thread canceller = new Thread(() -> {
                try {
                    barrier.await();
                } catch (Exception ignored) {}
                t.cancel("client gone");
            });
            // The thread that reaches the barrier last usually runs first, so alternate the start order to exercise both.
            Thread startedFirst = iter % 2 == 0 ? admitter : canceller;
            startedFirst.start();
            (startedFirst == admitter ? canceller : admitter).start();
            admitter.join();
            canceller.join();

            WorkloadGroupState state = accessor.getWorkloadGroupState(GROUP);
            assertEquals("iter " + iter + ": exactly one completion", 1, completions.get());
            assertEquals("iter " + iter + ": the queue is empty", 0, svc.retainedDepth(GROUP));
            assertEquals("iter " + iter + ": counted once, when it parked", 1, state.getTotalQueued());
            assertEquals("iter " + iter + ": its wait is recorded exactly once", 1, state.getTotalQueueWaitMillis());
        }
    }

    /**
     * A cancellation racing the denial that moves a PENDING_ACQUIRE request to WAITING keeps entries and exits paired:
     * either the cancellation wins and the request is neither counted nor records a wait, or it enters WAITING, is counted,
     * and its eviction then records its wait once. Never a recorded wait without its entry, nor an entry whose exit
     * records no wait.
     */
    public void testCancellationRacingTheWaitingTransitionKeepsEntriesAndExitsPaired() throws Exception {
        ThreadPool pool = advancingClockPool();
        for (int iter = 0; iter < 200; iter++) {
            WorkloadGroupsStateAccessor accessor = new WorkloadGroupsStateAccessor();
            accessor.addNewWorkloadGroup(GROUP);
            WorkloadGroupQueueService svc = new WorkloadGroupQueueService(pool, accessor);
            SearchTask t = task();
            AtomicInteger completions = new AtomicInteger();
            WorkloadGroupQueue.QueuedRequest pending = svc.tryRegisterPendingAcquire(
                GROUP,
                BUCKET,
                t,
                ActionListener.wrap(p -> completions.incrementAndGet(), e -> completions.incrementAndGet())
            );
            assertNotNull(pending);

            CyclicBarrier barrier = new CyclicBarrier(2);
            AtomicReference<WorkloadGroupQueue.WaitingTransition> transition = new AtomicReference<>();
            Thread denial = new Thread(() -> {
                try {
                    barrier.await();
                } catch (Exception ignored) {}
                transition.set(svc.transitionPendingAcquireToWaiting(pending, 1));
            });
            Thread canceller = new Thread(() -> {
                try {
                    barrier.await();
                } catch (Exception ignored) {}
                t.cancel("client gone");
            });
            // The thread that reaches the barrier last usually runs first, so alternate the start order to exercise both.
            Thread startedFirst = iter % 2 == 0 ? denial : canceller;
            startedFirst.start();
            (startedFirst == denial ? canceller : denial).start();
            denial.join();
            canceller.join();

            WorkloadGroupState state = accessor.getWorkloadGroupState(GROUP);
            assertEquals("iter " + iter + ": exactly one completion", 1, completions.get());
            assertEquals("iter " + iter + ": the queue is empty", 0, svc.retainedDepth(GROUP));
            if (transition.get() == WorkloadGroupQueue.WaitingTransition.WAITING) {
                assertEquals("iter " + iter + ": it entered WAITING", 1, state.getTotalQueued());
                assertEquals("iter " + iter + ": so its eviction records its wait exactly once", 1, state.getTotalQueueWaitMillis());
            } else {
                assertEquals("iter " + iter, WorkloadGroupQueue.WaitingTransition.REQUEST_GONE, transition.get());
                assertEquals("iter " + iter + ": evicted before it waited, so never counted", 0, state.getTotalQueued());
                assertEquals("iter " + iter + ": and no wait is recorded", 0, state.getTotalQueueWaitMillis());
            }
        }
    }
}
