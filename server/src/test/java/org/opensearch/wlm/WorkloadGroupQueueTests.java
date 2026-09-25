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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class WorkloadGroupQueueTests extends OpenSearchTestCase {

    private static WorkloadGroupTask task() {
        return new SearchTask(randomNonNegativeLong(), "", "", () -> "", null, null);
    }

    private static WorkloadGroupQueue.QueuedRequest req(String bucketKey) {
        return new WorkloadGroupQueue.QueuedRequest(ActionListener.wrap(r -> {}, e -> {}), bucketKey, task(), 0L);
    }

    private static WorkloadGroupQueue.QueuedRequest pending(String bucketKey) {
        return WorkloadGroupQueue.QueuedRequest.pendingAcquire(ActionListener.wrap(r -> {}, e -> {}), bucketKey, task());
    }

    public void testOfferRejectedWhenDisabled() {
        WorkloadGroupQueue queue = new WorkloadGroupQueue();
        assertFalse(queue.offer(req("g:group"), 0));
        assertEquals(0, queue.currentDepth());
    }

    public void testOfferUpToBucketCapacityThenReject() {
        WorkloadGroupQueue queue = new WorkloadGroupQueue();
        assertTrue(queue.offer(req("g:group"), 2));
        assertTrue(queue.offer(req("g:group"), 2));
        assertFalse(queue.offer(req("g:group"), 2)); // this bucket is full
        assertEquals(2, queue.currentDepth());
        assertEquals(2L, queue.peakDepth());
    }

    public void testBucketCapacityReflectsCurrentSizePerCall() {
        // The cap is re-read per offer (dynamic queue.size_per_bucket), not frozen at construction.
        WorkloadGroupQueue queue = new WorkloadGroupQueue();
        assertTrue(queue.offer(req("g:group"), 5)); // bucket depth 1, cap 5
        assertTrue(queue.offer(req("g:group"), 5)); // bucket depth 2, cap 5
        // Cap lowered to 2 mid-flight: the bucket already holds 2, so the next offer is rejected immediately. Requests
        // already parked are not evicted.
        assertFalse(queue.offer(req("g:group"), 2));
        assertEquals(2, queue.currentDepth());
        // Cap raised to 3: a further offer now succeeds.
        assertTrue(queue.offer(req("g:group"), 3));
        assertEquals(3, queue.currentDepth());
    }

    public void testCapacityIsPerBucketNotSharedAcrossBuckets() {
        // Cross-principal fairness: each bucket gets its own size_per_bucket budget, so one principal filling its queue
        // cannot deny another principal capacity (the pre-rename behavior, where one shared budget was consumed
        // first-come-first-served across buckets).
        WorkloadGroupQueue queue = new WorkloadGroupQueue();
        assertTrue(queue.offer(req("g:username:alice"), 2));
        assertTrue(queue.offer(req("g:username:alice"), 2));
        assertFalse(queue.offer(req("g:username:alice"), 2)); // alice's own bucket is full
        // bob and carol are unaffected by alice saturating hers.
        assertTrue(queue.offer(req("g:username:bob"), 2));
        assertTrue(queue.offer(req("g:username:bob"), 2));
        assertTrue(queue.offer(req("g:username:carol"), 2));
        assertEquals(5, queue.currentDepth());
    }

    public void testPendingAcquireDoesNotConsumeBucketWaitingCapacity() {
        WorkloadGroupQueue queue = new WorkloadGroupQueue();
        WorkloadGroupQueue.QueuedRequest waiting = req("g:group");
        WorkloadGroupQueue.QueuedRequest pending1 = pending("g:group");
        WorkloadGroupQueue.QueuedRequest pending2 = pending("g:group");

        assertTrue(queue.offer(waiting, 1));
        assertTrue(queue.offerPendingAcquire(pending1));
        assertTrue(queue.offerPendingAcquire(pending2));
        assertEquals(3, queue.currentDepth());
        assertEquals(1, queue.currentWaitingDepth());

        // Only the denial transition consumes queue.size_per_bucket. The exact pending request is removed if full.
        assertEquals(WorkloadGroupQueue.WaitingTransition.BUCKET_FULL, queue.transitionToWaiting(pending1, 1, 10L));
        assertEquals(2, queue.currentDepth());
        assertEquals(1, queue.currentWaitingDepth());
        assertSame(waiting, queue.pollOldest("g:group"));

        assertEquals(WorkloadGroupQueue.WaitingTransition.WAITING, queue.transitionToWaiting(pending2, 1, 20L));
        assertEquals(1, queue.currentDepth());
        assertEquals(1, queue.currentWaitingDepth());
        assertSame(pending2, queue.pollOldest("g:group"));
    }

    public void testCombinedGroupCeilingCountsPendingAndWaiting() {
        WorkloadGroupQueue queue = new WorkloadGroupQueue();
        int waiting = WorkloadGroupQueueSettings.MAX_GROUP_QUEUE_DEPTH / 2;
        for (int i = 0; i < waiting; i++) {
            assertTrue(queue.offer(req("g:username:waiting-" + i), 1));
        }
        for (int i = waiting; i < WorkloadGroupQueueSettings.MAX_GROUP_QUEUE_DEPTH; i++) {
            assertTrue(queue.offerPendingAcquire(pending("g:username:pending-" + i)));
        }
        assertEquals(WorkloadGroupQueueSettings.MAX_GROUP_QUEUE_DEPTH, queue.currentDepth());
        assertEquals(waiting, queue.currentWaitingDepth());
        assertFalse(queue.offerPendingAcquire(pending("g:username:pending-overflow")));
        assertFalse(queue.offer(req("g:username:waiting-overflow"), 1));
        assertEquals(WorkloadGroupQueueSettings.MAX_GROUP_QUEUE_DEPTH, queue.currentDepth());
    }

    public void testConcurrentPendingOffersNeverExceedCombinedGroupCeiling() throws Exception {
        WorkloadGroupQueue queue = new WorkloadGroupQueue();
        int attempts = WorkloadGroupQueueSettings.MAX_GROUP_QUEUE_DEPTH * 2;
        List<WorkloadGroupQueue.QueuedRequest> requests = new ArrayList<>(attempts);
        for (int i = 0; i < attempts; i++) {
            requests.add(pending("g:username:u" + i));
        }

        int workers = 8;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(workers);
        AtomicInteger accepted = new AtomicInteger();
        AtomicReference<Throwable> workerFailure = new AtomicReference<>();
        for (int worker = 0; worker < workers; worker++) {
            final int first = worker;
            new Thread(() -> {
                try {
                    start.await();
                    for (int i = first; i < attempts; i += workers) {
                        if (queue.offerPendingAcquire(requests.get(i))) {
                            accepted.incrementAndGet();
                        }
                    }
                } catch (Throwable t) {
                    workerFailure.compareAndSet(null, t);
                } finally {
                    done.countDown();
                }
            }).start();
        }

        start.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS));
        assertNull(workerFailure.get());
        assertEquals(WorkloadGroupQueueSettings.MAX_GROUP_QUEUE_DEPTH, accepted.get());
        assertEquals(WorkloadGroupQueueSettings.MAX_GROUP_QUEUE_DEPTH, queue.currentDepth());
        assertEquals(0, queue.currentWaitingDepth());
    }

    public void testGroupCeilingRejectsOnceTotalDepthIsReached() {
        // The fixed per-group ceiling bounds the coordinator's parked footprint regardless of bucket cardinality:
        // username/role bucket keys come from the request principal, so a purely per-bucket cap would let unbounded
        // distinct principals each allocate size_per_bucket slots. One request per bucket isolates the ceiling from the
        // per-bucket cap.
        WorkloadGroupQueue queue = new WorkloadGroupQueue();
        for (int i = 0; i < WorkloadGroupQueueSettings.MAX_GROUP_QUEUE_DEPTH; i++) {
            assertTrue(queue.offer(req("g:username:u" + i), 1));
        }
        assertEquals(WorkloadGroupQueueSettings.MAX_GROUP_QUEUE_DEPTH, queue.currentDepth());
        // A brand-new bucket is under its own cap but the group total is at the ceiling -> rejected.
        assertFalse(queue.offer(req("g:username:overflow"), 1));
        assertEquals(WorkloadGroupQueueSettings.MAX_GROUP_QUEUE_DEPTH, queue.currentDepth());
    }

    public void testGroupCeilingRejectionLeavesNoEmptyBucket() {
        // offer() reads the per-bucket depth before reserving the group counter, so a group-ceiling rejection must not
        // leave an empty bucket set behind — a present bucket key means "has a live waiter", which the drain paths rely
        // on (hasWaiter is a bucketKeys() membership check).
        WorkloadGroupQueue queue = new WorkloadGroupQueue();
        for (int i = 0; i < WorkloadGroupQueueSettings.MAX_GROUP_QUEUE_DEPTH; i++) {
            assertTrue(queue.offer(req("g:username:u" + i), 1));
        }
        assertFalse(queue.offer(req("g:username:overflow"), 1));
        assertFalse(queue.bucketKeys().contains("g:username:overflow"));
    }

    public void testBucketRejectionLeavesNoEmptyBucketAndDoesNotConsumeGroupBudget() {
        // A per-bucket rejection happens before the group counter is touched, so it neither inflates depth/peak nor
        // creates a bucket entry for a request that was never parked.
        WorkloadGroupQueue queue = new WorkloadGroupQueue();
        assertTrue(queue.offer(req("g:username:alice"), 1));
        assertFalse(queue.offer(req("g:username:alice"), 1)); // alice's bucket full
        assertEquals(1, queue.currentDepth());
        assertEquals(1L, queue.peakDepth()); // the rejected offer never reserved a slot
        // A disabled queue (size_per_bucket == 0) likewise creates nothing.
        assertFalse(queue.offer(req("g:username:bob"), 0));
        assertFalse(queue.bucketKeys().contains("g:username:bob"));
        assertEquals(1, queue.currentDepth());
    }

    public void testPollOldestIsPerBucketFifo() {
        WorkloadGroupQueue queue = new WorkloadGroupQueue();
        WorkloadGroupQueue.QueuedRequest a1 = req("g:username:alice");
        WorkloadGroupQueue.QueuedRequest a2 = req("g:username:alice");
        assertTrue(queue.offer(a1, 10));
        assertTrue(queue.offer(a2, 10));
        assertSame(a1, queue.peekOldest("g:username:alice")); // peek does not remove
        assertSame(a1, queue.pollOldest("g:username:alice")); // oldest first
        assertSame(a2, queue.pollOldest("g:username:alice"));
        assertNull(queue.pollOldest("g:username:alice"));
        assertEquals(0, queue.currentDepth());
    }

    public void testNoHeadOfLineBlockAcrossBuckets() {
        WorkloadGroupQueue queue = new WorkloadGroupQueue();
        WorkloadGroupQueue.QueuedRequest alice = req("g:username:alice");
        WorkloadGroupQueue.QueuedRequest bob = req("g:username:bob");
        assertTrue(queue.offer(alice, 10));
        assertTrue(queue.offer(bob, 10));
        // A drain for bob's bucket returns bob even though alice was enqueued first (different bucket, no HoL blocking).
        assertSame(bob, queue.pollOldest("g:username:bob"));
        assertEquals(1, queue.currentDepth());
        assertSame(alice, queue.pollOldest("g:username:alice"));
    }

    public void testRemoveDecrementsDepthAndPrunes() {
        WorkloadGroupQueue queue = new WorkloadGroupQueue();
        WorkloadGroupQueue.QueuedRequest r = req("g:group");
        assertTrue(queue.offer(r, 10));
        assertTrue(queue.bucketKeys().contains("g:group"));
        assertTrue(queue.remove(r));
        assertEquals(0, queue.currentDepth());
        assertFalse(queue.bucketKeys().contains("g:group")); // empty bucket pruned
        assertFalse(queue.remove(r)); // idempotent: already gone
    }

    public void testPeakTracksHighWaterMark() {
        WorkloadGroupQueue queue = new WorkloadGroupQueue();
        assertTrue(queue.offer(req("g:group"), 10));
        assertTrue(queue.offer(req("g:group"), 10));
        assertEquals(2L, queue.peakDepth());
        queue.pollOldest("g:group");
        assertEquals(1, queue.currentDepth());
        assertEquals(2L, queue.peakDepth()); // peak does not decrease
    }

    public void testWaitNanos() {
        long enqueue = 5_000_000L;
        WorkloadGroupQueue.QueuedRequest r = new WorkloadGroupQueue.QueuedRequest(
            ActionListener.wrap(x -> {}, e -> {}),
            "g:group",
            task(),
            enqueue
        );
        assertEquals(2000L, r.waitNanos(enqueue + 2000));
        assertEquals(0L, r.waitNanos(enqueue)); // admitted instantly
        assertEquals(0L, r.waitNanos(enqueue - 100)); // clock skew guard: never negative
    }

    public void testPendingWaitStartsOnlyOnDenialTransition() {
        WorkloadGroupQueue queue = new WorkloadGroupQueue();
        WorkloadGroupQueue.QueuedRequest pending = pending("g:group");
        assertTrue(queue.offerPendingAcquire(pending));
        assertEquals(0L, pending.waitNanos(10_000L));
        assertEquals(0, queue.currentWaitingDepth());
        assertEquals(0L, queue.peakDepth());

        assertEquals(WorkloadGroupQueue.WaitingTransition.WAITING, queue.transitionToWaiting(pending, 1, 5_000L));
        assertEquals(2_000L, pending.waitNanos(7_000L));
        assertEquals(1, queue.currentWaitingDepth());
        assertEquals(1L, queue.peakDepth());
    }

    public void testCapturesListenerAndBucket() {
        AtomicReference<Releasable> got = new AtomicReference<>();
        ActionListener<Releasable> listener = ActionListener.wrap(got::set, e -> {});
        WorkloadGroupQueue.QueuedRequest r = new WorkloadGroupQueue.QueuedRequest(listener, "g:username:alice", task(), 0L);
        assertEquals("g:username:alice", r.bucketKey());
        assertSame(listener, r.listener());
    }
}
