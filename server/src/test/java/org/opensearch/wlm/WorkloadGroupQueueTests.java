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

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class WorkloadGroupQueueTests extends OpenSearchTestCase {

    private static WorkloadGroupTask task() {
        return new SearchTask(randomNonNegativeLong(), "", "", () -> "", null, null);
    }

    private static WorkloadGroupQueue.QueuedRequest req(String bucketKey) {
        return new WorkloadGroupQueue.QueuedRequest(ActionListener.wrap(r -> {}, e -> {}), bucketKey, task(), 0L);
    }

    private static WorkloadGroupQueue.QueuedRequest req(String bucketKey, WorkloadGroupTask task) {
        return new WorkloadGroupQueue.QueuedRequest(ActionListener.wrap(r -> {}, e -> {}), bucketKey, task, 0L);
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

    public void testEvictCancelledRemovesOnlyCancelledPreservingSurvivors() {
        WorkloadGroupQueue queue = new WorkloadGroupQueue();
        WorkloadGroupQueue.QueuedRequest survivorA = req("g:group"); // live
        WorkloadGroupTask cancelledTask = task();
        WorkloadGroupQueue.QueuedRequest cancelled = req("g:group", cancelledTask);
        WorkloadGroupQueue.QueuedRequest survivorB = req("g:group"); // live
        assertTrue(queue.offer(survivorA, 10));
        assertTrue(queue.offer(cancelled, 10));
        assertTrue(queue.offer(survivorB, 10));

        cancelledTask.cancel("client disconnect");
        List<WorkloadGroupQueue.QueuedRequest> evicted = queue.evictCancelled("g:group");
        assertEquals(1, evicted.size());
        assertSame(cancelled, evicted.get(0));
        assertEquals(2, queue.currentDepth()); // both live survivors remain
        // Survivors keep their place and identity; no time-based eviction ever removes a live request.
        assertSame(survivorA, queue.pollOldest("g:group"));
        assertSame(survivorB, queue.pollOldest("g:group"));
    }

    public void testEvictCancelledLeavesLiveRequestsRegardlessOfAge() {
        // There is no wall-clock deadline: a live parked request is never evicted by the sweep no matter how long it
        // has waited. Only a cancelled task is removed.
        WorkloadGroupQueue queue = new WorkloadGroupQueue();
        WorkloadGroupQueue.QueuedRequest live = req("g:group");
        assertTrue(queue.offer(live, 10));
        assertTrue(queue.evictCancelled("g:group").isEmpty());
        assertEquals(1, queue.currentDepth());
        assertSame(live, queue.pollOldest("g:group"));
    }

    public void testEvictCancelledRemovesCancelledTaskAndPrunes() {
        WorkloadGroupQueue queue = new WorkloadGroupQueue();
        WorkloadGroupTask t = task();
        WorkloadGroupQueue.QueuedRequest r = req("g:group", t);
        assertTrue(queue.offer(r, 10));
        t.cancel("client disconnect");
        List<WorkloadGroupQueue.QueuedRequest> evicted = queue.evictCancelled("g:group");
        assertEquals(1, evicted.size());
        assertSame(r, evicted.get(0));
        assertEquals(0, queue.currentDepth());
        assertFalse(queue.bucketKeys().contains("g:group")); // pruned
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

    public void testCapturesListenerAndBucket() {
        AtomicReference<Releasable> got = new AtomicReference<>();
        ActionListener<Releasable> listener = ActionListener.wrap(got::set, e -> {});
        WorkloadGroupQueue.QueuedRequest r = new WorkloadGroupQueue.QueuedRequest(listener, "g:username:alice", task(), 0L);
        assertEquals("g:username:alice", r.bucketKey());
        assertSame(listener, r.listener());
    }
}
