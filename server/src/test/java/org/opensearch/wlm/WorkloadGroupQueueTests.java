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
        return new WorkloadGroupQueue.QueuedRequest(ActionListener.wrap(r -> {}, e -> {}), bucketKey, task(), null, 0L);
    }

    private static WorkloadGroupQueue.QueuedRequest req(String bucketKey, long deadlineNanos) {
        return new WorkloadGroupQueue.QueuedRequest(ActionListener.wrap(r -> {}, e -> {}), bucketKey, task(), null, deadlineNanos);
    }

    public void testOfferRejectedWhenDisabled() {
        WorkloadGroupQueue queue = new WorkloadGroupQueue();
        assertFalse(queue.offer(req("g:group"), 0));
        assertEquals(0, queue.currentDepth());
    }

    public void testOfferUpToCapacityThenReject() {
        WorkloadGroupQueue queue = new WorkloadGroupQueue();
        assertTrue(queue.offer(req("g:group"), 2));
        assertTrue(queue.offer(req("g:group"), 2));
        assertFalse(queue.offer(req("g:group"), 2)); // full
        assertEquals(2, queue.currentDepth());
        assertEquals(2L, queue.peakDepth());
    }

    public void testCapacityReflectsCurrentSizePerCall() {
        // The cap is re-read per offer (dynamic queue.size), not frozen at construction.
        WorkloadGroupQueue queue = new WorkloadGroupQueue();
        assertTrue(queue.offer(req("g:group"), 5)); // depth 1, cap 5
        assertTrue(queue.offer(req("g:group"), 5)); // depth 2, cap 5
        // Cap lowered to 2 mid-flight: depth is already 2, so the next offer is rejected immediately.
        assertFalse(queue.offer(req("g:group"), 2));
        assertEquals(2, queue.currentDepth());
        // Cap raised to 3: a further offer now succeeds.
        assertTrue(queue.offer(req("g:group"), 3));
        assertEquals(3, queue.currentDepth());
    }

    public void testCapacityIsSharedAcrossBuckets() {
        WorkloadGroupQueue queue = new WorkloadGroupQueue();
        assertTrue(queue.offer(req("g:username:alice"), 2));
        assertTrue(queue.offer(req("g:username:bob"), 2));
        assertFalse(queue.offer(req("g:username:carol"), 2)); // shared budget exhausted across buckets
        assertEquals(2, queue.currentDepth());
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

    public void testEvictExpiredAndCancelledRemovesOnlyDueEntriesPreservingSurvivors() {
        long now = 1_000_000L;
        WorkloadGroupQueue queue = new WorkloadGroupQueue();
        WorkloadGroupQueue.QueuedRequest survivor = req("g:group", now + 10_000); // deadline in the future
        WorkloadGroupQueue.QueuedRequest expired = req("g:group", now); // deadline reached
        WorkloadGroupQueue.QueuedRequest noDeadline = req("g:group", 0L); // never expires
        assertTrue(queue.offer(survivor, 10));
        assertTrue(queue.offer(expired, 10));
        assertTrue(queue.offer(noDeadline, 10));

        List<WorkloadGroupQueue.QueuedRequest> evicted = queue.evictExpiredAndCancelled("g:group", now);
        assertEquals(1, evicted.size());
        assertSame(expired, evicted.get(0));
        assertEquals(2, queue.currentDepth()); // survivor + noDeadline remain
        // Survivors keep their place and identity (deadline was never reset).
        assertSame(survivor, queue.pollOldest("g:group"));
        assertSame(noDeadline, queue.pollOldest("g:group"));
    }

    public void testEvictExpiredAndCancelledRemovesCancelledTask() {
        long now = 1_000_000L;
        WorkloadGroupQueue queue = new WorkloadGroupQueue();
        WorkloadGroupQueue.QueuedRequest r = req("g:group", now + 10_000); // not time-expired
        assertTrue(queue.offer(r, 10));
        r.task().cancel("client disconnect");
        List<WorkloadGroupQueue.QueuedRequest> evicted = queue.evictExpiredAndCancelled("g:group", now);
        assertEquals(1, evicted.size());
        assertSame(r, evicted.get(0));
        assertEquals(0, queue.currentDepth());
        assertFalse(queue.bucketKeys().contains("g:group")); // pruned
    }

    public void testDeadlineExpiry() {
        long now = 1_000_000L;
        assertFalse(req("g:group", 0L).isExpired(now)); // 0 deadline = never expires
        assertFalse(req("g:group", now + 1000).isExpired(now));
        assertTrue(req("g:group", now).isExpired(now));
    }

    public void testCapturesListenerAndBucket() {
        AtomicReference<Releasable> got = new AtomicReference<>();
        ActionListener<Releasable> listener = ActionListener.wrap(got::set, e -> {});
        WorkloadGroupQueue.QueuedRequest r = new WorkloadGroupQueue.QueuedRequest(
            listener,
            "g:username:alice",
            task(),
            "username|alice",
            0L
        );
        assertEquals("g:username:alice", r.bucketKey());
        assertEquals("username|alice", r.principal());
        assertSame(listener, r.listener());
    }
}
