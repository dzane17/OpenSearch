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

import java.util.concurrent.atomic.AtomicReference;

public class WorkloadGroupQueueTests extends OpenSearchTestCase {

    private static WorkloadGroupTask task() {
        return new SearchTask(randomNonNegativeLong(), "", "", () -> "", null, null);
    }

    private static WorkloadGroupQueue.QueuedRequest req(String bucketKey) {
        return new WorkloadGroupQueue.QueuedRequest(ActionListener.wrap(r -> {}, e -> {}), bucketKey, task(), null, 0L);
    }

    public void testOfferRejectedWhenDisabled() {
        WorkloadGroupQueue queue = new WorkloadGroupQueue(0);
        assertFalse(queue.offer(req("g:group")));
        assertEquals(0, queue.currentDepth());
    }

    public void testOfferUpToCapacityThenReject() {
        WorkloadGroupQueue queue = new WorkloadGroupQueue(2);
        assertTrue(queue.offer(req("g:group")));
        assertTrue(queue.offer(req("g:group")));
        assertFalse(queue.offer(req("g:group"))); // full
        assertEquals(2, queue.currentDepth());
        assertEquals(2L, queue.peakDepth());
    }

    public void testCapacityIsSharedAcrossBuckets() {
        WorkloadGroupQueue queue = new WorkloadGroupQueue(2);
        assertTrue(queue.offer(req("g:username:alice")));
        assertTrue(queue.offer(req("g:username:bob")));
        assertFalse(queue.offer(req("g:username:carol"))); // shared budget exhausted across buckets
        assertEquals(2, queue.currentDepth());
    }

    public void testPollOldestIsPerBucketFifo() {
        WorkloadGroupQueue queue = new WorkloadGroupQueue(10);
        WorkloadGroupQueue.QueuedRequest a1 = req("g:username:alice");
        WorkloadGroupQueue.QueuedRequest a2 = req("g:username:alice");
        assertTrue(queue.offer(a1));
        assertTrue(queue.offer(a2));
        assertSame(a1, queue.pollOldest("g:username:alice")); // oldest first
        assertSame(a2, queue.pollOldest("g:username:alice"));
        assertNull(queue.pollOldest("g:username:alice"));
        assertEquals(0, queue.currentDepth());
    }

    public void testNoHeadOfLineBlockAcrossBuckets() {
        WorkloadGroupQueue queue = new WorkloadGroupQueue(10);
        WorkloadGroupQueue.QueuedRequest alice = req("g:username:alice");
        WorkloadGroupQueue.QueuedRequest bob = req("g:username:bob");
        assertTrue(queue.offer(alice));
        assertTrue(queue.offer(bob));
        // A drain for bob's bucket returns bob even though alice was enqueued first (different bucket, no HoL blocking).
        assertSame(bob, queue.pollOldest("g:username:bob"));
        assertEquals(1, queue.currentDepth());
        assertSame(alice, queue.pollOldest("g:username:alice"));
    }

    public void testRemoveDecrementsDepthAndPrunes() {
        WorkloadGroupQueue queue = new WorkloadGroupQueue(10);
        WorkloadGroupQueue.QueuedRequest r = req("g:group");
        assertTrue(queue.offer(r));
        assertTrue(queue.bucketKeys().contains("g:group"));
        assertTrue(queue.remove(r));
        assertEquals(0, queue.currentDepth());
        assertFalse(queue.bucketKeys().contains("g:group")); // empty bucket pruned
        assertFalse(queue.remove(r)); // idempotent: already gone
    }

    public void testPeakTracksHighWaterMark() {
        WorkloadGroupQueue queue = new WorkloadGroupQueue(10);
        WorkloadGroupQueue.QueuedRequest a = req("g:group");
        WorkloadGroupQueue.QueuedRequest b = req("g:group");
        assertTrue(queue.offer(a));
        assertTrue(queue.offer(b));
        assertEquals(2L, queue.peakDepth());
        queue.pollOldest("g:group");
        assertEquals(1, queue.currentDepth());
        assertEquals(2L, queue.peakDepth()); // peak does not decrease
    }

    public void testDeadlineExpiry() {
        long now = 1_000_000L;
        WorkloadGroupQueue.QueuedRequest noDeadline = new WorkloadGroupQueue.QueuedRequest(
            ActionListener.wrap(r -> {}, e -> {}),
            "g:group",
            task(),
            null,
            0L
        );
        WorkloadGroupQueue.QueuedRequest future = new WorkloadGroupQueue.QueuedRequest(
            ActionListener.wrap(r -> {}, e -> {}),
            "g:group",
            task(),
            null,
            now + 1000
        );
        WorkloadGroupQueue.QueuedRequest past = new WorkloadGroupQueue.QueuedRequest(
            ActionListener.wrap(r -> {}, e -> {}),
            "g:group",
            task(),
            null,
            now
        );
        assertFalse(noDeadline.isExpired(now)); // 0 deadline = never expires
        assertFalse(future.isExpired(now));
        assertTrue(past.isExpired(now));
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
