/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.mockito.Mockito;

import static org.mockito.Mockito.when;

public class WorkloadGroupSharedThrottleServiceTests extends OpenSearchTestCase {

    private ClusterService clusterService;
    private ThreadPool threadPool;
    private TransportService transportService;
    private DiscoveryNode localNode;
    private Metadata metadata;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        localNode = new DiscoveryNode(
            "local",
            "local",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            org.opensearch.Version.CURRENT
        );
        clusterService = Mockito.mock(ClusterService.class);
        threadPool = Mockito.mock(ThreadPool.class);
        transportService = Mockito.mock(TransportService.class);

        ClusterState state = Mockito.mock(ClusterState.class);
        DiscoveryNodes singleDataNode = DiscoveryNodes.builder().add(localNode).localNodeId("local").build();
        when(state.nodes()).thenReturn(singleDataNode);
        // The TTL sweep resolves a bucket's live shared_limit from cluster-state metadata, so every test needs a
        // metadata stub. Default to "no workload groups"; tests that need a real limit re-stub workloadGroups().
        metadata = Mockito.mock(Metadata.class);
        when(metadata.workloadGroups()).thenReturn(Map.of());
        when(state.metadata()).thenReturn(metadata);
        when(clusterService.state()).thenReturn(state);
        when(clusterService.localNode()).thenReturn(localNode);
    }

    private WorkloadGroupSharedThrottleService newService() {
        // Single data node => this node owns every bucket => acquire uses the local short-circuit (no real network).
        WorkloadGroupSharedThrottleService service = new WorkloadGroupSharedThrottleService(clusterService, threadPool, transportService);
        // The ring is empty until a cluster-state change populates it (matches production: the constructor does not
        // read cluster state). Deliver a nodesChanged event with the current nodes.
        deliverNodesChanged(service, clusterService.state().nodes());
        return service;
    }

    private void deliverNodesChanged(WorkloadGroupSharedThrottleService service, DiscoveryNodes nodes) {
        ClusterState previous = Mockito.mock(ClusterState.class);
        when(previous.nodes()).thenReturn(DiscoveryNodes.EMPTY_NODES);
        ClusterState current = Mockito.mock(ClusterState.class);
        when(current.nodes()).thenReturn(nodes);
        ClusterChangedEvent event = new ClusterChangedEvent("test", current, previous);
        service.clusterChanged(event);
    }

    private static Releasable awaitGrant(WorkloadGroupSharedThrottleService service, String bucket, int limit) {
        AtomicReference<Releasable> permit = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        service.acquireAsync(bucket, limit, ActionListener.wrap(permit::set, failure::set));
        if (failure.get() != null) {
            throw failure.get() instanceof RuntimeException re ? re : new RuntimeException(failure.get());
        }
        return permit.get();
    }

    public void testTtlSweepDrivesOwnerPushForALeaseExpiredSlot() {
        // Regression: a permit reclaimed by the TTL sweep frees a shared slot with NO release RPC behind it (the holder
        // crashed, or its release was lost), so the sweep is the ONLY observer of that free slot. It must drive
        // owner-push. Previously sweepExpired() returned void and the sweep ignored the freed capacity, so a coordinator
        // with a parked request stayed registered as a waiter while the slot sat idle — and since parked requests have no
        // deadline, it stranded until some unrelated release happened to re-drive the bucket.
        final int sharedLimit = 1;
        final String groupId = "g1";
        final String bucket = groupId + ":group";

        // Controllable clock so the permit can be expired deterministically instead of sleeping past the 5-minute TTL.
        final AtomicLong nanos = new AtomicLong(0L);
        WorkloadGroupSharedThrottleService service = new WorkloadGroupSharedThrottleService(
            clusterService,
            threadPool,
            transportService,
            new SharedThrottleTracker(nanos::get)
        );
        deliverNodesChanged(service, clusterService.state().nodes());

        // The sweep resolves shared_limit from cluster state (an expiry carries no limit, unlike a release).
        Settings throttling = Settings.builder().put("attribute", "group").put("shared_limit", sharedLimit).build();
        WorkloadGroup group = new WorkloadGroup(
            "g1-name",
            groupId,
            new MutableWorkloadGroupFragment(
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.EMPTY,
                throttling
            ),
            1L
        );
        when(metadata.workloadGroups()).thenReturn(Map.of(groupId, group));

        final AtomicInteger admits = new AtomicInteger(0);
        service.setGrantConsumer((bucketKey, reservedPermit) -> {
            admits.incrementAndGet();
            return true; // stands in for the queue service admitting one parked request
        });

        // Take the only shared slot, then deny an acquire with wantsQueue=true so this coordinator is a registered waiter
        // with a parked request. Both happen at t=0, while the holder's permit is still live.
        assertNotNull("first acquire takes the only shared slot", awaitGrant(service, bucket, sharedLimit));
        AtomicReference<Exception> denial = new AtomicReference<>();
        service.acquireAsync(bucket, sharedLimit, true, ActionListener.wrap(p -> fail("must be denied while at limit"), denial::set));
        assertTrue("acquire at limit must be denied", denial.get() instanceof OpenSearchRejectedExecutionException);
        assertEquals("coordinator is registered as a waiter", 1, service.waiterCountForTest(bucket));

        // Simulate the holder vanishing: advance past the permit TTL WITHOUT any release RPC. Nothing has pruned the
        // permit yet (no acquire has touched the bucket), so the slot is expired-but-unreclaimed.
        nanos.set(WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS + 1);
        assertEquals("nothing can have been admitted before the sweep runs", 0, admits.get());

        // One sweep pass must reclaim the expired permit AND hand the freed slot to the waiting coordinator.
        service.sweepExpiredAndDrive();
        assertEquals("the sweep must drive owner-push for the slot it freed", 1, admits.get());
    }

    public void testRingPopulatesWhenNodeSetUnchangedVsPreviousState() {
        // Regression for the single-node no-op: the coordinator seeds the initial applied state already containing the
        // local node, so the first real clusterChanged has previous.nodes() == current.nodes() and nodesChanged() is
        // false. The ring must still populate (we compare eligible owner sets, not the delta), otherwise shared
        // throttling silently does nothing on a single-node cluster.
        WorkloadGroupSharedThrottleService service = new WorkloadGroupSharedThrottleService(clusterService, threadPool, transportService);
        DiscoveryNodes nodes = DiscoveryNodes.builder().add(localNode).localNodeId("local").build();
        ClusterState previous = Mockito.mock(ClusterState.class);
        when(previous.nodes()).thenReturn(nodes); // same node set as current -> nodesChanged() == false
        ClusterState current = Mockito.mock(ClusterState.class);
        when(current.nodes()).thenReturn(nodes);
        ClusterChangedEvent event = new ClusterChangedEvent("test", current, previous);
        assertFalse("precondition: this event reports no node change", event.nodesChanged());

        service.clusterChanged(event);

        // shared_limit=1 must now actually enforce (grant then reject), not fail open.
        assertNotNull(awaitGrant(service, "b", 1));
        expectThrows(OpenSearchRejectedExecutionException.class, () -> awaitGrant(service, "b", 1));
    }

    public void testSameIdRestartRebuildsRingWithFreshNode() {
        // A node can restart keeping its persistent id but with a new ephemeral id/address. Comparing only persistent
        // ids would treat this as "no change" and keep the stale DiscoveryNode in the ring, so nodeConnected(stale)
        // would fail open forever. The ring must rebuild and hold the FRESH node object.
        WorkloadGroupSharedThrottleService service = new WorkloadGroupSharedThrottleService(clusterService, threadPool, transportService);

        DiscoveryNode first = new DiscoveryNode(
            "n1",
            "n1",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            org.opensearch.Version.CURRENT
        );
        deliverNodesChanged(service, DiscoveryNodes.builder().add(first).localNodeId("n1").build());
        assertSame("ring should hold the first node instance", first, service.ring().ownerFor("b").orElseThrow());

        // Same persistent id "n1", but a brand-new DiscoveryNode instance (fresh ephemeral id + transport address).
        DiscoveryNode restarted = new DiscoveryNode(
            "n1",
            "n1",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            org.opensearch.Version.CURRENT
        );
        assertNotEquals("restarted node must not equal the old instance", first, restarted);
        deliverNodesChanged(service, DiscoveryNodes.builder().add(restarted).localNodeId("n1").build());

        assertSame("ring must have rebuilt with the restarted node instance", restarted, service.ring().ownerFor("b").orElseThrow());
    }

    public void testLocalOwnerGrantsThenDeniesAtLimit() {
        WorkloadGroupSharedThrottleService service = newService();
        Releasable p1 = awaitGrant(service, "b", 1);
        assertNotNull(p1);
        expectThrows(OpenSearchRejectedExecutionException.class, () -> awaitGrant(service, "b", 1));
        // release frees the shared slot
        p1.close();
        assertNotNull(awaitGrant(service, "b", 1));
    }

    public void testOwnerPushDrainsEveryParkedRequestOnOneCoordinator() {
        // Regression: one coordinator (here the local owner) parks SEVERAL requests for a shared bucket, but the owner
        // waiter registry holds one Set membership per coordinator. A grant must NOT deregister the coordinator on a
        // successful admit — it may still have more queued requests — so each successive freed slot drains the next
        // parked request. The bug drained only the first and stranded the rest until queue.timeout despite free capacity.
        final int sharedLimit = 1;
        final String bucket = "b";
        WorkloadGroupSharedThrottleService service = newService();

        // A stubbed coordinator-side consumer standing in for the queue service: it holds `parked` requests and admits
        // one per grant, capturing the reserved permit so the test can "complete" that request by closing it (which
        // re-drives owner-push, exactly like a real request finishing).
        final AtomicInteger parked = new AtomicInteger(3);
        final AtomicInteger admits = new AtomicInteger(0);
        final List<Releasable> heldPermits = new ArrayList<>();
        service.setGrantConsumer((bucketKey, reservedPermit) -> {
            if (parked.get() <= 0) {
                return false; // nothing left to admit -> caller returns the unused grant and deregisters this waiter
            }
            parked.decrementAndGet();
            admits.incrementAndGet();
            heldPermits.add(reservedPermit);
            return true;
        });

        // Fill the single shared slot, then issue 3 denied acquires with wantsQueue=true. Each denial registers this
        // (local) coordinator as a waiter — idempotently, so the Set holds exactly ONE membership for 3 parked requests.
        Releasable slotHolder = awaitGrant(service, bucket, sharedLimit);
        assertNotNull("first acquire takes the only shared slot", slotHolder);
        for (int i = 0; i < 3; i++) {
            AtomicReference<Exception> denial = new AtomicReference<>();
            service.acquireAsync(bucket, sharedLimit, true, ActionListener.wrap(p -> fail("must be denied while at limit"), denial::set));
            assertTrue("acquire at limit must be denied (429)", denial.get() instanceof OpenSearchRejectedExecutionException);
        }
        assertEquals("one Set membership for the coordinator regardless of parked count", 1, service.waiterCountForTest(bucket));

        // Release the in-flight slot -> owner-push admits the FIRST parked request and the coordinator stays registered.
        slotHolder.close();
        assertEquals("first freed slot drains exactly one parked request", 1, admits.get());
        assertEquals("coordinator must remain registered while it still has parked requests", 1, service.waiterCountForTest(bucket));

        // Each admitted request completing frees the slot again and must drain the NEXT parked request.
        heldPermits.remove(0).close();
        assertEquals("second freed slot drains the second parked request", 2, admits.get());
        assertEquals(1, service.waiterCountForTest(bucket));

        heldPermits.remove(0).close();
        assertEquals("third freed slot drains the third (last) parked request", 3, admits.get());

        // The last request completes with nothing left queued: the next grant comes back unused, so the coordinator
        // self-deregisters and the freed slot returns to the pool. No stranding, no leaked permit.
        heldPermits.remove(0).close();
        assertEquals("no further admits once the queue is empty", 3, admits.get());
        assertEquals("coordinator self-reconciles out of the registry when it has nothing queued", 0, service.waiterCountForTest(bucket));
        assertEquals("no shared permit leaked after the burst fully drains", 0, service.tracker().inFlight(bucket));
    }

    public void testDoubleCloseReleasesOnce() {
        WorkloadGroupSharedThrottleService service = newService();
        Releasable p = awaitGrant(service, "b", 2);
        assertNotNull(awaitGrant(service, "b", 2)); // second slot
        p.close();
        p.close(); // must not double-release
        // one slot still held, so only one more grant is available
        assertNotNull(awaitGrant(service, "b", 2));
        expectThrows(OpenSearchRejectedExecutionException.class, () -> awaitGrant(service, "b", 2));
    }

    public void testEmptyRingFailsOpen() {
        // Cluster with no eligible data node (coordinating/manager-only) => empty ring => fail open (null permit).
        DiscoveryNode managerOnly = new DiscoveryNode(
            "m",
            "m",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            org.opensearch.Version.CURRENT
        );
        ClusterState state = Mockito.mock(ClusterState.class);
        when(state.nodes()).thenReturn(DiscoveryNodes.builder().add(managerOnly).localNodeId("m").build());
        when(clusterService.state()).thenReturn(state);
        when(clusterService.localNode()).thenReturn(managerOnly);

        WorkloadGroupSharedThrottleService service = newService();
        AtomicReference<Releasable> permit = new AtomicReference<>();
        AtomicReference<Boolean> called = new AtomicReference<>(false);
        service.acquireAsync("b", 1, ActionListener.wrap(p -> {
            called.set(true);
            permit.set(p);
        }, e -> fail("must not fail: " + e)));
        assertTrue("listener must be invoked inline", called.get());
        assertNull("fail-open yields a null permit (admit, untracked)", permit.get());
    }

    public void testDisconnectedOwnerFailsOpenWithoutSendingRequest() {
        // Ring owner is a remote data node (not the local node), so this is NOT the local short-circuit path.
        DiscoveryNode remote = new DiscoveryNode(
            "remote",
            "remote",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            org.opensearch.Version.CURRENT
        );
        ClusterState state = Mockito.mock(ClusterState.class);
        // Only the remote node is a data node, so every bucket hashes to it; local is a coordinating-only node.
        when(state.nodes()).thenReturn(DiscoveryNodes.builder().add(remote).localNodeId("local").build());
        when(clusterService.state()).thenReturn(state);
        when(clusterService.localNode()).thenReturn(localNode);
        // The owner is known-disconnected.
        when(transportService.nodeConnected(remote)).thenReturn(false);

        WorkloadGroupSharedThrottleService service = newService();
        AtomicReference<Releasable> permit = new AtomicReference<>();
        AtomicReference<Boolean> called = new AtomicReference<>(false);
        service.acquireAsync("b", 1, ActionListener.wrap(p -> {
            called.set(true);
            permit.set(p);
        }, e -> fail("must not fail: " + e)));

        // The listener completed inline with a null permit. This proves the disconnected pre-check fired: on the
        // transport path the listener is only invoked from a sendRequest callback, but transportService is a plain
        // mock whose sendRequest is a no-op that never calls back — so an inline null response can only come from the
        // nodeConnected()==false fast-fail branch, not from a request sent into the dead socket.
        assertTrue("listener must be invoked inline (no RTT to a dead node)", called.get());
        assertNull("disconnected owner -> fail open with a null permit", permit.get());
    }

    public void testHandleAcquireIsOwnerSideAdmission() {
        WorkloadGroupSharedThrottleService service = newService();
        // Directly exercise the owner-side handler (what a remote coordinator's RPC would hit).
        assertTrue(
            service.handleAcquire(
                new WorkloadGroupSharedThrottleService.AcquirePermitRequest(
                    "b",
                    1,
                    "permit-1",
                    WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS
                )
            ).granted
        );
        assertFalse(
            service.handleAcquire(
                new WorkloadGroupSharedThrottleService.AcquirePermitRequest(
                    "b",
                    1,
                    "permit-2",
                    WorkloadGroupSharedThrottleService.PERMIT_TTL_NANOS
                )
            ).granted
        );
        assertEquals(1, service.tracker().inFlight("b"));
    }

    public void testAcquirePermitRequestSerializationRoundTrip() throws Exception {
        WorkloadGroupSharedThrottleService.AcquirePermitRequest original = new WorkloadGroupSharedThrottleService.AcquirePermitRequest(
            "grp1:username:alice",
            42,
            "permit-xyz",
            123_456_789L
        );
        WorkloadGroupSharedThrottleService.AcquirePermitRequest copy = copyWriteable(
            original,
            writableRegistry(),
            WorkloadGroupSharedThrottleService.AcquirePermitRequest::new
        );
        assertEquals(original.bucketKey, copy.bucketKey);
        assertEquals(original.sharedLimit, copy.sharedLimit);
        assertEquals(original.permitId, copy.permitId);
        assertEquals(original.ttlNanos, copy.ttlNanos);
    }

    public void testAcquirePermitResponseSerializationRoundTrip() throws Exception {
        for (boolean granted : new boolean[] { true, false }) {
            WorkloadGroupSharedThrottleService.AcquirePermitResponse original =
                new WorkloadGroupSharedThrottleService.AcquirePermitResponse(granted);
            WorkloadGroupSharedThrottleService.AcquirePermitResponse copy = copyWriteable(
                original,
                writableRegistry(),
                WorkloadGroupSharedThrottleService.AcquirePermitResponse::new
            );
            assertEquals(granted, copy.granted);
        }
    }

    public void testReleasePermitRequestSerializationRoundTrip() throws Exception {
        WorkloadGroupSharedThrottleService.ReleasePermitRequest original = new WorkloadGroupSharedThrottleService.ReleasePermitRequest(
            "grp1:group",
            "permit-abc"
        );
        WorkloadGroupSharedThrottleService.ReleasePermitRequest copy = copyWriteable(
            original,
            writableRegistry(),
            WorkloadGroupSharedThrottleService.ReleasePermitRequest::new
        );
        assertEquals(original.bucketKey, copy.bucketKey);
        assertEquals(original.permitId, copy.permitId);
    }

    // --- serde resilience -------------------------------------------------------------------------------------------
    // All three RPC bodies are ONE name-keyed map, so a peer from a different commit interoperates. These write the bytes
    // by hand to stand in for another build's writer, since our own writeTo can only emit the keys it knows.

    private static StreamInput bodyBytes(Map<String, Object> body, boolean withTaskPreamble) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        if (withTaskPreamble) {
            TaskId.EMPTY_TASK_ID.writeTo(out); // TransportRequest preamble that super(in) consumes; responses have none
        }
        out.writeMap(body, StreamOutput::writeString, StreamOutput::writeGenericValue);
        return out.bytes().streamInput();
    }

    public void testAcquirePermitRequestIgnoresFieldsAddedByANewerPeer() throws Exception {
        Map<String, Object> body = new HashMap<>();
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_BUCKET, "grp1:group");
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_SHARED_LIMIT, 5);
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_PERMIT_ID, "permit-abc");
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_TTL_NANOS, 1_000L);
        // A spread of registry types, since the point of generic values is that a future field keeps its natural type.
        body.put("requesting_node", "eph-1");
        body.put("wants_queue", true);
        body.put("future_list", List.of("a", "b"));
        body.put("future_null", null);

        StreamInput in = bodyBytes(body, true);
        WorkloadGroupSharedThrottleService.AcquirePermitRequest parsed = new WorkloadGroupSharedThrottleService.AcquirePermitRequest(in);
        assertEquals("grp1:group", parsed.bucketKey);
        assertEquals(5, parsed.sharedLimit);
        assertEquals("permit-abc", parsed.permitId);
        assertEquals(1_000L, parsed.ttlNanos);
        assertEquals("unknown keys must be consumed, not left as trailing bytes", 0, in.available());
    }

    public void testAcquirePermitRequestIsStrictBecauseADecodeErrorFailsOpen() throws Exception {
        // Deliberately NOT tolerant. A decode error reaches the coordinator's handleException, which admits the request
        // untracked (fail open). Guessing a default instead would let a mis-shaped acquire reach the tracker and be
        // DENIED, turning a shape mismatch into a spurious 429 — strictly worse than not enforcing the limit once.
        Map<String, Object> full = new HashMap<>();
        full.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_BUCKET, "grp1:group");
        full.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_SHARED_LIMIT, 5);
        full.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_PERMIT_ID, "permit-abc");
        full.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_TTL_NANOS, 1_000L);
        for (String omitted : full.keySet()) {
            Map<String, Object> partial = new HashMap<>(full);
            partial.remove(omitted);
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> new WorkloadGroupSharedThrottleService.AcquirePermitRequest(bodyBytes(partial, true))
            );
            assertTrue(e.getMessage(), e.getMessage().contains(omitted));
        }
    }

    public void testAcquirePermitRequestAcceptsAWidenedNumericType() throws Exception {
        // A peer that widens shared_limit to a long, or narrows ttl to an int, must still interoperate.
        Map<String, Object> body = new HashMap<>();
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_BUCKET, "grp1:group");
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_SHARED_LIMIT, 5L);
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_PERMIT_ID, "permit-abc");
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitRequest.KEY_TTL_NANOS, 1_000);
        WorkloadGroupSharedThrottleService.AcquirePermitRequest parsed = new WorkloadGroupSharedThrottleService.AcquirePermitRequest(
            bodyBytes(body, true)
        );
        assertEquals(5, parsed.sharedLimit);
        assertEquals(1_000L, parsed.ttlNanos);
    }

    public void testAcquirePermitResponseIgnoresFieldsAddedByANewerPeer() throws Exception {
        Map<String, Object> body = new HashMap<>();
        body.put(WorkloadGroupSharedThrottleService.AcquirePermitResponse.KEY_GRANTED, true);
        body.put("future_reason", "at_limit");
        body.put("future_retry_after_millis", 250L);
        StreamInput in = bodyBytes(body, false); // TransportResponse has no TaskId preamble
        WorkloadGroupSharedThrottleService.AcquirePermitResponse parsed = new WorkloadGroupSharedThrottleService.AcquirePermitResponse(in);
        assertTrue(parsed.granted);
        assertEquals(0, in.available());
    }

    public void testAcquirePermitResponseIsStrict() throws Exception {
        // Same reasoning as AcquirePermitRequest: an unreadable response fails open via handleException. Defaulting granted to
        // false would 429 the search; defaulting it to true would admit while believing a permit is held.
        expectThrows(
            IllegalStateException.class,
            () -> new WorkloadGroupSharedThrottleService.AcquirePermitResponse(bodyBytes(new HashMap<>(), false))
        );
    }

    public void testReleasePermitRequestIgnoresFieldsAddedByANewerPeer() throws Exception {
        Map<String, Object> body = new HashMap<>();
        body.put(WorkloadGroupSharedThrottleService.ReleasePermitRequest.KEY_BUCKET, "grp1:group");
        body.put(WorkloadGroupSharedThrottleService.ReleasePermitRequest.KEY_PERMIT_ID, "permit-abc");
        body.put("shared_limit", 5);              // both of these are added by the queueing branch
        body.put("queue_empty_on_node", "eph-1");
        StreamInput in = bodyBytes(body, true);
        WorkloadGroupSharedThrottleService.ReleasePermitRequest parsed = new WorkloadGroupSharedThrottleService.ReleasePermitRequest(in);
        assertEquals("grp1:group", parsed.bucketKey);
        assertEquals("permit-abc", parsed.permitId);
        assertEquals(0, in.available());
    }

    public void testReleasePermitRequestIsStrictLikeTheOthers() throws Exception {
        // Baseline fields are strict on every RPC, which is what the positional format already did for a malformed
        // message: a decode error before any handler runs. Tolerating them would buy nothing here — a release that throws
        // and a release that defaults to empty both leave the permit held until its TTL — while losing the error response
        // that makes the sender log "TTL will reclaim".
        Map<String, Object> full = new HashMap<>();
        full.put(WorkloadGroupSharedThrottleService.ReleasePermitRequest.KEY_BUCKET, "grp1:group");
        full.put(WorkloadGroupSharedThrottleService.ReleasePermitRequest.KEY_PERMIT_ID, "permit-abc");
        for (String omitted : full.keySet()) {
            Map<String, Object> partial = new HashMap<>(full);
            partial.remove(omitted);
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> new WorkloadGroupSharedThrottleService.ReleasePermitRequest(bodyBytes(partial, true))
            );
            assertTrue(e.getMessage(), e.getMessage().contains(omitted));
        }
    }

}
