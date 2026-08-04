/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Cluster-level ({@code shared_limit}) throttle tier. Each throttle bucket has one authoritative in-flight counter
 * living on the node that owns it per a consistent-hash ring ({@link ThrottleOwnerSelector}); this service is both
 * the <em>coordinator-side</em> client that acquires/releases a shared permit for an incoming request and the
 * <em>owner-side</em> host of the {@link SharedThrottleTracker} that answers those requests for the buckets it owns.
 * <p>
 * Latency: the acquire is asynchronous so the calling (transport) thread never blocks on the round-trip — critical
 * under the very bursts throttling defends against. When the coordinator itself owns the bucket, the acquire
 * short-circuits to a direct in-memory tracker call with no network hop at all.
 * <p>
 * Availability: any failure to reach the owner (timeout, disconnect, missing handler on an old node, or an empty
 * ring) fails <em>open</em> — the request is admitted with no shared permit. This is a deliberate, un-toggleable
 * choice: a single unreachable owner must not turn a network blip into a cluster-wide rejection storm for its
 * share of buckets. A stuck lease is instead reclaimed by the owner's TTL sweep.
 */
@ExperimentalApi
public class WorkloadGroupSharedThrottleService implements ClusterStateListener {

    /** Internal transport action names (not client-facing REST). */
    public static final String ACQUIRE_ACTION_NAME = "internal:wlm/throttle/shared/acquire";
    public static final String RELEASE_ACTION_NAME = "internal:wlm/throttle/shared/release";
    // Owner -> coordinator: a shared slot freed and this coordinator has a registered waiter for the bucket; here is a
    // reserved lease to admit one queued request against (queueing / owner-push tier).
    public static final String GRANT_ACTION_NAME = "internal:wlm/throttle/shared/grant";

    // Fixed operational constants. Deliberately not cluster settings: they are internal-coordination knobs an operator
    // would never need to tune, and fail-open + a generous TTL make them safe as constants. Promote to a setting later
    // (non-breaking) only if a real deployment need emerges.
    //
    // Timeout for the acquire round-trip to a bucket's owner. Small so a slow/unreachable owner fails open quickly
    // rather than adding latency to the request path.
    static final TimeValue ACQUIRE_TIMEOUT = TimeValue.timeValueMillis(200);
    // Time-to-live for a lease on the owner. Reclaims a lease left behind by a crashed coordinator or a lost release
    // RPC. Set generously above the typical request duration so a still-running request's lease is not reclaimed early.
    // Accepted tradeoff: leases are NOT renewed, so a search that runs longer than this TTL has its lease reclaimed
    // while still executing, which can transiently admit one-or-more requests beyond shared_limit for that bucket
    // (self-correcting, fail-open direction — same flavor as the ring-rebalance transient breach). Long-running search
    // is uncommon (no default search timeout, but heavy aggs/scripts can exceed it); if it becomes a problem the fix is
    // lease renewal or deriving the TTL from a request deadline, not a larger constant.
    static final long LEASE_TTL_NANOS = TimeValue.timeValueMinutes(5).nanos();
    // How often the owner sweeps expired leases. This is a pure memory-hygiene backstop, NOT a correctness mechanism:
    // tryAcquire() already prunes a bucket's expired leases on every acquire, so an active bucket self-heals and can
    // never over-reject due to a stuck lease. The sweep only reclaims the map entry for a bucket that abandoned a lease
    // and then went completely idle (rejects nothing). An entry can't be reclaimed before its TTL elapses anyway, so
    // sweeping faster than the TTL is pointless; run it infrequently.
    static final TimeValue SWEEP_INTERVAL = TimeValue.timeValueMinutes(5);

    private static final Logger logger = LogManager.getLogger(WorkloadGroupSharedThrottleService.class);

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final TransportService transportService;
    private final SharedThrottleTracker tracker;

    // Immutable ring snapshot, swapped wholesale on discovery-node changes so readers never see a half-built ring.
    private final AtomicReference<ThrottleOwnerSelector> ring = new AtomicReference<>();
    private volatile Scheduler.Cancellable sweepTask;

    // OWNER-SIDE waiter registry for owner-push queue draining: for each bucket this node owns, how many requests each
    // coordinator is waiting on. Counts only — no request identity (the requests live on their coordinators). A stale
    // count (a waiter that drained via a node permit) simply costs a wasted grant and self-drains as grants decrement
    // it; there is no pruning. Populated when an acquire denies with wantsQueue=true; consulted when a lease releases.
    private final Map<String, Map<DiscoveryNode, Integer>> waitersByBucket = new ConcurrentHashMap<>();

    // COORDINATOR-SIDE: how a pushed grant is turned into an admitted queued request. Late-bound (the queue service is
    // constructed after this service); returns true if a queued request was admitted with the reserved permit, false
    // if there was none (this service then returns the reserved slot to the owner). Null before wiring => no grant is
    // ever consumed (a received grant is returned), which is safe.
    private volatile GrantConsumer grantConsumer;

    public WorkloadGroupSharedThrottleService(ClusterService clusterService, ThreadPool threadPool, TransportService transportService) {
        this(clusterService, threadPool, transportService, new SharedThrottleTracker());
    }

    public WorkloadGroupSharedThrottleService(
        ClusterService clusterService,
        ThreadPool threadPool,
        TransportService transportService,
        SharedThrottleTracker tracker
    ) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.tracker = tracker;
        // Start with an empty ring (fail-open) rather than reading cluster state here: during node construction the
        // ClusterApplierService has no state yet. The ring is populated by the first clusterChanged whose eligible
        // owner set differs from this empty ring — which includes a single-node cluster's very first applied state
        // (its node set does not "change" relative to the coordinator-seeded initial state, so we must NOT gate on
        // nodesChanged()).
        this.ring.set(ThrottleOwnerSelector.fromDiscoveryNodes(DiscoveryNodes.EMPTY_NODES));
        clusterService.addListener(this);
        transportService.registerRequestHandler(
            ACQUIRE_ACTION_NAME,
            ThreadPool.Names.SAME,
            AcquireRequest::new,
            (request, channel, task) -> channel.sendResponse(handleAcquire(request))
        );
        transportService.registerRequestHandler(
            RELEASE_ACTION_NAME,
            ThreadPool.Names.SAME,
            ReleaseRequest::new,
            (request, channel, task) -> {
                tracker.release(request.bucketKey, request.leaseId);
                // A slot just freed on the owner. If a coordinator is waiting on this bucket, hand the freed slot to one
                // of them (reserve-then-grant) so its queued request can drain — owner-push. No-op if no waiters.
                onSharedSlotFreed(request.bucketKey, request.sharedLimit);
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            }
        );
        transportService.registerRequestHandler(GRANT_ACTION_NAME, ThreadPool.Names.SAME, GrantRequest::new, (request, channel, task) -> {
            handleGrant(request);
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        });
    }

    /** Starts the periodic TTL sweep. Idempotent-safe to call once at node start. */
    public void start() {
        // The ring is populated by clusterChanged (see the constructor), not here: at node start the initial cluster
        // state may not be applied yet, so reading clusterService.state() here can throw "initial cluster state not
        // set yet". This method only schedules the memory-hygiene sweep.
        sweepTask = threadPool.scheduleWithFixedDelay(() -> {
            try {
                tracker.sweepExpired();
            } catch (Exception e) {
                logger.warn("Shared throttle TTL sweep failed", e);
            }
        }, SWEEP_INTERVAL, ThreadPool.Names.GENERIC);
    }

    public void stop() {
        if (sweepTask != null) {
            sweepTask.cancel();
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // Rebuild whenever the eligible-owner set differs from the current ring, rather than gating on
        // event.nodesChanged(). nodesChanged() is a delta vs the previous applied state, and the coordinator seeds the
        // initial applied state already containing the local node — so on a single-node cluster (or any cluster whose
        // membership is stable after this service starts) nodesChanged() is never true and the ring would stay empty,
        // silently disabling shared throttling.
        //
        // Compare the eligible-node set FIRST, without building the ring (cheap: a version-filtered data-node scan, no
        // virtual-node hashing), so an unchanged membership — the common case, including every cluster-state update
        // while WLM is disabled — pays nothing. Compare whole DiscoveryNodes, not just persistent ids, so a same-id
        // restart (new ephemeral id/address) is treated as a change; otherwise the ring would keep the stale node and
        // its buckets would fail open forever.
        Set<DiscoveryNode> candidate = ThrottleOwnerSelector.eligibleNodeSet(event.state().nodes());
        if (ring.get().eligibleNodeSet().equals(candidate) == false) {
            ring.set(ThrottleOwnerSelector.fromDiscoveryNodes(event.state().nodes()));
        }
    }

    /**
     * Asynchronously acquires one cluster-level shared permit for {@code bucketKey}, notifying {@code listener} with:
     * <ul>
     *   <li>a non-null {@link Releasable} — granted; close it on request completion to release the shared permit;</li>
     *   <li>{@code null} — <em>fail open</em>: admit the request without a shared permit (owner unreachable, empty
     *       ring, or any error). Nothing to release.</li>
     * </ul>
     * A denial (bucket at its shared limit) is signalled via {@link ActionListener#onFailure} with a
     * <em>message-less</em> {@link OpenSearchRejectedExecutionException} — a pure "denied" marker. This service holds
     * only the opaque bucket key, not the human-readable group/attribute, so {@link WorkloadGroupService} recomposes
     * the user-facing 429 message; the exception's <em>type</em> (not its text) is what distinguishes a denial from a
     * transport error (which also arrives via {@code onFailure} but must pass through as fail-open, not a 429).
     * The listener may be invoked inline (local-owner short-circuit) or on a transport thread.
     */
    public void acquireAsync(String bucketKey, int sharedLimit, ActionListener<Releasable> listener) {
        acquireAsync(bucketKey, sharedLimit, false, listener);
    }

    /**
     * As {@link #acquireAsync(String, int, ActionListener)}, but {@code wantsQueue} tells the owner to register this
     * coordinator as a waiter for the bucket if the acquire is denied, so a later freed slot is pushed back here as a
     * grant (owner-push queue draining). Pass {@code true} only when this group has queueing enabled.
     */
    public void acquireAsync(String bucketKey, int sharedLimit, boolean wantsQueue, ActionListener<Releasable> listener) {
        final ThrottleOwnerSelector currentRing = ring.get();
        final DiscoveryNode owner = currentRing.ownerFor(bucketKey).orElse(null);
        if (owner == null) {
            listener.onResponse(null); // empty ring -> fail open
            return;
        }

        final String leaseId = leaseId();
        final long ttlNanos = LEASE_TTL_NANOS;

        // Local-owner short-circuit: this coordinator owns the bucket, so hit the tracker directly with no network hop.
        if (owner.getId().equals(clusterService.localNode().getId())) {
            if (tracker.tryAcquire(bucketKey, sharedLimit, leaseId, ttlNanos)) {
                listener.onResponse(releaseLocal(bucketKey, sharedLimit, leaseId));
            } else {
                if (wantsQueue) {
                    registerWaiter(bucketKey, clusterService.localNode());
                }
                listener.onFailure(deniedMarker());
            }
            return;
        }

        // Owner known-disconnected: fail open immediately rather than sending into a dead socket and waiting out
        // ACQUIRE_TIMEOUT. This is an in-memory connection-map lookup (ConcurrentHashMap.containsKey), not a network
        // call, so it costs nothing. It collapses the bulk of the post-node-drop detection window; the tiny remaining
        // sub-window (socket dead but not yet flagged) still hits the timeout and is handled by handleException below.
        if (transportService.nodeConnected(owner) == false) {
            listener.onResponse(null);
            return;
        }

        final TransportRequestOptions options = TransportRequestOptions.builder().withTimeout(ACQUIRE_TIMEOUT).build();
        transportService.sendRequest(
            owner,
            ACQUIRE_ACTION_NAME,
            new AcquireRequest(bucketKey, sharedLimit, leaseId, ttlNanos, clusterService.localNode(), wantsQueue),
            options,
            new TransportResponseHandler<AcquireResponse>() {
                @Override
                public AcquireResponse read(StreamInput in) throws IOException {
                    return new AcquireResponse(in);
                }

                @Override
                public void handleResponse(AcquireResponse response) {
                    if (response.granted) {
                        listener.onResponse(releaseRemote(owner, bucketKey, sharedLimit, leaseId));
                    } else {
                        listener.onFailure(deniedMarker());
                    }
                }

                @Override
                public void handleException(TransportException exp) {
                    // Owner unreachable / timed out -> fail open. The owner may have GRANTED this lease before the
                    // response was lost (e.g. the acquire arrived but the reply timed out), which would otherwise
                    // occupy a shared slot until its TTL and cause false 429s once connectivity recovers. Send a
                    // best-effort release for this leaseId to reclaim it immediately; release-by-id is idempotent, so
                    // if no lease was created it is a harmless no-op. Then admit (fail open).
                    logger.debug("Shared throttle acquire to owner [{}] for bucket [{}] failed; failing open", owner.getId(), bucketKey);
                    // Reclaim only (no owner-push): this lease likely never existed; UNSET_LIMIT tells the owner to
                    // release without driving a grant.
                    sendRelease(owner, bucketKey, WorkloadGroupThrottleSettings.UNSET_LIMIT, leaseId);
                    listener.onResponse(null);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            }
        );
    }

    // Owner-side admission. Package-private for tests.
    AcquireResponse handleAcquire(AcquireRequest request) {
        boolean granted = tracker.tryAcquire(request.bucketKey, request.sharedLimit, request.leaseId, request.ttlNanos);
        if (granted == false && request.wantsQueue && request.requestingNode != null) {
            // Denied and the coordinator will park the request -> remember it so a freed slot is pushed back as a grant.
            registerWaiter(request.bucketKey, request.requestingNode);
        }
        return new AcquireResponse(granted);
    }

    // A local release path must also drive owner-push when this node owns the bucket: releasing frees a slot that a
    // registered waiter should get. sharedLimit is threaded so the reserve step can re-check the limit.
    private Releasable releaseLocal(String bucketKey, int sharedLimit, String leaseId) {
        return releaseOnce(() -> {
            tracker.release(bucketKey, leaseId);
            onSharedSlotFreed(bucketKey, sharedLimit);
        });
    }

    private Releasable releaseRemote(DiscoveryNode owner, String bucketKey, int sharedLimit, String leaseId) {
        // The remote RELEASE RPC carries sharedLimit so the owner can drive owner-push after freeing the slot.
        return releaseOnce(() -> sendRelease(owner, bucketKey, sharedLimit, leaseId));
    }

    // Fire-and-forget RELEASE RPC to the bucket owner. Bounded by the same timeout as acquire so a half-open
    // connection can't leave the response handler pending until the connection is torn down. A lost release is not
    // fatal — the owner's TTL sweep reclaims the lease — so failures are logged at debug only. Carries sharedLimit so
    // the owner can drive owner-push (grant a waiter the freed slot) after releasing.
    private void sendRelease(DiscoveryNode owner, String bucketKey, int sharedLimit, String leaseId) {
        final TransportRequestOptions options = TransportRequestOptions.builder().withTimeout(ACQUIRE_TIMEOUT).build();
        transportService.sendRequest(
            owner,
            RELEASE_ACTION_NAME,
            new ReleaseRequest(bucketKey, sharedLimit, leaseId),
            options,
            new TransportResponseHandler<TransportResponse.Empty>() {
                @Override
                public TransportResponse.Empty read(StreamInput in) {
                    return TransportResponse.Empty.INSTANCE;
                }

                @Override
                public void handleResponse(TransportResponse.Empty response) {}

                @Override
                public void handleException(TransportException exp) {
                    logger.debug(
                        "Shared throttle release to owner [{}] for bucket [{}] failed; TTL will reclaim",
                        owner.getId(),
                        bucketKey
                    );
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            }
        );
    }

    // Guards against a double close (e.g. onRequestEnd and onRequestFailure) releasing twice.
    private static Releasable releaseOnce(Runnable release) {
        final AtomicBoolean released = new AtomicBoolean(false);
        return () -> {
            if (released.compareAndSet(false, true)) {
                release.run();
            }
        };
    }

    private static String leaseId() {
        return UUIDs.base64UUID();
    }

    /**
     * Late-binds the coordinator-side grant consumer (the queue service). A received grant admits one queued request
     * via this; before it is set, a grant is returned to the owner (safe: the reserved slot re-enters the pool).
     */
    public void setGrantConsumer(GrantConsumer grantConsumer) {
        this.grantConsumer = grantConsumer;
    }

    // OWNER-SIDE: record that a coordinator is waiting on a bucket this node owns. Counts only; increments the
    // coordinator's waiter count for the bucket.
    private void registerWaiter(String bucketKey, DiscoveryNode coordinator) {
        waitersByBucket.compute(bucketKey, (k, byNode) -> {
            if (byNode == null) {
                byNode = new ConcurrentHashMap<>();
            }
            byNode.merge(coordinator, 1, Integer::sum);
            return byNode;
        });
    }

    // OWNER-SIDE: a shared slot for this bucket just freed. If a coordinator is waiting, reserve the slot (a fresh TTL
    // lease) and push a grant to one waiter (round-robin over waiting coordinators). No waiters -> nothing to do.
    private void onSharedSlotFreed(String bucketKey, int sharedLimit) {
        if (sharedLimit == WorkloadGroupThrottleSettings.UNSET_LIMIT) {
            return; // reclaim-only release (e.g. failed-acquire cleanup): never drive a grant
        }
        final DiscoveryNode target = pickWaiter(bucketKey);
        if (target == null) {
            return;
        }
        final String reservedLeaseId = leaseId();
        // Reserve the freed slot so a concurrent acquire can't take it before the grant lands. If the bucket is somehow
        // already at the limit again (a racing acquire beat us), don't over-grant: put the waiter credit back.
        if (tracker.tryAcquire(bucketKey, sharedLimit, reservedLeaseId, LEASE_TTL_NANOS) == false) {
            registerWaiter(bucketKey, target);
            return;
        }
        if (target.getId().equals(clusterService.localNode().getId())) {
            // Local waiter: consume the grant in-process, no network hop.
            consumeGrant(bucketKey, sharedLimit, reservedLeaseId);
        } else {
            sendGrant(target, bucketKey, sharedLimit, reservedLeaseId);
        }
    }

    // OWNER-SIDE: pick a waiting coordinator (round-robin-ish: first key with a positive count) and decrement its
    // count. Returns null if no coordinator is waiting on the bucket.
    private DiscoveryNode pickWaiter(String bucketKey) {
        final DiscoveryNode[] picked = new DiscoveryNode[1];
        waitersByBucket.computeIfPresent(bucketKey, (k, byNode) -> {
            // Iterate a snapshot of keys so ordering rotates as counts change; pick the first with a positive count.
            for (DiscoveryNode node : new ArrayList<>(byNode.keySet())) {
                Integer count = byNode.get(node);
                if (count != null && count > 0) {
                    picked[0] = node;
                    if (count == 1) {
                        byNode.remove(node);
                    } else {
                        byNode.put(node, count - 1);
                    }
                    break;
                }
            }
            return byNode.isEmpty() ? null : byNode;
        });
        return picked[0];
    }

    // OWNER-SIDE: fire-and-forget GRANT RPC pushing a reserved lease to a waiting coordinator. If it can't be
    // delivered, reclaim the reserved slot (release by id) so it isn't lost until TTL.
    private void sendGrant(DiscoveryNode coordinator, String bucketKey, int sharedLimit, String reservedLeaseId) {
        if (transportService.nodeConnected(coordinator) == false) {
            tracker.release(bucketKey, reservedLeaseId); // waiter gone; reclaim immediately
            onSharedSlotFreed(bucketKey, sharedLimit); // try the next waiter
            return;
        }
        final TransportRequestOptions options = TransportRequestOptions.builder().withTimeout(ACQUIRE_TIMEOUT).build();
        transportService.sendRequest(
            coordinator,
            GRANT_ACTION_NAME,
            new GrantRequest(bucketKey, sharedLimit, reservedLeaseId),
            options,
            new TransportResponseHandler<TransportResponse.Empty>() {
                @Override
                public TransportResponse.Empty read(StreamInput in) {
                    return TransportResponse.Empty.INSTANCE;
                }

                @Override
                public void handleResponse(TransportResponse.Empty response) {}

                @Override
                public void handleException(TransportException exp) {
                    // Grant undeliverable: reclaim the reserved slot now (TTL is the ultimate backstop) and re-drive
                    // owner-push so a still-waiting coordinator gets the slot instead of it idling until TTL.
                    logger.debug("Shared throttle grant to [{}] for bucket [{}] failed; reclaiming", coordinator.getId(), bucketKey);
                    tracker.release(bucketKey, reservedLeaseId);
                    onSharedSlotFreed(bucketKey, sharedLimit);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            }
        );
    }

    // COORDINATOR-SIDE: a grant arrived. Hand the reserved lease to the queue service to admit one queued request; if
    // there is none (or no consumer wired yet), return the reserved slot — closing the permit releases the lease AND
    // re-drives owner-push so the owner tries the next waiter (this is how a stale waiter count self-drains).
    private void handleGrant(GrantRequest request) {
        consumeGrant(request.bucketKey, request.sharedLimit, request.leaseId);
    }

    private void consumeGrant(String bucketKey, int sharedLimit, String reservedLeaseId) {
        // The reserved permit closes exactly once: it releases the lease and (via the normal release path) re-drives
        // owner-push, so an unused grant hands the freed slot to the next waiting coordinator.
        final DiscoveryNode owner = ring.get().ownerFor(bucketKey).orElse(null);
        final Releasable permit = reservedPermit(owner, bucketKey, sharedLimit, reservedLeaseId);
        final GrantConsumer consumer = grantConsumer;
        if (consumer == null) {
            permit.close(); // not wired yet -> return the slot
            return;
        }
        boolean admitted;
        try {
            admitted = consumer.admit(bucketKey, permit);
        } catch (Exception e) {
            logger.warn("Queue grant admit failed for bucket [" + bucketKey + "]", e);
            permit.close();
            return;
        }
        if (admitted == false) {
            permit.close(); // no queued request on this coordinator -> return the reserved slot to the next waiter
        }
    }

    // A Releasable for a reserved lease, releasing the same way a normal acquired permit does: locally if this node
    // owns the bucket, else via a RELEASE RPC. Both paths re-drive owner-push (releaseLocal directly; releaseRemote via
    // the owner's RELEASE handler), so returning an unused grant flows the slot to the next waiter.
    private Releasable reservedPermit(DiscoveryNode owner, String bucketKey, int sharedLimit, String leaseId) {
        if (owner == null) {
            return releaseOnce(() -> {}); // ring empty; nothing to release remotely
        }
        if (owner.getId().equals(clusterService.localNode().getId())) {
            return releaseLocal(bucketKey, sharedLimit, leaseId);
        }
        return releaseRemote(owner, bucketKey, sharedLimit, leaseId);
    }

    // Message-less "denied" marker: the bucket is at its shared limit. Carries no user text because this service has
    // only the opaque bucket key; WorkloadGroupService recomposes the user-facing 429 with the group name/attribute.
    // The exception TYPE (OpenSearchRejectedExecutionException) is the signal — the orchestrator uses it to tell a
    // denial (recompose as a 429) apart from a transport error (pass through as fail-open).
    private static OpenSearchRejectedExecutionException deniedMarker() {
        return new OpenSearchRejectedExecutionException();
    }

    // Package-private accessor for tests.
    SharedThrottleTracker tracker() {
        return tracker;
    }

    ThrottleOwnerSelector ring() {
        return ring.get();
    }

    /**
     * Acquire RPC: a coordinator asks the bucket's owner to admit one request under {@code sharedLimit}.
     */
    public static class AcquireRequest extends TransportRequest {
        final String bucketKey;
        final int sharedLimit;
        final String leaseId;
        final long ttlNanos;
        // Owner-push: who is asking, and whether they will park the request on denial (so the owner should register a
        // waiter and later push a grant). requestingNode may be null from an older node that predates queueing.
        final DiscoveryNode requestingNode;
        final boolean wantsQueue;

        // Convenience for callers/tests that don't use owner-push (no waiter registration on denial).
        AcquireRequest(String bucketKey, int sharedLimit, String leaseId, long ttlNanos) {
            this(bucketKey, sharedLimit, leaseId, ttlNanos, null, false);
        }

        AcquireRequest(String bucketKey, int sharedLimit, String leaseId, long ttlNanos, DiscoveryNode requestingNode, boolean wantsQueue) {
            this.bucketKey = bucketKey;
            this.sharedLimit = sharedLimit;
            this.leaseId = leaseId;
            this.ttlNanos = ttlNanos;
            this.requestingNode = requestingNode;
            this.wantsQueue = wantsQueue;
        }

        AcquireRequest(StreamInput in) throws IOException {
            super(in);
            this.bucketKey = in.readString();
            this.sharedLimit = in.readVInt();
            this.leaseId = in.readString();
            this.ttlNanos = in.readVLong();
            this.requestingNode = in.readOptionalWriteable(DiscoveryNode::new);
            this.wantsQueue = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(bucketKey);
            out.writeVInt(sharedLimit);
            out.writeString(leaseId);
            out.writeVLong(ttlNanos);
            out.writeOptionalWriteable(requestingNode);
            out.writeBoolean(wantsQueue);
        }
    }

    /**
     * Acquire RPC response: whether the owner granted a shared permit.
     */
    public static class AcquireResponse extends TransportResponse {
        final boolean granted;

        AcquireResponse(boolean granted) {
            this.granted = granted;
        }

        AcquireResponse(StreamInput in) throws IOException {
            this.granted = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(granted);
        }
    }

    /**
     * Release RPC: fire-and-forget, tells the owner to drop a previously-granted lease.
     */
    public static class ReleaseRequest extends TransportRequest {
        final String bucketKey;
        // The bucket's shared limit, so the owner can drive owner-push (grant a waiter) after freeing the slot.
        // UNSET_LIMIT means "release only, do not drive push".
        final int sharedLimit;
        final String leaseId;

        // Convenience for callers/tests that don't drive owner-push on release.
        ReleaseRequest(String bucketKey, String leaseId) {
            this(bucketKey, WorkloadGroupThrottleSettings.UNSET_LIMIT, leaseId);
        }

        ReleaseRequest(String bucketKey, int sharedLimit, String leaseId) {
            this.bucketKey = bucketKey;
            this.sharedLimit = sharedLimit;
            this.leaseId = leaseId;
        }

        ReleaseRequest(StreamInput in) throws IOException {
            super(in);
            this.bucketKey = in.readString();
            this.sharedLimit = in.readVInt();
            this.leaseId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(bucketKey);
            out.writeVInt(sharedLimit);
            out.writeString(leaseId);
        }
    }

    /**
     * Owner -&gt; coordinator grant: a reserved shared lease for a bucket the coordinator is waiting on. Carries the
     * bucket's shared limit so a returned (unused) grant can re-drive owner-push toward the next waiter.
     */
    public static class GrantRequest extends TransportRequest {
        final String bucketKey;
        final int sharedLimit;
        final String leaseId;

        GrantRequest(String bucketKey, int sharedLimit, String leaseId) {
            this.bucketKey = bucketKey;
            this.sharedLimit = sharedLimit;
            this.leaseId = leaseId;
        }

        GrantRequest(StreamInput in) throws IOException {
            super(in);
            this.bucketKey = in.readString();
            this.sharedLimit = in.readVInt();
            this.leaseId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(bucketKey);
            out.writeVInt(sharedLimit);
            out.writeString(leaseId);
        }
    }

    /**
     * COORDINATOR-SIDE seam: consumes a pushed grant by admitting one queued request against the reserved permit.
     * Returns {@code true} if a queued request was admitted (it now owns the permit), {@code false} if there was none
     * (the caller returns the reserved slot). Implemented by the queue service; late-bound via
     * {@link #setGrantConsumer}.
     */
    @ExperimentalApi
    @FunctionalInterface
    public interface GrantConsumer {
        boolean admit(String bucketKey, Releasable reservedPermit);
    }
}
