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
import org.opensearch.cluster.metadata.WorkloadGroup;
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
import java.util.LinkedHashSet;
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
 * share of buckets. A stuck permit is instead reclaimed by the owner's TTL sweep.
 */
@ExperimentalApi
public class WorkloadGroupSharedThrottleService implements ClusterStateListener {

    /** Internal transport action names (not client-facing REST). */
    public static final String ACQUIRE_ACTION_NAME = "internal:wlm/throttle/shared/acquire";
    public static final String RELEASE_ACTION_NAME = "internal:wlm/throttle/shared/release";
    // Owner -> coordinator: a shared slot freed and this coordinator has a registered waiter for the bucket; here is a
    // reserved permit to admit one queued request against (queueing / owner-push tier).
    public static final String GRANT_ACTION_NAME = "internal:wlm/throttle/shared/grant";

    // Fixed operational constants. Deliberately not cluster settings: they are internal-coordination knobs an operator
    // would never need to tune, and fail-open + a generous TTL make them safe as constants. Promote to a setting later
    // (non-breaking) only if a real deployment need emerges.
    //
    // Timeout for the acquire round-trip to a bucket's owner. Small so a slow/unreachable owner fails open quickly
    // rather than adding latency to the request path.
    static final TimeValue ACQUIRE_TIMEOUT = TimeValue.timeValueMillis(200);
    // Time-to-live for a permit on the owner. Reclaims a permit left behind by a crashed coordinator or a lost release
    // RPC. Set generously above the typical request duration so a still-running request's permit is not reclaimed early.
    // Accepted tradeoff: permits are NOT renewed, so a search that runs longer than this TTL has its permit reclaimed
    // while still executing, which can transiently admit one-or-more requests beyond shared_limit for that bucket
    // (self-correcting, fail-open direction — same flavor as the ring-rebalance transient breach). Long-running search
    // is uncommon (no default search timeout, but heavy aggs/scripts can exceed it); if it becomes a problem the fix is
    // permit renewal or deriving the TTL from a request deadline, not a larger constant.
    static final long PERMIT_TTL_NANOS = TimeValue.timeValueMinutes(5).nanos();
    // How often the owner sweeps expired permits. Originally pure memory hygiene, on the reasoning that tryAcquire()
    // prunes a bucket's expired permits on every acquire, so an active bucket self-heals and can never over-reject.
    // QUEUEING MADE THIS A CORRECTNESS PATH TOO: a bucket can now hold parked requests while receiving no new acquires
    // (the clients already in the queue are waiting, not arriving), so there may be no tryAcquire to do that pruning. For
    // such a bucket this sweep is the only thing that reclaims a crashed holder's permit AND the only thing that then
    // drives owner-push for the freed slot (see start()). Consequence to be aware of: a permit frees at its TTL but is
    // only discovered on the next sweep, so worst-case discovery latency is TTL + SWEEP_INTERVAL. Parked requests have
    // no deadline, so they wait rather than fail — but they wait that long.
    static final TimeValue SWEEP_INTERVAL = TimeValue.timeValueMinutes(5);

    private static final Logger logger = LogManager.getLogger(WorkloadGroupSharedThrottleService.class);

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final TransportService transportService;
    private final SharedThrottleTracker tracker;

    // Immutable ring snapshot, swapped wholesale on discovery-node changes so readers never see a half-built ring.
    private final AtomicReference<ThrottleOwnerSelector> ring = new AtomicReference<>();
    private volatile Scheduler.Cancellable sweepTask;

    // OWNER-SIDE waiter registry for owner-push queue draining: for each bucket this node owns, the ordered SET of
    // coordinators that have at least one request parked for the bucket. A set (not a count) so registration is
    // idempotent and the registry is bounded at <= N coordinators per bucket — a coordinator that re-registers (e.g.
    // still parked across several acquires) does not inflate it. INSERTION-ORDERED (LinkedHashSet) so a grant rotates
    // the chosen coordinator to the tail (see pickAndRotate): membership persists across a successful hand-off (the
    // coordinator may still have more queued requests), and successive freed slots round-robin fairly across
    // coordinators instead of repeatedly serving whichever one hashes first. A coordinator is removed only when a grant
    // to it comes back unused (no more queued requests) or it disconnects, so the registry self-reconciles with real
    // demand. No request identity is held (the requests live on their coordinators). LinkedHashSet is NOT thread-safe,
    // so every access — read, size, add, remove, rotate — MUST go through compute/computeIfPresent on this map, whose
    // per-key exclusive remapping is the sole lock guarding the inner set.
    private final Map<String, LinkedHashSet<DiscoveryNode>> waitersByBucket = new ConcurrentHashMap<>();

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
            AcquirePermitRequest::new,
            (request, channel, task) -> channel.sendResponse(handleAcquire(request))
        );
        transportService.registerRequestHandler(
            RELEASE_ACTION_NAME,
            ThreadPool.Names.SAME,
            ReleasePermitRequest::new,
            (request, channel, task) -> {
                handleRelease(request);
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            }
        );
        transportService.registerRequestHandler(
            GRANT_ACTION_NAME,
            ThreadPool.Names.SAME,
            GrantPermitRequest::new,
            (request, channel, task) -> {
                handleGrant(request);
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            }
        );
    }

    /** Starts the periodic TTL sweep. Idempotent-safe to call once at node start. */
    public void start() {
        // The ring is populated by clusterChanged (see the constructor), not here: at node start the initial cluster
        // state may not be applied yet, so reading clusterService.state() here can throw "initial cluster state not
        // set yet". This method only schedules the memory-hygiene sweep.
        sweepTask = threadPool.scheduleWithFixedDelay(() -> {
            try {
                sweepExpiredAndDrive();
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

        final String permitId = permitId();
        final long ttlNanos = PERMIT_TTL_NANOS;

        // Local-owner short-circuit: this coordinator owns the bucket, so hit the tracker directly with no network hop.
        if (owner.getId().equals(clusterService.localNode().getId())) {
            if (tracker.tryAcquire(bucketKey, sharedLimit, permitId, ttlNanos)) {
                listener.onResponse(releaseLocal(bucketKey, permitId));
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
            new AcquirePermitRequest(bucketKey, sharedLimit, permitId, ttlNanos, clusterService.localNode().getId(), wantsQueue),
            options,
            new TransportResponseHandler<AcquirePermitResponse>() {
                @Override
                public AcquirePermitResponse read(StreamInput in) throws IOException {
                    return new AcquirePermitResponse(in);
                }

                @Override
                public void handleResponse(AcquirePermitResponse response) {
                    if (response.granted) {
                        listener.onResponse(releaseRemote(owner, bucketKey, permitId));
                    } else {
                        listener.onFailure(deniedMarker());
                    }
                }

                @Override
                public void handleException(TransportException exp) {
                    // Owner unreachable / timed out -> fail open. The owner may have GRANTED this permit before the
                    // response was lost (e.g. the acquire arrived but the reply timed out), which would otherwise
                    // occupy a shared slot until its TTL and cause false 429s once connectivity recovers. Send a
                    // best-effort release for this permitId to reclaim it immediately; release-by-id is idempotent, so
                    // if no permit was created it is a harmless no-op. Then admit (fail open).
                    logger.debug("Shared throttle acquire to owner [{}] for bucket [{}] failed; failing open", owner.getId(), bucketKey);
                    // Reclaim only (no owner-push): this permit likely never existed; UNSET_LIMIT tells the owner to
                    // release without driving a grant.
                    sendRelease(owner, bucketKey, permitId, "");
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
    AcquirePermitResponse handleAcquire(AcquirePermitRequest request) {
        boolean granted = tracker.tryAcquire(request.bucketKey, request.sharedLimit, request.permitId, request.ttlNanos);
        if (granted == false && request.wantsQueue && request.requestingNodeId.isEmpty() == false) {
            // Denied and the coordinator will park the request -> remember it so a freed slot is pushed back as a grant.
            // The RPC carries only the coordinator's persistent node id, so resolve it to the live DiscoveryNode here:
            // the waiter set must hold the real node because sendGrant needs its transport address. A node that has since
            // left the cluster resolves to null and is simply not registered, which is correct — there is nothing to push
            // a grant to.
            final DiscoveryNode requestingNode = clusterService.state().nodes().get(request.requestingNodeId);
            if (requestingNode != null) {
                registerWaiter(request.bucketKey, requestingNode);
            } else {
                logger.debug(
                    "Not registering waiter for bucket [{}]: node [{}] is no longer in the cluster",
                    request.bucketKey,
                    request.requestingNodeId
                );
            }
        }
        return new AcquirePermitResponse(granted);
    }

    // A local release path must also drive owner-push when this node owns the bucket: releasing frees a slot that a
    // registered waiter should get. sharedLimit is threaded so the reserve step can re-check the limit.
    private Releasable releaseLocal(String bucketKey, String permitId) {
        return releaseOnce(() -> {
            // Same rule as the remote path's handleRelease, so local-owner and remote-owner behave identically.
            if (tracker.release(bucketKey, permitId)) {
                onSharedSlotFreed(bucketKey, currentSharedLimit(bucketKey));
            }
        });
    }

    private Releasable releaseRemote(DiscoveryNode owner, String bucketKey, String permitId) {
        // The remote RELEASE RPC carries sharedLimit so the owner can drive owner-push after freeing the slot.
        return releaseOnce(() -> sendRelease(owner, bucketKey, permitId, ""));
    }

    // Returns an UNUSED remote grant: a RELEASE tagged with this coordinator's node id so the owner also deregisters it
    // from the bucket's waiter set before re-driving owner-push to the next waiter.
    private void sendReleaseUnusedGrant(DiscoveryNode owner, String bucketKey, String permitId, DiscoveryNode self) {
        sendRelease(owner, bucketKey, permitId, self.getId());
    }

    // Fire-and-forget RELEASE RPC to the bucket owner. Bounded by the same timeout as acquire so a half-open
    // connection can't leave the response handler pending until the connection is torn down. A lost release is not
    // fatal — the owner's TTL sweep reclaims the permit — so failures are logged at debug only. Carries sharedLimit so
    // the owner can drive owner-push (grant a waiter the freed slot) after releasing. {@code queueEmptyOnNodeId} is set
    // only when returning an unused grant, so the owner deregisters that coordinator; empty for a normal release.
    private void sendRelease(DiscoveryNode owner, String bucketKey, String permitId, String queueEmptyOnNodeId) {
        final TransportRequestOptions options = TransportRequestOptions.builder().withTimeout(ACQUIRE_TIMEOUT).build();
        transportService.sendRequest(
            owner,
            RELEASE_ACTION_NAME,
            new ReleasePermitRequest(bucketKey, permitId, queueEmptyOnNodeId),
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

    private static String permitId() {
        return UUIDs.base64UUID();
    }

    /**
     * Late-binds the coordinator-side grant consumer (the queue service). A received grant admits one queued request
     * via this; before it is set, a grant is returned to the owner (safe: the reserved slot re-enters the pool).
     */
    public void setGrantConsumer(GrantConsumer grantConsumer) {
        this.grantConsumer = grantConsumer;
    }

    // OWNER-SIDE: record that a coordinator is waiting on a bucket this node owns. Idempotent (a set), so re-registering
    // an already-known waiter is a no-op — it keeps its existing queue position (LinkedHashSet.add does not reorder an
    // element already present), so re-registration can't let a coordinator jump the rotation. Registry stays bounded at
    // <= N coordinators per bucket.
    private void registerWaiter(String bucketKey, DiscoveryNode coordinator) {
        waitersByBucket.compute(bucketKey, (k, nodes) -> {
            if (nodes == null) {
                nodes = new LinkedHashSet<>();
            }
            nodes.add(coordinator);
            return nodes;
        });
    }

    /**
     * OWNER-SIDE release: free the permit, deregister the coordinator if it reported its queue for the bucket is empty,
     * and drive owner-push if a slot genuinely freed. Package-private and shared with the transport handler rather than
     * duplicated, so a test can drive it without the two copies drifting apart.
     * <p>
     * Both decisions are the OWNER's, not the coordinator's. Whether a slot freed comes from whether the remove actually
     * hit a live permit — a coordinator's release may be speculative (see the lost acquire-reply path), so it cannot
     * know. The ceiling is resolved from this node's cluster state, whose view is the one being enforced. Deciding here
     * means a no-op release never produces a phantom grant and — the case a coordinator-supplied hint got wrong — a
     * release that DID free a slot always drives one.
     */
    void handleRelease(ReleasePermitRequest request) {
        final boolean freed = tracker.release(request.bucketKey, request.permitId);
        // If this release is a coordinator returning an UNUSED grant (it had no queued request for the bucket), drop it
        // from the waiter set so it stops drawing wasted grants (remote analog of the local removeWaiter).
        if (request.queueEmptyOnNodeId.isEmpty() == false) {
            removeWaiterByNodeId(request.bucketKey, request.queueEmptyOnNodeId);
        }
        if (freed) {
            onSharedSlotFreed(request.bucketKey, currentSharedLimit(request.bucketKey));
        }
    }

    // OWNER-SIDE: drop a coordinator from a bucket's waiter set (it reported no more queued requests for the bucket,
    // via an unused grant). Prunes the bucket entry when the last waiter leaves.
    private void removeWaiter(String bucketKey, DiscoveryNode coordinator) {
        removeWaiterByNodeId(bucketKey, coordinator.getId());
    }

    // As removeWaiter, but keyed on the coordinator's PERSISTENT node id alone — which is all the RELEASE RPC carries,
    // since deregistration needs identity and nothing else. One removal implementation, and one identity basis for the
    // whole registry: registration also resolves from the persistent id (see handleAcquire). The scan is O(n) where
    // remove(node) was O(1), which is irrelevant — n is bounded by the number of coordinators in the cluster. Stays
    // inside computeIfPresent because that per-key remapping is the sole lock guarding the non-thread-safe
    // LinkedHashSet, and removeIf preserves insertion order so pickAndRotate's round-robin survives.
    //
    // Note this also clears a stale entry left by a restarted coordinator: DiscoveryNode identity is ephemeralId, so a
    // restart leaves a second entry under the same persistent id, and matching on that id removes both. That is what we
    // want — the old incarnation's parked requests died with its JVM.
    private void removeWaiterByNodeId(String bucketKey, String nodeId) {
        waitersByBucket.computeIfPresent(bucketKey, (k, nodes) -> {
            nodes.removeIf(n -> n.getId().equals(nodeId));
            return nodes.isEmpty() ? null : nodes;
        });
    }

    /**
     * One TTL-sweep pass: reclaim expired permits, then drive owner-push for every bucket that gained free capacity.
     * Package-private so tests can run a sweep deterministically instead of waiting on the scheduler.
     * <p>
     * Reclaiming an expired permit frees a shared slot with NO release RPC behind it — the holder crashed, or its release
     * was lost — so this is the only place that free slot is ever observed. Without the owner-push call a coordinator
     * with a parked request stays registered as a waiter while capacity sits idle and, because parked requests have no
     * deadline, strands until some unrelated release happens to re-drive the bucket.
     */
    void sweepExpiredAndDrive() {
        for (String bucketKey : tracker.sweepExpired()) {
            onSharedSlotFreed(bucketKey, currentSharedLimit(bucketKey));
        }
    }

    /**
     * The current {@code shared_limit} for a bucket, resolved from cluster state, or
     * {@link WorkloadGroupThrottleSettings#UNSET_LIMIT} if the group is gone or the shared tier is not configured. Used
     * by the TTL sweep, which knows only a bucket key: unlike the release path, no {@code sharedLimit} travels with an
     * expiry. {@link #onSharedSlotFreed} treats {@code UNSET_LIMIT} as "do not grant", so an unresolvable bucket simply
     * skips owner-push rather than guessing a limit.
     */
    private int currentSharedLimit(String bucketKey) {
        // The group id is the bucketKey prefix before the first ':' (see WorkloadGroupService.buildBucketKey); group ids
        // are base64 UUIDs with no ':', so the first ':' is unambiguous.
        final int idx = bucketKey.indexOf(':');
        final String groupId = idx < 0 ? bucketKey : bucketKey.substring(0, idx);
        final WorkloadGroup workloadGroup = clusterService.state().metadata().workloadGroups().get(groupId);
        if (workloadGroup == null) {
            return WorkloadGroupThrottleSettings.UNSET_LIMIT;
        }
        return WorkloadGroupThrottleSettings.SHARED_LIMIT.get(workloadGroup.getMutableWorkloadGroupFragment().getThrottling());
    }

    // OWNER-SIDE: a shared slot for this bucket just freed. Reserve it and push a grant to one waiting coordinator.
    // Iterative, not recursive: if a chosen waiter turns out to be gone (disconnected), we reclaim and try the next
    // one in a bounded loop (at most one pass over the <= N waiters), so a burst of stale/undeliverable waiters can
    // never blow the stack. The unused-grant case (coordinator connected but has no queued request) is handled off
    // this thread by the grant round-trip, which removes the waiter and re-drives once — not looped here.
    private void onSharedSlotFreed(String bucketKey, int sharedLimit) {
        if (sharedLimit == WorkloadGroupThrottleSettings.UNSET_LIMIT) {
            return; // reclaim-only release (e.g. failed-acquire cleanup): never drive a grant
        }
        // Absolute safety cap on total iterations to bound work and rule out livelock, while still allowing the loop to
        // react to waiters that REGISTER during it. A fixed snapshot of waiterCount is not enough: a stale local waiter
        // or a disconnected waiter frees the reserved slot without handing it off, and a coordinator can registerWaiter
        // (on a concurrent denied acquire) after the snapshot — leaving a free slot with an un-granted waiter, which
        // would otherwise strand until the next unrelated release (a spurious queue.timeout despite free capacity).
        // The cap is generous (waiters at entry, doubled, plus a constant); each iteration makes progress (serves,
        // drops a stale/disconnected waiter, or stops), so the loop terminates well within it in practice.
        final int maxAttempts = Math.max(1, waiterCount(bucketKey) * 2 + 8);
        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            final DiscoveryNode target = pickAndRotate(bucketKey);
            if (target == null) {
                return; // no waiters currently registered
            }
            final String reservedPermitId = permitId();
            // Reserve the freed slot so a concurrent acquire can't take it before the grant lands. If the bucket is at
            // its limit again (a racing acquire beat us), don't over-grant: stop. The waiter stays registered (peek did
            // not remove it), so the next release re-drives owner-push to it.
            if (tracker.tryAcquire(bucketKey, sharedLimit, reservedPermitId, PERMIT_TTL_NANOS) == false) {
                return;
            }
            if (target.getId().equals(clusterService.localNode().getId())) {
                // Local waiter: consume in-process. Returns true (STOP) if it admitted a queued request or admission
                // failed unexpectedly; false only when this coordinator had no queued request — it then already
                // released the reserved slot AND removed itself from the set, so the loop advances to another waiter
                // with the re-freed slot (including any registered concurrently with this loop).
                if (consumeGrantLocal(bucketKey, sharedLimit, reservedPermitId, target)) {
                    return;
                }
                // fall through: slot freed but not handed off -> re-check for a (possibly newly-registered) waiter
            } else if (transportService.nodeConnected(target) == false) {
                // Disconnected waiter: reclaim the reserved slot, drop it, and loop to the next waiter — handled here
                // in the bounded loop rather than by recursing through sendGrant.
                tracker.release(bucketKey, reservedPermitId);
                removeWaiter(bucketKey, target);
                // fall through: slot freed but not handed off -> re-check
            } else {
                // Remote, connected waiter: fire the grant and stop. Delivery failure and unused-grant return are
                // handled asynchronously by sendGrant's response handler + the grant handler (which re-drive once).
                sendGrant(target, bucketKey, sharedLimit, reservedPermitId);
                return;
            }
        }
    }

    // OWNER-SIDE: consume a grant for a LOCAL waiter without a network hop. Returns true if the caller's drain loop
    // should STOP for this freed slot — either a queued request was admitted (slot handed off), or admission failed
    // unexpectedly and retrying is unsafe. Returns false ONLY when this coordinator had no queued request (reserved slot
    // released, waiter removed), so the caller's loop can try the next waiter with the re-freed slot.
    //
    // Recursion safety: on ADMIT we hand a self-re-driving reservedPermit — but its close() fires later, at request
    // completion (async), so re-driving then is fine and not re-entrant. On the NO-request path we release the reserved
    // permit DIRECTLY (not by closing a re-driving permit) and return false so the caller's bounded loop advances,
    // rather than recursing through onSharedSlotFreed.
    private boolean consumeGrantLocal(String bucketKey, int sharedLimit, String reservedPermitId, DiscoveryNode self) {
        final GrantConsumer consumer = grantConsumer;
        if (consumer == null) {
            tracker.release(bucketKey, reservedPermitId);
            removeWaiter(bucketKey, self);
            return false;
        }
        final DiscoveryNode owner = clusterService.localNode(); // local path: this node owns the bucket
        boolean admitted;
        try {
            admitted = consumer.admit(bucketKey, reservedPermit(owner, bucketKey, sharedLimit, reservedPermitId));
        } catch (Exception e) {
            // admit() dispatches on GENERIC (which never rejects); a throw here means the executor is shutting down.
            // Release the reserved slot and STOP the loop — do NOT retry. admitWithPermit polls the head request before
            // admit() can throw, so a retry against the still-registered waiter would poll-and-drop a further request on
            // every iteration during a dispatch outage. The waiter stays registered; a later release re-drives it.
            logger.debug("Queue grant admit failed for bucket [" + bucketKey + "]", e);
            tracker.release(bucketKey, reservedPermitId);
            return true;
        }
        if (admitted == false) {
            // admitWithPermit does not close the permit on a false return; release the reserved permit directly so the
            // slot is reused by this loop. (Not via the permit's close(), which would re-drive owner-push inline.)
            tracker.release(bucketKey, reservedPermitId);
            removeWaiter(bucketKey, self); // this coordinator has nothing queued for the bucket
            return false;
        }
        return true;
    }

    // Current number of coordinators waiting on a bucket (0 if none). Reads the size under the map's per-key remapping
    // lock (returning the set unchanged), since the LinkedHashSet is not safe to size concurrently with a rotate/add.
    private int waiterCount(String bucketKey) {
        final int[] count = new int[1];
        waitersByBucket.computeIfPresent(bucketKey, (k, nodes) -> {
            count[0] = nodes.size();
            return nodes;
        });
        return count[0];
    }

    // OWNER-SIDE: pick the head waiting coordinator for the bucket and ROTATE it to the tail, WITHOUT removing it.
    // Returns null if none. Membership means "this coordinator has at least one request parked for the bucket", so a
    // waiter stays registered across a successful grant hand-off — it may still have more queued requests, and each
    // subsequent release re-drives owner-push. Rotating the picked coordinator to the tail gives round-robin fairness:
    // successive freed slots serve different coordinators in turn instead of repeatedly serving whichever one is first,
    // so one coordinator's deep backlog can't starve another's single request. A waiter is dropped only by the explicit
    // self-reconciling signals: an unused grant returned (remote via ReleaseRequest.unusedGrantFrom, local via
    // consumeGrantLocal) or a disconnect. Removing it here on pick would sever the drain chain after the first request,
    // stranding the rest until queue.timeout despite free capacity. Runs under the map's per-key remapping lock (the
    // sole guard for the non-thread-safe LinkedHashSet).
    private DiscoveryNode pickAndRotate(String bucketKey) {
        final DiscoveryNode[] picked = new DiscoveryNode[1];
        waitersByBucket.computeIfPresent(bucketKey, (k, nodes) -> {
            final java.util.Iterator<DiscoveryNode> it = nodes.iterator();
            if (it.hasNext()) {
                final DiscoveryNode head = it.next();
                picked[0] = head;
                it.remove();       // detach from the head...
                nodes.add(head);   // ...and re-append at the tail (round-robin), keeping it registered
            }
            return nodes.isEmpty() ? null : nodes;
        });
        return picked[0];
    }

    // OWNER-SIDE: fire-and-forget GRANT RPC pushing a reserved permit to a waiting coordinator. If it can't be
    // delivered, reclaim the reserved slot (release by id) so it isn't lost until TTL.
    private void sendGrant(DiscoveryNode coordinator, String bucketKey, int sharedLimit, String reservedPermitId) {
        if (transportService.nodeConnected(coordinator) == false) {
            tracker.release(bucketKey, reservedPermitId); // waiter gone; reclaim immediately
            removeWaiter(bucketKey, coordinator); // it's disconnected; drop it from the set
            onSharedSlotFreed(bucketKey, sharedLimit); // try the next waiter (iterative; this call is not re-entrant here)
            return;
        }
        final TransportRequestOptions options = TransportRequestOptions.builder().withTimeout(ACQUIRE_TIMEOUT).build();
        transportService.sendRequest(
            coordinator,
            GRANT_ACTION_NAME,
            new GrantPermitRequest(bucketKey, sharedLimit, reservedPermitId),
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
                    tracker.release(bucketKey, reservedPermitId);
                    onSharedSlotFreed(bucketKey, sharedLimit);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            }
        );
    }

    // COORDINATOR-SIDE: a grant arrived. Hand the reserved permit to the queue service to admit one queued request; if
    // there is none (or no consumer wired yet), return the reserved slot — closing the permit releases the permit AND
    // re-drives owner-push so the owner tries the next waiter (this is how a stale waiter count self-drains).
    private void handleGrant(GrantPermitRequest request) {
        consumeGrant(request.bucketKey, request.sharedLimit, request.permitId);
    }

    private void consumeGrant(String bucketKey, int sharedLimit, String reservedPermitId) {
        final GrantConsumer consumer = grantConsumer;
        if (consumer == null) {
            returnUnusedGrant(bucketKey, reservedPermitId); // not wired yet -> return the slot + deregister
            return;
        }
        // The permit handed to a successfully-admitted request releases only the reserved permit on completion (which
        // re-drives owner-push at the owner). It does NOT carry the unused-grant deregister signal — an admitted
        // coordinator is a legitimate ongoing waiter if it has more queued requests.
        final DiscoveryNode owner = ring.get().ownerFor(bucketKey).orElse(null);
        boolean admitted;
        try {
            admitted = consumer.admit(bucketKey, reservedPermit(owner, bucketKey, sharedLimit, reservedPermitId));
        } catch (Exception e) {
            logger.warn("Queue grant admit failed for bucket [" + bucketKey + "]", e);
            returnUnusedGrant(bucketKey, reservedPermitId);
            return;
        }
        if (admitted == false) {
            // No queued request on this coordinator for the bucket: return the reserved slot AND tell the owner to
            // deregister this coordinator so it stops drawing wasted grants.
            returnUnusedGrant(bucketKey, reservedPermitId);
        }
    }

    // Returns an unused reserved slot to its owner, tagging the release so the owner deregisters this coordinator from
    // the bucket's waiter set (it has no queued request for the bucket) and then re-drives owner-push to the next
    // waiter. If this node is the owner, does it in-process.
    private void returnUnusedGrant(String bucketKey, String reservedPermitId) {
        final DiscoveryNode owner = ring.get().ownerFor(bucketKey).orElse(null);
        final DiscoveryNode self = clusterService.localNode();
        if (owner == null) {
            return; // ring empty; the reserved permit (if any) is reclaimed by TTL
        }
        if (owner.getId().equals(self.getId())) {
            // Same rules as the remote path's handleRelease, so local-owner and remote-owner behave identically: push is
            // driven only if a permit really went away, against this node's own view of the ceiling.
            final boolean freed = tracker.release(bucketKey, reservedPermitId);
            removeWaiter(bucketKey, self);
            if (freed) {
                onSharedSlotFreed(bucketKey, currentSharedLimit(bucketKey));
            }
        } else {
            sendReleaseUnusedGrant(owner, bucketKey, reservedPermitId, self);
        }
    }

    // A Releasable for a reserved permit, releasing the same way a normal acquired permit does: locally if this node
    // owns the bucket, else via a RELEASE RPC. Both paths re-drive owner-push (releaseLocal directly; releaseRemote via
    // the owner's RELEASE handler), so returning an unused grant flows the slot to the next waiter.
    private Releasable reservedPermit(DiscoveryNode owner, String bucketKey, int sharedLimit, String permitId) {
        if (owner == null) {
            return releaseOnce(() -> {}); // ring empty; nothing to release remotely
        }
        if (owner.getId().equals(clusterService.localNode().getId())) {
            return releaseLocal(bucketKey, permitId);
        }
        return releaseRemote(owner, bucketKey, permitId);
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

    // Package-private accessor for tests: number of coordinators currently registered as waiters on a bucket.
    int waiterCountForTest(String bucketKey) {
        return waiterCount(bucketKey);
    }

    /**
     * Shared body helpers for the RPC types below. All three serialize their body as ONE count-prefixed, name-keyed map
     * rather than as positional fields, so a peer built from a different commit of the same release stays interoperable.
     * OpenSearch transport serde is otherwise positional, and a {@code Version} gate cannot separate two builds that
     * report the same version — which is exactly what a blue/green deployment produces.
     * <p>
     * <b>The rule for reading depends on when a field was introduced, not on what the field does.</b>
     * <ul>
     *   <li><b>Baseline fields</b> — the ones below, present since this map format was introduced. Every build that speaks
     *       this protocol sends them, so absence means genuine corruption or a truly incompatible peer, never ordinary
     *       version skew. Read them with {@code require*}: a throw happens while the transport layer is decoding, before
     *       any handler runs, so it becomes a clean error response — exactly what the positional format already did for a
     *       malformed message. Strictness therefore costs nothing and preserves the existing behavior.</li>
     *   <li><b>Fields added after this point</b> — may be read strictly ONLY where the sender is guaranteed to have sent
     *       them, which in practice means gating the read on the stream's version. That version is the NEGOTIATED MINIMUM
     *       of the two nodes ({@code Version.min}, see NativeOutboundHandler), not the sender's alone — which is what
     *       makes the gate safe here: min is never above the sender, so a peer that predates the field always takes the
     *       else branch and the strict read never runs.
     *       <pre>
     *       if (in.getVersion().onOrAfter(V_X)) {{@code newField = requireString(body, KEY_NEW);}}
     *       else {{@code newField = SOME_DEFAULT;}}
     *       </pre>
     *       That is preferable to an unconditional default where it applies, because it still catches a peer that should
     *       have sent the field. It does NOT apply to the case this format exists for: two builds reporting the SAME
     *       version, one with the field and one without. A version gate cannot separate those, so a field that can appear
     *       in same-version skew must be read with a safe default — otherwise it throws on EVERY message from EVERY older
     *       build for the whole rollout window, systematically, discarding the benefit of this format.</li>
     * </ul>
     * Values are generic, so a future field keeps its natural type. Stick to types in {@code StreamOutput.WRITERS}
     * (String, Integer, Long, Boolean, List, Map): a value whose type an older peer's {@code readGenericValue} does not
     * know would throw while decoding, defeating the tolerance.
     */
    private static Map<String, Object> readBody(StreamInput in) throws IOException {
        return in.readMap(StreamInput::readString, StreamInput::readGenericValue);
    }

    // --- baseline fields: strict, matching what the positional format did for a malformed message ---

    private static String requireString(Map<String, Object> body, String key) {
        final Object value = body.get(key);
        if (value instanceof String s) {
            return s;
        }
        throw new IllegalStateException("wlm shared-throttle: key [" + key + "] missing or not a String [" + value + "]");
    }

    private static Number requireNumber(Map<String, Object> body, String key) {
        final Object value = body.get(key);
        if (value instanceof Number n) {
            return n;
        }
        throw new IllegalStateException("wlm shared-throttle: key [" + key + "] missing or not a Number [" + value + "]");
    }

    private static boolean requireBoolean(Map<String, Object> body, String key) {
        final Object value = body.get(key);
        if (value instanceof Boolean b) {
            return b;
        }
        throw new IllegalStateException("wlm shared-throttle: key [" + key + "] missing or not a Boolean [" + value + "]");
    }

    // --- fields added after the map format shipped: optional, with a safe default. See the rule above. ---
    //
    // Queueing added four such fields (requesting_node_id / wants_queue on acquire, shared_limit /
    // queue_empty_on_node_id on release). A peer predating queueing sends none of them, so reading them strictly would
    // throw on every message from every such node. Each default reproduces the pre-queueing behaviour exactly.

    private static String optionalString(Map<String, Object> body, String key, String fallback) {
        final Object value = body.get(key);
        if (value instanceof String s) {
            return s;
        }
        if (value != null) {
            logger.debug("wlm shared-throttle: key [{}] is not a String ([{}]); using [{}]", key, value, fallback);
        }
        return fallback;
    }

    private static long optionalLong(Map<String, Object> body, String key, long fallback) {
        final Object value = body.get(key);
        if (value instanceof Number n) {
            return n.longValue();
        }
        if (value != null) {
            logger.debug("wlm shared-throttle: key [{}] is not a Number ([{}]); using [{}]", key, value, fallback);
        }
        return fallback;
    }

    private static boolean optionalBoolean(Map<String, Object> body, String key, boolean fallback) {
        final Object value = body.get(key);
        if (value instanceof Boolean b) {
            return b;
        }
        if (value != null) {
            logger.debug("wlm shared-throttle: key [{}] is not a Boolean ([{}]); using [{}]", key, value, fallback);
        }
        return fallback;
    }

    /**
     * {@code coord -> owner}. Acquire RPC: a coordinator asks the bucket's owner to admit one request under
     * {@code sharedLimit}.
     */
    public static class AcquirePermitRequest extends TransportRequest {
        static final String KEY_BUCKET = "bucket_key";
        static final String KEY_SHARED_LIMIT = "shared_limit";
        static final String KEY_PERMIT_ID = "permit_id";
        static final String KEY_TTL_NANOS = "ttl_nanos";
        static final String KEY_REQUESTING_NODE_ID = "requesting_node_id";
        static final String KEY_WANTS_QUEUE = "wants_queue";

        final String bucketKey;
        final int sharedLimit;
        final String permitId;
        final long ttlNanos;
        /**
         * Owner-push: who is asking, and whether they will park the request on denial (so the owner should register a
         * waiter and later push a grant). Both are ADDED fields — read with {@code optional*} and a safe default, never
         * {@code require*}, because a peer predating queueing does not send them (see the read policy above).
         * <p>
         * Only the coordinator's PERSISTENT node id travels, not the {@link DiscoveryNode}: a DiscoveryNode is not in the
         * {@code writeGenericValue} registry so it cannot ride in the map, and the owner does not need the object on the
         * wire — it resolves the live node from cluster state via {@code DiscoveryNodes.get(nodeId)}, which is O(1) and
         * indexed on exactly this id. The owner needs the resolved node (not just an identity) because {@code sendGrant}
         * addresses it. Empty means "not an owner-push acquire".
         */
        final String requestingNodeId;
        final boolean wantsQueue;

        // Convenience for callers/tests that don't use owner-push (no waiter registration on denial).
        AcquirePermitRequest(String bucketKey, int sharedLimit, String permitId, long ttlNanos) {
            this(bucketKey, sharedLimit, permitId, ttlNanos, "", false);
        }

        AcquirePermitRequest(
            String bucketKey,
            int sharedLimit,
            String permitId,
            long ttlNanos,
            String requestingNodeId,
            boolean wantsQueue
        ) {
            this.bucketKey = bucketKey;
            this.sharedLimit = sharedLimit;
            this.permitId = permitId;
            this.ttlNanos = ttlNanos;
            this.requestingNodeId = requestingNodeId == null ? "" : requestingNodeId;
            this.wantsQueue = wantsQueue;
        }

        AcquirePermitRequest(StreamInput in) throws IOException {
            super(in);
            final Map<String, Object> body = readBody(in);
            this.bucketKey = requireString(body, KEY_BUCKET);
            this.sharedLimit = requireNumber(body, KEY_SHARED_LIMIT).intValue();
            this.permitId = requireString(body, KEY_PERMIT_ID);
            this.ttlNanos = requireNumber(body, KEY_TTL_NANOS).longValue();
            // Added by queueing, so optional: a peer predating it sends neither key. The defaults are the pre-queueing
            // behaviour — no owner-push, no waiter registered — so an older peer's acquire is handled exactly as before.
            this.requestingNodeId = optionalString(body, KEY_REQUESTING_NODE_ID, "");
            this.wantsQueue = optionalBoolean(body, KEY_WANTS_QUEUE, false);
            // Any other key is a field this build does not know about: ignored on purpose. That is the tolerance.
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(
                Map.of(
                    KEY_BUCKET,
                    bucketKey,
                    KEY_SHARED_LIMIT,
                    sharedLimit,
                    KEY_PERMIT_ID,
                    permitId,
                    KEY_TTL_NANOS,
                    ttlNanos,
                    KEY_REQUESTING_NODE_ID,
                    requestingNodeId,
                    KEY_WANTS_QUEUE,
                    wantsQueue
                ),
                StreamOutput::writeString,
                StreamOutput::writeGenericValue
            );
        }
    }

    /**
     * {@code owner -> coord}. Acquire RPC response: whether the owner granted a shared permit.
     */
    public static class AcquirePermitResponse extends TransportResponse {
        static final String KEY_GRANTED = "granted";

        final boolean granted;

        AcquirePermitResponse(boolean granted) {
            this.granted = granted;
        }

        AcquirePermitResponse(StreamInput in) throws IOException {
            final Map<String, Object> body = readBody(in);
            this.granted = requireBoolean(body, KEY_GRANTED);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(Map.of(KEY_GRANTED, granted), StreamOutput::writeString, StreamOutput::writeGenericValue);
        }
    }

    /**
     * {@code coord -> owner}. Release RPC: fire-and-forget, tells the owner to drop a previously-granted permit.
     */
    public static class ReleasePermitRequest extends TransportRequest {
        static final String KEY_BUCKET = "bucket_key";
        static final String KEY_PERMIT_ID = "permit_id";
        static final String KEY_QUEUE_EMPTY_ON_NODE_ID = "queue_empty_on_node_id";

        final String bucketKey;
        final String permitId;
        /**
         * Set to the coordinator's PERSISTENT node id only when this release is returning an UNUSED grant: the owner then
         * deregisters that coordinator from the bucket's waiter set, since it has no queued request. Empty for a normal
         * release, and empty is also the default when an older peer omits the key.
         * <p>
         * Persistent node id rather than a {@link DiscoveryNode} for the same reason as {@code requestingNodeId} above,
         * and rather than an ephemeralId so that registration and deregistration key the waiter set on ONE identity.
         */
        final String queueEmptyOnNodeId;

        // Convenience for a normal release (not returning an unused grant).
        ReleasePermitRequest(String bucketKey, String permitId) {
            this(bucketKey, permitId, "");
        }

        ReleasePermitRequest(String bucketKey, String permitId, String queueEmptyOnNodeId) {
            this.bucketKey = bucketKey;
            this.permitId = permitId;
            this.queueEmptyOnNodeId = queueEmptyOnNodeId == null ? "" : queueEmptyOnNodeId;
        }

        ReleasePermitRequest(StreamInput in) throws IOException {
            super(in);
            final Map<String, Object> body = readBody(in);
            this.bucketKey = requireString(body, KEY_BUCKET);
            this.permitId = requireString(body, KEY_PERMIT_ID);
            // Added by queueing, so optional; empty means "not an unused-grant return", which is the pre-queueing
            // behaviour. Note the bucket's shared limit is deliberately NOT on the wire: the owner resolves it from its
            // own cluster state, and decides whether to drive owner-push from whether a permit was really removed.
            this.queueEmptyOnNodeId = optionalString(body, KEY_QUEUE_EMPTY_ON_NODE_ID, "");
            // Any other key is a field this build does not know about: ignored on purpose. That is the tolerance.
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(
                Map.of(KEY_BUCKET, bucketKey, KEY_PERMIT_ID, permitId, KEY_QUEUE_EMPTY_ON_NODE_ID, queueEmptyOnNodeId),
                StreamOutput::writeString,
                StreamOutput::writeGenericValue
            );
        }
    }

    /**
     * {@code owner -> coord}. Grant: a reserved shared permit for a bucket the coordinator is waiting on. Carries the
     * bucket's shared limit so a returned (unused) grant can re-drive owner-push toward the next waiter.
     */
    public static class GrantPermitRequest extends TransportRequest {
        // Born with the map format, so all three keys are baseline and read strictly.
        static final String KEY_BUCKET = "bucket_key";
        static final String KEY_SHARED_LIMIT = "shared_limit";
        static final String KEY_PERMIT_ID = "permit_id";

        final String bucketKey;
        final int sharedLimit;
        final String permitId;

        GrantPermitRequest(String bucketKey, int sharedLimit, String permitId) {
            this.bucketKey = bucketKey;
            this.sharedLimit = sharedLimit;
            this.permitId = permitId;
        }

        GrantPermitRequest(StreamInput in) throws IOException {
            super(in);
            final Map<String, Object> body = readBody(in);
            this.bucketKey = requireString(body, KEY_BUCKET);
            this.sharedLimit = requireNumber(body, KEY_SHARED_LIMIT).intValue();
            this.permitId = requireString(body, KEY_PERMIT_ID);
            // Any other key is a field this build does not know about: ignored on purpose. That is the tolerance.
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(
                Map.of(KEY_BUCKET, bucketKey, KEY_SHARED_LIMIT, sharedLimit, KEY_PERMIT_ID, permitId),
                StreamOutput::writeString,
                StreamOutput::writeGenericValue
            );
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
