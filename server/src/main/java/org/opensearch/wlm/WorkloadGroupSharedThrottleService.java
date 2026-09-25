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
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.UUIDs;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Setting;
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
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

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
 * share of buckets. An expired permit is reclaimed lazily by an acquire on the saturated bucket or by the owner's
 * periodic TTL sweep; the sweep also offers reclaimed capacity to registered waiters.
 */
@ExperimentalApi
public class WorkloadGroupSharedThrottleService implements ClusterStateListener {

    /** Internal transport action names (not client-facing REST). */
    public static final String ACQUIRE_ACTION_NAME = "internal:wlm/throttle/shared/acquire";
    public static final String RELEASE_ACTION_NAME = "internal:wlm/throttle/shared/release";
    // Owner -> coordinator: a shared slot freed and this coordinator has a registered waiter for the bucket; here is a
    // reserved permit to admit one queued request against (queueing / owner-push tier).
    public static final String GRANT_ACTION_NAME = "internal:wlm/throttle/shared/grant";
    // Coordinator -> new owner after the consistent-hash ring changes: restore this coordinator's waiter membership
    // for a retained bucket and immediately use any capacity already available on the new owner.
    public static final String RE_REGISTER_WAITER_AFTER_OWNER_CHANGE_ACTION_NAME =
        "internal:wlm/throttle/shared/re_register_waiter_after_owner_change";

    // Fixed operational constants. Deliberately not cluster settings: they are internal-coordination knobs an operator
    // would never need to tune, and fail-open + a generous TTL make them safe as constants. Promote to a setting later
    // (non-breaking) only if a real deployment need emerges.
    //
    // Timeout for the acquire round-trip to a bucket's owner. Small so a slow/unreachable owner fails open quickly
    // rather than adding latency to the request path.
    static final TimeValue ACQUIRE_TIMEOUT = TimeValue.timeValueMillis(200);
    // A pushed grant is off the request hot path: its request is already retained on the coordinator. Give a temporarily
    // paused coordinator substantially longer to respond than ACQUIRE, while still reclaiming the reservation promptly
    // enough that one unresponsive waiter cannot hold a shared slot for the permit TTL.
    static final TimeValue GRANT_TIMEOUT = TimeValue.timeValueSeconds(5);
    // Time-to-live for a permit on the owner. Reclaims a permit left behind by a crashed coordinator or a lost release
    // RPC. Set generously above the typical request duration so a still-running request's permit is not reclaimed early.
    // Accepted tradeoff: permits are NOT renewed, so a search that runs longer than this TTL has its permit reclaimed
    // while still executing, which can transiently admit one-or-more requests beyond shared_limit for that bucket
    // (self-correcting, fail-open direction — same flavor as the ring-rebalance transient breach). Long-running search
    // is uncommon (no default search timeout, but heavy aggs/scripts can exceed it); if it becomes a problem the fix is
    // permit renewal or deriving the TTL from a request deadline, not a larger constant.
    static final long PERMIT_TTL_NANOS = TimeValue.timeValueMinutes(5).nanos();
    // How often the owner sweeps expired permits. Originally pure memory hygiene, on the reasoning that a saturated
    // tryAcquire() prunes once minExpiry says a record may have expired, so an active bucket self-heals.
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
    // coordinator process incarnations that have at least one retained request for the bucket. A set (not a count) makes
    // registration idempotent per exact DiscoveryNode — repeated acquires from one incarnation do not inflate it. A
    // stale and replacement incarnation can coexist during topology convergence, so this is not strictly bounded by the
    // current node count. INSERTION-ORDERED (LinkedHashSet) so a grant rotates
    // the chosen coordinator to the tail (see pickAndRotateEligibleWaiter): membership persists across a successful
    // hand-off (the coordinator may still have more queued requests), and successive freed slots round-robin fairly
    // across coordinators instead of repeatedly serving whichever one hashes first. A coordinator is removed when a
    // grant comes back unused, when an offer selects it while disconnected, when a failed GRANT has no fresh
    // re-registration, or when this node loses ownership. Until one of those events, stale incarnations may remain.
    // No request identity is held (the requests live on their coordinators).
    // LinkedHashSet is NOT thread-safe, so every access — read, size, add, remove, rotate — MUST go through
    // compute/computeIfPresent on this map, whose per-key exclusive remapping is the sole lock guarding the inner set.
    private final Map<String, LinkedHashSet<DiscoveryNode>> waitersByBucket = new ConcurrentHashMap<>();
    // OWNER-SIDE transient delivery state. During one uninterrupted ownership tenure, at most one remote GRANT may await
    // a response for the same bucket and exact coordinator process incarnation. Without this fence a bulk capacity event
    // can rotate back to one waiter and send several grants before the first response arrives; if that waiter is paused,
    // it can reserve every shared slot.
    //
    // This is deliberately not a permit ledger: the response handler already closes over the permit id. Each value is
    // only a unique callback token plus whether a fresh denial re-registered the coordinator while its grant was
    // pending. Ownership cleanup cannot cancel an already-sent transport request, so if the bucket maps away and back an
    // older hand-off may briefly coexist with a newer one. The token prevents the older callback from clearing the newer
    // marker. On failure, an unchanged registration is removed before the permit is released; a fresh registration is
    // preserved because it is evidence that the coordinator has resumed communicating.
    private final Map<PendingGrantKey, PendingGrant> grantsAwaitingResponse = new ConcurrentHashMap<>();

    // COORDINATOR-SIDE: how a pushed grant is turned into an admitted queued request. Late-bound (the queue service is
    // constructed after this service); returns true if a queued request was admitted with the reserved permit, false
    // if there was none (this service then returns the reserved slot to the owner). Null before wiring => no grant is
    // ever consumed (a received grant is returned), which is safe.
    private volatile GrantConsumer grantConsumer;
    // COORDINATOR-SIDE: snapshot of bucket keys with retained PENDING_ACQUIRE or WAITING requests. Late-bound with the
    // queue service, just like grantConsumer. Null before wiring means an early ring update has no queue to reconcile.
    private volatile Supplier<Set<String>> retainedBucketKeysSupplier;

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
        transportService.registerRequestHandler(
            RE_REGISTER_WAITER_AFTER_OWNER_CHANGE_ACTION_NAME,
            ThreadPool.Names.SAME,
            ReRegisterWaiterAfterOwnerChangeRequest::new,
            (request, channel, task) -> channel.sendResponse(handleReRegisterWaiterAfterOwnerChange(request))
        );
    }

    /** Starts the periodic TTL sweep. Idempotent-safe to call once at node start. */
    public void start() {
        // The ring is populated by clusterChanged (see the constructor), not here: at node start the initial cluster
        // state may not be applied yet, so reading clusterService.state() here can throw "initial cluster state not
        // set yet". This method only schedules expiry reclamation and the corresponding owner-push recovery.
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
        final ThrottleOwnerSelector previousRing = ring.get();
        if (previousRing.eligibleNodeSet().equals(candidate) == false) {
            final ThrottleOwnerSelector currentRing = ThrottleOwnerSelector.fromDiscoveryNodes(event.state().nodes());
            ring.set(currentRing);

            // This node is no longer authoritative for waiters on buckets that moved away. Drop that owner-only routing
            // state immediately. Old permit records deliberately remain until release/TTL; while ownership is elsewhere,
            // the isStillOwner fence prevents them from creating another permit.
            removeWaitersForLostOwnership(currentRing);

            // Every coordinator owns the authoritative request queue for searches that arrived there. Re-register only
            // its retained buckets whose exact owner incarnation changed. Never enumerate queues or send transport
            // requests on the cluster-applier thread.
            scheduleWaiterReRegistrationAfterOwnerChange(previousRing, currentRing);
        }

        // A numeric shared_limit increase creates several slots without any corresponding release RPC. Offer exactly
        // the added capacity from this configuration-change hook; ordinary request completion remains a one-slot event
        // and therefore never runs a fill-to-limit loop on the hot release path.
        scheduleSharedLimitIncreaseDrives(event);
    }

    private void removeWaitersForLostOwnership(ThrottleOwnerSelector currentRing) {
        for (String bucketKey : waitersByBucket.keySet()) {
            if (isLocalOwner(currentRing, bucketKey) == false) {
                waitersByBucket.remove(bucketKey);
            }
        }
        // A callback for a hand-off sent during an earlier ownership tenure must not remove a waiter if this bucket
        // quickly maps back here and the coordinator re-registers. Clearing the marker makes that callback a no-op.
        // The old permit remains recorded
        // until release/TTL; if this process regains the bucket first, it may temporarily count against the live limit.
        grantsAwaitingResponse.keySet().removeIf(key -> isLocalOwner(currentRing, key.bucketKey) == false);
    }

    private void scheduleWaiterReRegistrationAfterOwnerChange(ThrottleOwnerSelector previousRing, ThrottleOwnerSelector currentRing) {
        final Supplier<Set<String>> supplier = retainedBucketKeysSupplier;
        if (supplier == null) {
            return;
        }
        try {
            threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
                final Set<String> retainedBucketKeys;
                try {
                    retainedBucketKeys = supplier.get();
                } catch (Exception e) {
                    logger.warn("Failed to snapshot retained buckets after a shared-throttle owner-ring change", e);
                    return;
                }
                for (String bucketKey : retainedBucketKeys) {
                    try {
                        final DiscoveryNode previousOwner = previousRing.ownerFor(bucketKey).orElse(null);
                        final DiscoveryNode currentOwner = currentRing.ownerFor(bucketKey).orElse(null);
                        if (currentOwner == null || Objects.equals(previousOwner, currentOwner)) {
                            continue;
                        }
                        // A later cluster state may already have superseded this task. Its own listener invocation will
                        // reconcile against the newer ring, so never send a stale registration to an intermediate owner.
                        if (Objects.equals(ring.get().ownerFor(bucketKey).orElse(null), currentOwner) == false) {
                            continue;
                        }
                        if (currentSharedLimit(bucketKey) == WorkloadGroupThrottleSettings.UNSET_LIMIT) {
                            continue; // node-only queue, deleted group, or shared tier removed in the same state
                        }
                        reRegisterWaiterAfterOwnerChange(bucketKey, currentOwner);
                    } catch (Exception e) {
                        // One malformed/deleted bucket or one send failure must not prevent reconciliation of every
                        // retained bucket that follows it in this snapshot.
                        logger.debug("Failed to re-register shared-throttle waiter for bucket [" + bucketKey + "] after owner change", e);
                    }
                }
            });
        } catch (Exception e) {
            // Shutdown can reject the executor hand-off. Queued tasks are already being cancelled during shutdown, and
            // a later request/ring change is the normal best-effort recovery if the node remains alive.
            logger.debug("Could not schedule shared-throttle waiter re-registration after an owner-ring change", e);
        }
    }

    private void scheduleSharedLimitIncreaseDrives(ClusterChangedEvent event) {
        if (event.state().metadata() == null || event.previousState().metadata() == null) {
            return;
        }
        final Map<String, WorkloadGroup> currentGroups = event.state().metadata().workloadGroups();
        final Map<String, WorkloadGroup> previousGroups = event.previousState().metadata().workloadGroups();
        if (currentGroups == null || previousGroups == null) {
            return;
        }

        final Map<String, Integer> addedSlotsByGroup = new HashMap<>();
        for (Map.Entry<String, WorkloadGroup> entry : currentGroups.entrySet()) {
            final WorkloadGroup previousGroup = previousGroups.get(entry.getKey());
            if (previousGroup == null) {
                continue;
            }
            if (queueDrainSupersedesLimitIncrease(previousGroup, entry.getValue())) {
                continue;
            }
            final int previousLimit = sharedLimit(previousGroup);
            final int currentLimit = sharedLimit(entry.getValue());
            // Adding the shared tier is handled by the live-config queue drain. This path is only for a numeric increase
            // while the tier remains configured.
            if (previousLimit != WorkloadGroupThrottleSettings.UNSET_LIMIT && currentLimit > previousLimit) {
                addedSlotsByGroup.put(entry.getKey(), currentLimit - previousLimit);
            }
        }
        if (addedSlotsByGroup.isEmpty()) {
            return;
        }

        try {
            threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
                // Snapshot the keys because grants and unused-grant returns can concurrently prune the waiter map.
                for (String bucketKey : Set.copyOf(waitersByBucket.keySet())) {
                    final Integer addedSlots = addedSlotsByGroup.get(groupIdOf(bucketKey));
                    if (addedSlots == null || isStillOwner(bucketKey) == false) {
                        continue;
                    }
                    try {
                        offerSharedSlots(bucketKey, addedSlots);
                    } catch (Exception e) {
                        // One malformed/deleted bucket or one transport failure must not prevent the hook from offering
                        // added capacity to every other affected bucket.
                        logger.debug("Failed to offer added shared capacity for bucket [" + bucketKey + "]", e);
                    }
                }
            });
        } catch (Exception e) {
            // Shutdown can reject the executor hand-off. No safety invariant depends on filling newly-added capacity;
            // later arrivals can consume it directly, and ordinary releases continue draining queued demand.
            logger.debug("Could not schedule shared-throttle grants after a shared_limit increase", e);
        }
    }

    // Keep this numeric-increase hook disjoint from WorkloadGroupService's run-all policy. When that policy applies,
    // queued requests are admitted untracked and pushing shared permits at the old queue concurrently is unnecessary.
    private static boolean queueDrainSupersedesLimitIncrease(WorkloadGroup previousGroup, WorkloadGroup currentGroup) {
        if (previousGroup.getResiliencyMode() != MutableWorkloadGroupFragment.ResiliencyMode.MONITOR
            && currentGroup.getResiliencyMode() == MutableWorkloadGroupFragment.ResiliencyMode.MONITOR) {
            return true;
        }
        if (Objects.equals(throttleAttribute(previousGroup), throttleAttribute(currentGroup)) == false) {
            return true;
        }
        return tierConfigured(previousGroup, WorkloadGroupThrottleSettings.NODE_LIMIT) != tierConfigured(
            currentGroup,
            WorkloadGroupThrottleSettings.NODE_LIMIT
        );
    }

    private static String throttleAttribute(WorkloadGroup workloadGroup) {
        return WorkloadGroupThrottleSettings.ATTRIBUTE.get(workloadGroup.getMutableWorkloadGroupFragment().getThrottling());
    }

    private static boolean tierConfigured(WorkloadGroup workloadGroup, Setting<Integer> limitSetting) {
        return limitSetting.get(
            workloadGroup.getMutableWorkloadGroupFragment().getThrottling()
        ) != WorkloadGroupThrottleSettings.UNSET_LIMIT;
    }

    private static int sharedLimit(WorkloadGroup workloadGroup) {
        return WorkloadGroupThrottleSettings.SHARED_LIMIT.get(workloadGroup.getMutableWorkloadGroupFragment().getThrottling());
    }

    private static String groupIdOf(String bucketKey) {
        final int idx = bucketKey.indexOf(':');
        return idx < 0 ? bucketKey : bucketKey.substring(0, idx);
    }

    private boolean isStillOwner(String bucketKey) {
        return isLocalOwner(ring.get(), bucketKey);
    }

    private boolean isLocalOwner(ThrottleOwnerSelector ownerRing, String bucketKey) {
        final DiscoveryNode localNode = clusterService.localNode();
        return localNode != null && ownerRing.ownerFor(bucketKey).filter(localNode::equals).isPresent();
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
     * transport error. Transport failures are handled internally and reported as {@code onResponse(null)}; under normal
     * operation only an actual owner denial reaches {@code onFailure}.
     * The listener may be invoked inline (local-owner short-circuit) or on a transport thread.
     */
    public void acquireAsync(String bucketKey, int sharedLimit, ActionListener<Releasable> listener) {
        acquireAsync(bucketKey, sharedLimit, listener, null);
    }

    /**
     * As {@link #acquireAsync(String, int, ActionListener)}, but for a caller that has already retained the exact request
     * as PENDING_ACQUIRE before this RPC. On denial, the owner registers this coordinator as a bucket waiter so a later
     * freed slot is pushed back here as a grant, and the coordinator then transitions the request to WAITING. Passing a
     * non-null {@code onNoWaiterRegistered} requests registration and provides the required terminal path when the owner
     * cannot register demand.
     * <p>
     * {@code onNoWaiterRegistered} runs INSTEAD of {@link ActionListener#onFailure} when the acquire was denied and the
     * owner reported that it could not register this coordinator — see {@link AcquirePermitResponse#registered}. No grant
     * will ever arrive in that case, and the retained request has no future wake-up, so the caller must fail it rather
     * than wait. Every other outcome is reported through {@code listener} exactly as documented above.
     *
     * @param onNoWaiterRegistered run on a denial the owner could not register; {@code null} to not request registration
     */
    public void acquireAsync(
        String bucketKey,
        int sharedLimit,
        ActionListener<Releasable> listener,
        @Nullable Runnable onNoWaiterRegistered
    ) {
        final boolean wantsQueue = onNoWaiterRegistered != null;
        final ThrottleOwnerSelector currentRing = ring.get();
        final DiscoveryNode owner = currentRing.ownerFor(bucketKey).orElse(null);
        if (owner == null) {
            listener.onResponse(null); // empty ring -> fail open
            return;
        }

        final String permitId = permitId();
        final long ttlNanos = PERMIT_TTL_NANOS;

        // Local-owner short-circuit: this coordinator owns the bucket, so hit the tracker directly with no network hop.
        if (owner.equals(clusterService.localNode())) {
            // The ring can change after owner was read above. Treat a local node that has already lost ownership exactly
            // like a remote stale owner: do not create a permit, and do not promise a waiter registration.
            if (isStillOwner(bucketKey) == false) {
                if (wantsQueue) {
                    onNoWaiterRegistered.run();
                } else {
                    listener.onFailure(deniedMarker());
                }
                return;
            }
            if (tracker.tryAcquire(bucketKey, sharedLimit, permitId, ttlNanos)) {
                // Close the race where the ring changed after the pre-check but before the tracker mutation. Do not
                // expose a newly-created former-owner permit; reclaim it and make the caller retry through the new owner.
                if (isStillOwner(bucketKey) == false) {
                    tracker.release(bucketKey, permitId);
                    if (wantsQueue) {
                        onNoWaiterRegistered.run();
                    } else {
                        listener.onFailure(deniedMarker());
                    }
                    return;
                }
                listener.onResponse(releaseLocal(bucketKey, permitId));
            } else {
                // Local owner: the waiter is this very node, always resolvable and always "connected" (nodeConnected
                // short-circuits on the local node), so registration cannot fail the way the remote path's can.
                if (wantsQueue) {
                    if (registerWaiter(bucketKey, clusterService.localNode()) == false) {
                        onNoWaiterRegistered.run();
                        return;
                    }
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
                    } else if (wantsQueue && response.registered == false) {
                        // Denied, and the owner cannot push us a grant (we are absent from its applied cluster state, or
                        // present but unreachable, or it predates queueing). Waiting would strand the retained request.
                        logger.debug(
                            "Owner [{}] denied bucket [{}] without registering this node as a waiter; failing the retained request",
                            owner.getId(),
                            bucketKey
                        );
                        onNoWaiterRegistered.run();
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
                    // Send a normal exact-id release. If the permit never existed this is a no-op; if it did, the owner
                    // reclaims it and may offer the genuinely freed slot to another registered waiter.
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
        // A coordinator can send using a ring snapshot that became stale in flight. Once this node has applied the
        // owner change it must neither mint another permit nor register demand it no longer owns.
        if (isStillOwner(request.bucketKey) == false) {
            return new AcquirePermitResponse(false, false);
        }
        boolean granted = tracker.tryAcquire(request.bucketKey, request.sharedLimit, request.permitId, request.ttlNanos);
        if (granted && isStillOwner(request.bucketKey) == false) {
            // Ownership moved between the pre-check and tracker mutation. Reclaim locally and report that this stale
            // recipient neither granted nor registered the request.
            tracker.release(request.bucketKey, request.permitId);
            granted = false;
        }
        boolean registered = false;
        if (granted == false && request.wantsQueue && request.requestingNodeId.isEmpty() == false) {
            // Denied and the coordinator already retained the request -> remember it so a freed slot is pushed back as a
            // grant. A successful response lets the coordinator transition the exact PENDING_ACQUIRE entry to WAITING.
            // The RPC carries only the coordinator's persistent node id, so resolve it to the live DiscoveryNode here:
            // the waiter set must hold the real node because sendGrant needs its transport address.
            //
            // Register ONLY if we can actually reach that node, and tell the coordinator either way (the `registered` bit
            // on the response). Presence in cluster state alone is not enough, for two reasons, both rooted in this
            // node's applied state being able to lag the coordinator's:
            // - NOT PRESENT: a node that just joined (or left) resolves to null. Registering is impossible.
            // - PRESENT BUT STALE: a node that restarted keeps its persistent id but gets a NEW ephemeral id, and
            // DiscoveryNode identity IS the ephemeral id — so until this node applies the rejoin, get() hands back the
            // PREVIOUS incarnation. Registering that would look like success, then the first freed slot would find it
            // absent from the connection map, reclaim, and quietly deregister it (see offerSharedSlots).
            // nodeConnected covers both, plus a transient disconnect, in one lock-free connection-map lookup. (The
            // coordinator makes the mirror-image check about the OWNER before sending an acquire — same method, opposite
            // direction.) A coordinator told `registered=false` fails the request with the normal throttle 429 instead of
            // parking it for a grant that is never coming.
            final DiscoveryNode requestingNode = clusterService.state().nodes().get(request.requestingNodeId);
            registered = requestingNode != null
                && transportService.nodeConnected(requestingNode)
                && registerWaiter(request.bucketKey, requestingNode);
            if (registered == false) {
                logger.debug(
                    "Not registering waiter for bucket [{}]: this node no longer owns it, or node [{}] is not reachable "
                        + "(absent from the applied cluster state, or present but not connected)",
                    request.bucketKey,
                    request.requestingNodeId
                );
            }
        }
        return new AcquirePermitResponse(granted, registered);
    }

    // A local release path must also drive owner-push when this node owns the bucket: releasing frees a slot that a
    // registered waiter should get.
    private Releasable releaseLocal(String bucketKey, String permitId) {
        return releaseOnce(() -> {
            // Same rule as the remote path's handleRelease, so local-owner and remote-owner behave identically.
            if (tracker.release(bucketKey, permitId)) {
                offerSharedSlots(bucketKey, 1);
            }
        });
    }

    private Releasable releaseRemote(DiscoveryNode owner, String bucketKey, String permitId) {
        // The owner resolves the live shared limit from its own cluster state before replacing the freed slot.
        return releaseOnce(() -> sendRelease(owner, bucketKey, permitId, ""));
    }

    // Returns an UNUSED remote grant: a RELEASE tagged with this coordinator's node id so the owner also deregisters it
    // from the bucket's waiter set before re-driving owner-push to the next waiter.
    private void sendReleaseUnusedGrant(DiscoveryNode owner, String bucketKey, String permitId, DiscoveryNode self) {
        sendRelease(owner, bucketKey, permitId, self.getId());
    }

    // Fire-and-forget RELEASE RPC to the bucket owner. Bounded by the same timeout as acquire so a half-open
    // connection can't leave the response handler pending until the connection is torn down. A lost release is not
    // fatal — the owner's TTL sweep reclaims the permit — so failures are logged at debug only.
    // {@code queueEmptyOnNodeId} is set only when returning an unused grant, so the owner deregisters that coordinator;
    // empty for a normal release.
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
     * via this; before it is set, a grant is returned using the current owner ring. This normally reclaims the reserved
     * slot; after an ownership change the current owner may treat the old permit id as unknown, leaving the former
     * owner's record to expire.
     */
    public void setGrantConsumer(GrantConsumer grantConsumer) {
        this.grantConsumer = grantConsumer;
    }

    /**
     * Late-binds the coordinator-local retained-bucket snapshot used only after owner-ring changes. The supplier must
     * return a snapshot, since reconciliation runs asynchronously and the underlying queues continue to mutate.
     */
    public void setRetainedBucketKeysSupplier(Supplier<Set<String>> retainedBucketKeysSupplier) {
        this.retainedBucketKeysSupplier = Objects.requireNonNull(retainedBucketKeysSupplier);
    }

    /**
     * COORDINATOR-SIDE owner-change recovery. The request queue lives here, so after the ring remaps this node tells the
     * new owner that it still has demand for {@code bucketKey}. The call is asynchronous and best-effort: a later owner
     * change or denied acquire re-registers again if this attempt races cluster-state application or connectivity.
     */
    void reRegisterWaiterAfterOwnerChange(String bucketKey, DiscoveryNode newOwner) {
        if (Objects.equals(ring.get().ownerFor(bucketKey).orElse(null), newOwner) == false) {
            return; // superseded by a newer ring before this asynchronous reconciliation ran
        }
        final DiscoveryNode self = clusterService.localNode();
        final ReRegisterWaiterAfterOwnerChangeRequest request = new ReRegisterWaiterAfterOwnerChangeRequest(bucketKey, self.getId());
        if (newOwner.equals(self)) {
            handleReRegisterWaiterAfterOwnerChange(request);
            return;
        }
        if (transportService.nodeConnected(newOwner) == false) {
            logger.debug(
                "Cannot re-register waiter for bucket [{}] after owner change: new owner [{}] is not connected",
                bucketKey,
                newOwner.getId()
            );
            return;
        }
        final TransportRequestOptions options = TransportRequestOptions.builder().withTimeout(ACQUIRE_TIMEOUT).build();
        transportService.sendRequest(
            newOwner,
            RE_REGISTER_WAITER_AFTER_OWNER_CHANGE_ACTION_NAME,
            request,
            options,
            new TransportResponseHandler<ReRegisterWaiterAfterOwnerChangeResponse>() {
                @Override
                public ReRegisterWaiterAfterOwnerChangeResponse read(StreamInput in) throws IOException {
                    return new ReRegisterWaiterAfterOwnerChangeResponse(in);
                }

                @Override
                public void handleResponse(ReRegisterWaiterAfterOwnerChangeResponse response) {
                    if (response.registered == false) {
                        logger.debug(
                            "New owner [{}] did not re-register this node as a waiter for bucket [{}]",
                            newOwner.getId(),
                            bucketKey
                        );
                    }
                }

                @Override
                public void handleException(TransportException exp) {
                    logger.debug(
                        "Waiter re-registration to new owner [" + newOwner.getId() + "] for bucket [" + bucketKey + "] failed",
                        exp
                    );
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            }
        );
    }

    // NEW-OWNER SIDE: restore one coordinator's waiter membership and immediately try to use capacity that may already
    // be free. Registration alone is insufficient because an ownership change may produce no later release event to
    // trigger owner-push.
    ReRegisterWaiterAfterOwnerChangeResponse handleReRegisterWaiterAfterOwnerChange(ReRegisterWaiterAfterOwnerChangeRequest request) {
        if (isStillOwner(request.bucketKey) == false) {
            return new ReRegisterWaiterAfterOwnerChangeResponse(false);
        }
        final int sharedLimit = currentSharedLimit(request.bucketKey);
        if (sharedLimit == WorkloadGroupThrottleSettings.UNSET_LIMIT) {
            return new ReRegisterWaiterAfterOwnerChangeResponse(false);
        }
        final DiscoveryNode self = clusterService.localNode();
        final DiscoveryNode requestingNode = self.getId().equals(request.requestingNodeId)
            ? self
            : clusterService.state().nodes().get(request.requestingNodeId);
        if (requestingNode == null || (requestingNode.equals(self) == false && transportService.nodeConnected(requestingNode) == false)) {
            return new ReRegisterWaiterAfterOwnerChangeResponse(false);
        }
        if (registerWaiter(request.bucketKey, requestingNode) == false) {
            return new ReRegisterWaiterAfterOwnerChangeResponse(false);
        }
        // Offer up to the entire live limit so retained demand can resume without waiting for a release event that may
        // never arrive on this owner. tryAcquire enforces the actual availability, including any old records retained
        // if this same process previously owned and then regained the bucket.
        offerSharedSlots(request.bucketKey, sharedLimit);
        return new ReRegisterWaiterAfterOwnerChangeResponse(isStillOwner(request.bucketKey));
    }

    // OWNER-SIDE: record that a coordinator is waiting on a bucket this node owns. Idempotent (a set), so re-registering
    // an already-known waiter is a no-op — it keeps its existing queue position (LinkedHashSet.add does not reorder an
    // element already present), so re-registration can't let a coordinator jump the rotation. During topology
    // convergence, stale and replacement incarnations may coexist until selected while disconnected, unused-grant
    // cleanup, or ownership loss.
    private boolean registerWaiter(String bucketKey, DiscoveryNode coordinator) {
        if (isStillOwner(bucketKey) == false) {
            return false;
        }
        waitersByBucket.compute(bucketKey, (k, nodes) -> {
            if (nodes == null) {
                nodes = new LinkedHashSet<>();
            }
            nodes.add(coordinator);
            final PendingGrantKey pendingGrantKey = pendingGrantKey(bucketKey, coordinator);
            if (pendingGrantKey != null) {
                grantsAwaitingResponse.computeIfPresent(pendingGrantKey, (ignored, pendingGrant) -> {
                    pendingGrant.registeredAgain = true;
                    return pendingGrant;
                });
            }
            return nodes;
        });
        // Close the race where ownership moved after the pre-check but before insertion. If clusterChanged already
        // pruned before this insertion, this post-check performs the cleanup; if it runs afterwards, clusterChanged does.
        if (isStillOwner(bucketKey) == false) {
            removeWaiter(bucketKey, coordinator);
            return false;
        }
        return true;
    }

    /**
     * OWNER-SIDE release: free the permit, deregister the coordinator if it reported its queue for the bucket is empty,
     * and drive owner-push if a slot genuinely freed. Package-private and shared with the transport handler rather than
     * duplicated, so a test can drive it without the two copies drifting apart.
     * <p>
     * Both decisions are the OWNER's, not the coordinator's. Whether a slot freed comes from whether the remove actually
     * hit a recorded permit — a coordinator's release may be speculative (see the lost acquire-reply path), so it cannot
     * know. The ceiling is resolved from this node's cluster state, whose view is the one being enforced. Deciding here
     * means a no-op release never produces a phantom grant and — the case a coordinator-supplied hint got wrong — a
     * release that DID free a slot always drives one.
     */
    void handleRelease(ReleasePermitRequest request) {
        final boolean freed = tracker.release(request.bucketKey, request.permitId);
        // If this release is a coordinator returning an UNUSED grant (it had no queued request for the bucket), drop it
        // from the waiter set so it stops drawing wasted grants (remote analog of the local removeWaiter). Only trust the
        // signal when the permit id belonged to THIS tracker: a grant issued by the previous owner can cross a ring
        // change and be returned to the new owner, where its unknown id must not deregister a valid new-owner waiter.
        if (freed && request.queueEmptyOnNodeId.isEmpty() == false) {
            removeWaiterByNodeId(request.bucketKey, request.queueEmptyOnNodeId);
        }
        if (freed) {
            offerSharedSlots(request.bucketKey, 1);
        }
    }

    // OWNER-SIDE: drop a coordinator from a bucket's waiter set (it reported no more queued requests, was selected while
    // disconnected, or failed a grant without a fresh registration). DiscoveryNode equality uses the ephemeral id, so
    // this removes only the exact process incarnation and cannot erase a replacement node that reused the persistent id.
    private void removeWaiter(String bucketKey, DiscoveryNode coordinator) {
        waitersByBucket.computeIfPresent(bucketKey, (k, nodes) -> {
            nodes.remove(coordinator);
            return nodes.isEmpty() ? null : nodes;
        });
    }

    // Unlike removeWaiter's exact-incarnation removal, this is keyed on the coordinator's PERSISTENT node id because that
    // is all the RELEASE RPC carries. The scan is O(n); n is normally small, although stale process incarnations can
    // temporarily coexist until an offer or cleanup event observes them. Stays inside computeIfPresent because that
    // per-key remapping is the sole lock guarding the non-thread-safe LinkedHashSet, and removeIf preserves insertion
    // order so round-robin order survives.
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
     * was lost. For a bucket with no subsequent acquire, this sweep is the only place that free slot is observed.
     * Without the owner-push call a coordinator with a retained request stays registered while capacity sits idle and,
     * because WAITING has no queue timeout, remains stranded until some unrelated release re-drives the bucket.
     */
    void sweepExpiredAndDrive() {
        for (Map.Entry<String, Integer> freed : tracker.sweepExpiredCounts().entrySet()) {
            offerSharedSlots(freed.getKey(), freed.getValue());
        }
    }

    /**
     * The current {@code shared_limit} for a bucket, resolved from cluster state, or
     * {@link WorkloadGroupThrottleSettings#UNSET_LIMIT} if the group is gone or the shared tier is not configured. All
     * owner-push paths resolve the live value before reserving capacity, including releases, expiry sweeps, numeric limit
     * increases, and owner-change recovery. An unresolvable bucket simply skips owner-push rather than guessing a limit.
     */
    private int currentSharedLimit(String bucketKey) {
        final ClusterState state = clusterService.state();
        if (state == null || state.metadata() == null || state.metadata().workloadGroups() == null) {
            return WorkloadGroupThrottleSettings.UNSET_LIMIT;
        }
        final String groupId = groupIdOf(bucketKey);
        final WorkloadGroup workloadGroup = state.metadata().workloadGroups().get(groupId);
        if (workloadGroup == null) {
            return WorkloadGroupThrottleSettings.UNSET_LIMIT;
        }
        return WorkloadGroupThrottleSettings.SHARED_LIMIT.get(workloadGroup.getMutableWorkloadGroupFragment().getThrottling());
    }

    /**
     * OWNER-SIDE: offer a bounded number of newly-available shared slots to registered coordinators.
     * <p>
     * The caller supplies the event's exact slot budget:
     * <ul>
     *   <li>an ordinary permit return offers one;</li>
     *   <li>a {@code shared_limit} increase offers {@code newLimit - oldLimit} once from the cluster-change hook;</li>
     *   <li>a TTL sweep offers the number of permits reclaimed in that sweep;</li>
     *   <li>a newly-selected owner may offer up to the live limit because no later release may trigger a drive.</li>
     * </ul>
     * Thus normal request completion stays O(one grant); only the uncommon event that creates several slots runs a
     * bulk offer. The owner intentionally tracks only waiter membership, not remote queue depth, so a bulk event can
     * speculatively send excess grants to a shallow queue. Those are returned through the existing unused-grant path.
     * The speculative batch is capped by the maximum possible retained demand represented by the registered
     * coordinators, preventing an extreme configured limit from creating an unbounded message burst.
     */
    private void offerSharedSlots(String bucketKey, int slotsToOffer) {
        if (isStillOwner(bucketKey) == false) {
            // Old permit records are allowed to drain or expire after a remap. While this node is the former owner it
            // must never turn one into a new reservation. Prune any late waiter insertion left by an ownership race too.
            waitersByBucket.remove(bucketKey);
            return;
        }
        if (slotsToOffer <= 0) {
            return;
        }

        final int initialWaiterCount = waiterCount(bucketKey);
        if (initialWaiterCount == 0) {
            return;
        }
        // Every coordinator can retain at most MAX_GROUP_QUEUE_DEPTH requests for this group, across all buckets.
        // Multiplying by registered coordinators is therefore a conservative upper bound for this bucket's demand.
        final long maximumRepresentedDemand = (long) initialWaiterCount * WorkloadGroupQueueSettings.MAX_GROUP_QUEUE_DEPTH;
        final int offerBudget = (int) Math.min((long) slotsToOffer, Math.min(maximumRepresentedDemand, Integer.MAX_VALUE));

        // Absolute safety cap on total iterations to bound work and rule out livelock, while still allowing the loop to
        // react to waiters that REGISTER during it. A fixed snapshot of waiterCount is not enough: a stale local waiter
        // or a disconnected waiter frees the reserved slot without handing it off, and a coordinator can registerWaiter
        // concurrently. Successful hand-offs consume the finite offer budget; unsuccessful ones remove/reconcile a
        // waiter or stop, so the extra waiter-based allowance is only for stale-node cleanup.
        final long maxAttempts = Math.max(1L, (long) offerBudget + (long) initialWaiterCount * 2L + 8L);
        int offered = 0;
        for (long attempt = 0; attempt < maxAttempts && offered < offerBudget; attempt++) {
            final PendingGrantSelection selection = pickAndRotateEligibleWaiter(bucketKey);
            if (selection == null) {
                return; // no waiters, or every remote waiter already has an unacknowledged grant
            }
            final DiscoveryNode target = selection.coordinator;
            // Re-read the live limit for each speculative reservation. Bulk offers are uncommon, and this O(1)
            // cluster-state map lookup prevents an overlapping limit decrease from continuing to reserve against the
            // stale, higher ceiling that originally triggered the batch.
            final int sharedLimit = currentSharedLimit(bucketKey);
            if (sharedLimit == WorkloadGroupThrottleSettings.UNSET_LIMIT) {
                clearPendingGrant(selection);
                return;
            }
            final String reservedPermitId = permitId();
            // Reserve the freed slot so a concurrent acquire can't take it before the grant lands. If the bucket is at
            // its limit again (a racing acquire beat us), don't over-grant: stop. The waiter stays registered (peek did
            // not remove it), so the next release re-drives owner-push to it.
            if (tracker.tryAcquire(bucketKey, sharedLimit, reservedPermitId, PERMIT_TTL_NANOS) == false) {
                clearPendingGrant(selection);
                return;
            }
            if (isStillOwner(bucketKey) == false) {
                // Ownership moved between the fence at method entry and the reservation. Reclaim instead of handing a
                // fresh former-owner permit to either a local or remote coordinator.
                tracker.release(bucketKey, reservedPermitId);
                clearPendingGrant(selection);
                waitersByBucket.remove(bucketKey);
                return;
            }
            if (selection.pendingGrantKey == null) {
                final LocalGrantResult result = consumeGrantLocal(bucketKey, sharedLimit, reservedPermitId, target);
                if (result == LocalGrantResult.STOP) {
                    return;
                }
                if (result == LocalGrantResult.ADMITTED) {
                    offered++;
                }
                // NO_DEMAND released the reservation and removed this waiter, so retry the same event slot elsewhere.
            } else if (transportService.nodeConnected(target) == false) {
                // Disconnected waiter: reclaim the reserved slot, drop it, and loop to the next waiter — handled here
                // in the bounded loop rather than by recursing through sendGrant.
                removeWaiter(bucketKey, target);
                clearPendingGrant(selection);
                tracker.release(bucketKey, reservedPermitId);
                // fall through: slot freed but not handed off -> re-check
            } else {
                // pickAndRotateEligibleWaiter installed this coordinator/bucket's transient pending marker before the
                // reservation. A concurrent capacity event therefore skips this waiter until the response resolves.
                if (sendGrant(target, bucketKey, sharedLimit, reservedPermitId, selection)) {
                    offered++;
                }
            }
        }
    }

    private enum LocalGrantResult {
        ADMITTED,
        NO_DEMAND,
        STOP
    }

    // OWNER-SIDE: consume a grant for a LOCAL waiter without a network hop. The explicit result distinguishes a real
    // admission (consume one event slot) from an empty queue (release and retry that slot elsewhere) and an unexpected
    // dispatch failure (stop, because retrying could poll and drop more requests).
    //
    // Recursion safety: on ADMIT we hand a self-re-driving reservedPermit — but its close() fires later, at request
    // completion (async), so re-driving then is fine and not re-entrant. On the NO-request path we release the reserved
    // permit DIRECTLY (not by closing a re-driving permit), so the caller's bounded loop advances without recursion.
    private LocalGrantResult consumeGrantLocal(String bucketKey, int sharedLimit, String reservedPermitId, DiscoveryNode self) {
        final GrantConsumer consumer = grantConsumer;
        if (consumer == null) {
            tracker.release(bucketKey, reservedPermitId);
            removeWaiter(bucketKey, self);
            return LocalGrantResult.NO_DEMAND;
        }
        final DiscoveryNode owner = clusterService.localNode(); // local path: this node owns the bucket
        boolean admitted;
        try {
            admitted = consumer.admit(bucketKey, reservedPermit(owner, bucketKey, sharedLimit, reservedPermitId));
        } catch (Exception e) {
            // A throw means dispatch is unavailable (typically executor shutdown). Release the reserved slot and STOP the
            // loop — do NOT retry. admitWithPermit polls the head request before
            // admit() can throw, so a retry against the still-registered waiter would poll-and-drop a further request on
            // every iteration during a dispatch outage. The waiter stays registered; a later release re-drives it.
            logger.debug("Queue grant admit failed for bucket [" + bucketKey + "]", e);
            tracker.release(bucketKey, reservedPermitId);
            return LocalGrantResult.STOP;
        }
        if (admitted == false) {
            // admitWithPermit does not close the permit on a false return; release the reserved permit directly so the
            // slot is reused by this loop. (Not via the permit's close(), which would re-drive owner-push inline.)
            tracker.release(bucketKey, reservedPermitId);
            removeWaiter(bucketKey, self); // this coordinator has nothing queued for the bucket
            return LocalGrantResult.NO_DEMAND;
        }
        return LocalGrantResult.ADMITTED;
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

    // OWNER-SIDE: pick the first eligible waiter and rotate it to the tail. A local waiter is always eligible because it
    // is consumed synchronously. A remote waiter is eligible only if this method can atomically install its transient
    // "grant awaiting response" marker. Thus concurrent capacity events and one bulk event cannot send two unacknowledged
    // grants to the same coordinator/bucket. Re-registration while a grant is pending preserves membership but does not
    // clear the marker.
    private PendingGrantSelection pickAndRotateEligibleWaiter(String bucketKey) {
        final PendingGrantSelection[] picked = new PendingGrantSelection[1];
        waitersByBucket.computeIfPresent(bucketKey, (k, nodes) -> {
            final DiscoveryNode localNode = clusterService.localNode();
            for (DiscoveryNode candidate : nodes) {
                if (candidate.equals(localNode)) {
                    picked[0] = new PendingGrantSelection(candidate, null, null);
                    break;
                }
                final PendingGrantKey pendingGrantKey = pendingGrantKey(bucketKey, candidate);
                final PendingGrant pendingGrant = new PendingGrant();
                if (grantsAwaitingResponse.putIfAbsent(pendingGrantKey, pendingGrant) == null) {
                    picked[0] = new PendingGrantSelection(candidate, pendingGrantKey, pendingGrant);
                    break;
                }
            }
            if (picked[0] != null) {
                nodes.remove(picked[0].coordinator);       // detach from its current position...
                nodes.add(picked[0].coordinator);          // ...and re-append at the tail, keeping it registered
            }
            return nodes.isEmpty() ? null : nodes;
        });
        return picked[0];
    }

    private PendingGrantKey pendingGrantKey(String bucketKey, DiscoveryNode coordinator) {
        if (coordinator.equals(clusterService.localNode())) {
            return null;
        }
        return new PendingGrantKey(bucketKey, coordinator.getEphemeralId());
    }

    private void clearPendingGrant(PendingGrantSelection selection) {
        if (selection.pendingGrantKey != null) {
            grantsAwaitingResponse.remove(selection.pendingGrantKey, selection.pendingGrant);
        }
    }

    /**
     * Atomically resolves one failed GRANT against waiter registration for the same bucket. Selection and registration
     * use the same {@code waitersByBucket.compute*} lock, so no capacity event can select the coordinator between clearing
     * its pending marker and removing its unchanged registration.
     */
    private void resolveFailedGrant(PendingGrantSelection selection) {
        waitersByBucket.compute(selection.pendingGrantKey.bucketKey, (bucketKey, nodes) -> {
            if (grantsAwaitingResponse.remove(selection.pendingGrantKey, selection.pendingGrant) == false) {
                return nodes; // ownership loss or another terminal callback already resolved it
            }
            if (selection.pendingGrant.registeredAgain == false && nodes != null) {
                nodes.remove(selection.coordinator);
            }
            return nodes == null || nodes.isEmpty() ? null : nodes;
        });
    }

    // OWNER-SIDE: push one reserved permit to a waiting coordinator. The caller has already installed pendingGrantKey,
    // which remains until a transport terminal callback resolves it or ownership cleanup removes it. Returns whether the
    // asynchronous hand-off started.
    private boolean sendGrant(
        DiscoveryNode coordinator,
        String bucketKey,
        int sharedLimit,
        String reservedPermitId,
        PendingGrantSelection selection
    ) {
        // Ownership may have changed after offerSharedSlots reserved the permit. Reclaim locally and stop; the
        // coordinator-side cluster-change hook registers retained demand with the new owner.
        if (isStillOwner(bucketKey) == false) {
            tracker.release(bucketKey, reservedPermitId);
            clearPendingGrant(selection);
            waitersByBucket.remove(bucketKey);
            return false;
        }
        if (transportService.nodeConnected(coordinator) == false) {
            removeWaiter(bucketKey, coordinator); // it's disconnected; drop it from the set
            clearPendingGrant(selection);
            tracker.release(bucketKey, reservedPermitId); // waiter gone; reclaim immediately
            return false;
        }
        final TransportRequestOptions options = TransportRequestOptions.builder().withTimeout(GRANT_TIMEOUT).build();
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
                public void handleResponse(TransportResponse.Empty response) {
                    if (grantsAwaitingResponse.remove(selection.pendingGrantKey, selection.pendingGrant)) {
                        // A bulk capacity event may have stopped because this was the only waiter and it was pending.
                        // Continue one slot at a time; tracker.tryAcquire prevents this from exceeding the live limit.
                        offerSharedSlots(bucketKey, 1);
                    }
                }

                @Override
                public void handleException(TransportException exp) {
                    // No GRANT retry: delivery is ambiguous after a timeout. Resolve waiter membership BEFORE making the
                    // slot available, so an unchanged registration cannot be selected again in a failure loop. A denial
                    // that re-registered the coordinator while this grant was pending is preserved as evidence that it
                    // is responsive again. If the original grant arrives late, that timed-out hand-off can admit at most
                    // one extra request.
                    logger.debug(
                        "Shared throttle grant to [{}] for bucket [{}] failed; resolving waiter and reclaiming",
                        coordinator.getId(),
                        bucketKey
                    );
                    resolveFailedGrant(selection);
                    // Release by exact permit id even if an ownership change already cleared this callback's marker.
                    // The id cannot affect a newer hand-off, and reclaiming it avoids carrying an old reservation until
                    // TTL if this node later becomes the bucket owner again.
                    if (tracker.release(bucketKey, reservedPermitId)) {
                        offerSharedSlots(bucketKey, 1);
                    }
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            }
        );
        return true;
    }

    // COORDINATOR-SIDE: a grant arrived. Hand the reserved permit to the queue service to admit one retained request; if
    // there is none (or no consumer wired yet), return it through the explicit unused-grant release, which also asks the
    // current owner to deregister this coordinator before re-driving another waiter.
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

    // Returns an unused reserved slot to the owner selected by the coordinator's CURRENT ring, tagging the release so
    // that owner deregisters this coordinator and then re-drives owner-push. If ownership changed since the grant was
    // issued, the current owner treats an unknown id as a no-op; the former issuer's record remains until release/TTL.
    // If this node is the current owner, handle the return in-process.
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
            if (freed) {
                // As on the remote release path, an unknown grant id may belong to a previous owner and must not
                // deregister this node from the current owner's waiter set.
                removeWaiter(bucketKey, self);
                offerSharedSlots(bucketKey, 1);
            }
        } else {
            sendReleaseUnusedGrant(owner, bucketKey, reservedPermitId, self);
        }
    }

    // A Releasable for a CONSUMED grant. It releases to the owner snapshot selected from this coordinator's ring when the
    // grant was received: locally if that is this node, otherwise by RELEASE RPC. Unused grants bypass this wrapper and
    // use returnUnusedGrant so they can carry the waiter-deregistration signal.
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

    // Package-private accessor for tests: number of remote coordinator incarnations with an unacknowledged GRANT.
    int pendingGrantCountForTest(String bucketKey) {
        int count = 0;
        for (PendingGrantKey key : grantsAwaitingResponse.keySet()) {
            if (key.bucketKey.equals(bucketKey)) {
                count++;
            }
        }
        return count;
    }

    private static final class PendingGrant {
        // Accessed only while holding the corresponding waitersByBucket.compute* lock.
        private boolean registeredAgain;
    }

    private static final class PendingGrantSelection {
        private final DiscoveryNode coordinator;
        @Nullable
        private final PendingGrantKey pendingGrantKey;
        @Nullable
        private final PendingGrant pendingGrant;

        private PendingGrantSelection(
            DiscoveryNode coordinator,
            @Nullable PendingGrantKey pendingGrantKey,
            @Nullable PendingGrant pendingGrant
        ) {
            this.coordinator = coordinator;
            this.pendingGrantKey = pendingGrantKey;
            this.pendingGrant = pendingGrant;
        }
    }

    private static final class PendingGrantKey {
        private final String bucketKey;
        private final String coordinatorEphemeralId;

        private PendingGrantKey(String bucketKey, String coordinatorEphemeralId) {
            this.bucketKey = bucketKey;
            this.coordinatorEphemeralId = coordinatorEphemeralId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PendingGrantKey that = (PendingGrantKey) o;
            return bucketKey.equals(that.bucketKey) && coordinatorEphemeralId.equals(that.coordinatorEphemeralId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(bucketKey, coordinatorEphemeralId);
        }
    }

    /**
     * Shared body helpers for the RPC types below. They serialize their body as ONE count-prefixed, name-keyed map
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
    // Queueing added four such fields: requesting_node_id / wants_queue on the acquire request, registered on the
    // acquire response, and queue_empty_on_node_id on the release request. A peer predating queueing sends none of them,
    // so reading them strictly would throw on every message from every such node. Each default reproduces the
    // pre-queueing behaviour exactly.

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
         * Owner-push: who is asking, and whether the coordinator already retained the request as PENDING_ACQUIRE and
         * therefore needs waiter registration on denial. Both are ADDED fields — read with {@code optional*} and a safe
         * default, never {@code require*}, because a peer predating queueing does not send them (see the read policy above).
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
     * {@code owner -> coord}. Acquire RPC response: whether the owner granted a shared permit, and — when it did not —
     * whether it registered the asking coordinator for owner-push.
     */
    public static class AcquirePermitResponse extends TransportResponse {
        static final String KEY_GRANTED = "granted";
        static final String KEY_REGISTERED = "registered";

        final boolean granted;
        /**
         * Owner-push contract on a DENIAL: {@code true} means "I have you in this bucket's waiter set and I will push a
         * grant when a slot frees", {@code false} means "no grant will ever come from me" — so a coordinator that parked
         * the request must fail it rather than wait forever (there is no queue timeout).
         * <p>
         * Meaningful only when the acquire set {@code wants_queue}; it is {@code false} otherwise, since the owner does
         * not register a waiter for a caller that is not going to park.
         * <p>
         * Defaults to {@code false} when the key is absent, which is what makes a PRE-QUEUEING owner behave correctly:
         * such an owner ignores {@code wants_queue}, never registers a waiter and never sends a grant, so a coordinator
         * that parked on its denial would strand for the whole upgrade window. Reading the missing key as "not
         * registered" turns that into the pre-queueing 429 instead.
         */
        final boolean registered;

        AcquirePermitResponse(boolean granted) {
            this(granted, false);
        }

        AcquirePermitResponse(boolean granted, boolean registered) {
            this.granted = granted;
            this.registered = registered;
        }

        AcquirePermitResponse(StreamInput in) throws IOException {
            final Map<String, Object> body = readBody(in);
            this.granted = requireBoolean(body, KEY_GRANTED);
            // Added by queueing, so optional; see the field javadoc for why `false` is the right default.
            this.registered = optionalBoolean(body, KEY_REGISTERED, false);
            // Any other key is a field this build does not know about: ignored on purpose. That is the tolerance.
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(
                Map.of(KEY_GRANTED, granted, KEY_REGISTERED, registered),
                StreamOutput::writeString,
                StreamOutput::writeGenericValue
            );
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
         * The waiter set itself is keyed by exact {@link DiscoveryNode} incarnation. This persistent id is the compact
         * release hint available on the wire; cleanup scans and removes every matching incarnation, including stale ones
         * left by a restart.
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
     * {@code coord -> new owner}. Restore waiter membership for a retained bucket after its ring owner changes. The new
     * owner resolves the live coordinator from its own cluster state, registers it idempotently, and immediately tries
     * to use any free shared capacity.
     */
    public static class ReRegisterWaiterAfterOwnerChangeRequest extends TransportRequest {
        static final String KEY_BUCKET = "bucket_key";
        static final String KEY_REQUESTING_NODE_ID = "requesting_node_id";

        final String bucketKey;
        final String requestingNodeId;

        ReRegisterWaiterAfterOwnerChangeRequest(String bucketKey, String requestingNodeId) {
            this.bucketKey = bucketKey;
            this.requestingNodeId = requestingNodeId;
        }

        ReRegisterWaiterAfterOwnerChangeRequest(StreamInput in) throws IOException {
            super(in);
            final Map<String, Object> body = readBody(in);
            this.bucketKey = requireString(body, KEY_BUCKET);
            this.requestingNodeId = requireString(body, KEY_REQUESTING_NODE_ID);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(
                Map.of(KEY_BUCKET, bucketKey, KEY_REQUESTING_NODE_ID, requestingNodeId),
                StreamOutput::writeString,
                StreamOutput::writeGenericValue
            );
        }
    }

    /** Acknowledges whether the receiving node accepted the owner-change waiter registration. */
    public static class ReRegisterWaiterAfterOwnerChangeResponse extends TransportResponse {
        static final String KEY_REGISTERED = "registered";

        final boolean registered;

        ReRegisterWaiterAfterOwnerChangeResponse(boolean registered) {
            this.registered = registered;
        }

        ReRegisterWaiterAfterOwnerChangeResponse(StreamInput in) throws IOException {
            final Map<String, Object> body = readBody(in);
            this.registered = requireBoolean(body, KEY_REGISTERED);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(Map.of(KEY_REGISTERED, registered), StreamOutput::writeString, StreamOutput::writeGenericValue);
        }
    }

    /**
     * {@code owner -> coord}. Grant: a reserved shared permit for a bucket the coordinator is waiting on. The historical
     * wire shape carries {@code sharedLimit}, but return/re-drive accounting resolves the owner's live limit from cluster
     * state rather than trusting this value.
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
