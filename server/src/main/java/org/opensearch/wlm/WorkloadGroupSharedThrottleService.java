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
import org.opensearch.action.support.ContextPreservingActionListener;
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
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.RemoteTransportException;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Cluster-level ({@code shared_limit}) throttle tier. Each throttle bucket has one authoritative in-flight counter on
 * the node that owns it per a consistent-hash ring ({@link ThrottleOwnerSelector}). This service is both the
 * coordinator-side client that acquires and releases shared permits and the owner-side host of the
 * {@link SharedThrottleTracker} for the buckets this node owns.
 * <p>
 * The acquire is asynchronous so the calling thread never blocks on the round trip; when the coordinator owns the
 * bucket itself it short-circuits to the local tracker with no network hop.
 * <p>
 * Fail-closed: if the owner gives no answer (timeout, disconnect, missing handler, empty ring), the caller is told the
 * shared tier is unavailable ({@link #isUnavailable}) and rejects the request (MONITOR still admits), so
 * {@code shared_limit} is never silently exceeded. The cost is that overflow for an unreachable owner's buckets is
 * rejected.
 * <p>
 * Release ordering: an acquire answered too late is reported unavailable by the coordinator's own timer, but its
 * response handler stays registered and releases a late grant when it arrives. A release is therefore only ever sent
 * after the owner granted, so it can never overtake the acquire and strand the permit. A permit whose release never
 * arrives is purged when its coordinator leaves the cluster, or else expires with its TTL.
 * <p>
 * Queueing (owner-push): a coordinator that retained a denied request asks the owner to register it as a waiter on the
 * bucket. When a slot frees (a release, a TTL reclaim, a coordinator purge, a {@code shared_limit} increase, or an
 * ownership change), the owner reserves a permit and pushes it to a registered coordinator ({@link #GRANT_ACTION_NAME}),
 * which admits its oldest retained request with it. An unavailable shared tier never parks a request: it is rejected.
 */
@ExperimentalApi
public class WorkloadGroupSharedThrottleService implements ClusterStateListener {

    static final String ACQUIRE_ACTION_NAME = "internal:wlm/throttle/shared/acquire";
    static final String RELEASE_ACTION_NAME = "internal:wlm/throttle/shared/release";
    // Owner -> coordinator: a shared slot freed and this coordinator has a registered waiter for the bucket; here is a
    // reserved permit to admit one queued request against (queueing / owner-push tier).
    static final String GRANT_ACTION_NAME = "internal:wlm/throttle/shared/grant";
    // Coordinator -> new owner after the consistent-hash ring changes: restore this coordinator's waiter membership
    // for a retained bucket and immediately use any capacity already available on the new owner.
    static final String RE_REGISTER_WAITER_AFTER_OWNER_CHANGE_ACTION_NAME =
        "internal:wlm/throttle/shared/re_register_waiter_after_owner_change";

    // Fixed constants rather than cluster settings: internal coordination knobs an operator should not need to tune.
    //
    // How long a request waits for the owner's acquire reply before it is rejected as unavailable. Enforced by a
    // coordinator-side timer, NOT a transport timeout: the response handler outlives it (up to ACQUIRE_REPLY_BACKSTOP),
    // so a late grant is still seen and released after the owner granted it.
    static final TimeValue ACQUIRE_TIMEOUT = TimeValue.timeValueMillis(200);
    // Overrides ACQUIRE_TIMEOUT for tests. Deliberately not registered: a node rejects the key unless a test plugin
    // registers it, so it is not an operator setting.
    static final Setting<TimeValue> ACQUIRE_TIMEOUT_SETTING = Setting.timeSetting(
        "wlm.workload_group.throttle.shared_acquire_timeout",
        ACQUIRE_TIMEOUT,
        Setting.Property.NodeScope
    );
    // Transport timeout of the acquire's response handler. Only bounds how long a reply that never comes (half-open
    // connection) keeps the handler registered; a grant later than this falls back to the coordinator purge or the TTL.
    static final TimeValue ACQUIRE_REPLY_BACKSTOP = TimeValue.timeValueSeconds(30);
    // Timeout for the fire-and-forget RELEASE. Nothing waits on it and the TTL is the backstop, so it is long; it is
    // still bounded so a half-open connection can't pin the response handler.
    // Intentional: an owner stalled longer than these 30s windows makes TransportService log one late-response WARN per
    // pending acquire or release when it recovers. Accepted as rare rather than adding a per-owner fast-fail.
    static final TimeValue RELEASE_TIMEOUT = TimeValue.timeValueSeconds(30);
    // A pushed grant is off the request hot path: its request is already retained on the coordinator. Give a temporarily
    // paused coordinator substantially longer to respond than ACQUIRE, while still reclaiming the reservation promptly
    // enough that one unresponsive waiter cannot hold a shared slot for the permit TTL.
    static final TimeValue GRANT_TIMEOUT = TimeValue.timeValueSeconds(5);
    // Time-to-live for a permit on the owner: the backstop for a permit whose release never arrives (lost release, a
    // grant later than ACQUIRE_REPLY_BACKSTOP, or a coordinator that was not purged). Well above typical request duration.
    // Accepted tradeoff: permits are NOT renewed, so a search running longer than the TTL loses its permit while still
    // executing and the bucket can transiently admit beyond shared_limit. If that becomes a problem, the fix is permit
    // renewal or a deadline-derived TTL, not a larger constant.
    static final long PERMIT_TTL_NANOS = TimeValue.timeValueMinutes(5).nanos();
    // How often the owner sweeps expired permits. A saturated bucket already prunes its own expired permits on acquire
    // (see SharedThrottleTracker), but a bucket with parked requests may receive no acquires at all, so for it this sweep
    // is the only thing that reclaims a lost permit and drives owner-push for the freed slot (see sweepExpiredAndDrive).
    // Worst-case wake-up latency for such a bucket is therefore TTL + SWEEP_INTERVAL.
    static final TimeValue SWEEP_INTERVAL = TimeValue.timeValueMinutes(5);

    private static final Logger logger = LogManager.getLogger(WorkloadGroupSharedThrottleService.class);

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final TransportService transportService;
    private final SharedThrottleTracker tracker;
    private final TimeValue acquireTimeout;

    // Immutable ring snapshot, swapped wholesale so readers never see a half-built ring.
    private final AtomicReference<ThrottleOwnerSelector> ring = new AtomicReference<>();
    // Set once the first cluster-state event has been compared in full; only touched on the cluster applier thread.
    private boolean ringInitialized;
    private volatile Scheduler.Cancellable sweepTask;
    // Test seams, no-ops in production: after a successful tracker acquire (before the ownership re-check), and just
    // before a remote acquire is sent (its timer already scheduled).
    volatile Runnable afterTrackerAcquire = () -> {};
    volatile Consumer<RemoteAcquire> beforeRemoteAcquireSent = acquire -> {};
    // Test seam: runs right after a queueing acquire is denied by the tracker, before its waiter registration, so a test
    // can release the last permit inside that window. A no-op in production.
    volatile Runnable afterTrackerDenial = () -> {};

    // OWNER-SIDE waiter registry for owner-push queue draining: for each bucket this node owns, the ordered SET of
    // coordinator process incarnations that have at least one retained request for the bucket. A set (not a count) makes
    // registration idempotent per exact DiscoveryNode — repeated acquires from one incarnation do not inflate it. A
    // stale and replacement incarnation can coexist during topology convergence, so this is not strictly bounded by the
    // current node count. INSERTION-ORDERED (LinkedHashSet) so a grant rotates the chosen coordinator to the tail (see
    // pickAndRotateEligibleWaiter): membership persists across a successful hand-off (the coordinator may still have
    // more queued requests), and successive freed slots round-robin fairly across coordinators instead of repeatedly
    // serving whichever one hashes first. A coordinator is removed when a grant comes back unused, when an offer selects
    // it while disconnected, when a failed GRANT has no fresh re-registration, when its group loses the shared tier, when
    // the coordinator leaves the cluster, or when this node loses ownership. No request identity is held (the requests
    // live on their coordinators).
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
    // COORDINATOR-SIDE: told the bucket key when this coordinator's retained WAITING demand for it can no longer be
    // re-registered with an authoritative owner (empty ring, owner unreachable, re-registration refused or failed). No
    // grant can then arrive, so fail-closed requires rejecting those requests rather than leaving them parked.
    // Late-bound by WorkloadGroupService; null before wiring means there is nothing to reject.
    private volatile Consumer<String> sharedTierUnavailableHandler;

    public WorkloadGroupSharedThrottleService(ClusterService clusterService, ThreadPool threadPool, TransportService transportService) {
        this(clusterService, threadPool, transportService, ACQUIRE_TIMEOUT_SETTING.get(clusterService.getSettings()));
    }

    // Package-private: lets unit tests choose the acquire timeout.
    WorkloadGroupSharedThrottleService(
        ClusterService clusterService,
        ThreadPool threadPool,
        TransportService transportService,
        TimeValue acquireTimeout
    ) {
        this(clusterService, threadPool, transportService, acquireTimeout, new SharedThrottleTracker());
    }

    // Package-private: additionally lets tests drive the tracker's clock, e.g. to expire permits without waiting the TTL.
    WorkloadGroupSharedThrottleService(
        ClusterService clusterService,
        ThreadPool threadPool,
        TransportService transportService,
        TimeValue acquireTimeout,
        SharedThrottleTracker tracker
    ) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.acquireTimeout = acquireTimeout;
        this.tracker = tracker;
        // Start with an empty ring: there is no applied cluster state yet during node construction. The first
        // clusterChanged populates it (see there).
        this.ring.set(ThrottleOwnerSelector.fromDiscoveryNodes(DiscoveryNodes.EMPTY_NODES));
        clusterService.addListener(this);
        // None of the handlers trips the in-flight breaker: the messages are tiny, and tripping it would turn one owner's
        // heap pressure into fail-closed 429s for every bucket it owns (and strand breaker-rejected releases until the TTL,
        // breaker-rejected grants until GRANT_TIMEOUT).
        transportService.registerRequestHandler(
            ACQUIRE_ACTION_NAME,
            ThreadPool.Names.SAME,
            false,
            false,
            AcquirePermitRequest::new,
            (request, channel, task) -> channel.sendResponse(handleAcquire(request))
        );
        transportService.registerRequestHandler(
            RELEASE_ACTION_NAME,
            ThreadPool.Names.SAME,
            false,
            false,
            ReleasePermitRequest::new,
            (request, channel, task) -> {
                handleRelease(request);
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            }
        );
        transportService.registerRequestHandler(
            GRANT_ACTION_NAME,
            ThreadPool.Names.SAME,
            false,
            false,
            GrantPermitRequest::new,
            (request, channel, task) -> {
                handleGrant(request);
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            }
        );
        transportService.registerRequestHandler(
            RE_REGISTER_WAITER_AFTER_OWNER_CHANGE_ACTION_NAME,
            ThreadPool.Names.SAME,
            false,
            false,
            ReRegisterWaiterAfterOwnerChangeRequest::new,
            (request, channel, task) -> channel.sendResponse(handleReRegisterWaiterAfterOwnerChange(request))
        );
    }

    /** Starts the periodic TTL sweep. Call exactly once, at node start: a second call would schedule a duplicate sweep. */
    public void start() {
        // Does not touch the ring: the initial cluster state may not be applied yet at node start.
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
        updateRingIfNodesChanged(event);

        // The hooks below react to WORKLOAD GROUP METADATA, not membership, so they must run on metadata-only events too:
        // the ring update's early return on events without node changes only skips the ring scan and ownership hooks.
        // An event that kept the same Metadata instance cannot have changed any group, so it skips them without iterating.
        if (event.metadataChanged() == false) {
            return;
        }

        // Deleting a group or removing its shared tier drains coordinator-local queues, but there is no per-bucket
        // deregistration RPC for that configuration cutover. Prune the corresponding owner-side waiter memberships here;
        // otherwise currentSharedLimit() stays unset and future release/expiry events can never reconcile them.
        scheduleWaiterRemovalForLostSharedTiers(event);

        // A numeric shared_limit increase creates several slots without any corresponding release RPC. Offer exactly
        // the added capacity from this configuration-change hook; ordinary request completion remains a one-slot event
        // and therefore never runs a fill-to-limit loop on the hot release path.
        scheduleSharedLimitIncreaseDrives(event);
    }

    // Rebuilds the owner ring when the eligible-owner set changed, and runs the ownership-change hooks for the new ring.
    private void updateRingIfNodesChanged(ClusterChangedEvent event) {
        // The first event must NOT be gated on nodesChanged(): the initial applied state already contains the local node,
        // so on a single-node (or otherwise stable) cluster nodesChanged() is never true and the ring would stay empty,
        // failing every shared acquire closed for the node's lifetime.
        // After that, an event without node changes cannot change the eligible set and is skipped. nodesChanged() compares
        // by ephemeral id, so a same-id restart still counts. Any future eligibility input that can change without a node
        // change must bypass this early return.
        if (ringInitialized && event.nodesChanged() == false) {
            return;
        }
        // A coordinator that left will never release its permits, so free them now rather than after the TTL, and drop
        // its waiter registrations (its parked requests died with it). Removal is by ephemeral id (a same-id restart
        // purges the old incarnation) and independent of the ring comparison below, since any removed node, whatever its
        // roles, may have been a coordinator.
        if (event.nodesChanged() && event.nodesDelta().removed()) {
            purgeRemovedCoordinators(event.nodesDelta().removedNodes());
        }
        // Compared by DiscoveryNode equality (ephemeral id) so a same-id restart replaces the stale node; rebuilt only when
        // the eligible set actually differs.
        Set<DiscoveryNode> candidate = ThrottleOwnerSelector.eligibleNodeSet(event.state().nodes());
        final ThrottleOwnerSelector previousRing = ring.get();
        if (previousRing.eligibleNodeSet().equals(candidate) == false) {
            final ThrottleOwnerSelector currentRing = ThrottleOwnerSelector.fromDiscoveryNodes(event.state().nodes());
            ring.set(currentRing);

            // This node is no longer authoritative for waiters on buckets that moved away. Drop that owner-only routing
            // state immediately. Old permit records deliberately remain until release/TTL; while ownership is elsewhere,
            // the former-owner fence prevents them from creating another permit.
            removeWaitersForLostOwnership(currentRing);

            // Every coordinator owns the authoritative request queue for searches that arrived there. Re-register only
            // its retained buckets whose exact owner incarnation changed. Never enumerate queues or send transport
            // requests on the cluster-applier thread.
            scheduleWaiterReRegistrationAfterOwnerChange(previousRing, currentRing);
        }
        ringInitialized = true;
    }

    private void purgeRemovedCoordinators(Iterable<DiscoveryNode> removedNodes) {
        final Set<String> removed = new HashSet<>();
        for (DiscoveryNode node : removedNodes) {
            removed.add(node.getEphemeralId());
        }
        final Map<String, Integer> freedByBucket = tracker.releaseAllFrom(removed);
        for (String bucketKey : waitersByBucket.keySet()) {
            waitersByBucket.computeIfPresent(bucketKey, (k, nodes) -> {
                nodes.removeIf(n -> removed.contains(n.getEphemeralId()));
                return nodes.isEmpty() ? null : nodes;
            });
        }
        if (freedByBucket.isEmpty()) {
            return;
        }
        try {
            // Offering sends GRANTs, so never on the cluster-applier thread. offerSharedSlots re-checks ownership.
            threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
                for (Map.Entry<String, Integer> freed : freedByBucket.entrySet()) {
                    try {
                        offerSharedSlots(freed.getKey(), freed.getValue());
                    } catch (Exception e) {
                        logger.debug("Failed to offer shared capacity freed by a coordinator purge for bucket [" + freed.getKey() + "]", e);
                    }
                }
            });
        } catch (Exception e) {
            // Shutdown can reject the hand-off; later releases and the sweep still drive owner-push.
            logger.debug("Could not schedule shared-throttle grants after a coordinator purge", e);
        }
    }

    private void removeWaitersForLostOwnership(ThrottleOwnerSelector currentRing) {
        for (String bucketKey : waitersByBucket.keySet()) {
            if (isLocalOwner(currentRing, bucketKey) == false) {
                waitersByBucket.remove(bucketKey);
            }
        }
        // A callback for a hand-off sent during an earlier ownership tenure must not remove a waiter if this bucket
        // quickly maps back here and the coordinator re-registers. Clearing the marker makes that callback a no-op.
        // The old permit remains recorded until release/TTL; if this process regains the bucket first, it may temporarily
        // count against the live limit.
        grantsAwaitingResponse.keySet().removeIf(key -> isLocalOwner(currentRing, key.bucketKey) == false);
    }

    private void scheduleWaiterRemovalForLostSharedTiers(ClusterChangedEvent event) {
        final Map<String, WorkloadGroup> currentGroups = workloadGroups(event.state());
        final Map<String, WorkloadGroup> previousGroups = workloadGroups(event.previousState());
        if (currentGroups == null || previousGroups == null) {
            return;
        }

        final Set<String> groupsWithoutSharedTier = new HashSet<>();
        for (Map.Entry<String, WorkloadGroup> entry : previousGroups.entrySet()) {
            if (sharedLimit(entry.getValue()) < 1) {
                continue;
            }
            final WorkloadGroup currentGroup = currentGroups.get(entry.getKey());
            if (currentGroup == null || sharedLimit(currentGroup) < 1) {
                groupsWithoutSharedTier.add(entry.getKey());
            }
        }
        if (groupsWithoutSharedTier.isEmpty()) {
            return;
        }

        try {
            threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
                // Snapshot on the worker, not the cluster-applier thread. This also captures waiter registrations that
                // raced the cluster-state update but completed before the cleanup task began.
                for (String bucketKey : Set.copyOf(waitersByBucket.keySet())) {
                    if (groupsWithoutSharedTier.contains(groupIdOf(bucketKey))) {
                        try {
                            // A newer cluster state may have restored the shared tier before this asynchronous task ran.
                            // Re-check under the same per-bucket map lock used by waiter registration so stale cleanup
                            // cannot erase a waiter registered against the restored configuration.
                            waitersByBucket.computeIfPresent(bucketKey, (key, nodes) -> currentSharedLimit(key) < 1 ? null : nodes);
                        } catch (Exception e) {
                            logger.debug("Failed to remove stale shared-throttle waiters for bucket [" + bucketKey + "]", e);
                        }
                    }
                }
                // Leave grantsAwaitingResponse intact: their transport callbacks still own reserved permits and will
                // settle those exact records. Removing waiter membership prevents further grants while preserving
                // callback race handling.
            });
        } catch (Exception e) {
            // Shutdown may reject cleanup. The process is already discarding this in-memory owner state in that case.
            logger.debug("Could not schedule shared-throttle waiter cleanup after a group lost its shared tier", e);
        }
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
                        if (Objects.equals(previousOwner, currentOwner)) {
                            continue;
                        }
                        // A later cluster state may already have superseded this task. Its own listener invocation will
                        // reconcile against the newer ring, so never send a stale registration to an intermediate owner.
                        if (Objects.equals(ring.get().ownerFor(bucketKey).orElse(null), currentOwner) == false) {
                            continue;
                        }
                        if (currentSharedLimit(bucketKey) < 1) {
                            continue; // node-only queue, deleted group, or shared tier removed in the same state
                        }
                        if (currentOwner == null) {
                            // The ring emptied: no owner can ever push a grant, so the shared tier is unavailable.
                            notifySharedTierUnavailable(bucketKey);
                            continue;
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
        final Map<String, WorkloadGroup> currentGroups = workloadGroups(event.state());
        final Map<String, WorkloadGroup> previousGroups = workloadGroups(event.previousState());
        if (currentGroups == null || previousGroups == null) {
            return;
        }

        final Map<String, Integer> addedSlotsByGroup = new HashMap<>();
        for (Map.Entry<String, WorkloadGroup> entry : currentGroups.entrySet()) {
            final WorkloadGroup previousGroup = previousGroups.get(entry.getKey());
            if (previousGroup == null || previousGroup == entry.getValue()) {
                continue;
            }
            if (queueDrainSupersedesLimitIncrease(previousGroup, entry.getValue())) {
                continue;
            }
            final int previousLimit = sharedLimit(previousGroup);
            final int currentLimit = sharedLimit(entry.getValue());
            // Adding the shared tier is handled by the live-config queue drain. This path is only for a numeric increase
            // while the tier remains active.
            if (previousLimit >= 1 && currentLimit > previousLimit) {
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
        if (Objects.equals(throttleBy(previousGroup), throttleBy(currentGroup)) == false) {
            return true;
        }
        return (nodeLimit(previousGroup) >= 1) != (nodeLimit(currentGroup) >= 1);
    }

    @Nullable
    private static Map<String, WorkloadGroup> workloadGroups(@Nullable ClusterState state) {
        if (state == null || state.metadata() == null) {
            return null;
        }
        return state.metadata().workloadGroups();
    }

    // The effective throttle dimension, or null if this node cannot interpret the group's throttling config.
    @Nullable
    private static String throttleBy(WorkloadGroup workloadGroup) {
        try {
            return WorkloadGroupThrottleSettings.getEffectiveBy(workloadGroup.getMutableWorkloadGroupFragment().getThrottling());
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    // A limit below 1 never admits through its tier (WorkloadGroupService treats it like an unset tier), so every
    // owner-push path treats it as "no shared tier". An uninterpretable config reads as unset rather than throwing.
    private static int sharedLimit(WorkloadGroup workloadGroup) {
        return limit(workloadGroup, true);
    }

    private static int nodeLimit(WorkloadGroup workloadGroup) {
        return limit(workloadGroup, false);
    }

    private static int limit(WorkloadGroup workloadGroup, boolean shared) {
        final Settings throttling = workloadGroup.getMutableWorkloadGroupFragment().getThrottling();
        try {
            return shared
                ? WorkloadGroupThrottleSettings.SHARED_LIMIT.get(throttling)
                : WorkloadGroupThrottleSettings.NODE_LIMIT.get(throttling);
        } catch (IllegalArgumentException e) {
            return WorkloadGroupThrottleSettings.UNSET_LIMIT;
        }
    }

    // The group id is the bucketKey prefix before the first delimiter (see WorkloadGroupService.BUCKET_KEY_DELIMITER).
    // Group ids are base64 UUIDs with no delimiter, so the first one is unambiguous.
    private static String groupIdOf(String bucketKey) {
        final int idx = bucketKey.indexOf(WorkloadGroupService.BUCKET_KEY_DELIMITER);
        return idx < 0 ? bucketKey : bucketKey.substring(0, idx);
    }

    /**
     * Asynchronously acquires one shared permit for {@code bucketKey}, notifying {@code listener} with:
     * <ul>
     *   <li>{@code onResponse} with a {@link Releasable}: granted; close it when the request completes;</li>
     *   <li>{@code onFailure} with a message-less marker: {@link #isDenial} (bucket at its shared limit) or
     *       {@link #isUnavailable} (no authoritative answer from the owner). {@link WorkloadGroupService} composes the
     *       user-facing 429, since this service only knows the opaque bucket key;</li>
     *   <li>{@code onFailure} with any other {@link OpenSearchRejectedExecutionException}: the search pool rejected the
     *       hand-off of a grant, which was already released. Never delivered when {@code proceedsOnDenial} is true.</li>
     * </ul>
     * <p>
     * Threading: the local short-circuit notifies inline. A remote outcome after which the search continues (every grant,
     * and every outcome when {@code proceedsOnDenial}) is delivered on the {@link ThreadPool.Names#SEARCH} pool, so the
     * coordinator's pre-fan-out work does not run on the few network threads of one hot owner's connections. A refusal
     * that becomes a 429 is delivered inline, so a throttled tenant's overflow does not queue ahead of other tenants'
     * shard work (see {@code deliverRefusal}). If the search pool rejects the hand-off, refusals and MONITOR grants are
     * delivered inline; any other grant is released and the rejection passed through.
     *
     * @param proceedsOnDenial whether the caller continues the search even when denied or unavailable (MONITOR)
     */
    void acquireAsync(String bucketKey, int sharedLimit, boolean proceedsOnDenial, ActionListener<Releasable> listener) {
        acquireAsync(bucketKey, sharedLimit, proceedsOnDenial, listener, null);
    }

    /**
     * As {@link #acquireAsync(String, int, boolean, ActionListener)}, but for a caller that has already retained the exact
     * request as PENDING_ACQUIRE before this RPC. On denial, the owner registers this coordinator as a bucket waiter so a
     * later freed slot is pushed back here as a grant ({@link GrantConsumer}), and the denial marker tells the caller to
     * transition the request to WAITING. Passing a non-null {@code onNoWaiterRegistered} requests registration and
     * provides the required terminal path when the owner cannot register demand. Requesting registration requires
     * {@code proceedsOnDenial == false}: a caller that proceeds on every outcome (MONITOR) never retains a request.
     * <p>
     * {@code onNoWaiterRegistered} runs INSTEAD of a denial when the owner denied the acquire and reported that it could
     * not register this coordinator — see {@link AcquirePermitResponse#registered}. No grant will ever arrive in that
     * case, and the retained request has no future wake-up, so the caller must reject it rather than wait. Like a denial
     * it only moves (or rejects) the retained entry, so it is delivered inline with the caller's thread context restored.
     * Every other outcome, including an unavailable shared tier and a search-pool rejection of a grant's hand-off, is
     * reported through {@code listener} exactly as documented above; a request is never registered as a waiter on an
     * unavailable outcome.
     *
     * @param onNoWaiterRegistered run on a denial the owner could not register; {@code null} to not request registration
     */
    void acquireAsync(
        String bucketKey,
        int sharedLimit,
        boolean proceedsOnDenial,
        ActionListener<Releasable> listener,
        @Nullable Runnable onNoWaiterRegistered
    ) {
        final boolean wantsQueue = onNoWaiterRegistered != null;
        assert wantsQueue == false || proceedsOnDenial == false : "a caller that proceeds on denial never retains a request";
        final ThrottleOwnerSelector currentRing = ring.get();
        final DiscoveryNode owner = currentRing.ownerFor(bucketKey).orElse(null);
        if (owner == null) {
            listener.onFailure(unavailableMarker()); // empty ring: no owner to ask
            return;
        }

        final String permitId = permitId();

        // Local-owner short-circuit; the post-acquire fence re-reads the live ring.
        final DiscoveryNode localNode = clusterService.localNode();
        if (owner.equals(localNode)) {
            if (tracker.tryAcquire(bucketKey, sharedLimit, permitId, PERMIT_TTL_NANOS, localNode.getEphemeralId()) == false) {
                // The waiter is this very node, always resolvable and reachable, so registration can only fail because
                // ownership moved after the ring was read: then no authoritative answer was obtained.
                if (wantsQueue) {
                    afterTrackerDenial.run();
                }
                if (wantsQueue && registerWaiter(bucketKey, localNode) == false) {
                    listener.onFailure(unavailableMarker());
                } else {
                    if (wantsQueue) {
                        offerIfReleasedBeforeRegistration(bucketKey, sharedLimit);
                    }
                    listener.onFailure(deniedMarker());
                }
            } else if (fenceIfOwnershipLost(bucketKey, permitId)) {
                listener.onFailure(unavailableMarker());
            } else {
                listener.onResponse(releaseLocal(bucketKey, permitId));
            }
            return;
        }

        // Owner known to be disconnected (an in-memory lookup): report unavailable now rather than wait out the timer.
        if (transportService.nodeConnected(owner) == false) {
            listener.onFailure(unavailableMarker());
            return;
        }

        // A denial the owner could not register is routed to onNoWaiterRegistered, with the caller's context restored.
        final ActionListener<Releasable> contextListener = ContextPreservingActionListener.wrapPreservingContext(
            wantsQueue ? routeNotRegistered(listener, onNoWaiterRegistered) : listener,
            threadPool.getThreadContext()
        );
        final RemoteAcquire acquire = new RemoteAcquire(owner, bucketKey, permitId, proceedsOnDenial, wantsQueue, contextListener);
        try {
            acquire.timer = threadPool.schedule(acquire::onTimeout, acquireTimeout, ThreadPool.Names.GENERIC);
        } catch (OpenSearchRejectedExecutionException e) {
            // The scheduler is shutting down: nothing was sent, so there is nothing to release.
            contextListener.onFailure(unavailableMarker());
            return;
        }
        beforeRemoteAcquireSent.accept(acquire);
        final TransportRequestOptions options = TransportRequestOptions.builder().withTimeout(ACQUIRE_REPLY_BACKSTOP).build();
        transportService.sendRequest(
            owner,
            ACQUIRE_ACTION_NAME,
            new AcquirePermitRequest(
                bucketKey,
                sharedLimit,
                permitId,
                PERMIT_TTL_NANOS,
                localNode.getEphemeralId(),
                wantsQueue ? localNode.getId() : "",
                wantsQueue
            ),
            options,
            acquire
        );
    }

    private static ActionListener<Releasable> routeNotRegistered(ActionListener<Releasable> listener, Runnable onNoWaiterRegistered) {
        return new ActionListener<>() {
            @Override
            public void onResponse(Releasable permit) {
                listener.onResponse(permit);
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof NotRegisteredMarkerException) {
                    onNoWaiterRegistered.run();
                } else {
                    listener.onFailure(e);
                }
            }
        };
    }

    /**
     * One remote acquire. Its outcome is decided exactly once, by the first of the owner's reply, a transport failure or
     * the acquire timer; anything after that only cleans up (a late grant is released). Deciding drops the reference to
     * the caller's listener, so a handler still registered after the timer fired does not pin the answered request.
     */
    final class RemoteAcquire implements TransportResponseHandler<AcquirePermitResponse> {
        private final DiscoveryNode owner;
        private final String bucketKey;
        private final String permitId;
        // Whether a denial must come with a waiter registration (see AcquirePermitResponse#registered).
        private final boolean wantsQueue;
        private final boolean proceedsOnDenial;
        // The caller's listener until the outcome is decided, then null; claiming it is the decision.
        private final AtomicReference<ActionListener<Releasable>> listener;
        // Assigned before the request is sent, so every outcome sees it.
        private volatile Scheduler.ScheduledCancellable timer;

        RemoteAcquire(
            DiscoveryNode owner,
            String bucketKey,
            String permitId,
            boolean proceedsOnDenial,
            boolean wantsQueue,
            ActionListener<Releasable> listener
        ) {
            this.owner = owner;
            this.bucketKey = bucketKey;
            this.permitId = permitId;
            this.proceedsOnDenial = proceedsOnDenial;
            this.wantsQueue = wantsQueue;
            this.listener = new AtomicReference<>(listener);
        }

        // Returns the caller's listener, or null if already decided; cancels the timer.
        private ActionListener<Releasable> decide() {
            final ActionListener<Releasable> claimed = listener.getAndSet(null);
            if (claimed != null) {
                final Scheduler.ScheduledCancellable pending = timer;
                if (pending != null) {
                    pending.cancel();
                }
            }
            return claimed;
        }

        void onTimeout() {
            final ActionListener<Releasable> claimed = listener.getAndSet(null);
            if (claimed != null) {
                // Intentionally no release: it could overtake the in-flight acquire and strand a later grant. A late grant
                // is released by handleResponse once it arrives. A queueing caller rejects its PENDING_ACQUIRE entry as
                // unavailable; a late registered denial only costs one unused grant the coordinator hands back.
                logger.debug(
                    "Shared throttle acquire to owner [{}] for bucket [{}] timed out; shared tier unavailable",
                    owner.getId(),
                    bucketKey
                );
                deliverRefusal(claimed, unavailableMarker());
            }
        }

        // Visible for tests: whether the outcome is still undecided.
        boolean holdsListener() {
            return listener.get() != null;
        }

        // Visible for tests.
        Scheduler.ScheduledCancellable timer() {
            return timer;
        }

        @Override
        public AcquirePermitResponse read(StreamInput in) throws IOException {
            return new AcquirePermitResponse(in);
        }

        @Override
        public void handleResponse(AcquirePermitResponse response) {
            final ActionListener<Releasable> claimed = decide();
            if (claimed == null) {
                // Already reported unavailable by the timer: give a late grant straight back (safe, it follows the grant).
                if (response.granted) {
                    sendRelease(owner, bucketKey, permitId, "");
                }
                return;
            }
            if (response.granted) {
                final Releasable permit = releaseRemote(owner, bucketKey, permitId);
                deliverOnSearch(() -> claimed.onResponse(permit), rejection -> {
                    if (proceedsOnDenial) {
                        claimed.onResponse(permit);
                    } else {
                        permit.close();
                        claimed.onFailure(rejection);
                    }
                });
                return;
            }
            if (response.notOwner) {
                // Our ring and the owner's disagree mid-rebalance, so there is no authoritative answer.
                deliverRefusal(claimed, unavailableMarker());
            } else if (wantsQueue && response.registered == false) {
                // Denied, and the owner cannot push us a grant (we are absent from its applied cluster state, present but
                // unreachable, or it predates queueing). Waiting would strand the retained request, so the caller rejects it.
                logger.debug("Owner [{}] denied bucket [{}] without registering this node as a waiter", owner.getId(), bucketKey);
                deliverRefusal(claimed, new NotRegisteredMarkerException());
            } else {
                deliverRefusal(claimed, deniedMarker());
            }
        }

        @Override
        public void handleException(TransportException exp) {
            // A RemoteTransportException means a reply came back (an error, or one we could not read, which the native
            // transport also wraps): the owner already processed the acquire, so a release now follows any grant and is a
            // no-op otherwise. Any other failure gives no such ordering, so nothing is sent; the purge or TTL reclaims it.
            if (exp instanceof RemoteTransportException) {
                sendRelease(owner, bucketKey, permitId, "");
            }
            final ActionListener<Releasable> claimed = decide();
            if (claimed != null) {
                logger.debug(
                    "Shared throttle acquire to owner [{}] for bucket [{}] failed; shared tier unavailable",
                    owner.getId(),
                    bucketKey
                );
                deliverRefusal(claimed, unavailableMarker());
            }
        }

        // Inline when the caller turns it into a 429, otherwise on the search pool (inline if the pool rejects it).
        // Intentional: an inline 429 runs on the network or timer thread, so a caller that starts new work from it (_msearch's
        // next sub-search) may run that work's pre-fan-out there. Rare, and not worth a thread hop on every denial.
        private void deliverRefusal(ActionListener<Releasable> claimed, Exception marker) {
            if (proceedsOnDenial) {
                deliverOnSearch(() -> claimed.onFailure(marker), rejection -> claimed.onFailure(marker));
            } else {
                claimed.onFailure(marker);
            }
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }

    // Runs outcome on the search pool; onRejection runs inline on the calling thread if the pool rejects it.
    private void deliverOnSearch(Runnable outcome, Consumer<Exception> onRejection) {
        threadPool.executor(ThreadPool.Names.SEARCH).execute(new AbstractRunnable() {
            @Override
            protected void doRun() {
                outcome.run();
            }

            @Override
            public void onRejection(Exception e) {
                onRejection.accept(e);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("Unexpected failure delivering shared throttle acquire outcome", e);
            }
        });
    }

    // Owner-side admission. Package-private for tests.
    //
    // Former-owner fence: a coordinator still on an old ring may keep routing here after the bucket moved, and granting
    // would count against a stale counter while the new owner counts from zero, so a non-owner refuses with not_owner.
    // The check is repeated after the acquire to narrow the race with a concurrent ring swap.
    // Accepted: a coordinator removed from the cluster but still able to reach this owner (asymmetric partition) can be
    // granted permits after its purge; those fall back to the TTL. Rare.
    AcquirePermitResponse handleAcquire(AcquirePermitRequest request) {
        if (isStillOwner(request.bucketKey) == false) {
            return AcquirePermitResponse.NOT_OWNER;
        }
        // Intentional: the owner trusts the coordinator's limit rather than re-resolving it, so while a shared_limit change
        // propagates, coordinators that have not applied it yet enforce the old value. Short and self-correcting.
        if (tracker.tryAcquire(
            request.bucketKey,
            request.sharedLimit,
            request.permitId,
            request.ttlNanos,
            request.coordinatorId
        ) == false) {
            return request.wantsQueue ? registerRemoteWaiterOnDenial(request) : AcquirePermitResponse.DENIED;
        }
        return fenceIfOwnershipLost(request.bucketKey, request.permitId) ? AcquirePermitResponse.NOT_OWNER : AcquirePermitResponse.GRANTED;
    }

    // Denied and the coordinator already retained the request -> remember it so a freed slot is pushed back as a grant.
    // A registered response lets the coordinator transition the exact PENDING_ACQUIRE entry to WAITING. The RPC carries
    // only the coordinator's persistent node id, so resolve it to the live DiscoveryNode here: the waiter set must hold
    // the real node because sendGrant needs its transport address.
    //
    // Register ONLY if we can actually reach that node, and tell the coordinator either way (the `registered` bit on the
    // response). Presence in cluster state alone is not enough, because this node's applied state can lag the
    // coordinator's:
    // - NOT PRESENT: a node that just joined (or left) resolves to null. Registering is impossible.
    // - PRESENT BUT STALE: a node that restarted keeps its persistent id but gets a NEW ephemeral id, and DiscoveryNode
    // identity IS the ephemeral id — so until this node applies the rejoin, get() hands back the PREVIOUS incarnation.
    // Registering that would look like success, then the first freed slot would find it absent from the connection map,
    // reclaim, and quietly deregister it (see offerSharedSlots).
    // nodeConnected covers both, plus a transient disconnect, in one lock-free connection-map lookup. A coordinator told
    // `registered=false` rejects the request with the normal throttle 429 instead of parking it for a grant that is never
    // coming. Losing ownership while registering is reported as not_owner, the same as the fences above.
    private AcquirePermitResponse registerRemoteWaiterOnDenial(AcquirePermitRequest request) {
        afterTrackerDenial.run();
        final ClusterState state = clusterService.state();
        final DiscoveryNode requestingNode = request.requestingNodeId.isEmpty() || state == null
            ? null
            : state.nodes().get(request.requestingNodeId);
        if (requestingNode == null || transportService.nodeConnected(requestingNode) == false) {
            logger.debug(
                "Not registering waiter for bucket [{}]: node [{}] is not reachable (absent from the applied cluster state, or "
                    + "present but not connected)",
                request.bucketKey,
                request.requestingNodeId
            );
            return AcquirePermitResponse.DENIED;
        }
        if (registerWaiter(request.bucketKey, requestingNode) == false) {
            return AcquirePermitResponse.NOT_OWNER;
        }
        offerIfReleasedBeforeRegistration(request.bucketKey, request.sharedLimit);
        return AcquirePermitResponse.DENIED_AND_REGISTERED;
    }

    // Closes the deny-then-register race: a release that lands between the failed tryAcquire and registerWaiter sees no
    // waiter and offers its slot to nobody, so without this re-check the newly registered request would wait for an
    // unrelated later release. Offering now is safe even before the coordinator has seen the denial: a grant admits its
    // still-PENDING_ACQUIRE request, and the denial that follows finds the entry gone. offerSharedSlots re-reads the live
    // limit and reserves through tryAcquire, so this can never over-admit.
    private void offerIfReleasedBeforeRegistration(String bucketKey, int sharedLimit) {
        if (tracker.inFlight(bucketKey) < sharedLimit) {
            offerSharedSlots(bucketKey, 1);
        }
    }

    // Re-checks ownership after a successful tracker acquire; if the bucket moved in between, returns the permit and true.
    private boolean fenceIfOwnershipLost(String bucketKey, String permitId) {
        afterTrackerAcquire.run();
        if (isStillOwner(bucketKey)) {
            return false;
        }
        tracker.release(bucketKey, permitId);
        return true;
    }

    private boolean isStillOwner(String bucketKey) {
        return isLocalOwner(ring.get(), bucketKey);
    }

    // DiscoveryNode equality is by ephemeral id, the identity the ring is rebuilt on.
    private boolean isLocalOwner(ThrottleOwnerSelector selector, String bucketKey) {
        final DiscoveryNode localNode = clusterService.localNode();
        return selector.ownerFor(bucketKey).filter(localNode::equals).isPresent();
    }

    // A local release must also drive owner-push when this node owns the bucket: releasing frees a slot that a registered
    // waiter should get. Same rule as the remote path's handleRelease, so local-owner and remote-owner behave identically.
    private Releasable releaseLocal(String bucketKey, String permitId) {
        return releaseOnce(() -> {
            if (tracker.release(bucketKey, permitId)) {
                offerSharedSlots(bucketKey, 1);
            }
        });
    }

    // Closing does not wait for the owner to apply the release (see WorkloadGroupService#releaseThrottlePermitBeforeCompletion).
    private Releasable releaseRemote(DiscoveryNode owner, String bucketKey, String permitId) {
        return releaseOnce(() -> sendRelease(owner, bucketKey, permitId, ""));
    }

    // Returns an UNUSED remote grant: a RELEASE tagged with this coordinator's node id so the owner also deregisters it
    // from the bucket's waiter set before re-driving owner-push to the next waiter.
    private void sendReleaseUnusedGrant(DiscoveryNode owner, String bucketKey, String permitId, DiscoveryNode self) {
        sendRelease(owner, bucketKey, permitId, self.getId());
    }

    // Fire-and-forget RELEASE to the bucket owner. A lost release is not fatal (the permit expires with its TTL), so
    // failures are only logged at debug. queueEmptyOnNodeId is set only when returning an unused grant (see handleRelease).
    private void sendRelease(DiscoveryNode owner, String bucketKey, String permitId, String queueEmptyOnNodeId) {
        final TransportRequestOptions options = TransportRequestOptions.builder().withTimeout(RELEASE_TIMEOUT).build();
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
                    // Accepted: not retried. If the connection to the owner briefly drops while both nodes stay in the
                    // cluster, the permit stays counted until its TTL, which can cause temporary false 429s for the bucket.
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
     * Late-binds the coordinator-local request queue: pushed grants admit its oldest retained request for the bucket, and
     * an owner-ring change re-registers its retained buckets with their new owners. Called once during node construction.
     */
    public void setQueueService(WorkloadGroupQueueService queueService) {
        setGrantConsumer(queueService::admitWithPermit);
        setRetainedBucketKeysSupplier(queueService::retainedBucketKeys);
    }

    /**
     * Late-binds the coordinator-side grant consumer. A received grant admits one queued request via this; before it is
     * set, a grant is returned using the current owner ring. This normally reclaims the reserved slot; after an ownership
     * change the current owner may treat the old permit id as unknown, leaving the former owner's record to expire.
     */
    void setGrantConsumer(GrantConsumer grantConsumer) {
        this.grantConsumer = grantConsumer;
    }

    /**
     * Late-binds the coordinator-local retained-bucket snapshot used only after owner-ring changes. The supplier must
     * return a snapshot, since reconciliation runs asynchronously and the underlying queues continue to mutate.
     */
    void setRetainedBucketKeysSupplier(Supplier<Set<String>> retainedBucketKeysSupplier) {
        this.retainedBucketKeysSupplier = Objects.requireNonNull(retainedBucketKeysSupplier);
    }

    /**
     * Late-binds the coordinator-side handler that rejects this coordinator's WAITING requests for a bucket whose
     * waiter registration can no longer be confirmed with an authoritative owner (see {@link #reconcileRetainedWaiters}).
     */
    void setSharedTierUnavailableHandler(Consumer<String> sharedTierUnavailableHandler) {
        this.sharedTierUnavailableHandler = Objects.requireNonNull(sharedTierUnavailableHandler);
    }

    private void notifySharedTierUnavailable(String bucketKey) {
        final Consumer<String> handler = sharedTierUnavailableHandler;
        if (handler == null) {
            return;
        }
        try {
            handler.accept(bucketKey);
        } catch (Exception e) {
            logger.warn("Failed to reject retained requests for bucket [" + bucketKey + "] after the shared tier became unavailable", e);
        }
    }

    /**
     * COORDINATOR-SIDE backstop for retained WAITING demand, run periodically by the coordinator's queue sweep. Owner-push
     * is this coordinator's only wake-up for a shared-tier WAITING request, and that wake-up can be lost: a failed or
     * timed-out GRANT drops the waiter, an unused-grant release can overtake a newer registration, and an owner change
     * can race the re-registration. Re-registering with the bucket's current owner is idempotent and makes that owner
     * offer any capacity that is already free, so a lost wake-up costs at most one sweep interval.
     * <p>
     * Fail-closed: if there is no owner, or the registration cannot be confirmed (owner unreachable, refused, transport
     * failure), no grant can arrive and the WAITING requests are rejected through the unavailable handler.
     */
    void reconcileRetainedWaiters(String bucketKey) {
        final DiscoveryNode owner = ring.get().ownerFor(bucketKey).orElse(null);
        if (owner == null) {
            notifySharedTierUnavailable(bucketKey);
            return;
        }
        reRegisterWaiterAfterOwnerChange(bucketKey, owner);
    }

    /**
     * COORDINATOR-SIDE owner-change recovery. The request queue lives here, so after the ring remaps this node tells the
     * new owner that it still has demand for {@code bucketKey}. The call is asynchronous. If the registration cannot be
     * confirmed (owner not connected, refused, or a transport failure), no grant can arrive, so the bucket's WAITING
     * requests are rejected as shared-tier unavailable; still-PENDING_ACQUIRE requests settle through their own acquire.
     */
    void reRegisterWaiterAfterOwnerChange(String bucketKey, DiscoveryNode newOwner) {
        if (Objects.equals(ring.get().ownerFor(bucketKey).orElse(null), newOwner) == false) {
            return; // superseded by a newer ring before this asynchronous reconciliation ran
        }
        final DiscoveryNode self = clusterService.localNode();
        final ReRegisterWaiterAfterOwnerChangeRequest request = new ReRegisterWaiterAfterOwnerChangeRequest(bucketKey, self.getId());
        if (newOwner.equals(self)) {
            if (handleReRegisterWaiterAfterOwnerChange(request).registered == false) {
                notifySharedTierUnavailable(bucketKey);
            }
            return;
        }
        if (transportService.nodeConnected(newOwner) == false) {
            logger.debug(
                "Cannot re-register waiter for bucket [{}] after owner change: new owner [{}] is not connected",
                bucketKey,
                newOwner.getId()
            );
            notifySharedTierUnavailable(bucketKey);
            return;
        }
        final TransportRequestOptions options = TransportRequestOptions.builder().withTimeout(acquireTimeout).build();
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
                        notifySharedTierUnavailable(bucketKey);
                    }
                }

                @Override
                public void handleException(TransportException exp) {
                    logger.debug(
                        "Waiter re-registration to new owner [" + newOwner.getId() + "] for bucket [" + bucketKey + "] failed",
                        exp
                    );
                    notifySharedTierUnavailable(bucketKey);
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
    // trigger owner-push. Package-private for tests.
    ReRegisterWaiterAfterOwnerChangeResponse handleReRegisterWaiterAfterOwnerChange(ReRegisterWaiterAfterOwnerChangeRequest request) {
        if (isStillOwner(request.bucketKey) == false) {
            return ReRegisterWaiterAfterOwnerChangeResponse.NOT_REGISTERED;
        }
        final int sharedLimit = currentSharedLimit(request.bucketKey);
        if (sharedLimit < 1) {
            return ReRegisterWaiterAfterOwnerChangeResponse.NOT_REGISTERED;
        }
        final DiscoveryNode self = clusterService.localNode();
        final DiscoveryNode requestingNode = self.getId().equals(request.requestingNodeId)
            ? self
            : clusterService.state().nodes().get(request.requestingNodeId);
        if (requestingNode == null || (requestingNode.equals(self) == false && transportService.nodeConnected(requestingNode) == false)) {
            return ReRegisterWaiterAfterOwnerChangeResponse.NOT_REGISTERED;
        }
        if (registerWaiter(request.bucketKey, requestingNode) == false) {
            return ReRegisterWaiterAfterOwnerChangeResponse.NOT_REGISTERED;
        }
        // Offer up to the entire live limit so retained demand can resume without waiting for a release event that may
        // never arrive on this owner. tryAcquire enforces the actual availability, including any old records retained
        // if this same process previously owned and then regained the bucket.
        offerSharedSlots(request.bucketKey, sharedLimit);
        return isStillOwner(request.bucketKey)
            ? ReRegisterWaiterAfterOwnerChangeResponse.REGISTERED
            : ReRegisterWaiterAfterOwnerChangeResponse.NOT_REGISTERED;
    }

    // OWNER-SIDE: record that a coordinator is waiting on a bucket this node owns. Idempotent (a set), so re-registering
    // an already-known waiter is a no-op — it keeps its existing queue position (LinkedHashSet.add does not reorder an
    // element already present), so re-registration can't let a coordinator jump the rotation. During topology
    // convergence, stale and replacement incarnations may coexist until selected while disconnected, unused-grant
    // cleanup, or ownership loss. Returns false only if this node does not own the bucket.
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
     * hit a recorded permit — a coordinator's release may be speculative (after an acquire failed with a remote error),
     * so it cannot know. The ceiling is resolved from this node's cluster state, whose view is the one being enforced. Deciding here
     * means a no-op release never produces a phantom grant and a release that did free a slot always drives one.
     */
    void handleRelease(ReleasePermitRequest request) {
        final boolean freed = tracker.release(request.bucketKey, request.permitId);
        // If this release is a coordinator returning an UNUSED grant (it had no queued request for the bucket), drop it
        // from the waiter set so it stops drawing wasted grants. Only trust the signal when the permit id belonged to THIS
        // tracker: a grant issued by the previous owner can cross a ring change and be returned to the new owner, where
        // its unknown id must not deregister a valid new-owner waiter.
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
    // order so round-robin order survives. This also clears a stale entry left by a restarted coordinator (same
    // persistent id, new ephemeral id): the old incarnation's parked requests died with its JVM.
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
     * increases, and owner-change recovery. A bucket without a positive limit simply skips owner-push rather than
     * guessing one.
     */
    private int currentSharedLimit(String bucketKey) {
        final Map<String, WorkloadGroup> groups = workloadGroups(clusterService.state());
        final WorkloadGroup workloadGroup = groups == null ? null : groups.get(groupIdOf(bucketKey));
        return workloadGroup == null ? WorkloadGroupThrottleSettings.UNSET_LIMIT : sharedLimit(workloadGroup);
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
            if (sharedLimit < 1) {
                clearPendingGrant(selection);
                return;
            }
            final String reservedPermitId = permitId();
            // Reserve the freed slot so a concurrent acquire can't take it before the grant lands, recorded against the
            // target coordinator so its purge covers the reservation too. If the bucket is at
            // its limit again (a racing acquire beat us), don't over-grant: stop. The waiter stays registered (the pick
            // only rotated it), so the next release re-drives owner-push to it.
            if (tracker.tryAcquire(bucketKey, sharedLimit, reservedPermitId, PERMIT_TTL_NANOS, target.getEphemeralId()) == false) {
                clearPendingGrant(selection);
                return;
            }
            if (fenceIfOwnershipLost(bucketKey, reservedPermitId)) {
                // Ownership moved between the fence at method entry and the reservation, which has been reclaimed rather
                // than handed to either a local or remote coordinator as a fresh former-owner permit.
                clearPendingGrant(selection);
                waitersByBucket.remove(bucketKey);
                return;
            }
            if (selection.pendingGrantKey == null) {
                final LocalGrantResult result = consumeGrantLocal(bucketKey, reservedPermitId, target);
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
    // Recursion safety: on ADMIT we hand a self-re-driving permit — but its close() fires later, at request completion
    // (async), so re-driving then is fine and not re-entrant. On the NO-request path we release the reserved permit
    // DIRECTLY (not by closing a re-driving permit), so the caller's bounded loop advances without recursion.
    private LocalGrantResult consumeGrantLocal(String bucketKey, String reservedPermitId, DiscoveryNode self) {
        final GrantConsumer consumer = grantConsumer;
        if (consumer == null) {
            tracker.release(bucketKey, reservedPermitId);
            removeWaiter(bucketKey, self);
            return LocalGrantResult.NO_DEMAND;
        }
        final boolean admitted;
        try {
            admitted = consumer.admit(bucketKey, releaseLocal(bucketKey, reservedPermitId));
        } catch (Exception e) {
            // A throw means dispatch is unavailable (typically executor shutdown). Release the reserved slot and STOP the
            // loop — do NOT retry. The queue polls the head request before its dispatch can throw, so a retry against the
            // still-registered waiter would poll-and-drop a further request on every iteration during a dispatch outage.
            // The waiter stays registered; a later release re-drives it.
            logger.debug("Queue grant admit failed for bucket [" + bucketKey + "]", e);
            tracker.release(bucketKey, reservedPermitId);
            return LocalGrantResult.STOP;
        }
        if (admitted == false) {
            // The consumer does not close the permit on a false return; release the reserved permit directly so the slot
            // is reused by this loop. (Not via the permit's close(), which would re-drive owner-push inline.)
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
                nodes.remove(picked[0].coordinator); // detach from its current position...
                nodes.add(picked[0].coordinator); // ...and re-append at the tail, keeping it registered
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
    // current owner to deregister this coordinator before re-driving another waiter. Runs on the transport thread: the
    // queue only polls its head under a short lock here and completes the admitted request's listener on an executor.
    private void handleGrant(GrantPermitRequest request) {
        final GrantConsumer consumer = grantConsumer;
        if (consumer == null) {
            returnUnusedGrant(request.bucketKey, request.permitId); // not wired yet -> return the slot + deregister
            return;
        }
        // The permit handed to an admitted request releases only the reserved permit on completion (which re-drives
        // owner-push at the owner). It does NOT carry the unused-grant deregister signal — an admitted coordinator is a
        // legitimate ongoing waiter if it has more queued requests.
        final DiscoveryNode owner = ring.get().ownerFor(request.bucketKey).orElse(null);
        final boolean admitted;
        try {
            admitted = consumer.admit(request.bucketKey, reservedPermit(owner, request.bucketKey, request.permitId));
        } catch (Exception e) {
            logger.warn("Queue grant admit failed for bucket [" + request.bucketKey + "]", e);
            returnUnusedGrant(request.bucketKey, request.permitId);
            return;
        }
        if (admitted == false) {
            // No queued request on this coordinator for the bucket: return the reserved slot AND tell the owner to
            // deregister this coordinator so it stops drawing wasted grants.
            returnUnusedGrant(request.bucketKey, request.permitId);
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
        if (owner.equals(self)) {
            // Same rules as the remote path's handleRelease: push is driven only if a permit really went away, and an
            // unknown grant id may belong to a previous owner, so it must not deregister this node from the waiter set.
            if (tracker.release(bucketKey, reservedPermitId)) {
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
    private Releasable reservedPermit(DiscoveryNode owner, String bucketKey, String permitId) {
        if (owner == null) {
            return releaseOnce(() -> {}); // ring empty; the owner's TTL reclaims the reservation
        }
        if (owner.equals(clusterService.localNode())) {
            return releaseLocal(bucketKey, permitId);
        }
        return releaseRemote(owner, bucketKey, permitId);
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

    private record PendingGrantSelection(DiscoveryNode coordinator, @Nullable PendingGrantKey pendingGrantKey,
        @Nullable PendingGrant pendingGrant) {
    }

    private record PendingGrantKey(String bucketKey, String coordinatorEphemeralId) {
    }

    // Message-less markers; WorkloadGroupService composes the user-facing 429. "Denied": the bucket is at its shared limit.
    // Package-private for tests.
    static OpenSearchRejectedExecutionException deniedMarker() {
        return new DeniedMarkerException();
    }

    /** Whether {@code e} is this service's shared-limit denial, as opposed to e.g. a search-pool rejection. */
    static boolean isDenial(Exception e) {
        return e instanceof DeniedMarkerException;
    }

    // "Unavailable": no answer could be obtained from the bucket's owner. Package-private for tests.
    static RuntimeException unavailableMarker() {
        return new UnavailableMarkerException();
    }

    /** Whether {@code e} means the shared tier could not answer (empty ring, owner unreachable, timeout, not_owner). */
    static boolean isUnavailable(Exception e) {
        return e instanceof UnavailableMarkerException;
    }

    private static final class UnavailableMarkerException extends RuntimeException {
        @Override
        public Throwable fillInStackTrace() {
            return this;
        }
    }

    private static final class DeniedMarkerException extends OpenSearchRejectedExecutionException {
        @Override
        public Throwable fillInStackTrace() {
            return this;
        }
    }

    // Internal to acquireAsync: a denial the owner could not register (routed to onNoWaiterRegistered, never to a caller).
    private static final class NotRegisteredMarkerException extends RuntimeException {
        @Override
        public Throwable fillInStackTrace() {
            return this;
        }
    }

    /**
     * Raw number of permit records this node holds for {@code bucketKey}, not filtered by expiry or ownership. Public
     * only for cross-package tests ({@code WlmClusterThrottlingIT}).
     */
    public int ownedInFlight(String bucketKey) {
        return tracker.inFlight(bucketKey);
    }

    // Visible for tests.
    SharedThrottleTracker tracker() {
        return tracker;
    }

    ThrottleOwnerSelector ring() {
        return ring.get();
    }

    /**
     * Each RPC body below is one name-keyed map rather than positional fields, so peers built from different commits of
     * the same release (e.g. a blue/green deployment, which a {@code Version} gate cannot tell apart) stay
     * interoperable: unknown keys are ignored.
     * <p>
     * Reading rules: baseline fields are read strictly ({@code require*}); a malformed message then fails while
     * decoding, as a positional one would. A field added later must be read with a safe default if a same-version peer
     * may lack it, or behind a stream-version gate otherwise (the stream version is the negotiated minimum, so an older
     * peer never takes the strict branch). Use only types {@code readGenericValue} knows (String, Integer, Long, Boolean,
     * List, Map).
     */
    private static Map<String, Object> readBody(StreamInput in) throws IOException {
        return in.readMap(StreamInput::readString, StreamInput::readGenericValue);
    }

    // Baseline fields: strict.

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

    // Fields added after the baseline: a same-version peer may omit them, so read with a default.

    private static boolean optionalBoolean(Map<String, Object> body, String key, boolean defaultValue) {
        final Object value = body.get(key);
        return value instanceof Boolean b ? b : defaultValue;
    }

    private static String optionalString(Map<String, Object> body, String key, String defaultValue) {
        final Object value = body.get(key);
        return value instanceof String s ? s : defaultValue;
    }

    // Queueing added such fields: requesting_node_id / wants_queue on the acquire request, registered on the acquire
    // response, and queue_empty_on_node_id on the release request. Each default reproduces the pre-queueing behaviour.
    /** {@code coord -> owner}: admit one request under {@code sharedLimit}. */
    static final class AcquirePermitRequest extends TransportRequest {
        static final String KEY_BUCKET = "bucket_key";
        static final String KEY_SHARED_LIMIT = "shared_limit";
        static final String KEY_PERMIT_ID = "permit_id";
        static final String KEY_TTL_NANOS = "ttl_nanos";
        // Added after the baseline: the coordinator's ephemeral id, for the purge on its removal. A peer that omits it
        // reads as UNKNOWN_COORDINATOR, whose permits fall back to the TTL.
        static final String KEY_COORDINATOR = "coordinator_node_id";
        // Added after the baseline, for owner-push: written only when wants_queue is set, read with safe defaults.
        static final String KEY_REQUESTING_NODE_ID = "requesting_node_id";
        static final String KEY_WANTS_QUEUE = "wants_queue";

        final String bucketKey;
        final int sharedLimit;
        final String permitId;
        final long ttlNanos;
        final String coordinatorId;
        // Whether the coordinator already retained the request as PENDING_ACQUIRE and needs waiter registration on denial,
        // and its PERSISTENT node id, which the owner resolves to the live node. Empty means "not queueing".
        final String requestingNodeId;
        final boolean wantsQueue;

        AcquirePermitRequest(String bucketKey, int sharedLimit, String permitId, long ttlNanos, String coordinatorId) {
            this(bucketKey, sharedLimit, permitId, ttlNanos, coordinatorId, "", false);
        }

        AcquirePermitRequest(
            String bucketKey,
            int sharedLimit,
            String permitId,
            long ttlNanos,
            String coordinatorId,
            String requestingNodeId,
            boolean wantsQueue
        ) {
            this.bucketKey = bucketKey;
            this.sharedLimit = sharedLimit;
            this.permitId = permitId;
            this.ttlNanos = ttlNanos;
            this.coordinatorId = coordinatorId;
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
            this.coordinatorId = optionalString(body, KEY_COORDINATOR, SharedThrottleTracker.UNKNOWN_COORDINATOR);
            this.requestingNodeId = optionalString(body, KEY_REQUESTING_NODE_ID, "");
            this.wantsQueue = optionalBoolean(body, KEY_WANTS_QUEUE, false);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            final Map<String, Object> body = wantsQueue
                ? Map.of(
                    KEY_BUCKET,
                    bucketKey,
                    KEY_SHARED_LIMIT,
                    sharedLimit,
                    KEY_PERMIT_ID,
                    permitId,
                    KEY_TTL_NANOS,
                    ttlNanos,
                    KEY_COORDINATOR,
                    coordinatorId,
                    KEY_REQUESTING_NODE_ID,
                    requestingNodeId,
                    KEY_WANTS_QUEUE,
                    true
                )
                : Map.of(
                    KEY_BUCKET,
                    bucketKey,
                    KEY_SHARED_LIMIT,
                    sharedLimit,
                    KEY_PERMIT_ID,
                    permitId,
                    KEY_TTL_NANOS,
                    ttlNanos,
                    KEY_COORDINATOR,
                    coordinatorId
                );
            out.writeMap(body, StreamOutput::writeString, StreamOutput::writeGenericValue);
        }
    }

    /**
     * {@code owner -> coord}: granted, denied, or {@code not_owner} (the answering node no longer owns the bucket), and
     * whether a denial registered the asking coordinator for owner-push ({@code registered}).
     */
    static final class AcquirePermitResponse extends TransportResponse {
        static final String KEY_GRANTED = "granted";
        // Added after the baseline: written only when true and read with a default of false, so a peer that predates it
        // reads not_owner as a plain denial.
        static final String KEY_NOT_OWNER = "not_owner";
        // Added after the baseline, same encoding as not_owner. See the registered field for why false is the default.
        static final String KEY_REGISTERED = "registered";

        static final AcquirePermitResponse GRANTED = new AcquirePermitResponse(true, false, false);
        static final AcquirePermitResponse DENIED = new AcquirePermitResponse(false, false, false);
        static final AcquirePermitResponse DENIED_AND_REGISTERED = new AcquirePermitResponse(false, false, true);
        static final AcquirePermitResponse NOT_OWNER = new AcquirePermitResponse(false, true, false);

        final boolean granted;
        final boolean notOwner;
        /**
         * Owner-push contract on a DENIAL: {@code true} means "I have you in this bucket's waiter set and I will push a
         * grant when a slot frees", {@code false} means "no grant will ever come from me" — so a coordinator that retained
         * the request must reject it rather than wait forever (there is no queue timeout). Meaningful only when the
         * acquire set {@code wants_queue}.
         * <p>
         * Defaults to {@code false} when the key is absent, which is what makes a PRE-QUEUEING owner behave correctly:
         * such an owner ignores {@code wants_queue} and never sends a grant, so reading the missing key as "not
         * registered" turns a would-be stranded request into the pre-queueing 429.
         */
        final boolean registered;

        AcquirePermitResponse(boolean granted, boolean notOwner, boolean registered) {
            assert (granted && notOwner) == false : "a grant cannot also be a not-owner refusal";
            assert registered == false || (granted == false && notOwner == false) : "only a denial can register a waiter";
            this.granted = granted;
            this.notOwner = notOwner;
            this.registered = registered;
        }

        AcquirePermitResponse(StreamInput in) throws IOException {
            final Map<String, Object> body = readBody(in);
            this.granted = requireBoolean(body, KEY_GRANTED);
            this.notOwner = optionalBoolean(body, KEY_NOT_OWNER, false);
            this.registered = optionalBoolean(body, KEY_REGISTERED, false);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            final Map<String, Object> body;
            if (notOwner) {
                body = Map.of(KEY_GRANTED, granted, KEY_NOT_OWNER, true);
            } else if (registered) {
                body = Map.of(KEY_GRANTED, granted, KEY_REGISTERED, true);
            } else {
                body = Map.of(KEY_GRANTED, granted);
            }
            out.writeMap(body, StreamOutput::writeString, StreamOutput::writeGenericValue);
        }
    }

    /** {@code coord -> owner}: fire-and-forget, drop a previously granted permit. */
    static final class ReleasePermitRequest extends TransportRequest {
        static final String KEY_BUCKET = "bucket_key";
        static final String KEY_PERMIT_ID = "permit_id";
        // Added after the baseline, for owner-push: written only when set, read with a default of "".
        static final String KEY_QUEUE_EMPTY_ON_NODE_ID = "queue_empty_on_node_id";

        final String bucketKey;
        final String permitId;
        // Set to the coordinator's PERSISTENT node id only when this release returns an UNUSED grant: the owner then
        // deregisters that coordinator from the bucket's waiter set, since it has no queued request. Empty for a normal
        // release, and empty is also the default when an older peer omits the key. The bucket's shared limit is
        // deliberately not on the wire: the owner resolves it from its own cluster state.
        final String queueEmptyOnNodeId;

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
            this.queueEmptyOnNodeId = optionalString(body, KEY_QUEUE_EMPTY_ON_NODE_ID, "");
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            final Map<String, Object> body = queueEmptyOnNodeId.isEmpty()
                ? Map.of(KEY_BUCKET, bucketKey, KEY_PERMIT_ID, permitId)
                : Map.of(KEY_BUCKET, bucketKey, KEY_PERMIT_ID, permitId, KEY_QUEUE_EMPTY_ON_NODE_ID, queueEmptyOnNodeId);
            out.writeMap(body, StreamOutput::writeString, StreamOutput::writeGenericValue);
        }
    }

    /**
     * {@code coord -> new owner}. Restore waiter membership for a retained bucket after its ring owner changes. The new
     * owner resolves the live coordinator from its own cluster state, registers it idempotently, and immediately tries
     * to use any free shared capacity. Born with the map format, so both keys are baseline and read strictly.
     */
    static final class ReRegisterWaiterAfterOwnerChangeRequest extends TransportRequest {
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
            // Any other key is a field this build does not know about: ignored on purpose. That is the tolerance.
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

    /** {@code new owner -> coord}. Whether the receiving node accepted the owner-change waiter registration. */
    static final class ReRegisterWaiterAfterOwnerChangeResponse extends TransportResponse {
        static final String KEY_REGISTERED = "registered";

        static final ReRegisterWaiterAfterOwnerChangeResponse REGISTERED = new ReRegisterWaiterAfterOwnerChangeResponse(true);
        static final ReRegisterWaiterAfterOwnerChangeResponse NOT_REGISTERED = new ReRegisterWaiterAfterOwnerChangeResponse(false);

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
     * {@code owner -> coord}. Grant: a reserved shared permit for a bucket the coordinator is waiting on. The coordinator
     * does not use {@code shared_limit}: return/re-drive accounting resolves the owner's live limit from cluster state.
     * Born with the map format, so all three keys are baseline and read strictly.
     */
    static final class GrantPermitRequest extends TransportRequest {
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
     * (the caller returns the reserved slot). Implemented by {@link WorkloadGroupQueueService#admitWithPermit}.
     */
    @FunctionalInterface
    interface GrantConsumer {
        boolean admit(String bucketKey, Releasable reservedPermit);
    }
}
