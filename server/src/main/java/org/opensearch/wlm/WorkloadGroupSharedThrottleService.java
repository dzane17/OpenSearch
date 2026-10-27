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
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.tasks.TaskManager;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.RemoteTransportException;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;
import org.opensearch.wlm.SharedThrottleTracker.ExpiredPermit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongPredicate;
import java.util.function.LongSupplier;
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
 * Fail-closed: if the owner gives no answer (timeout, disconnect, missing handler, not_owner), the caller is told the
 * shared tier is unavailable ({@link #isUnavailable}) and never admits the request untracked, so
 * {@code shared_limit} is never silently exceeded: the request parks when its group queues and has room (see the
 * queueing paragraph below), and is rejected otherwise (MONITOR still admits). The cost is that overflow for an
 * unreachable owner's buckets waits, or is rejected without a queue. With no eligible owner at all
 * ({@link #hasEligibleOwner}), no node counts {@code shared_limit}, so {@link WorkloadGroupService} treats it as unset
 * instead.
 * <p>
 * Release ordering: an acquire answered too late is reported unavailable by the coordinator's own timer, but its
 * response handler stays registered and releases a late grant when it arrives. A release is therefore only ever sent
 * after the owner granted, so it can never overtake the acquire and strand the permit. A permit whose release never
 * arrives is purged when its coordinator leaves the cluster. At TTL expiry, the owner checks the coordinator's existing
 * task manager: active searches renew their permits, and finished searches free their slots. A search still waiting in
 * the coordinator's queue holds no permit, so its check frees the slot too (a grant whose reply was lost), which is then
 * offered to the bucket's waiters. An owner-push reservation names a placeholder task (see RESERVATION_TASK_ID) only
 * until its grant's outcome re-keys it to the queued search it admitted (the GRANT's reply, or in place for a local
 * waiter), so its checks then follow that search like any other permit's. A failed check extends the permit briefly, so
 * admission and the periodic sweep can retry without a separate backoff timer.
 * <p>
 * Queueing (owner-push): a coordinator that retained a denied request asks the owner to register it as a waiter on the
 * bucket. When a slot frees (a release, a reclaimed expired permit, a task check that finds its search finished or
 * waiting in its coordinator's queue, a coordinator purge, a {@code shared_limit} increase, or an ownership change), the
 * owner reserves a permit and pushes it to a registered coordinator ({@link #GRANT_ACTION_NAME}), which admits its oldest
 * retained request with it. A request also parks when the owner denied it without registering this coordinator, or could
 * not be asked at all; the coordinator's backstop pass ({@link #reconcileRetainedWaiters}, about every 5s) then registers
 * it with the bucket's current owner, as it does for any lost wake-up. A registration that cannot be confirmed (refused,
 * failed, timed out, or an owner that is not connected) rejects nothing: the parked requests wait for the next pass.
 */
@ExperimentalApi
public class WorkloadGroupSharedThrottleService implements ClusterStateListener {

    static final String ACQUIRE_ACTION_NAME = "internal:wlm/throttle/shared/acquire";
    static final String RELEASE_ACTION_NAME = "internal:wlm/throttle/shared/release";
    static final String CHECK_TASKS_ACTION_NAME = "internal:wlm/throttle/shared/check_tasks";
    // Owner -> coordinator: a shared slot freed and this coordinator has a registered waiter for the bucket; here is a
    // reserved permit to admit one queued request against (queueing / owner-push tier).
    static final String GRANT_ACTION_NAME = "internal:wlm/throttle/shared/grant";
    // Coordinator -> the bucket's current owner: idempotently register this coordinator as a waiter for a bucket where it
    // retains demand, and have the owner immediately offer any capacity already free there. Sent when an owner-ring change
    // moves a retained bucket, and by reconcileRetainedWaiters on every queue backstop pass (~5s) for each remote-owned
    // bucket with WAITING requests whose group still has a shared tier, as the backstop for a lost owner-push wake-up and
    // the registration of requests that parked without one.
    static final String REGISTER_WAITER_ACTION_NAME = "internal:wlm/throttle/shared/register_waiter";

    // Fixed constants rather than cluster settings: internal coordination knobs an operator should not need to tune.
    //
    // How long a request waits for the owner's acquire reply before the shared tier is reported unavailable (the caller
    // then parks the request if its group queues and has room, else rejects it). Enforced by a coordinator-side timer,
    // NOT a transport timeout: the response handler outlives it (up to ACQUIRE_REPLY_BACKSTOP), so a late grant is still
    // seen and released after the owner granted it.
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
    // Timeout for the fire-and-forget RELEASE. Nothing waits on it and expiry checks are the backstop, so it is long; it is
    // still bounded so a half-open connection can't pin the response handler.
    // Intentional: an owner stalled longer than these 30s windows while still a member makes TransportService log one
    // late-response WARN per pending acquire or release when it recovers, and an acquire it grants then has no handler left
    // to release it, so that permit stays counted until its expiry check (about PERMIT_TTL). Accepted as rare: fault
    // detection normally removes an owner stalled that long, moving its buckets, rather than adding a per-owner fast-fail.
    static final TimeValue RELEASE_TIMEOUT = TimeValue.timeValueSeconds(30);
    // A pushed grant is off the request hot path: its request is already retained on the coordinator. Give a temporarily
    // paused coordinator substantially longer to respond than ACQUIRE, while still reclaiming the reservation promptly
    // enough that one unresponsive waiter cannot hold a shared slot for the permit TTL. A waiter registration
    // (REGISTER_WAITER) uses it too, being off the hot path as well, but there the timeout decides nothing: the owner
    // handles the request when it arrives (registering this coordinator if it can), whatever the timeout, and the
    // coordinator only logs the outcome, so a failed or timed-out registration leaves the requests parked until the next
    // backstop pass sends it again. The timeout only bounds how long the reply handler stays registered; a reply after it
    // makes TransportService log a late-response WARN, which the 200ms acquire budget would cause for any slow owner.
    static final TimeValue GRANT_TIMEOUT = TimeValue.timeValueSeconds(5);
    // Only expired permits trigger task checks. Short searches add no task lookup, renewal message, or scheduled timer.
    // A checked permit stays counted until its task is confirmed finished or found waiting in its coordinator's queue (see
    // checkTasks); an active task renews it for another TTL.
    // An owner-push reservation is checked against the search its GRANT admitted (see RESERVATION_TASK_ID).
    static final long PERMIT_TTL_NANOS = TimeValue.timeValueMinutes(5).nanos();
    // How often the owner sweeps expired permits. A full bucket also finds expired permits in tracker.tryAcquire, which
    // runs on every acquire and on every offer, including the offer each waiter registration triggers
    // (reconcileRetainedWaiters, about every 5s from each coordinator holding WAITING requests for the bucket). So a lost
    // permit in a bucket with parked requests is found about one backstop pass after its TTL, and the task check that finds
    // its search finished (a lost release), or waiting in its coordinator's queue (a lost grant reply), offers the slot (see
    // completeCheck). This sweep covers buckets that see neither, where TTL + SWEEP_INTERVAL bounds only how long a leaked
    // permit stays counted.
    static final TimeValue SWEEP_INTERVAL = TimeValue.timeValueMinutes(5);
    static final TimeValue CHECK_TIMEOUT = TimeValue.timeValueSeconds(5);
    // Failed checks renew for this shorter TTL; the existing admission and sweep paths trigger the next check.
    // Intentional: repeated failures can keep permits counted indefinitely while the coordinator remains in membership,
    // preserving accounting for potentially active searches at the cost of extra 429s. A successful check resolves
    // liveness; node removal purges the coordinator's permits.
    static final TimeValue FAILED_CHECK_TTL = TimeValue.timeValueSeconds(30);
    static final int MAX_TASKS_PER_CHECK = 500;
    // Cap on expired permits waiting for a check. It holds only permits that outlived their TTL (long-running or leaked),
    // so it should never fill; an overflowing permit is treated like a failed check (extended by FAILED_CHECK_TTL).
    static final int MAX_QUEUED_CHECKS = 1_000;
    // Placeholder coordinator task id recorded on an owner-push reservation (see offerSharedSlots). The owner cannot know
    // which queued request the grant will admit, so the reservation first names a task that cannot exist (TaskManager
    // assigns positive ids). The placeholder lasts only until the GRANT's reply: one that names the admitted task re-keys
    // the reservation to it (SharedThrottleTracker#rekeyTask), so its expiry checks renew it while that search runs; one
    // that admitted nothing, or a failed GRANT, releases it. A local waiter's grant is re-keyed the same way, in place. Were
    // neither callback ever to run, the reservation's first expiry check would find no such task and free the slot.
    static final long RESERVATION_TASK_ID = -1L;

    private static final Logger logger = LogManager.getLogger(WorkloadGroupSharedThrottleService.class);

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final TransportService transportService;
    private final TaskManager taskManager;
    private final SharedThrottleTracker tracker;
    private final TimeValue acquireTimeout;
    // Owner-side queue only for expired permits; no coordinator registry or per-search renewal timer.
    private final BlockingQueue<ExpiredPermit> expiredChecks = new LinkedBlockingQueue<>(MAX_QUEUED_CHECKS);
    private final AtomicBoolean checksScheduled = new AtomicBoolean();
    private volatile Map<String, DiscoveryNode> coordinatorsByEphemeralId = Map.of();
    private volatile boolean stopped;

    // Immutable ring snapshot, swapped wholesale so readers never see a half-built ring.
    private final AtomicReference<ThrottleOwnerSelector> ring = new AtomicReference<>();
    // Set once the first cluster-state event has been compared in full. Written only on the cluster applier thread; read by
    // owner-side admission, which trusts the coordinator's routing until it is set.
    private volatile boolean ringInitialized;
    private volatile Scheduler.Cancellable sweepTask;

    // OWNER-SIDE waiter registry for owner-push queue draining: for each bucket this node owns, the ordered SET of
    // coordinator process incarnations that have at least one retained request for the bucket. A set (not a count) makes
    // registration idempotent per exact DiscoveryNode — repeated acquires from one incarnation do not inflate it. A
    // stale and replacement incarnation can coexist during topology convergence, so this is not strictly bounded by the
    // current node count. INSERTION-ORDERED (LinkedHashSet) so a waiter rotates to the tail once a slot is reserved for it
    // (see offerSharedSlots): membership persists across a successful hand-off (the coordinator may still have more
    // queued requests), and successive freed slots round-robin fairly across coordinators instead of repeatedly serving
    // whichever one hashes first. A coordinator is removed when a grant to it admits nothing (a local grant that finds the
    // queue empty or no consumer wired yet; a remote GRANT whose reply reports nothing admitted, or that fails, unless the
    // coordinator registered again while it was in flight); when an offer selects it while disconnected; when its group
    // loses the shared tier; when the coordinator leaves the cluster; or when this node loses ownership, including a
    // registration that races the loss. No request identity is held (the requests live on their coordinators).
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
    // marker. When a GRANT fails or its reply reports nothing admitted, an unchanged registration is removed before the
    // permit is released; a fresh registration is preserved because it is evidence that the coordinator is communicating
    // and has demand the grant did not see.
    private final Map<PendingGrantKey, PendingGrant> grantsAwaitingResponse = new ConcurrentHashMap<>();

    // COORDINATOR-SIDE: how a pushed grant is turned into an admitted queued request. Late-bound (the queue service is
    // constructed after this service); returns the task id of the queued request admitted with the reserved permit, or
    // empty if there was none (the GRANT's reply then reports nothing admitted, and the owner reclaims the reserved slot).
    // Null before wiring => no grant is ever consumed (a received grant is reported unused), which is safe.
    private volatile GrantConsumer grantConsumer;
    // COORDINATOR-SIDE: snapshot of bucket keys with retained PENDING_ACQUIRE or WAITING requests. Late-bound with the
    // queue service, just like grantConsumer. Null before wiring means an early ring update has no queue to reconcile.
    private volatile Supplier<Set<String>> retainedBucketKeysSupplier;
    // COORDINATOR-SIDE: whether a task has a request WAITING in this coordinator's queue, which a task check reports
    // inactive (see checkTasks). Late-bound with the queue service, just like grantConsumer. Null before wiring means no
    // task is waiting: every registered task is reported active.
    private volatile LongPredicate taskWaitingInQueue;
    // COORDINATOR-SIDE: told the bucket key when the ring has no eligible owner for a bucket where this coordinator
    // retains requests. Then no node counts the bucket's shared_limit, which admission treats as unset (see
    // hasEligibleOwner), so the requests are never rejected for it: the handler hands the bucket to the node tier. With a
    // node_limit, free node permits admit them and the rest wait for one; without one, all are admitted untracked.
    // Late-bound by WorkloadGroupService; null before wiring means nothing to decide.
    private volatile Consumer<String> noEligibleOwnerHandler;

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
        this(clusterService, threadPool, transportService, acquireTimeout, System::nanoTime);
    }

    WorkloadGroupSharedThrottleService(
        ClusterService clusterService,
        ThreadPool threadPool,
        TransportService transportService,
        TimeValue acquireTimeout,
        LongSupplier nanoTimeSupplier
    ) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.taskManager = transportService.getTaskManager();
        this.tracker = new SharedThrottleTracker(nanoTimeSupplier, this::checkExpiredPermits);
        this.acquireTimeout = acquireTimeout;
        // Start with an empty ring: there is no applied cluster state yet during node construction. The first
        // clusterChanged populates it (see there).
        this.ring.set(ThrottleOwnerSelector.fromDiscoveryNodes(DiscoveryNodes.EMPTY_NODES));
        clusterService.addListener(this);
        // None of the handlers trips the in-flight breaker: messages are small and bounded, and tripping it would turn one
        // owner's heap pressure into fail-closed 429s for every bucket it owns and strand breaker-rejected releases (and
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
                channel.sendResponse(ReleasePermitResponse.INSTANCE);
            }
        );
        transportService.registerRequestHandler(
            GRANT_ACTION_NAME,
            ThreadPool.Names.SAME,
            false,
            false,
            GrantPermitRequest::new,
            (request, channel, task) -> channel.sendResponse(handleGrant(request))
        );
        transportService.registerRequestHandler(
            CHECK_TASKS_ACTION_NAME,
            ThreadPool.Names.SAME,
            false,
            false,
            TaskCheckRequest::new,
            (request, channel, task) -> channel.sendResponse(checkTasks(request))
        );
        transportService.registerRequestHandler(
            REGISTER_WAITER_ACTION_NAME,
            ThreadPool.Names.SAME,
            false,
            false,
            RegisterWaiterRequest::new,
            (request, channel, task) -> channel.sendResponse(handleRegisterWaiter(request))
        );
    }

    /** Starts the periodic TTL sweep. Call exactly once, at node start: a second call would schedule a duplicate sweep. */
    public void start() {
        // Does not touch the ring: the initial cluster state may not be applied yet at node start.
        sweepTask = threadPool.scheduleWithFixedDelay(() -> {
            try {
                tracker.sweepExpired();
            } catch (Exception e) {
                logger.warn("Shared throttle TTL sweep failed", e);
            }
        }, SWEEP_INTERVAL, ThreadPool.Names.GENERIC);
    }

    public void stop() {
        stopped = true;
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
        final Map<String, DiscoveryNode> coordinators = new HashMap<>();
        for (DiscoveryNode node : event.state().nodes()) {
            coordinators.put(node.getEphemeralId(), node);
        }
        coordinatorsByEphemeralId = Map.copyOf(coordinators);
        // Free a departed coordinator's permits now rather than after the TTL, and drop its waiter registrations (its parked
        // requests died with it). Intentional: a coordinator removed while still alive (e.g. failed follower checks during a
        // long GC) keeps running its in-flight searches, so the bucket can briefly admit up to that coordinator's share above
        // shared_limit until they drain; their later releases are no-ops. Removal is by ephemeral id (a same-id restart
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
        // quickly maps back here and the coordinator re-registers. Clearing the marker stops that callback from removing
        // the waiter; it still settles its own reservation, re-keying it to the search it admitted (it then stays counted
        // until that search's release or expiry check) or releasing it and offering the freed slot. A reservation still
        // recorded when this process regains the bucket counts against the live limit.
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
            final WorkloadGroup currentGroup = currentGroups.get(entry.getKey());
            if (currentGroup == entry.getValue()) {
                continue; // the same instance: the group did not change, so it cannot have lost its shared tier
            }
            if (sharedLimit(entry.getValue()) < 1) {
                continue;
            }
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
                    if (groupsWithoutSharedTier.contains(WorkloadGroupService.groupIdOf(bucketKey))) {
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
                            // The ring emptied: no node counts shared_limit any more, so the node tier decides.
                            notifyNoEligibleOwner(bucketKey);
                            continue;
                        }
                        registerWaiterWithOwner(bucketKey, currentOwner);
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
            // Keep this numeric-increase hook disjoint from WorkloadGroupService's run-all policy. When that policy
            // applies, queued requests are admitted untracked and pushing shared permits at the old queue concurrently is
            // unnecessary.
            if (WorkloadGroupService.requiresQueueDrain(previousGroup, entry.getValue())) {
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
                // Snapshot the keys because grant outcomes can concurrently prune the waiter map.
                for (String bucketKey : Set.copyOf(waitersByBucket.keySet())) {
                    final Integer addedSlots = addedSlotsByGroup.get(WorkloadGroupService.groupIdOf(bucketKey));
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

    @Nullable
    private static Map<String, WorkloadGroup> workloadGroups(@Nullable ClusterState state) {
        if (state == null || state.metadata() == null) {
            return null;
        }
        return state.metadata().workloadGroups();
    }

    // A limit below 1 never admits through its tier (WorkloadGroupService treats it like an unset tier), so every
    // owner-push path treats it as "no shared tier". An uninterpretable config reads as unset rather than throwing.
    private static int sharedLimit(WorkloadGroup workloadGroup) {
        try {
            return WorkloadGroupThrottleSettings.SHARED_LIMIT.get(workloadGroup.getMutableWorkloadGroupFragment().getThrottling());
        } catch (IllegalArgumentException e) {
            return WorkloadGroupThrottleSettings.UNSET_LIMIT;
        }
    }

    /**
     * Whether any node can own a shared bucket. The ring is empty only when this node's applied cluster state has no data
     * node on {@link WorkloadGroupThrottleSettings#MIN_SUPPORTED_VERSION} or later: every data node is down, or the
     * cluster has none (one that only coordinates cross-cluster searches). Then no node counts {@code shared_limit}, so
     * rejecting would protect nothing. Unlike an owner that exists but cannot be reached, which may still be counting
     * running searches.
     */
    boolean hasEligibleOwner() {
        return ring.get().isEmpty() == false;
    }

    /**
     * Asynchronously acquires one shared permit for {@code bucketKey}, notifying {@code listener} with:
     * <ul>
     *   <li>{@code onResponse} with a {@link Releasable}: granted; close it when the request completes;</li>
     *   <li>{@code onFailure} with a message-less marker: {@link #isDenial} (bucket at its shared limit) or
     *       {@link #isUnavailable} (no authoritative answer from the owner). {@link WorkloadGroupService} decides whether
     *       the request parks or is rejected, and composes the user-facing 429, since this service only knows the opaque
     *       bucket key;</li>
     *   <li>{@code onFailure} with any other {@link OpenSearchRejectedExecutionException}: the search pool rejected the
     *       hand-off of a grant, which was already released. Never delivered when {@code proceedsOnDenial} is true.</li>
     * </ul>
     * <p>
     * Threading: the local short-circuit notifies inline. A remote outcome after which the search continues (every grant,
     * and every outcome when {@code proceedsOnDenial}) is delivered on the {@link ThreadPool.Names#SEARCH} pool, so the
     * coordinator's pre-fan-out work does not run on the few network threads of one hot owner's connections. A refusal
     * that the caller turns into a 429 or a park is delivered inline, so a throttled tenant's overflow does not queue
     * ahead of other tenants' shard work (see {@code deliverRefusal}). If the search pool rejects the hand-off, refusals
     * and MONITOR grants are delivered inline; any other grant is released and the rejection passed through.
     *
     * @param coordinatorTaskId the search task the permit is for, checked with the coordinator once the permit outlives its TTL
     * @param proceedsOnDenial  whether the caller continues the search even when denied or unavailable (MONITOR)
     */
    void acquireAsync(
        String bucketKey,
        int sharedLimit,
        long coordinatorTaskId,
        boolean proceedsOnDenial,
        ActionListener<Releasable> listener
    ) {
        acquireAsync(bucketKey, sharedLimit, coordinatorTaskId, proceedsOnDenial, listener, null);
    }

    /**
     * As {@link #acquireAsync(String, int, long, boolean, ActionListener)}, but for a caller that has already retained the exact
     * request as PENDING_ACQUIRE before this RPC. On denial, the owner registers this coordinator as a bucket waiter so a
     * later freed slot is pushed back here as a grant ({@link GrantConsumer}), and the denial marker tells the caller to
     * transition the request to WAITING. Passing a non-null {@code onNoWaiterRegistered} requests registration and
     * provides the path for a denial the owner could not register. Requesting registration requires
     * {@code proceedsOnDenial == false}: a caller that proceeds on every outcome (MONITOR) never retains a request.
     * <p>
     * {@code onNoWaiterRegistered} runs INSTEAD of a denial when the owner denied the acquire and reported that it could
     * not register this coordinator — see {@link AcquirePermitResponse#registered}. No grant follows from that denial, so
     * the caller still parks the retained request, and its backstop pass ({@link #reconcileRetainedWaiters}) registers
     * this coordinator with the owner later. Like a denial it only moves (or rejects) the retained entry, so it is
     * delivered inline with the caller's thread context restored. Every other outcome, including an unavailable shared
     * tier and a search-pool rejection of a grant's hand-off, is reported through {@code listener} exactly as documented
     * above. An unavailable outcome registers no waiter either: a caller that parks on it relies on the same backstop
     * pass.
     *
     * @param coordinatorTaskId the local task holding the permit, whose liveness the owner checks once the permit's TTL
     *        expires
     * @param onNoWaiterRegistered run on a denial the owner could not register; {@code null} to not request registration
     */
    void acquireAsync(
        String bucketKey,
        int sharedLimit,
        long coordinatorTaskId,
        boolean proceedsOnDenial,
        ActionListener<Releasable> listener,
        @Nullable Runnable onNoWaiterRegistered
    ) {
        final boolean wantsQueue = onNoWaiterRegistered != null;
        assert wantsQueue == false || proceedsOnDenial == false : "a caller that proceeds on denial never retains a request";
        final ThrottleOwnerSelector currentRing = ring.get();
        final DiscoveryNode owner = currentRing.ownerFor(bucketKey).orElse(null);
        if (owner == null) {
            // Empty ring: no owner to ask. The caller checks hasEligibleOwner first, so this only covers a ring that
            // emptied after that check.
            listener.onFailure(unavailableMarker());
            return;
        }

        final String permitId = permitId();

        // Local-owner short-circuit; the post-acquire fence re-reads the live ring.
        final DiscoveryNode localNode = clusterService.localNode();
        final AcquirePermitRequest request = new AcquirePermitRequest(
            bucketKey,
            sharedLimit,
            permitId,
            PERMIT_TTL_NANOS,
            localNode.getEphemeralId(),
            coordinatorTaskId,
            wantsQueue
        );
        if (owner.equals(localNode)) {
            if (tracker.tryAcquire(request) == false) {
                // The waiter is this very node, always resolvable and reachable, so registration can only fail because
                // ownership moved after the ring was read: then no authoritative answer was obtained.
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
        final TransportRequestOptions options = TransportRequestOptions.builder().withTimeout(ACQUIRE_REPLY_BACKSTOP).build();
        transportService.sendRequest(owner, ACQUIRE_ACTION_NAME, request, options, acquire);
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
                // is released by handleResponse once it arrives, and admits nothing. A queueing caller parks its
                // PENDING_ACQUIRE entry as WAITING (or rejects it as unavailable when its queue is full); a late registered
                // denial has then registered this coordinator, so the owner's next pushed grant can admit it.
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
                    sendRelease(owner, bucketKey, permitId);
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
                // Denied, and the owner cannot push us a grant (we are absent from its applied cluster state, or present but
                // unreachable). The caller still parks the retained request; its backstop pass registers this coordinator.
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
            // no-op otherwise. Any other failure gives no such ordering, so nothing is sent; the purge or TTL reclaims it,
            // also when the request parks on this outcome (its task check then reports it inactive, see checkTasks).
            if (exp instanceof RemoteTransportException) {
                sendRelease(owner, bucketKey, permitId);
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

        // Inline when the caller does not proceed on it (it rejects or parks the request), otherwise on the search pool
        // (inline if the pool rejects it).
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
    // A joining node gets acquires for its buckets from coordinators that applied its join before it applied its own first
    // cluster state, and until then its ring is empty. The fence is skipped in that window: refusing would only fail those
    // requests closed, since the joiner's tracker is empty anyway, and any over-admission is the bounded rebalance effect.
    AcquirePermitResponse handleAcquire(AcquirePermitRequest request) {
        if (ringInitialized && isStillOwner(request.bucketKey) == false) {
            return AcquirePermitResponse.NOT_OWNER;
        }
        // Intentional: the owner trusts the coordinator's limit rather than re-resolving it, so while a shared_limit change
        // propagates, coordinators that have not applied it yet enforce the old value. Short and self-correcting.
        if (tracker.tryAcquire(request) == false) {
            return request.wantsQueue ? registerRemoteWaiterOnDenial(request) : AcquirePermitResponse.DENIED;
        }
        return ringInitialized && fenceIfOwnershipLost(request.bucketKey, request.permitId)
            ? AcquirePermitResponse.NOT_OWNER
            : AcquirePermitResponse.GRANTED;
    }

    // Denied and the coordinator already retained the request -> remember it so a freed slot is pushed back as a grant.
    // A registered response lets the coordinator transition the exact PENDING_ACQUIRE entry to WAITING. The waiter set
    // must hold the real DiscoveryNode because sendGrant needs its transport address, so look coordinator_node_id (the
    // ephemeral id, which the purge and the expiry check use too) up in this node's applied membership.
    //
    // Register ONLY if that exact process is a member here and connected, and tell the coordinator either way (the
    // `registered` bit on the response). The lookup is by exact process, and this node's applied state can lag the
    // coordinator's: a node that just joined, or a restarted coordinator under its new ephemeral id, is simply absent until
    // this node applies its join or rejoin, never resolved to a previous incarnation. Its registration is refused, and the
    // coordinator's next backstop pass retries it (REGISTER_WAITER); told `registered=false`, the coordinator still parks
    // the request if its queue has room. nodeConnected adds a transient disconnect, in one lock-free connection-map lookup.
    // Losing ownership while registering is reported as not_owner, the same as the fences above.
    // Accepted: coordinatorsByEphemeralId is refreshed by this service's cluster-state listener just after the applier sets
    // the new state, so a registration that arrives in that window is refused and retried, as one arriving before it is.
    private AcquirePermitResponse registerRemoteWaiterOnDenial(AcquirePermitRequest request) {
        final DiscoveryNode coordinator = reachableCoordinator(request.bucketKey, request.coordinatorId);
        if (coordinator == null) {
            return AcquirePermitResponse.DENIED;
        }
        if (registerWaiter(request.bucketKey, coordinator) == false) {
            return AcquirePermitResponse.NOT_OWNER;
        }
        offerIfReleasedBeforeRegistration(request.bucketKey, request.sharedLimit);
        return AcquirePermitResponse.DENIED_AND_REGISTERED;
    }

    // OWNER-SIDE: the coordinator process a waiter registration names by its ephemeral id, or null if this node cannot
    // register it (see registerRemoteWaiterOnDenial): the id is absent from this node's applied membership, or the node is
    // not connected.
    @Nullable
    private DiscoveryNode reachableCoordinator(String bucketKey, String coordinatorId) {
        final DiscoveryNode coordinator = coordinatorsByEphemeralId.get(coordinatorId);
        if (coordinator == null || transportService.nodeConnected(coordinator) == false) {
            logger.debug(
                "Not registering waiter [{}] for bucket [{}]: {}",
                coordinatorId,
                bucketKey,
                coordinator == null ? "absent from the applied cluster state" : "not connected"
            );
            return null;
        }
        return coordinator;
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

    /**
     * Checks permits whose TTL expired (found by the periodic sweep or by an acquire on a full bucket). Each check ends in
     * exactly one outcome:
     * <ul>
     *   <li>task still registered on its coordinator, and not waiting in its queue: the permit is renewed for the full
     *       TTL;</li>
     *   <li>task finished (which also covers a lost release), or waiting in its coordinator's queue (which covers a grant
     *       whose reply was lost, see {@link #checkTasks}): the permit is removed and its slot freed;</li>
     *   <li>coordinator no longer in cluster membership: the permit is removed, as in the departed-coordinator purge;</li>
     *   <li>check could not complete (coordinator still a member but not connected, RPC failure or timeout, executor
     *       rejection, shutdown, or a full check queue): the permit is extended by {@link #FAILED_CHECK_TTL}.</li>
     * </ul>
     * The last outcome repeats for as long as its cause persists, but paced: an extended permit is checked again only when
     * a later scan finds it expired, so at most once per {@link #FAILED_CHECK_TTL}, batched per coordinator. Retries end
     * when a check succeeds or the coordinator leaves the cluster; until then the permit stays counted (extra 429s rather
     * than over-admission). A check that removes a permit offers the freed slot to the bucket's waiters, since no release
     * RPC will ever do so.
     */
    private void checkExpiredPermits(List<ExpiredPermit> expired) {
        if (stopped) {
            retryChecks(expired);
            return;
        }
        for (ExpiredPermit permit : expired) {
            if (expiredChecks.offer(permit) == false) {
                tracker.renewForRetry(permit, FAILED_CHECK_TTL.nanos());
            }
        }
        scheduleChecks();
    }

    private void scheduleChecks() {
        if (checksScheduled.compareAndSet(false, true) == false) {
            return;
        }
        // Intentional: a drain triggered by admission inherits that request's ThreadContext (GENERIC preserves it), as
        // ACQUIRE/RELEASE do. CHECK_TASKS only reads the coordinator's task manager, and a GRANT it triggers resumes the
        // parked request in that request's own context, which the queue preserved when it parked.
        try {
            threadPool.executor(ThreadPool.Names.GENERIC).execute(new AbstractRunnable() {
                @Override
                protected void doRun() {
                    try {
                        drainChecks();
                    } finally {
                        finishChecks();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    retryQueuedChecks();
                    finishChecks();
                    logger.debug("Shared throttle task checks could not be scheduled", e);
                }
            });
        } catch (Exception e) {
            retryQueuedChecks();
            finishChecks();
            logger.debug("Shared throttle task checks could not be scheduled", e);
        }
    }

    private void finishChecks() {
        checksScheduled.set(false);
        if (expiredChecks.isEmpty() == false) {
            scheduleChecks();
        }
    }

    private void retryQueuedChecks() {
        ExpiredPermit expired;
        while ((expired = expiredChecks.poll()) != null) {
            tracker.renewForRetry(expired, FAILED_CHECK_TTL.nanos());
        }
    }

    private void drainChecks() {
        if (stopped) {
            retryQueuedChecks();
            return;
        }
        final Map<String, List<ExpiredPermit>> byCoordinator = new HashMap<>();
        ExpiredPermit expired;
        while ((expired = expiredChecks.poll()) != null) {
            byCoordinator.computeIfAbsent(expired.coordinatorId(), ignored -> new ArrayList<>()).add(expired);
        }
        for (Entry<String, List<ExpiredPermit>> entry : byCoordinator.entrySet()) {
            final DiscoveryNode coordinator = coordinatorsByEphemeralId.get(entry.getKey());
            if (coordinator == null) {
                // The coordinator incarnation is absent from the applied membership, matching removal cleanup.
                entry.getValue().forEach(permit -> completeCheck(permit, false));
                continue;
            }
            final List<ExpiredPermit> permits = entry.getValue();
            for (int offset = 0; offset < permits.size(); offset += MAX_TASKS_PER_CHECK) {
                checkCoordinator(coordinator, List.copyOf(permits.subList(offset, Math.min(offset + MAX_TASKS_PER_CHECK, permits.size()))));
            }
        }
    }

    private void checkCoordinator(DiscoveryNode coordinator, List<ExpiredPermit> expired) {
        final long[] taskIds = new long[expired.size()];
        for (int i = 0; i < taskIds.length; i++) {
            taskIds[i] = expired.get(i).coordinatorTaskId();
        }
        final TaskCheckRequest request = new TaskCheckRequest(coordinator.getEphemeralId(), taskIds);
        try {
            if (coordinator.equals(clusterService.localNode())) {
                completeChecks(expired, checkTasks(request));
            } else if (transportService.nodeConnected(coordinator) == false) {
                retryChecks(expired);
            } else {
                transportService.sendRequest(
                    coordinator,
                    CHECK_TASKS_ACTION_NAME,
                    request,
                    TransportRequestOptions.builder().withTimeout(CHECK_TIMEOUT).build(),
                    new TransportResponseHandler<TaskCheckResponse>() {
                        @Override
                        public TaskCheckResponse read(StreamInput in) throws IOException {
                            return new TaskCheckResponse(in);
                        }

                        @Override
                        public void handleResponse(TaskCheckResponse response) {
                            completeChecks(expired, response);
                        }

                        @Override
                        public void handleException(TransportException e) {
                            retryChecks(expired);
                            logger.debug(() -> "Shared throttle task check to coordinator [" + coordinator.getId() + "] failed", e);
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.SAME;
                        }
                    }
                );
            }
        } catch (Exception e) {
            retryChecks(expired);
            logger.debug(() -> "Shared throttle task check to coordinator [" + coordinator.getId() + "] failed", e);
        }
    }

    TaskCheckResponse checkTasks(TaskCheckRequest request) {
        if (clusterService.localNode().getEphemeralId().equals(request.coordinatorId) == false) {
            throw new IllegalStateException("Shared throttle task check addressed to a different coordinator incarnation");
        }
        final LongPredicate waitingInQueue = taskWaitingInQueue;
        final boolean[] active = new boolean[request.taskIds.length];
        for (int i = 0; i < request.taskIds.length; i++) {
            final long taskId = request.taskIds[i];
            // Cancellation is not completion: a cancelled task stays counted while still registered. A task WAITING in this
            // coordinator's queue is reported inactive although it is registered: a WAITING request never holds a shared
            // permit (the node tier is tried first, and every admission takes the request out of WAITING), so a permit
            // recorded for it is a grant whose reply was lost (a transport failure that sends no release), after which
            // the request parked. Renewed, that orphan would hold its slot for as long as the request waits, and in a
            // full bucket the request would wait for a slot only it holds. Freed, the slot goes to the bucket's waiters.
            active[i] = taskManager.getTask(taskId) != null && (waitingInQueue == null || waitingInQueue.test(taskId) == false);
        }
        return new TaskCheckResponse(active);
    }

    private void completeChecks(List<ExpiredPermit> expired, TaskCheckResponse response) {
        if (response.active.length != expired.size()) {
            retryChecks(expired);
            return;
        }
        for (int i = 0; i < response.active.length; i++) {
            completeCheck(expired.get(i), response.active[i]);
        }
    }

    // The permit of a task reported inactive (finished, or waiting in its coordinator's queue) frees a shared slot with no
    // release RPC behind it, so drive owner-push here.
    private void completeCheck(ExpiredPermit expired, boolean active) {
        if (tracker.completeCheck(expired, active)) {
            offerSharedSlots(expired.bucketKey, 1);
        }
    }

    // The "check could not complete" outcome described on checkExpiredPermits.
    private void retryChecks(List<ExpiredPermit> expired) {
        for (ExpiredPermit permit : expired) {
            tracker.renewForRetry(permit, FAILED_CHECK_TTL.nanos());
        }
    }

    // A local release must also drive owner-push when this node owns the bucket: releasing frees a slot that a registered
    // waiter should get. Same rule as the remote path's handleRelease, so local-owner and remote-owner behave identically.
    // releaseOnce: a double close (e.g. from both a completion and a failure path) releases only once.
    private Releasable releaseLocal(String bucketKey, String permitId) {
        return Releasables.releaseOnce(() -> {
            if (tracker.release(bucketKey, permitId)) {
                offerSharedSlots(bucketKey, 1);
            }
        });
    }

    // Closing does not wait for the owner to apply the release. SearchExecutionGate#execute closes the permit before
    // notifying, so a follow-up acquire (an _msearch's next sub-search) can reach the owner first and see the bucket full,
    // a rare false 429 under saturation. Accepted.
    private Releasable releaseRemote(DiscoveryNode owner, String bucketKey, String permitId) {
        return Releasables.releaseOnce(() -> sendRelease(owner, bucketKey, permitId));
    }

    // Fire-and-forget RELEASE to the bucket owner. A lost release is reclaimed by the expired permit's task check, so
    // failures are only logged at debug.
    private void sendRelease(DiscoveryNode owner, String bucketKey, String permitId) {
        final TransportRequestOptions options = TransportRequestOptions.builder().withTimeout(RELEASE_TIMEOUT).build();
        transportService.sendRequest(
            owner,
            RELEASE_ACTION_NAME,
            new ReleasePermitRequest(bucketKey, permitId),
            options,
            new TransportResponseHandler<ReleasePermitResponse>() {
                @Override
                public ReleasePermitResponse read(StreamInput in) throws IOException {
                    return new ReleasePermitResponse(in);
                }

                @Override
                public void handleResponse(ReleasePermitResponse response) {}

                @Override
                public void handleException(TransportException exp) {
                    // Accepted: not retried. If the connection to the owner briefly drops while both nodes stay in the
                    // cluster, the permit stays counted until a task check, which can cause temporary false 429s for the bucket.
                    logger.debug(
                        "Shared throttle release to owner [{}] for bucket [{}] failed; expiry check will reclaim",
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

    private static String permitId() {
        return UUIDs.base64UUID();
    }

    /**
     * Late-binds the coordinator-local request queue: pushed grants admit its oldest retained request for the bucket, an
     * owner-ring change re-registers its retained buckets with their new owners, and a task check reports a task waiting in
     * it as inactive. Called once during node construction.
     */
    public void setQueueService(WorkloadGroupQueueService queueService) {
        setGrantConsumer(queueService::admitWithPermit);
        setRetainedBucketKeysSupplier(queueService::retainedBucketKeys);
        setTaskWaitingInQueue(queueService::isWaiting);
    }

    /**
     * Late-binds the coordinator-side grant consumer. A received grant admits one queued request via this; before it is
     * set, a grant's reply reports nothing admitted, so the owner that issued it reclaims the reserved slot.
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
     * Late-binds the coordinator-side test of whether a task has a request WAITING in this coordinator's queue, which a
     * task check reports inactive (see {@link #checkTasks}). Before it is set, no task counts as waiting.
     */
    void setTaskWaitingInQueue(LongPredicate taskWaitingInQueue) {
        this.taskWaitingInQueue = Objects.requireNonNull(taskWaitingInQueue);
    }

    /**
     * Late-binds the coordinator-side handler told when the ring has no eligible owner for a bucket where this coordinator
     * retains requests (see {@link #reconcileRetainedWaiters}). No node counts that bucket's {@code shared_limit}, so the
     * handler lets the node tier decide for those requests rather than rejecting them.
     */
    void setNoEligibleOwnerHandler(Consumer<String> noEligibleOwnerHandler) {
        this.noEligibleOwnerHandler = Objects.requireNonNull(noEligibleOwnerHandler);
    }

    private void notifyNoEligibleOwner(String bucketKey) {
        final Consumer<String> handler = noEligibleOwnerHandler;
        if (handler == null) {
            return;
        }
        try {
            handler.accept(bucketKey);
        } catch (Exception e) {
            logger.warn("Failed to hand the retained requests for bucket [" + bucketKey + "] to the node tier with no eligible owner", e);
        }
    }

    /**
     * COORDINATOR-SIDE backstop for retained WAITING demand, run on every queue backstop pass ({@code
     * WorkloadGroupService#doRun}, about every 5s). Owner-push is this coordinator's only wake-up for a shared-tier WAITING
     * request, and the owner may not have this coordinator registered: a failed or timed-out GRANT drops the waiter, a
     * local grant that found the queue empty can drop a registration that raced it, an owner change can race the
     * re-registration, and a request that parked on a denial the owner could not register, or on an acquire the shared
     * tier could not answer, was never registered at all. Re-registering with the bucket's current owner is idempotent
     * and makes that owner offer any capacity that is already free, so once the owner can register this coordinator, a
     * lost or missing wake-up costs at most one coordinator backstop pass
     * ({@code WorkloadGroupService#QUEUE_BACKSTOP_SWEEP_INTERVAL_NANOS}, not this class's owner-side {@code SWEEP_INTERVAL}).
     * <p>
     * With an empty ring there is no owner to register with, and no node counts the bucket's {@code shared_limit}, so
     * nothing is rejected: the no-eligible-owner handler hands the bucket to the node tier. With a {@code node_limit},
     * free node permits admit its requests and the rest wait for one; without one, all are admitted untracked. A
     * registration that cannot be confirmed rejects nothing either (see {@link #registerWaiterWithOwner}): the WAITING
     * requests stay parked and the next pass tries again.
     * <p>
     * Accepted: a coordinator that cannot reach a bucket's owner while both stay in the cluster (a link failure between
     * only those two nodes) keeps that bucket's parked requests, and parks new ones up to the queue limits, until a
     * registration goes through, a grant arrives, or the requests are cancelled. The requests wait rather than fail,
     * bounded by the queue limits and by cancellation (a search on client disconnect or its
     * {@code cancel_after_time_interval}, a scroll page only through the tasks API), and every pass retries the
     * registration, so they resume once the link is back.
     */
    void reconcileRetainedWaiters(String bucketKey) {
        final DiscoveryNode owner = ring.get().ownerFor(bucketKey).orElse(null);
        if (owner == null) {
            notifyNoEligibleOwner(bucketKey);
            return;
        }
        registerWaiterWithOwner(bucketKey, owner);
    }

    /**
     * COORDINATOR-SIDE waiter (re-)registration with the bucket's current owner. The request queue lives here, so this
     * node tells the owner it still has demand for {@code bucketKey}; the owner registers it idempotently and offers any
     * free capacity. Called for retained buckets whose owner changed after a ring remap, and by
     * {@link #reconcileRetainedWaiters} on every queue backstop pass. The call is asynchronous, and an outcome other than a
     * registration is only logged: a refusal (an owner that has not applied its first cluster state, applied a ring change
     * after this node, or cannot resolve or reach this node yet), an RPC that fails or outlasts {@code GRANT_TIMEOUT}, and
     * an owner this node is not connected to all leave the bucket's retained requests parked, and the next backstop pass
     * sends the registration again. There is deliberately no retry timer and no time limit: a parked request leaves the
     * queue only through a grant, a node permit, the no-eligible-owner handling, cancellation of its task, or a live
     * configuration change.
     */
    void registerWaiterWithOwner(String bucketKey, DiscoveryNode owner) {
        if (isCurrentOwner(bucketKey, owner) == false) {
            return; // superseded by a newer ring before this asynchronous reconciliation ran
        }
        final DiscoveryNode self = clusterService.localNode();
        final RegisterWaiterRequest request = new RegisterWaiterRequest(bucketKey, self.getEphemeralId());
        if (owner.equals(self)) {
            if (handleRegisterWaiter(request).registered == false) {
                logger.debug("This node did not register itself as a waiter for bucket [{}] it owns", bucketKey);
            }
            return;
        }
        if (transportService.nodeConnected(owner) == false) {
            logger.debug("Cannot register waiter for bucket [{}]: owner [{}] is not connected", bucketKey, owner.getId());
            return;
        }
        // Off the request hot path like GRANT (see GRANT_TIMEOUT), not the 200ms acquire budget.
        final TransportRequestOptions options = TransportRequestOptions.builder().withTimeout(GRANT_TIMEOUT).build();
        transportService.sendRequest(
            owner,
            REGISTER_WAITER_ACTION_NAME,
            request,
            options,
            new TransportResponseHandler<RegisterWaiterResponse>() {
                @Override
                public RegisterWaiterResponse read(StreamInput in) throws IOException {
                    return new RegisterWaiterResponse(in);
                }

                @Override
                public void handleResponse(RegisterWaiterResponse response) {
                    if (response.registered == false) {
                        logger.debug("Owner [{}] did not register this node as a waiter for bucket [{}]", owner.getId(), bucketKey);
                    }
                }

                @Override
                public void handleException(TransportException exp) {
                    logger.debug("Waiter registration with owner [" + owner.getId() + "] for bucket [" + bucketKey + "] failed", exp);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            }
        );
    }

    // Whether this node's ring still maps bucketKey to owner, so a registration with owner is the one that counts.
    private boolean isCurrentOwner(String bucketKey, DiscoveryNode owner) {
        return Objects.equals(ring.get().ownerFor(bucketKey).orElse(null), owner);
    }

    // OWNER SIDE: (re-)register one coordinator as a waiter and immediately try to use capacity that may already be free.
    // Registration alone is insufficient: after an ownership change, after a lost wake-up that the coordinator's periodic
    // reconcile is recovering, or for requests that parked without a registration, no later release may arrive to trigger
    // owner-push. Package-private for tests.
    RegisterWaiterResponse handleRegisterWaiter(RegisterWaiterRequest request) {
        if (isStillOwner(request.bucketKey) == false) {
            return RegisterWaiterResponse.NOT_REGISTERED;
        }
        final int sharedLimit = currentSharedLimit(request.bucketKey);
        if (sharedLimit < 1) {
            return RegisterWaiterResponse.NOT_REGISTERED;
        }
        // This node registers itself without a lookup; any other coordinator is looked up as on a denial (see
        // registerRemoteWaiterOnDenial), by the exact process its ephemeral id names.
        final DiscoveryNode self = clusterService.localNode();
        final DiscoveryNode coordinator = self.getEphemeralId().equals(request.coordinatorId)
            ? self
            : reachableCoordinator(request.bucketKey, request.coordinatorId);
        if (coordinator == null) {
            return RegisterWaiterResponse.NOT_REGISTERED;
        }
        if (registerWaiter(request.bucketKey, coordinator) == false) {
            return RegisterWaiterResponse.NOT_REGISTERED;
        }
        // Offer up to the entire live limit so retained demand can resume without waiting for a release event that may
        // never arrive on this owner. tryAcquire enforces the actual availability, including any old records retained
        // if this same process previously owned and then regained the bucket.
        offerSharedSlots(request.bucketKey, sharedLimit);
        return isStillOwner(request.bucketKey) ? RegisterWaiterResponse.REGISTERED : RegisterWaiterResponse.NOT_REGISTERED;
    }

    // OWNER-SIDE: record that a coordinator is waiting on a bucket this node owns. Idempotent (a set), so re-registering
    // an already-known waiter is a no-op — it keeps its existing queue position (LinkedHashSet.add does not reorder an
    // element already present), so re-registration can't let a coordinator jump the rotation. During topology
    // convergence, stale and replacement incarnations may coexist until the stale one is selected while disconnected,
    // purged with its departed incarnation, or dropped with ownership. Returns false only if this node does not own the
    // bucket.
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
     * OWNER-SIDE release: free the permit and drive owner-push if a slot genuinely freed. A RELEASE carries no
     * waiter-membership signal: a grant that admitted nothing is returned by the GRANT's reply (see sendGrant). The offer
     * a freed slot drives can still drop a waiter, as any offer can. Package-private and shared with the transport
     * handler rather than duplicated, so a test can drive it without the two copies drifting apart.
     * <p>
     * The decision is the OWNER's, not the coordinator's. Whether a slot freed comes from whether the remove actually hit
     * a recorded permit — a coordinator's release may be speculative (after an acquire failed with a remote error), so it
     * cannot know. The ceiling is resolved from this node's cluster state, whose view is the one being enforced. Deciding
     * here means a no-op release never produces a phantom grant and a release that did free a slot always drives one.
     */
    void handleRelease(ReleasePermitRequest request) {
        if (tracker.release(request.bucketKey, request.permitId)) {
            offerSharedSlots(request.bucketKey, 1);
        }
    }

    // OWNER-SIDE: drop a coordinator from a bucket's waiter set (a local grant found nothing to admit, it was selected
    // while disconnected, or its registration raced an ownership loss). DiscoveryNode equality uses the ephemeral id, so
    // this removes only the exact process incarnation and cannot erase a replacement node that reused the persistent id.
    private void removeWaiter(String bucketKey, DiscoveryNode coordinator) {
        waitersByBucket.computeIfPresent(bucketKey, (k, nodes) -> {
            nodes.remove(coordinator);
            return nodes.isEmpty() ? null : nodes;
        });
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
        final WorkloadGroup workloadGroup = groups == null ? null : groups.get(WorkloadGroupService.groupIdOf(bucketKey));
        return workloadGroup == null ? WorkloadGroupThrottleSettings.UNSET_LIMIT : sharedLimit(workloadGroup);
    }

    /**
     * OWNER-SIDE: offer a bounded number of newly-available shared slots to registered coordinators.
     * <p>
     * The caller supplies the event's exact slot budget:
     * <ul>
     *   <li>an ordinary permit return offers one;</li>
     *   <li>a {@code shared_limit} increase offers {@code newLimit - oldLimit} once from the cluster-change hook;</li>
     *   <li>a task check that finds an expired permit's search finished, or waiting in its coordinator's queue, offers one
     *       (a sweep only starts checks);</li>
     *   <li>a newly-selected owner, or an owner answering a coordinator's periodic waiter reconcile, may offer up to the
     *       live limit because no later release may trigger a drive (tryAcquire stops at the capacity actually free, so a
     *       full bucket costs one failed reservation).</li>
     * </ul>
     * Thus normal request completion stays O(one grant); only the uncommon event that creates several slots runs a
     * bulk offer. The owner intentionally tracks only waiter membership, not remote queue depth, so a bulk event can
     * speculatively send excess grants to a shallow queue. Their replies report nothing admitted, which returns them.
     * The speculative batch is capped by the maximum possible retained demand represented by the registered
     * coordinators, preventing an extreme configured limit from creating an unbounded message burst.
     */
    private void offerSharedSlots(String bucketKey, int slotsToOffer) {
        // No registered waiter (always the case for a group without a queue): nothing to offer and nothing to prune, so
        // skip the ring lookup. Lock-free. Every registration path offers again right after registering
        // (offerIfReleasedBeforeRegistration, handleRegisterWaiter), so a waiter that registers after this read still gets
        // the freed slot.
        if (waitersByBucket.containsKey(bucketKey) == false) {
            return;
        }
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
            final PendingGrantSelection selection = pickEligibleWaiter(bucketKey);
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
            // target coordinator so its purge covers the reservation too. If the bucket is at its limit again (a racing
            // acquire beat us), don't over-grant: stop. The pick did not rotate the waiter, so it keeps its place at the
            // head and the next freed slot goes to it.
            // The owner does not know yet which queued request the grant will admit, so the reservation names the
            // RESERVATION_TASK_ID placeholder until the grant's outcome re-keys it to the admitted task (consumeGrantLocal
            // for a local waiter, the GRANT's reply for a remote one, see sendGrant) or releases it. From then on its expiry
            // checks renew it while the admitted search runs, like any directly acquired permit.
            if (tracker.tryAcquire(
                new AcquirePermitRequest(
                    bucketKey,
                    sharedLimit,
                    reservedPermitId,
                    PERMIT_TTL_NANOS,
                    target.getEphemeralId(),
                    RESERVATION_TASK_ID,
                    false
                )
            ) == false) {
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
            // The turn is used only once a slot is reserved: move the waiter to the tail (it stays registered). A failed
            // reservation above leaves it at the head, so the next freed slot still goes to it. Accepted: two offers running
            // at once can both pick a local head waiter (it carries no pending marker); the second one's grant then admits
            // its next queued request or finds none and moves on to the next waiter.
            waitersByBucket.computeIfPresent(bucketKey, (k, nodes) -> {
                if (nodes.remove(target)) {
                    nodes.add(target);
                }
                return nodes.isEmpty() ? null : nodes;
            });
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
                // pickEligibleWaiter installed this coordinator/bucket's transient pending marker before the reservation. A
                // concurrent capacity event therefore skips this waiter until the response resolves.
                if (sendGrant(target, bucketKey, reservedPermitId, selection)) {
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
    // dispatch failure (stop, because retrying could poll and drop more requests). An admission re-keys the reservation
    // to the admitted task in place, as a remote GRANT's reply does (see sendGrant).
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
        final OptionalLong admitted;
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
        if (admitted.isEmpty()) {
            // The consumer does not close the permit when it admits nothing; release the reserved permit directly so the
            // slot is reused by this loop. (Not via the permit's close(), which would re-drive owner-push inline.)
            tracker.release(bucketKey, reservedPermitId);
            removeWaiter(bucketKey, self); // this coordinator has nothing queued for the bucket
            return LocalGrantResult.NO_DEMAND;
        }
        // The admitted search may already have finished and released the reservation; the re-key never re-creates it.
        tracker.rekeyTask(bucketKey, reservedPermitId, RESERVATION_TASK_ID, admitted.getAsLong());
        return LocalGrantResult.ADMITTED;
    }

    // Current number of coordinators waiting on a bucket (0 if none). Reads the size under the map's per-key remapping
    // lock (returning the set unchanged), since the LinkedHashSet is not safe to size concurrently with a rotate/add.
    // Visible for tests.
    int waiterCount(String bucketKey) {
        final int[] count = new int[1];
        waitersByBucket.computeIfPresent(bucketKey, (k, nodes) -> {
            count[0] = nodes.size();
            return nodes;
        });
        return count[0];
    }

    // OWNER-SIDE: pick the first eligible waiter, installing the pending marker for a remote one; it does not rotate.
    // offerSharedSlots rotates the waiter once a slot is reserved for it, so a failed reservation does not cost the head
    // waiter its turn. A local waiter is always eligible because it is consumed synchronously. A remote waiter is eligible
    // only if this method can atomically install its transient "grant awaiting response" marker. Thus concurrent capacity
    // events and one bulk event cannot send two unacknowledged grants to the same coordinator/bucket. Re-registration
    // while a grant is pending preserves membership but does not clear the marker.
    private PendingGrantSelection pickEligibleWaiter(String bucketKey) {
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
            return nodes;
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
     * OWNER-SIDE: ends a remote hand-off whose grant admitted nothing this owner learned of (its reply reported no
     * admitted request, or it failed or timed out). First the coordinator is deregistered, unless it registered again
     * while the grant was in flight, atomically against waiter registration for the same bucket: selection and
     * registration use the same {@code waitersByBucket.compute*} lock, so no capacity event can select the coordinator
     * between clearing its pending marker and removing its unchanged registration. Only then is the reservation released,
     * so an unchanged registration cannot be offered the slot it frees. The release is by exact permit id even if an
     * ownership change already cleared the marker: the id cannot affect a newer hand-off, and reclaiming it avoids
     * carrying an old reservation until TTL if this node later becomes the bucket owner again. A slot that freed is
     * offered to the next waiter.
     */
    private void reclaimUnusedGrant(PendingGrantSelection selection, String reservedPermitId) {
        final String bucketKey = selection.pendingGrantKey.bucketKey;
        waitersByBucket.compute(bucketKey, (k, nodes) -> {
            if (grantsAwaitingResponse.remove(selection.pendingGrantKey, selection.pendingGrant) == false) {
                return nodes; // ownership loss or another terminal callback already resolved it
            }
            if (selection.pendingGrant.registeredAgain == false && nodes != null) {
                nodes.remove(selection.coordinator);
            }
            return nodes == null || nodes.isEmpty() ? null : nodes;
        });
        if (tracker.release(bucketKey, reservedPermitId)) {
            offerSharedSlots(bucketKey, 1);
        }
    }

    // OWNER-SIDE: push one reserved permit to a waiting coordinator. The caller has already installed pendingGrantKey,
    // which remains until a transport terminal callback resolves it or ownership cleanup removes it. The reply names the
    // task the grant admitted, which the reservation is re-keyed to, or none, which returns the reservation. Returns
    // whether the asynchronous hand-off started.
    private boolean sendGrant(DiscoveryNode coordinator, String bucketKey, String reservedPermitId, PendingGrantSelection selection) {
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
            new GrantPermitRequest(bucketKey, reservedPermitId),
            options,
            new TransportResponseHandler<GrantPermitResponse>() {
                @Override
                public GrantPermitResponse read(StreamInput in) throws IOException {
                    return new GrantPermitResponse(in);
                }

                @Override
                public void handleResponse(GrantPermitResponse response) {
                    if (response.admittedTaskId.isEmpty()) {
                        // The coordinator had no request to admit: return the reservation, and drop the waiter unless it
                        // registered again while this grant was in flight.
                        reclaimUnusedGrant(selection, reservedPermitId);
                        return;
                    }
                    // Re-key the reservation to the admitted search, so its expiry checks renew it while that search runs,
                    // and only then clear the marker that ends the hand-off. A release that overtook this reply already
                    // removed the record, and the re-key never re-creates it; a check pending on the placeholder is fenced
                    // by the record's identity. Done even if an ownership change cleared the marker: the record still
                    // counts a running search.
                    tracker.rekeyTask(bucketKey, reservedPermitId, RESERVATION_TASK_ID, response.admittedTaskId.getAsLong());
                    if (grantsAwaitingResponse.remove(selection.pendingGrantKey, selection.pendingGrant)) {
                        // A bulk capacity event may have stopped because this was the only waiter and it was pending.
                        // Continue one slot at a time; tracker.tryAcquire prevents this from exceeding the live limit.
                        offerSharedSlots(bucketKey, 1);
                    }
                }

                @Override
                public void handleException(TransportException exp) {
                    // No GRANT retry: delivery is ambiguous after a failure or timeout, so the hand-off ends as if the
                    // coordinator had admitted nothing. A denial that re-registered the coordinator while this grant was
                    // pending is preserved as evidence that it is responsive again.
                    // Accepted: if the coordinator did admit a request with this grant (its reply was lost, or slower than
                    // GRANT_TIMEOUT), the reservation is reclaimed anyway and never re-keyed, so that search runs uncounted
                    // until it finishes (its later release is a no-op): at most one search beyond shared_limit per lost
                    // reply. Rare: the coordinator replies right after one in-memory queue poll.
                    logger.debug(
                        "Shared throttle grant to [{}] for bucket [{}] failed; resolving waiter and reclaiming",
                        coordinator.getId(),
                        bucketKey
                    );
                    reclaimUnusedGrant(selection, reservedPermitId);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            }
        );
        return true;
    }

    // COORDINATOR-SIDE: a grant arrived. Hand the reserved permit to the queue service to admit one retained request, and
    // answer with that request's task id, which the owner re-keys the reservation to. Answer with none when there is no
    // request, no consumer wired yet, or dispatch failed: the issuing owner then reclaims the reservation and deregisters
    // this coordinator unless it registered again while the grant was in flight. Either way the reply goes to the owner
    // that issued the grant, even if this node's ring has moved the bucket since. Runs on the transport thread: the queue
    // only polls its head under a short lock here and completes the admitted request's listener on an executor.
    // Package-private for tests.
    GrantPermitResponse handleGrant(GrantPermitRequest request) {
        final GrantConsumer consumer = grantConsumer;
        if (consumer == null) {
            return GrantPermitResponse.NOT_ADMITTED;
        }
        // The permit handed to an admitted request releases the reserved permit on completion, which re-drives owner-push
        // at the owner. An admitted coordinator stays a waiter: it may have more queued requests.
        final DiscoveryNode owner = ring.get().ownerFor(request.bucketKey).orElse(null);
        final OptionalLong admitted;
        try {
            admitted = consumer.admit(request.bucketKey, reservedPermit(owner, request.bucketKey, request.permitId));
        } catch (Exception e) {
            logger.warn("Queue grant admit failed for bucket [" + request.bucketKey + "]", e);
            return GrantPermitResponse.NOT_ADMITTED;
        }
        return new GrantPermitResponse(admitted);
    }

    // A Releasable for a CONSUMED grant. It releases to the owner snapshot selected from this coordinator's ring when the
    // grant was received: locally if that is this node, otherwise by RELEASE RPC. A grant that admitted nothing is never
    // released this way: its reply returns it.
    // Accepted: that release goes to the owner this coordinator's ring named when the grant arrived, deliberately not
    // routed to the node that issued the grant (the grant does not name it). If this coordinator's ring had already moved
    // the bucket by then, the release is a no-op on that node, and the issuing owner frees the slot at its expiry check,
    // once the admitted search its reservation was re-keyed to is gone. A ring change after the grant arrived does not
    // redirect the release.
    private Releasable reservedPermit(DiscoveryNode owner, String bucketKey, String permitId) {
        if (owner == null) {
            return () -> {}; // ring empty; the re-keyed reservation's expiry check reclaims it once its search ends
        }
        if (owner.equals(clusterService.localNode())) {
            return releaseLocal(bucketKey, permitId);
        }
        return releaseRemote(owner, bucketKey, permitId);
    }

    // Visible for tests: number of remote coordinator incarnations with an unacknowledged GRANT.
    int pendingGrantCount(String bucketKey) {
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
    BlockingQueue<ExpiredPermit> queuedChecks() {
        return expiredChecks;
    }

    // Visible for tests.
    SharedThrottleTracker tracker() {
        return tracker;
    }

    ThrottleOwnerSelector ring() {
        return ring.get();
    }

    /**
     * Every shared-throttle RPC body, request and response, is a name-keyed map rather than positional fields, so peers on
     * different versions, or built from different commits of the same release (e.g. a blue/green deployment, which a
     * {@code Version} gate cannot tell apart), stay interoperable without version gates: unknown keys are ignored.
     * <p>
     * Reading rules: fields that shipped with the first release (3.10) are read strictly ({@code require*}), so a
     * malformed message fails while decoding rather than being guessed. A field added later must be read with a default
     * that keeps the older behavior, or strictly behind a stream-version gate (the stream version is the negotiated
     * minimum, so an older peer never takes the strict branch). Use only types {@code readGenericValue} knows (String,
     * Integer, Long, Boolean, List, Map).
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

    // A bounded list: a task check carries at most MAX_TASKS_PER_CHECK entries, a grant's reply at most one admitted task.
    private static List<?> requireList(Map<String, Object> body, String key, int maxSize) throws IOException {
        final Object value = body.get(key);
        if (value instanceof List<?> list) {
            if (list.size() > maxSize) {
                throw new IOException("Invalid shared throttle [" + key + "] size: " + list.size() + " > " + maxSize);
            }
            return list;
        }
        throw new IllegalStateException("wlm shared-throttle: key [" + key + "] missing or not a List [" + value + "]");
    }

    private static <T> T requireElement(Object value, Class<T> type, String key) {
        if (type.isInstance(value)) {
            return type.cast(value);
        }
        throw new IllegalStateException(
            "wlm shared-throttle: key [" + key + "] holds an element that is not a " + type.getSimpleName() + " [" + value + "]"
        );
    }

    private static void writeBody(StreamOutput out, Map<String, Object> body) throws IOException {
        out.writeMap(body, StreamOutput::writeString, StreamOutput::writeGenericValue);
    }

    /**
     * {@code owner -> coord}: which of these coordinator tasks are still active: registered, and not WAITING in the
     * coordinator's queue (see {@link #checkTasks}). Task ids only, at most {@link #MAX_TASKS_PER_CHECK}, so the
     * coordinator performs map lookups without constructing task information.
     */
    static final class TaskCheckRequest extends TransportRequest {
        static final String KEY_COORDINATOR = "coordinator_node_id";
        static final String KEY_TASK_IDS = "task_ids";

        final String coordinatorId;
        final long[] taskIds;

        TaskCheckRequest(String coordinatorId, long[] taskIds) {
            this.coordinatorId = coordinatorId;
            this.taskIds = taskIds;
        }

        TaskCheckRequest(StreamInput in) throws IOException {
            super(in);
            final Map<String, Object> body = readBody(in);
            this.coordinatorId = requireString(body, KEY_COORDINATOR);
            final List<?> ids = requireList(body, KEY_TASK_IDS, MAX_TASKS_PER_CHECK);
            this.taskIds = new long[ids.size()];
            for (int i = 0; i < taskIds.length; i++) {
                taskIds[i] = requireElement(ids.get(i), Number.class, KEY_TASK_IDS).longValue();
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            writeBody(out, Map.of(KEY_COORDINATOR, coordinatorId, KEY_TASK_IDS, Arrays.stream(taskIds).boxed().toList()));
        }
    }

    /**
     * {@code coord -> owner}: whether each task is still active (registered, and not WAITING in the coordinator's queue),
     * in request order.
     */
    static final class TaskCheckResponse extends TransportResponse {
        static final String KEY_ACTIVE = "active";

        final boolean[] active;

        TaskCheckResponse(boolean[] active) {
            this.active = active;
        }

        TaskCheckResponse(StreamInput in) throws IOException {
            final Map<String, Object> body = readBody(in);
            final List<?> flags = requireList(body, KEY_ACTIVE, MAX_TASKS_PER_CHECK);
            this.active = new boolean[flags.size()];
            for (int i = 0; i < active.length; i++) {
                active[i] = requireElement(flags.get(i), Boolean.class, KEY_ACTIVE);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            final List<Boolean> flags = new ArrayList<>(active.length);
            for (boolean taskActive : active) {
                flags.add(taskActive);
            }
            writeBody(out, Map.of(KEY_ACTIVE, flags));
        }
    }

    /** {@code coord -> owner}: admit one request under {@code sharedLimit}. */
    static final class AcquirePermitRequest extends TransportRequest {
        static final String KEY_BUCKET = "bucket_key";
        static final String KEY_SHARED_LIMIT = "shared_limit";
        static final String KEY_PERMIT_ID = "permit_id";
        // The coordinator's ephemeral id: the exact process the owner purges on its removal, checks the task with, and
        // registers as a waiter on a denial.
        static final String KEY_COORDINATOR = "coordinator_node_id";
        // The coordinator's search task, checked with CHECK_TASKS once the permit outlives its TTL.
        static final String KEY_COORDINATOR_TASK_ID = "coordinator_task_id";
        // Whether the coordinator retained the request as PENDING_ACQUIRE and needs waiter registration on a denial.
        static final String KEY_WANTS_QUEUE = "wants_queue";

        final String bucketKey;
        final int sharedLimit;
        final String permitId;
        // Not on the wire: the owner applies its own PERMIT_TTL_NANOS to every acquire it decodes. Only in-memory requests
        // (tests, the local short-circuit) carry another value.
        final long ttlNanos;
        final String coordinatorId;
        final long coordinatorTaskId;
        final boolean wantsQueue;

        AcquirePermitRequest(
            String bucketKey,
            int sharedLimit,
            String permitId,
            long ttlNanos,
            String coordinatorId,
            long coordinatorTaskId,
            boolean wantsQueue
        ) {
            this.bucketKey = bucketKey;
            this.sharedLimit = sharedLimit;
            this.permitId = permitId;
            this.ttlNanos = ttlNanos;
            this.coordinatorId = coordinatorId;
            this.coordinatorTaskId = coordinatorTaskId;
            this.wantsQueue = wantsQueue;
        }

        AcquirePermitRequest(StreamInput in) throws IOException {
            super(in);
            final Map<String, Object> body = readBody(in);
            this.bucketKey = requireString(body, KEY_BUCKET);
            this.sharedLimit = requireNumber(body, KEY_SHARED_LIMIT).intValue();
            this.permitId = requireString(body, KEY_PERMIT_ID);
            this.ttlNanos = PERMIT_TTL_NANOS;
            this.coordinatorId = requireString(body, KEY_COORDINATOR);
            this.coordinatorTaskId = requireNumber(body, KEY_COORDINATOR_TASK_ID).longValue();
            this.wantsQueue = requireBoolean(body, KEY_WANTS_QUEUE);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            writeBody(
                out,
                Map.of(
                    KEY_BUCKET,
                    bucketKey,
                    KEY_SHARED_LIMIT,
                    sharedLimit,
                    KEY_PERMIT_ID,
                    permitId,
                    KEY_COORDINATOR,
                    coordinatorId,
                    KEY_COORDINATOR_TASK_ID,
                    coordinatorTaskId,
                    KEY_WANTS_QUEUE,
                    wantsQueue
                )
            );
        }
    }

    /**
     * {@code owner -> coord}: granted, denied, or {@code not_owner} (the answering node no longer owns the bucket), and
     * whether a denial registered the asking coordinator for owner-push ({@code registered}).
     */
    static final class AcquirePermitResponse extends TransportResponse {
        static final String KEY_GRANTED = "granted";
        static final String KEY_NOT_OWNER = "not_owner";
        static final String KEY_REGISTERED = "registered";

        static final AcquirePermitResponse GRANTED = new AcquirePermitResponse(true, false, false);
        static final AcquirePermitResponse DENIED = new AcquirePermitResponse(false, false, false);
        static final AcquirePermitResponse DENIED_AND_REGISTERED = new AcquirePermitResponse(false, false, true);
        static final AcquirePermitResponse NOT_OWNER = new AcquirePermitResponse(false, true, false);

        final boolean granted;
        final boolean notOwner;
        /**
         * Owner-push contract on a DENIAL: {@code true} means "I have you in this bucket's waiter set and I will push a
         * grant when a slot frees", {@code false} means "I could not register you, so no grant follows from this denial".
         * A coordinator that retained the request parks it either way; after {@code false} its backstop pass registers it
         * with the owner (REGISTER_WAITER). Meaningful only when the acquire set {@code wants_queue}.
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
            this.notOwner = requireBoolean(body, KEY_NOT_OWNER);
            this.registered = requireBoolean(body, KEY_REGISTERED);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            writeBody(out, Map.of(KEY_GRANTED, granted, KEY_NOT_OWNER, notOwner, KEY_REGISTERED, registered));
        }
    }

    /**
     * {@code coord -> owner}: fire-and-forget, drop a previously granted permit. The bucket's shared limit is deliberately
     * not on the wire: the owner resolves it from its own cluster state. A release carries no waiter-membership signal: a
     * grant that admitted nothing is returned by the GRANT's reply instead (see {@link GrantPermitResponse}).
     */
    static final class ReleasePermitRequest extends TransportRequest {
        static final String KEY_BUCKET = "bucket_key";
        static final String KEY_PERMIT_ID = "permit_id";

        final String bucketKey;
        final String permitId;

        ReleasePermitRequest(String bucketKey, String permitId) {
            this.bucketKey = bucketKey;
            this.permitId = permitId;
        }

        ReleasePermitRequest(StreamInput in) throws IOException {
            super(in);
            final Map<String, Object> body = readBody(in);
            this.bucketKey = requireString(body, KEY_BUCKET);
            this.permitId = requireString(body, KEY_PERMIT_ID);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            writeBody(out, Map.of(KEY_BUCKET, bucketKey, KEY_PERMIT_ID, permitId));
        }
    }

    /** {@code owner -> coord}: acknowledges a release. No fields yet; a map body, so a later version can add some. */
    static final class ReleasePermitResponse extends TransportResponse {
        static final ReleasePermitResponse INSTANCE = new ReleasePermitResponse();

        private ReleasePermitResponse() {}

        ReleasePermitResponse(StreamInput in) throws IOException {
            readBody(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            writeBody(out, Map.of());
        }
    }

    /**
     * {@code coord -> owner}. Register this coordinator as a waiter for a bucket where it retains demand. Sent after the
     * bucket's ring owner changes and periodically by the coordinator's waiter reconcile. The owner looks the coordinator
     * up by its ephemeral id in its own applied membership, registers that exact process idempotently, and immediately
     * tries to use any free shared capacity.
     */
    static final class RegisterWaiterRequest extends TransportRequest {
        static final String KEY_BUCKET = "bucket_key";
        // The coordinator's ephemeral id, as on an acquire: the exact process to register.
        static final String KEY_COORDINATOR = "coordinator_node_id";

        final String bucketKey;
        final String coordinatorId;

        RegisterWaiterRequest(String bucketKey, String coordinatorId) {
            this.bucketKey = bucketKey;
            this.coordinatorId = coordinatorId;
        }

        RegisterWaiterRequest(StreamInput in) throws IOException {
            super(in);
            final Map<String, Object> body = readBody(in);
            this.bucketKey = requireString(body, KEY_BUCKET);
            this.coordinatorId = requireString(body, KEY_COORDINATOR);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            writeBody(out, Map.of(KEY_BUCKET, bucketKey, KEY_COORDINATOR, coordinatorId));
        }
    }

    /** {@code owner -> coord}. Whether the receiving node accepted the waiter registration. */
    static final class RegisterWaiterResponse extends TransportResponse {
        static final String KEY_REGISTERED = "registered";

        static final RegisterWaiterResponse REGISTERED = new RegisterWaiterResponse(true);
        static final RegisterWaiterResponse NOT_REGISTERED = new RegisterWaiterResponse(false);

        final boolean registered;

        RegisterWaiterResponse(boolean registered) {
            this.registered = registered;
        }

        RegisterWaiterResponse(StreamInput in) throws IOException {
            final Map<String, Object> body = readBody(in);
            this.registered = requireBoolean(body, KEY_REGISTERED);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            writeBody(out, Map.of(KEY_REGISTERED, registered));
        }
    }

    /**
     * {@code owner -> coord}. Grant: a reserved shared permit for a bucket the coordinator is waiting on. Carries no limit:
     * the coordinator needs none, and the owner resolves its live limit from its own cluster state when the permit is
     * released or returned.
     */
    static final class GrantPermitRequest extends TransportRequest {
        static final String KEY_BUCKET = "bucket_key";
        static final String KEY_PERMIT_ID = "permit_id";

        final String bucketKey;
        final String permitId;

        GrantPermitRequest(String bucketKey, String permitId) {
            this.bucketKey = bucketKey;
            this.permitId = permitId;
        }

        GrantPermitRequest(StreamInput in) throws IOException {
            super(in);
            final Map<String, Object> body = readBody(in);
            this.bucketKey = requireString(body, KEY_BUCKET);
            this.permitId = requireString(body, KEY_PERMIT_ID);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            writeBody(out, Map.of(KEY_BUCKET, bucketKey, KEY_PERMIT_ID, permitId));
        }
    }

    /**
     * {@code coord -> owner}: the outcome of a grant. Either the coordinator task the grant admitted, which the owner
     * re-keys the reservation to so its expiry checks follow that search, or none: the coordinator had no request to
     * admit, so the owner reclaims the reservation and deregisters the waiter unless it registered again meanwhile.
     */
    static final class GrantPermitResponse extends TransportResponse {
        // The admitted coordinator task as a list of zero or one ids: empty means nothing was admitted, so no task id value
        // has to be reserved to say so.
        static final String KEY_ADMITTED_TASK_IDS = "admitted_task_ids";

        static final GrantPermitResponse NOT_ADMITTED = new GrantPermitResponse(OptionalLong.empty());

        final OptionalLong admittedTaskId;

        GrantPermitResponse(OptionalLong admittedTaskId) {
            this.admittedTaskId = Objects.requireNonNull(admittedTaskId);
        }

        GrantPermitResponse(StreamInput in) throws IOException {
            final Map<String, Object> body = readBody(in);
            final List<?> ids = requireList(body, KEY_ADMITTED_TASK_IDS, 1);
            this.admittedTaskId = ids.isEmpty()
                ? OptionalLong.empty()
                : OptionalLong.of(requireElement(ids.get(0), Number.class, KEY_ADMITTED_TASK_IDS).longValue());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            final List<Long> ids = admittedTaskId.isPresent() ? List.of(admittedTaskId.getAsLong()) : List.of();
            writeBody(out, Map.of(KEY_ADMITTED_TASK_IDS, ids));
        }
    }

    /**
     * COORDINATOR-SIDE callback: consumes a pushed grant by admitting one queued request against the reserved permit.
     * Returns the admitted request's task id (the request now owns the permit), or empty if there was none (the permit is
     * unused and goes back to the owner, by the GRANT's reply or, for a local waiter, directly). Implemented by
     * {@link WorkloadGroupQueueService#admitWithPermit}.
     */
    @FunctionalInterface
    interface GrantConsumer {
        OptionalLong admit(String bucketKey, Releasable reservedPermit);
    }
}
