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
import org.opensearch.ResourceNotFoundException;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.monitor.process.ProcessProbe;
import org.opensearch.search.backpressure.trackers.NodeDuressTrackers;
import org.opensearch.search.backpressure.trackers.NodeDuressTrackers.NodeDuressTracker;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.cancellation.WorkloadGroupTaskCancellationService;
import org.opensearch.wlm.stats.WorkloadGroupState;
import org.opensearch.wlm.stats.WorkloadGroupStats;
import org.opensearch.wlm.stats.WorkloadGroupStats.WorkloadGroupStatsHolder;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.opensearch.wlm.tracker.WorkloadGroupResourceUsageTrackerService.TRACKED_RESOURCES;

/**
 * As of now this is a stub and main implementation PR will be raised soon.Coming PR will collate these changes with core WorkloadGroupService changes
 * @opensearch.experimental
 */
public class WorkloadGroupService extends AbstractLifecycleComponent
    implements
        ClusterStateListener,
        TaskResourceTrackingService.TaskCompletionListener {

    private static final Logger logger = LogManager.getLogger(WorkloadGroupService.class);
    private final WorkloadGroupTaskCancellationService taskCancellationService;
    private volatile Scheduler.Cancellable scheduledFuture;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final WorkloadManagementSettings workloadManagementSettings;
    private Set<WorkloadGroup> activeWorkloadGroups;
    private final Set<WorkloadGroup> deletedWorkloadGroups;
    private final NodeDuressTrackers nodeDuressTrackers;
    private final WorkloadGroupsStateAccessor workloadGroupsStateAccessor;
    // Node-local in-flight throttle counters, keyed by throttle bucket. No cross-node coordination in this tier.
    private final NodeThrottleTracker throttleTracker = new NodeThrottleTracker();
    // Cluster-level (shared_limit) tier; late-bound after the transport service exists. Null => only the local tier.
    private volatile WorkloadGroupSharedThrottleService sharedThrottleService;
    // Coordinator-local request queues; late-bound. Null => no queueing (throttle denial rejects immediately, as before).
    private volatile WorkloadGroupQueueService queueService;

    public WorkloadGroupService(
        WorkloadGroupTaskCancellationService taskCancellationService,
        ClusterService clusterService,
        ThreadPool threadPool,
        WorkloadManagementSettings workloadManagementSettings,
        WorkloadGroupsStateAccessor workloadGroupsStateAccessor
    ) {

        this(
            taskCancellationService,
            clusterService,
            threadPool,
            workloadManagementSettings,
            new NodeDuressTrackers(
                Map.of(
                    ResourceType.CPU,
                    new NodeDuressTracker(
                        () -> workloadManagementSettings.getNodeLevelCpuCancellationThreshold() < ProcessProbe.getInstance()
                            .getProcessCpuPercent() / 100.0,
                        workloadManagementSettings::getDuressStreak
                    ),
                    ResourceType.MEMORY,
                    new NodeDuressTracker(
                        () -> workloadManagementSettings.getNodeLevelMemoryCancellationThreshold() <= JvmStats.jvmStats()
                            .getMem()
                            .getHeapUsedPercent() / 100.0,
                        workloadManagementSettings::getDuressStreak
                    )
                )
            ),
            workloadGroupsStateAccessor,
            new HashSet<>(),
            new HashSet<>()
        );
    }

    public WorkloadGroupService(
        WorkloadGroupTaskCancellationService taskCancellationService,
        ClusterService clusterService,
        ThreadPool threadPool,
        WorkloadManagementSettings workloadManagementSettings,
        NodeDuressTrackers nodeDuressTrackers,
        WorkloadGroupsStateAccessor workloadGroupsStateAccessor,
        Set<WorkloadGroup> activeWorkloadGroups,
        Set<WorkloadGroup> deletedWorkloadGroups
    ) {
        this.taskCancellationService = taskCancellationService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.workloadManagementSettings = workloadManagementSettings;
        this.nodeDuressTrackers = nodeDuressTrackers;
        this.activeWorkloadGroups = activeWorkloadGroups;
        this.deletedWorkloadGroups = deletedWorkloadGroups;
        this.workloadGroupsStateAccessor = workloadGroupsStateAccessor;
        activeWorkloadGroups.forEach(workloadGroup -> this.workloadGroupsStateAccessor.addNewWorkloadGroup(workloadGroup.get_id()));
        this.workloadGroupsStateAccessor.addNewWorkloadGroup(WorkloadGroupTask.DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get());
        this.clusterService.addListener(this);
    }

    /**
     * run at regular interval
     */
    void doRun() {
        if (workloadManagementSettings.getWlmMode() == WlmMode.DISABLED) {
            return;
        }
        taskCancellationService.cancelTasks(nodeDuressTrackers::isNodeInDuress, activeWorkloadGroups, deletedWorkloadGroups);
        taskCancellationService.pruneDeletedWorkloadGroups(deletedWorkloadGroups);
        // Backstop sweep: reap cancelled queued requests (defense-in-depth for the per-request cancel callback) and, as
        // a node-tier backstop, admit a waiter if a local permit is free. Owner-push and node-completion are the primary
        // drains; task cancellation (client disconnect / cancel_after_time_interval) is the primary parked-request exit.
        final WorkloadGroupQueueService qs = queueService;
        if (qs != null) {
            qs.sweep(this::sweepDrainNode);
        }
    }

    /**
     * {@link AbstractLifecycleComponent} lifecycle method
     */
    @Override
    protected void doStart() {
        scheduledFuture = threadPool.scheduleWithFixedDelay(() -> {
            try {
                doRun();
            } catch (Exception e) {
                logger.debug("Exception occurred in Workload Group service", e);
            }
        }, this.workloadManagementSettings.getWorkloadGroupServiceRunInterval(), ThreadPool.Names.GENERIC);
    }

    @Override
    protected void doStop() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel();
        }
    }

    @Override
    protected void doClose() throws IOException {}

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // Retrieve the current and previous cluster states
        Metadata previousMetadata = event.previousState().metadata();
        Metadata currentMetadata = event.state().metadata();

        // Extract the workload groups from both the current and previous cluster states
        Map<String, WorkloadGroup> previousWorkloadGroups = previousMetadata.workloadGroups();
        Map<String, WorkloadGroup> currentWorkloadGroups = currentMetadata.workloadGroups();

        // Detect new workload groups added in the current cluster state
        for (String workloadGroupName : currentWorkloadGroups.keySet()) {
            if (!previousWorkloadGroups.containsKey(workloadGroupName)) {
                // New workload group detected
                WorkloadGroup newWorkloadGroup = currentWorkloadGroups.get(workloadGroupName);
                // Perform any necessary actions with the new workload group
                workloadGroupsStateAccessor.addNewWorkloadGroup(newWorkloadGroup.get_id());
            }
        }

        // Detect workload groups deleted in the current cluster state
        for (String workloadGroupName : previousWorkloadGroups.keySet()) {
            if (!currentWorkloadGroups.containsKey(workloadGroupName)) {
                // Workload group deleted
                WorkloadGroup deletedWorkloadGroup = previousWorkloadGroups.get(workloadGroupName);
                // Perform any necessary actions with the deleted workload group
                this.deletedWorkloadGroups.add(deletedWorkloadGroup);
                workloadGroupsStateAccessor.removeWorkloadGroup(deletedWorkloadGroup.get_id());
            }
        }
        this.activeWorkloadGroups = new HashSet<>(currentMetadata.workloadGroups().values());
        releaseBacklogForUnthrottledGroups(currentWorkloadGroups);
    }

    /**
     * Immediately releases the parked backlog of any group that no longer has a throttle limit (throttling was disabled,
     * or the group was deleted). Nothing remains for those requests to wait for, and an unthrottled group takes the
     * no-permit fast path — so it generates no permit completions to drive a drain chain, and a parked request has no
     * deadline. Reacting to the config change is therefore the <em>only</em> thing that frees these requests: there is no
     * periodic backstop for this case, so if this listener does not reach a node holding a backlog (e.g. its queue service
     * is not wired yet), those requests wait for client cancellation.
     * <p>
     * Note disabling throttling necessarily disables queueing in the same update ({@code WorkloadGroup} rejects a queue
     * with no throttle limit, and {@code validateMergedConfig} rejects a throttling block whose limits are all unset), so
     * this is the only reachable way to end up with a backlog and nothing to drain it.
     * <p>
     * Runs on the cluster-applier thread, so the actual release is dispatched to {@code GENERIC}: emptying a deep queue
     * polls under each bucket's lock and completes one listener per request, which must not sit on the applier thread.
     */
    private void releaseBacklogForUnthrottledGroups(Map<String, WorkloadGroup> currentWorkloadGroups) {
        final WorkloadGroupQueueService qs = queueService;
        if (qs == null) {
            return;
        }
        for (String groupId : qs.queuedGroupIds()) {
            if (qs.currentDepth(groupId) == 0) {
                continue; // no backlog to release
            }
            WorkloadGroup workloadGroup = currentWorkloadGroups.get(groupId);
            if (workloadGroup != null) {
                Settings throttling = workloadGroup.getMutableWorkloadGroupFragment().getThrottling();
                if (WorkloadGroupThrottleSettings.NODE_LIMIT.get(throttling) != WorkloadGroupThrottleSettings.UNSET_LIMIT
                    || WorkloadGroupThrottleSettings.SHARED_LIMIT.get(throttling) != WorkloadGroupThrottleSettings.UNSET_LIMIT) {
                    continue; // still throttled: the normal drain paths own this backlog
                }
            }
            // Group is gone, or has no limit left. (A deleted group's tasks are also being cancelled; admit() re-checks
            // cancellation per request, so releasing here is safe either way and self-heals a missed cancellation.)
            threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
                try {
                    int released = qs.admitAllUntracked(groupId);
                    if (released > 0) {
                        logger.info(
                            "Released {} queued request(s) for workload group [{}]: throttling is no longer configured, "
                                + "so there is nothing left to wait for.",
                            released,
                            groupId
                        );
                    }
                } catch (Exception e) {
                    logger.warn("Failed to release the queued backlog for workload group [" + groupId + "]", e);
                }
            });
        }
    }

    /**
     * updates the failure stats for the workload group
     *
     * @param workloadGroupId workload group identifier
     */
    public void incrementFailuresFor(final String workloadGroupId) {
        WorkloadGroupState workloadGroupState = workloadGroupsStateAccessor.getWorkloadGroupState(workloadGroupId);
        // This can happen if the request failed for a deleted workload group
        // or new workloadGroup is being created and has not been acknowledged yet
        if (workloadGroupState == null) {
            return;
        }
        workloadGroupState.failures.inc();
    }

    /**
     * @return node level workload group stats
     */
    public WorkloadGroupStats nodeStats(Set<String> workloadGroupIds, Boolean requestedBreached) {
        final Map<String, WorkloadGroupStatsHolder> statsHolderMap = new HashMap<>();
        Map<String, WorkloadGroupState> existingStateMap = workloadGroupsStateAccessor.getWorkloadGroupStateMap();
        if (!workloadGroupIds.contains("_all")) {
            for (String id : workloadGroupIds) {
                if (!existingStateMap.containsKey(id)) {
                    throw new ResourceNotFoundException("WorkloadGroup with id " + id + " does not exist");
                }
            }
        }
        final WorkloadGroupQueueService qs = queueService;
        if (existingStateMap != null) {
            existingStateMap.forEach((workloadGroupId, currentState) -> {
                boolean shouldInclude = workloadGroupIds.contains("_all") || workloadGroupIds.contains(workloadGroupId);
                if (shouldInclude) {
                    if (requestedBreached == null || requestedBreached == resourceLimitBreached(workloadGroupId, currentState)) {
                        long queuedCurrent = qs == null ? 0L : qs.currentDepth(workloadGroupId);
                        long queuePeak = qs == null ? 0L : qs.peakDepth(workloadGroupId);
                        statsHolderMap.put(workloadGroupId, WorkloadGroupStatsHolder.from(currentState, queuedCurrent, queuePeak));
                    }
                }
            });
        }
        return new WorkloadGroupStats(statsHolderMap);
    }

    /**
     * @return if the WorkloadGroup breaches any resource limit based on the LastRecordedUsage
     */
    public boolean resourceLimitBreached(String id, WorkloadGroupState currentState) {
        WorkloadGroup workloadGroup = clusterService.state().metadata().workloadGroups().get(id);
        if (workloadGroup == null) {
            throw new ResourceNotFoundException("WorkloadGroup with id " + id + " does not exist");
        }

        for (ResourceType resourceType : TRACKED_RESOURCES) {
            if (workloadGroup.getResourceLimits().containsKey(resourceType)) {
                final double threshold = getNormalisedRejectionThreshold(workloadGroup.getResourceLimits().get(resourceType), resourceType);
                final double lastRecordedUsage = currentState.getResourceState().get(resourceType).getLastRecordedUsage();
                if (threshold < lastRecordedUsage) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * @param workloadGroupId workload group identifier
     */
    public void rejectIfNeeded(String workloadGroupId) {
        if (workloadManagementSettings.getWlmMode() != WlmMode.ENABLED) {
            return;
        }

        if (workloadGroupId == null || workloadGroupId.equals(WorkloadGroupTask.DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get())) return;
        WorkloadGroupState workloadGroupState = workloadGroupsStateAccessor.getWorkloadGroupState(workloadGroupId);

        // This can happen if the request failed for a deleted workload group
        // or new workloadGroup is being created and has not been acknowledged yet or invalid workload group id
        if (workloadGroupState == null) {
            return;
        }

        // rejections will not happen for SOFT mode WorkloadGroups unless node is in duress
        Optional<WorkloadGroup> optionalWorkloadGroup = activeWorkloadGroups.stream()
            .filter(x -> x.get_id().equals(workloadGroupId))
            .findFirst();

        if (optionalWorkloadGroup.isPresent()
            && (optionalWorkloadGroup.get().getResiliencyMode() == MutableWorkloadGroupFragment.ResiliencyMode.SOFT
                && !nodeDuressTrackers.isNodeInDuress())) return;

        optionalWorkloadGroup.ifPresent(workloadGroup -> {
            boolean reject = false;
            final StringBuilder reason = new StringBuilder();
            for (ResourceType resourceType : TRACKED_RESOURCES) {
                if (workloadGroup.getResourceLimits().containsKey(resourceType)) {
                    final double threshold = getNormalisedRejectionThreshold(
                        workloadGroup.getResourceLimits().get(resourceType),
                        resourceType
                    );
                    final double lastRecordedUsage = workloadGroupState.getResourceState().get(resourceType).getLastRecordedUsage();
                    if (threshold < lastRecordedUsage) {
                        reject = true;
                        reason.append(resourceType)
                            .append(" limit is breaching for workload group ")
                            .append(workloadGroup.get_id())
                            .append(", ")
                            .append(threshold)
                            .append(" < ")
                            .append(lastRecordedUsage)
                            .append(", wlm mode is ")
                            .append(workloadGroup.getResiliencyMode())
                            .append(". ");
                        workloadGroupState.getResourceState().get(resourceType).rejections.inc();
                        // should not double count even if both the resource limits are breaching
                        break;
                    }
                }
            }
            if (reject) {
                workloadGroupState.totalRejections.inc();
                throw new OpenSearchRejectedExecutionException(
                    "WorkloadGroup " + workloadGroupId + " is already contended. " + reason.toString()
                );
            }
        });
    }

    /**
     * Late-binds the cluster-level ({@code shared_limit}) throttle tier. It is constructed after the transport
     * service (which it needs for the owner round-trip), so it cannot be a constructor argument. When unset, only
     * the node-local tier applies and a shared-only config fails open.
     */
    public void setSharedThrottleService(WorkloadGroupSharedThrottleService sharedThrottleService) {
        this.sharedThrottleService = sharedThrottleService;
    }

    /**
     * Late-binds the coordinator-local request-queue service. When unset, a throttle denial rejects immediately (the
     * pre-queueing behavior); when set, a denial may park the request instead (if the group's
     * {@code queue.size_per_bucket} > 0).
     */
    public void setQueueService(WorkloadGroupQueueService queueService) {
        this.queueService = queueService;
    }

    /**
     * Two-tier throttle admission for a search request. Notifies {@code listener} with:
     * <ul>
     *   <li>a non-null {@link Releasable} — admitted; close it exactly once on request completion to release the
     *       permit (works for both a node-local and a cluster-level shared permit);</li>
     *   <li>{@code null} — admitted but not throttle-tracked (throttling disabled/not configured for this request,
     *       request not attributable to a bucket, or a fail-open path); nothing to release;</li>
     *   <li>{@link ActionListener#onFailure} with an {@link OpenSearchRejectedExecutionException} (HTTP 429) — the
     *       bucket is at its limit.</li>
     * </ul>
     * The node-local tier is checked synchronously (zero added latency on the common path); only an overflow to the
     * shared tier does the asynchronous owner round-trip, so the calling thread is never blocked. The listener may
     * therefore be invoked inline or, for the shared-tier overflow, on a transport thread.
     *
     * @param task            the search task (carries the workload group id; observed for cancellation while queued)
     * @param principal       the raw {@code WORKLOAD_GROUP_PRINCIPAL_HEADER} value, or {@code null}
     * @param listener        receives the permit / null / 429
     */
    public void acquireThrottlePermit(WorkloadGroupTask task, String principal, ActionListener<Releasable> listener) {
        final String workloadGroupId = task.getWorkloadGroupId();
        final ThrottlePlan plan;
        try {
            plan = resolveThrottlePlan(workloadGroupId, principal);
        } catch (Exception e) {
            // A bug in the throttle-resolution path must never fail an otherwise-valid search, so fail open.
            logger.warn("Skipping throttle for workload group [" + workloadGroupId + "] due to an error", e);
            listener.onResponse(null);
            return;
        }
        if (plan == null) {
            listener.onResponse(null); // not throttled / not attributable -> fail open
            return;
        }

        // Node-local tier: synchronous, no cross-node coordination. Granting here is the zero-latency common path.
        if (plan.nodeLimit != WorkloadGroupThrottleSettings.UNSET_LIMIT) {
            Releasable localPermit = acquireNodePermit(plan);
            if (localPermit != null) {
                listener.onResponse(localPermit);
                return;
            }
            // Local allowance exhausted -> fall through to the shared pool if one is configured.
        }

        // Cluster-level shared tier (asynchronous owner round-trip).
        if (plan.sharedLimit != WorkloadGroupThrottleSettings.UNSET_LIMIT && sharedThrottleService != null) {
            final WorkloadGroupQueueService qs = queueService;
            // Queueing is active only when the group has a non-zero queue AND is not in MONITOR mode (monitor observes
            // and always admits — it must never park). When active, take the ENQUEUE-FIRST path; otherwise the request
            // runs directly on grant and is admitted (monitor) or 429'd (no queue) on denial.
            final boolean queueingActive = qs != null && plan.queueSizePerBucket > 0 && plan.monitorMode == false;

            if (queueingActive) {
                // ENQUEUE-FIRST: park the request BEFORE contacting the owner. The owner registers this coordinator as a
                // waiter synchronously when it denies an acquire (in handleAcquire, before the reply is even sent), so if
                // we enqueued only after the denial reply there would be a window in which the owner believes we are
                // waiting while our queue is still empty — a racing grant would then find nothing to admit, return the
                // slot "unused", and deregister us, stranding the request (worst on a shared-only group with no node-tier
                // drain). Parking first closes that window by construction: whenever a grant or owner-push arrives, the
                // request is already in the queue. The acquire below is now a pure "supply" signal — a granted slot
                // drains the OLDEST queued request (FIFO), which may differ from this one; that is fine, since supply is
                // matched to demand by count and shared_limit is still gated solely by the owner's tryAcquire.
                if (qs.tryEnqueue(plan.workloadGroupId, plan.bucketKey, task, plan.queueSizePerBucket, listener) == false) {
                    // Queue full (bucket or group ceiling): reject with the throttle 429 (tryEnqueue already counted the
                    // queue rejection).
                    incrementThrottled(plan.workloadGroupId);
                    listener.onFailure(new OpenSearchRejectedExecutionException("Request throttled: " + plan.describeBreach(false) + "."));
                    return;
                }
                // Request is parked (its listener is held by the queue). Ask the owner for a shared slot; wantsQueue=true
                // so a denial registers this coordinator for owner-push. This callback NEVER completes the request's
                // listener directly — it only supplies a permit to the queue.
                sharedThrottleService.acquireAsync(plan.bucketKey, plan.sharedLimit, true, ActionListener.wrap(permit -> {
                    if (permit != null) {
                        // Granted a shared slot: hand it to the oldest queued request. If a concurrent drain (owner-push
                        // or node-tier completion) already emptied the bucket, release the slot rather than hold it.
                        if (qs.admitWithPermit(plan.bucketKey, permit) == false) {
                            permit.close();
                        }
                    } else {
                        // Fail-open (owner unreachable / empty ring): admit one queued request with no shared permit
                        // (untracked), matching the shipped fail-open semantics. No-op if the bucket already drained.
                        qs.admitWithPermit(plan.bucketKey, () -> {});
                    }
                }, e -> {
                    // acquireAsync only ever fails with the message-less denial marker (transport errors fail open via
                    // onResponse(null)). Denied: the request stays parked and the owner has registered this coordinator
                    // (wantsQueue), so owner-push drains it when a slot frees. Nothing to complete here.
                    if (e instanceof OpenSearchRejectedExecutionException == false) {
                        logger.warn(
                            "Unexpected shared-acquire failure for a queued request in workload group [" + workloadGroupId + "]",
                            e
                        );
                    }
                }));
                return;
            }

            // Non-queueing shared path (queue disabled or MONITOR mode): unchanged — acquire with the request's own
            // listener; a denial admits (monitor) or 429s via onThrottleBreach.
            sharedThrottleService.acquireAsync(plan.bucketKey, plan.sharedLimit, false, ActionListener.wrap(listener::onResponse, e -> {
                if (e instanceof OpenSearchRejectedExecutionException) {
                    onThrottleBreach(plan, false, task, listener);
                } else {
                    listener.onFailure(e);
                }
            }));
            return;
        }

        if (plan.nodeLimit != WorkloadGroupThrottleSettings.UNSET_LIMIT && plan.sharedLimit == WorkloadGroupThrottleSettings.UNSET_LIMIT) {
            // Node-only config with the local allowance exhausted and no shared tier to overflow to.
            onThrottleBreach(plan, true, task, listener);
        } else {
            // Either a shared tier was configured but is unavailable (not yet wired), or a shared-only config with no
            // wired tier. Fail open rather than reject, consistent with every other shared-tier-unavailable path.
            listener.onResponse(null);
        }
    }

    // Acquires a node-local permit for admission, wrapped so its close() drains the bucket's queue.
    private Releasable acquireNodePermit(ThrottlePlan plan) {
        return wrapNodePermit(throttleTracker.tryAcquire(plan.bucketKey, plan.nodeLimit), plan.workloadGroupId, plan.bucketKey);
    }

    /**
     * The group's <em>current</em> {@code node_limit} from cluster state, or {@link WorkloadGroupThrottleSettings#UNSET_LIMIT}
     * if the group is gone or the node tier is not configured. Read fresh (not captured) everywhere a drain re-acquires,
     * so a live {@code node_limit} update takes effect on the very next drain.
     */
    private int currentNodeLimit(String groupId) {
        WorkloadGroup workloadGroup = getWorkloadGroupById(groupId);
        if (workloadGroup == null) {
            return WorkloadGroupThrottleSettings.UNSET_LIMIT;
        }
        return WorkloadGroupThrottleSettings.NODE_LIMIT.get(workloadGroup.getMutableWorkloadGroupFragment().getThrottling());
    }

    // Wraps a raw node permit so its close() releases the slot AND drains one waiter for the bucket — a freed node
    // permit creates room for one waiting request on this coordinator. Returns null if the raw permit is null (limit
    // reached). The wrapping is applied to BOTH the initial admission permit and the permit handed to a drained
    // waiter, so the node-completion drain chains continuously instead of dying after one hop (each hop is a separate,
    // asynchronous request completion, so there is no synchronous recursion). Guarded by a cheap per-group depth check
    // so the common unthrottled path pays only a single map lookup past the shipped decrement.
    //
    // node_limit is re-read from cluster state on each drain rather than captured when the chain started: a busy bucket's
    // drain chain can run for a long time (one hop per request completion), so a captured value would keep admitting
    // against a stale limit across a live node_limit update — over-admitting above a lowered limit until the chain broke.
    // The re-read sits INSIDE the depth guard, so it costs nothing unless this group actually has a backlog to drain.
    private Releasable wrapNodePermit(Releasable raw, String groupId, String bucketKey) {
        if (raw == null) {
            return null;
        }
        final WorkloadGroupQueueService qs = queueService;
        if (qs == null) {
            return raw;
        }
        return () -> {
            raw.close();
            // Fast-out scoped to THIS group: drainNode only ever admits a waiter for (groupId, bucketKey), so guard on
            // this group's depth, not the cluster-wide total. A cheap single-map lookup, and it avoids a spurious
            // lock-acquire + permit re-acquire/release when some *other* group is the one that is backlogged.
            if (qs.currentDepth(groupId) > 0) {
                final int liveNodeLimit = currentNodeLimit(groupId);
                if (liveNodeLimit == WorkloadGroupThrottleSettings.UNSET_LIMIT) {
                    // Node tier no longer configured (or group deleted): this tier must not admit — a still-configured
                    // shared_limit would be bypassed. If NO limit remains, the backlog has nothing to wait for, but it is
                    // not stranded: clusterChanged releases it as soon as throttling is disabled.
                    return;
                }
                qs.drainNode(groupId, bucketKey, key -> wrapNodePermit(throttleTracker.tryAcquire(key, liveNodeLimit), groupId, key));
            }
        };
    }

    // Terminal handling when a request would be throttled at a tier's limit. Order: MONITOR observes only (log + admit,
    // no stat); else try to park the request in the queue (if queueing is enabled and has room); else count the
    // rejection and fail with the user-facing 429.
    private void onThrottleBreach(ThrottlePlan plan, boolean nodeTier, WorkloadGroupTask task, ActionListener<Releasable> listener) {
        if (plan.monitorMode) {
            // DEBUG, not INFO: this fires once per would-be-throttled request, so INFO would spam a hot bucket under
            // load. The message names the throttle attribute value (username/role), but that is the caller's own
            // identity and is already surfaced in the enforced-mode 429, so it is safe to log at a diagnostic level.
            logger.debug("Request would be throttled (monitor mode, not rejected): {}.", plan.describeBreach(nodeTier));
            listener.onResponse(null);
            return;
        }
        // Queue-then-reject: hold the request instead of rejecting, if queueing is enabled and both the request's bucket
        // and the group have room. A parked request holds no thread — only its listener + open connection — and is
        // admitted later by a node-completion drain or an owner grant, or evicted by task cancellation (client
        // disconnect / cancel_after_time_interval). There is no queue timeout: a parked request has no wall-clock
        // deadline.
        final WorkloadGroupQueueService qs = queueService;
        if (qs != null
            && plan.queueSizePerBucket > 0
            && qs.tryEnqueue(plan.workloadGroupId, plan.bucketKey, task, plan.queueSizePerBucket, listener)) {
            return; // parked; listener completed later
        }
        incrementThrottled(plan.workloadGroupId);
        listener.onFailure(new OpenSearchRejectedExecutionException("Request throttled: " + plan.describeBreach(nodeTier) + "."));
    }

    /**
     * Node-tier backstop drain for the sweep: admit the oldest waiter for {@code bucketKey} against a freshly acquired
     * node permit if one is free; a no-op otherwise. Wired into {@link WorkloadGroupQueueService#sweep}. This recovers
     * a request the node-completion chain missed without re-running full admission or re-contacting the shared owner
     * (the owner recovers its own lost grants and reservations), and — crucially — without dequeuing-and-re-parking, so
     * a still-waiting request stays parked in place (there is no queue timeout; its only deadline is task cancellation
     * via {@code cancel_after_time_interval} / client disconnect).
     */
    private void sweepDrainNode(String groupId, String bucketKey) {
        // node_limit is not carried per-bucket here; the drain lambda re-derives the permit for the bucket. We only
        // engage the node tier as a backstop — the shared tier is drained by owner-push. Read the group's CURRENT
        // node_limit (same live read the completion drain uses); if the node tier isn't configured, nothing to drain.
        final int nodeLimit = currentNodeLimit(groupId);
        if (nodeLimit == WorkloadGroupThrottleSettings.UNSET_LIMIT) {
            // Group deleted, or shared-only config: the node tier can't admit; owner-push handles the shared tier. A
            // group with NO limit left is not this path's problem either — clusterChanged releases that backlog the
            // moment throttling is disabled (see releaseBacklogForUnthrottledGroups).
            return;
        }
        queueService.drainNode(groupId, bucketKey, key -> wrapNodePermit(throttleTracker.tryAcquire(key, nodeLimit), groupId, key));
    }

    // Records a throttle rejection. Uses the raw state map, not the DEFAULT-fallback accessor, so a not-yet-registered
    // group isn't misattributed to DEFAULT, and never lets a stats failure swallow the 429.
    private void incrementThrottled(String workloadGroupId) {
        try {
            WorkloadGroupState workloadGroupState = workloadGroupsStateAccessor.getWorkloadGroupStateMap().get(workloadGroupId);
            if (workloadGroupState != null) {
                workloadGroupState.totalThrottled.inc();
            }
        } catch (Exception statsException) {
            logger.warn("Failed to record throttle stat for workload group [" + workloadGroupId + "]", statsException);
        }
    }

    /**
     * Resolves the throttle configuration for a request into a bucket key plus the node and shared limits, or
     * {@code null} when the request is not throttled: WLM disabled, default/unknown group, throttling not configured
     * (both limits unset), or the request can't be attributed to a bucket (e.g. username/role with no principal).
     */
    private ThrottlePlan resolveThrottlePlan(String workloadGroupId, String principal) {
        if (workloadManagementSettings.getWlmMode() != WlmMode.ENABLED) {
            return null;
        }
        if (workloadGroupId == null || workloadGroupId.equals(WorkloadGroupTask.DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get())) {
            return null;
        }
        WorkloadGroup workloadGroup = getWorkloadGroupById(workloadGroupId);
        if (workloadGroup == null) {
            return null;
        }
        Settings throttling = workloadGroup.getMutableWorkloadGroupFragment().getThrottling();
        int nodeLimit = WorkloadGroupThrottleSettings.NODE_LIMIT.get(throttling);
        int sharedLimit = WorkloadGroupThrottleSettings.SHARED_LIMIT.get(throttling);
        if (nodeLimit == WorkloadGroupThrottleSettings.UNSET_LIMIT && sharedLimit == WorkloadGroupThrottleSettings.UNSET_LIMIT) {
            return null; // throttling not configured
        }
        String attribute = WorkloadGroupThrottleSettings.ATTRIBUTE.get(throttling);
        String value = resolveAttributeValue(attribute, principal); // null for whole-group; the principal value otherwise
        String bucketKey = buildBucketKey(workloadGroupId, attribute, value);
        if (bucketKey == null) {
            return null; // can't attribute the request to a bucket -> fail open
        }
        // MONITOR mode observes only: the limit is still evaluated so a breach can be logged, but the request is never
        // rejected and no stat is updated (consistent with how MONITOR is dormant on the resource cancellation path).
        boolean monitorMode = workloadGroup.getResiliencyMode() == MutableWorkloadGroupFragment.ResiliencyMode.MONITOR;
        // Queue config (used only if a throttle limit is breached). Absent => 0 => no queueing (reject immediately).
        // queue.size_per_bucket is the only queue knob (the per-group total is the fixed MAX_GROUP_QUEUE_DEPTH ceiling);
        // there is no queue timeout — a client's own deadline comes from cancel_after_time_interval, and a parked
        // request has no wall-clock deadline.
        Settings queue = workloadGroup.getMutableWorkloadGroupFragment().getQueue();
        int queueSizePerBucket = WorkloadGroupQueueSettings.SIZE_PER_BUCKET.get(queue);
        return new ThrottlePlan(
            workloadGroupId,
            bucketKey,
            nodeLimit,
            sharedLimit,
            workloadGroup.getName(),
            attribute,
            value,
            monitorMode,
            queueSizePerBucket
        );
    }

    // The resolved throttle configuration for a single request. Carries the human-readable group name and the throttle
    // dimension (attribute + resolved value) purely so a rejection message can name them; the opaque bucketKey remains
    // the identity used by both tracker tiers.
    private static class ThrottlePlan {
        final String workloadGroupId; // id (not name) — the key for total_throttled stat updates
        final String bucketKey;
        final int nodeLimit;
        final int sharedLimit;
        final String groupName;
        final String attribute;   // "group" | "username" | "role"
        final String value;       // the resolved principal value for username/role; null for whole-group throttling
        final boolean monitorMode; // group is in MONITOR resiliency mode -> observe (log), never reject or count
        final int queueSizePerBucket; // queue.size_per_bucket for this group (0 => queueing disabled)

        ThrottlePlan(
            String workloadGroupId,
            String bucketKey,
            int nodeLimit,
            int sharedLimit,
            String groupName,
            String attribute,
            String value,
            boolean monitorMode,
            int queueSizePerBucket
        ) {
            this.workloadGroupId = workloadGroupId;
            this.bucketKey = bucketKey;
            this.nodeLimit = nodeLimit;
            this.sharedLimit = sharedLimit;
            this.groupName = groupName;
            this.attribute = attribute;
            this.value = value;
            this.monitorMode = monitorMode;
            this.queueSizePerBucket = queueSizePerBucket;
        }

        // "workload group [analytics]" or "workload group [analytics] for username [alice]".
        String describeTarget() {
            String base = "workload group [" + groupName + "]";
            if ("group".equals(attribute) || value == null) {
                return base;
            }
            return base + " for " + attribute + " [" + value + "]";
        }

        // "workload group [analytics] for username [alice] reached its per-node limit of 5 concurrent requests" —
        // the shared clause used identically by the 429 rejection and the monitor-mode "would reject" log, so the two
        // never drift.
        String describeBreach(boolean nodeTier) {
            return describeTarget()
                + " reached its "
                + (nodeTier ? "per-node" : "cluster-wide")
                + " limit of "
                + (nodeTier ? nodeLimit : sharedLimit)
                + " concurrent requests";
        }
    }

    /**
     * Resolves the throttle attribute's value for a request: {@code null} for whole-group throttling
     * ({@code attribute == "group"}), otherwise the value of the matching {@code username}/{@code role} subfield
     * parsed out of the principal header. Returns {@code null} (fail open, not throttled) when a keyed attribute has
     * no usable principal token.
     */
    private String resolveAttributeValue(String attribute, String principal) {
        if ("group".equals(attribute)) {
            return null;
        }
        if (principal == null || principal.isEmpty()) {
            return null;
        }
        String subfieldPrefix = attribute + "|";
        for (String token : principal.split(WorkloadGroupTask.WORKLOAD_GROUP_PRINCIPAL_VALUE_DELIMITER)) {
            String trimmed = token.trim();
            if (trimmed.startsWith(subfieldPrefix)) {
                String value = trimmed.substring(subfieldPrefix.length());
                if (value.isEmpty() == false) {
                    return value;
                }
            }
        }
        return null;
    }

    /**
     * Assembles the opaque bucket key used by both tracker tiers: {@code <groupId>:group} for whole-group throttling,
     * or {@code <groupId>:<attribute>:<value>} for a {@code username}/{@code role} bucket. Returns {@code null} when a
     * keyed attribute has no resolved value (fail open).
     */
    private String buildBucketKey(String workloadGroupId, String attribute, String value) {
        if ("group".equals(attribute)) {
            return workloadGroupId + ":group";
        }
        if (value == null) {
            return null;
        }
        return workloadGroupId + ":" + attribute + ":" + value;
    }

    private double getNormalisedRejectionThreshold(double limit, ResourceType resourceType) {
        if (resourceType == ResourceType.CPU) {
            return limit * workloadManagementSettings.getNodeLevelCpuRejectionThreshold();
        } else if (resourceType == ResourceType.MEMORY) {
            return limit * workloadManagementSettings.getNodeLevelMemoryRejectionThreshold();
        }
        throw new IllegalArgumentException(resourceType + " is not supported in WLM yet");
    }

    public Set<WorkloadGroup> getActiveWorkloadGroups() {
        return activeWorkloadGroups;
    }

    /**
     * Returns the workload group with the given ID, or null if not found.
     * @param workloadGroupId the workload group identifier
     * @return the WorkloadGroup or null
     */
    public WorkloadGroup getWorkloadGroupById(String workloadGroupId) {
        return clusterService.state().metadata().workloadGroups().get(workloadGroupId);
    }

    /**
     * Returns the workload group attached to the calling thread context, or null if the current
     * request does not map to a workload group (no header set, or the referenced group does not
     * exist).
     */
    public WorkloadGroup getCurrentWorkloadGroup() {
        String workloadGroupId = threadPool.getThreadContext().getHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER);
        if (workloadGroupId == null) {
            return null;
        }
        return getWorkloadGroupById(workloadGroupId);
    }

    public Set<WorkloadGroup> getDeletedWorkloadGroups() {
        return deletedWorkloadGroups;
    }

    /**
     * This method determines whether the task should be accounted by SBP if both features co-exist
     * @param t WorkloadGroupTask
     * @return whether or not SBP handle it
     */
    public boolean shouldSBPHandle(Task t) {
        WorkloadGroupTask task = (WorkloadGroupTask) t;
        boolean isInvalidWorkloadGroupTask = true;
        if (task.isWorkloadGroupSet() && !WorkloadGroupTask.DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get().equals(task.getWorkloadGroupId())) {
            isInvalidWorkloadGroupTask = activeWorkloadGroups.stream()
                .noneMatch(workloadGroup -> workloadGroup.get_id().equals(task.getWorkloadGroupId()));
        }
        return workloadManagementSettings.getWlmMode() != WlmMode.ENABLED || isInvalidWorkloadGroupTask;
    }

    @Override
    public void onTaskCompleted(Task task) {
        if (!(task instanceof WorkloadGroupTask workloadGroupTask) || !workloadGroupTask.isWorkloadGroupSet()) {
            return;
        }
        String workloadGroupId = workloadGroupTask.getWorkloadGroupId();

        // set the default workloadGroupId if not existing in the active workload groups
        String finalWorkloadGroupId = workloadGroupId;
        boolean exists = activeWorkloadGroups.stream().anyMatch(workloadGroup -> workloadGroup.get_id().equals(finalWorkloadGroupId));

        if (!exists) {
            workloadGroupId = WorkloadGroupTask.DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get();
        }

        workloadGroupsStateAccessor.getWorkloadGroupState(workloadGroupId).totalCompletions.inc();
    }
}
