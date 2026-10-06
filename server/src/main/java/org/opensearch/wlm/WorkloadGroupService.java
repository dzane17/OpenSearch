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
import org.opensearch.common.Nullable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.tasks.TaskCancelledException;
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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

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

    /**
     * Separator between the segments of a throttle bucket key,
     * {@code <workload_group_id><delimiter><dimension><delimiter><dimension_value>}. Safe as a separator because no
     * segment can contain it: the id is a base64 UUID, the dimension is the internal group scope or one of
     * {@link WorkloadGroupThrottleSettings#ALLOWED_BY_VALUES}, and only the trailing segment is caller-supplied.
     */
    static final String BUCKET_KEY_DELIMITER = ":";

    // Cadence of the recovery-only node-tier queue sweep (see doRun).
    static final long QUEUE_BACKSTOP_SWEEP_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(5);

    private final WorkloadGroupTaskCancellationService taskCancellationService;
    private volatile Scheduler.Cancellable scheduledFuture;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final WorkloadManagementSettings workloadManagementSettings;
    private Set<WorkloadGroup> activeWorkloadGroups;
    private final Set<WorkloadGroup> deletedWorkloadGroups;
    private final NodeDuressTrackers nodeDuressTrackers;
    private final WorkloadGroupsStateAccessor workloadGroupsStateAccessor;
    // Node-local in-flight counters per throttle bucket.
    private final WorkloadGroupThrottleTracker throttleTracker = new WorkloadGroupThrottleTracker();
    private volatile WorkloadGroupSharedThrottleService sharedThrottleService;
    // Coordinator-local request queues; late-bound. Null => no queueing (throttle denial rejects immediately).
    private volatile WorkloadGroupQueueService queueService;
    // The service loop also drives node-duress cancellation, so keep that cadence and gate only the cheaper,
    // recovery-only queue sweep independently.
    private final AtomicLong nextQueueBackstopSweepNanos = new AtomicLong(0L);

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
        this.workloadManagementSettings.addWlmModeChangeListener(this::onWlmModeChanged);
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
        // Recovery-only node-tier queue sweep. Normal completion transfers its still-held permit directly to the queue,
        // and task cancellation evicts through its per-request callback, so this can run less frequently without slowing
        // either primary path. Keep it separate from the WLM enforcement cadence above.
        final WorkloadGroupQueueService qs = queueService;
        if (qs != null && queueBackstopSweepDue(threadPool.relativeTimeInNanos())) {
            qs.sweep(this::sweepDrainNode);
            sweepSharedWaiters(qs);
        }
    }

    // Shared-tier backstop: re-confirm this coordinator's waiter registration for every bucket that holds WAITING
    // shared-tier requests (see WorkloadGroupSharedThrottleService#reconcileRetainedWaiters). The owner-push wake-up is
    // otherwise the only way such a request leaves the queue, and WAITING has no deadline.
    private void sweepSharedWaiters(WorkloadGroupQueueService qs) {
        final WorkloadGroupSharedThrottleService sharedService = sharedThrottleService;
        if (sharedService == null) {
            return;
        }
        for (String bucketKey : qs.bucketKeysWithWaiting()) {
            try {
                if (currentSharedLimit(groupIdOf(bucketKey)) >= 1) {
                    sharedService.reconcileRetainedWaiters(bucketKey);
                }
            } catch (Exception e) {
                logger.debug("Failed to reconcile shared-throttle waiters for bucket [" + bucketKey + "]", e);
            }
        }
    }

    /**
     * Fail-closed completion for WAITING shared-tier requests whose waiter registration can no longer be confirmed with
     * an authoritative owner (no owner, owner unreachable, registration refused or failed): no grant can arrive, so each
     * is rejected with the shared-unavailable 429, or observed and admitted in MONITOR, exactly as a new arrival would be.
     * Runs on the generic pool, never on the transport or applier thread that observed the failure; if that hand-off is
     * rejected the requests stay retained and the next sweep retries. A request whose group no longer throttles is
     * admitted without a permit, as a new arrival would be.
     */
    private void rejectWaitingSharedRequests(String bucketKey) {
        final WorkloadGroupQueueService qs = queueService;
        if (qs == null) {
            return;
        }
        try {
            threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
                for (WorkloadGroupQueue.QueuedRequest req : qs.pollWaiting(bucketKey)) {
                    try {
                        completeUnavailableWaiter(req);
                    } catch (Exception e) {
                        logger.warn("Failed to complete a queued request after its shared tier became unavailable", e);
                    }
                }
            });
        } catch (RejectedExecutionException e) {
            logger.debug("The generic executor rejected the shared-unavailable rejection task", e);
        }
    }

    private void completeUnavailableWaiter(WorkloadGroupQueue.QueuedRequest req) {
        final WorkloadGroupTask task = req.task();
        final ActionListener<Releasable> listener = req.listener();
        if (task.isCancelled()) {
            listener.onFailure(new TaskCancelledException("task cancelled while queued"));
            return;
        }
        ThrottlePlan plan;
        try {
            plan = resolveThrottlePlan(task.getWorkloadGroupId(), task.getThrottlePrincipal());
        } catch (Exception e) {
            plan = null;
        }
        if (plan == null || plan.sharedLimit() < 1) {
            // The group no longer has a shared tier to wait on (a policy change the live-config drain also covers).
            listener.onResponse(null);
            return;
        }
        deliverThrottleBreach(plan, true, task, listener);
    }

    // The group's current shared_limit, or UNSET_LIMIT if the group is gone or its config cannot be read.
    private int currentSharedLimit(String groupId) {
        try {
            WorkloadGroup workloadGroup = getWorkloadGroupById(groupId);
            if (workloadGroup == null) {
                return WorkloadGroupThrottleSettings.UNSET_LIMIT;
            }
            return WorkloadGroupThrottleSettings.SHARED_LIMIT.get(workloadGroup.getMutableWorkloadGroupFragment().getThrottling());
        } catch (Exception e) {
            return WorkloadGroupThrottleSettings.UNSET_LIMIT;
        }
    }

    private static String groupIdOf(String bucketKey) {
        final int idx = bucketKey.indexOf(BUCKET_KEY_DELIMITER);
        return idx < 0 ? bucketKey : bucketKey.substring(0, idx);
    }

    private boolean queueBackstopSweepDue(long nowNanos) {
        while (true) {
            final long nextSweep = nextQueueBackstopSweepNanos.get();
            if (nowNanos < nextSweep) {
                return false;
            }
            final long followingSweep = nowNanos > Long.MAX_VALUE - QUEUE_BACKSTOP_SWEEP_INTERVAL_NANOS
                ? Long.MAX_VALUE
                : nowNanos + QUEUE_BACKSTOP_SWEEP_INTERVAL_NANOS;
            if (nextQueueBackstopSweepNanos.compareAndSet(nextSweep, followingSweep)) {
                return true;
            }
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
        Set<String> groupIdsToDrain = new HashSet<>();

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
            if (requiresQueueDrain(previousWorkloadGroups.get(workloadGroupName), currentWorkloadGroups.get(workloadGroupName))) {
                groupIdsToDrain.add(workloadGroupName);
            }
        }
        this.activeWorkloadGroups = new HashSet<>(currentMetadata.workloadGroups().values());
        drainQueuedRequestsUntracked(groupIdsToDrain, "workload group admission policy changed");
    }

    /**
     * Queue-drain policy for live configuration changes. Existing retained requests are admitted untracked when a group
     * is deleted, its throttle dimension ({@code by}) changes, its active throttle tiers change (whether {@code node_limit}
     * or {@code shared_limit} is set to a positive value), or it enters {@code MONITOR}; the WLM-mode listener below does
     * the same when global WLM leaves {@code ENABLED}. Numeric limit changes with the same tier set, queue-depth changes,
     * and unrelated group updates deliberately do not drain the queue. A limit below 1 never admits through its tier
     * (see {@link #acquireThrottlePermit}), so moving between 0 and unset is not a tier change.
     * <p>
     * This is intentionally a best-effort cutover rather than a policy-generation barrier: the drain runs asynchronously,
     * so a request overlapping the update can land on either side of the drain. There is no strict instantaneous boundary
     * between the old and new configurations.
     */
    private static boolean requiresQueueDrain(WorkloadGroup previousGroup, WorkloadGroup currentGroup) {
        if (currentGroup == null) {
            return true;
        }
        if (previousGroup == currentGroup) {
            return false;
        }
        if (previousGroup.getResiliencyMode() != MutableWorkloadGroupFragment.ResiliencyMode.MONITOR
            && currentGroup.getResiliencyMode() == MutableWorkloadGroupFragment.ResiliencyMode.MONITOR) {
            return true;
        }
        Settings previousThrottling = previousGroup.getMutableWorkloadGroupFragment().getThrottling();
        Settings currentThrottling = currentGroup.getMutableWorkloadGroupFragment().getThrottling();
        if (Objects.equals(effectiveBy(previousThrottling), effectiveBy(currentThrottling)) == false) {
            return true;
        }
        return isThrottleTierActive(previousThrottling, true) != isThrottleTierActive(currentThrottling, true)
            || isThrottleTierActive(previousThrottling, false) != isThrottleTierActive(currentThrottling, false);
    }

    // The effective throttle dimension, or null if this node cannot interpret the throttling config.
    private static String effectiveBy(Settings throttling) {
        try {
            return WorkloadGroupThrottleSettings.getEffectiveBy(throttling);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static boolean isThrottleTierActive(Settings throttling, boolean nodeTier) {
        try {
            return (nodeTier ? WorkloadGroupThrottleSettings.NODE_LIMIT : WorkloadGroupThrottleSettings.SHARED_LIMIT).get(throttling) > 0;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    private void onWlmModeChanged(WlmMode previousMode, WlmMode currentMode) {
        if (previousMode == WlmMode.ENABLED && currentMode != WlmMode.ENABLED) {
            final WorkloadGroupQueueService qs = queueService;
            if (qs != null) {
                drainQueuedRequestsUntracked(new HashSet<>(qs.queuedGroupIds()), "global WLM mode left enabled");
            }
        }
    }

    /**
     * Submits one asynchronous best-effort task to release retained requests observed for the supplied groups. Concurrent
     * arrivals may land after the queue snapshot and remain governed by the new policy. Config updates run on
     * cluster-state/settings application threads; polling deep queues and completing one listener per request must not.
     */
    private void drainQueuedRequestsUntracked(Set<String> groupIds, String reason) {
        final WorkloadGroupQueueService qs = queueService;
        if (qs == null || groupIds.isEmpty()) {
            return;
        }
        final Set<String> groupIdsSnapshot = new HashSet<>(groupIds);
        try {
            threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
                for (String groupId : groupIdsSnapshot) {
                    try {
                        int released = qs.admitAllUntracked(groupId);
                        if (released > 0) {
                            logger.info("Released {} queued request(s) for workload group [{}]: {}.", released, groupId, reason);
                        }
                    } catch (Exception e) {
                        logger.warn("Failed to release the queued backlog for workload group [" + groupId + "]", e);
                    }
                }
            });
        } catch (RejectedExecutionException e) {
            logger.debug("The generic executor rejected the queued-request release task", e);
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

    /** Late-binds the cluster-level ({@code shared_limit}) tier, which needs the transport service built after this one. */
    public void setSharedThrottleService(WorkloadGroupSharedThrottleService sharedThrottleService) {
        this.sharedThrottleService = sharedThrottleService;
        sharedThrottleService.setSharedTierUnavailableHandler(this::rejectWaitingSharedRequests);
    }

    /**
     * Late-binds the coordinator-local retained-request service. When unset, a throttle denial rejects immediately (the
     * pre-queueing behavior). When set and the group's {@code queue.size_per_bucket} is positive, a node-tier denial can
     * enter WAITING, while a shared-tier request is retained as PENDING_ACQUIRE before the owner verdict and transitions
     * to WAITING only after a registered denial. MONITOR groups and an unavailable shared tier never park a request.
     */
    public void setQueueService(WorkloadGroupQueueService queueService) {
        this.queueService = queueService;
    }

    /**
     * Admits a search against the node-local allowance ({@code node_limit}), then the shared allowance
     * ({@code shared_limit}, a cluster-wide overflow pool on top of every node's local allowance) if the local bucket is
     * full. Only shared admission can complete asynchronously. A counted parent exempts nested searches at both tiers.
     * MONITOR records would-be throttling without rejecting; SOFT and ENFORCED reject at the effective limit, unless the
     * group's {@code queue.size_per_bucket} lets the request wait for a permit instead (see {@link #setQueueService}).
     *
     * @param task the search task to mark as counted when admitted against a throttle
     * @param parentAlreadyCounted supplies whether this task inherits an existing throttle charge
     * @param listener receives a permit to release on completion, {@code null} when no permit is needed, or a 429
     */
    public void acquireThrottlePermit(WorkloadGroupTask task, BooleanSupplier parentAlreadyCounted, ActionListener<Releasable> listener) {
        acquireThrottlePermit(task, parentAlreadyCounted, true, listener);
    }

    /**
     * As {@link #acquireThrottlePermit(WorkloadGroupTask, BooleanSupplier, ActionListener)}, but a throttle breach is
     * never retained in the group's queue: it is rejected (or observed, in MONITOR) at once. For scroll continuations,
     * whose search context is bounded by the scroll's {@code keep_alive}: a page parked behind a backlog could outlive
     * that context and turn a prompt, retryable 429 into a late, unrecoverable missing-context failure.
     *
     * @param task the search task to mark as counted when admitted against a throttle
     * @param listener receives a permit to release on completion, {@code null} when no permit is needed, or a 429
     */
    public void acquireThrottlePermitWithoutQueueing(WorkloadGroupTask task, ActionListener<Releasable> listener) {
        acquireThrottlePermit(task, () -> false, false, listener);
    }

    private void acquireThrottlePermit(
        WorkloadGroupTask task,
        BooleanSupplier parentAlreadyCounted,
        boolean mayQueue,
        ActionListener<Releasable> listener
    ) {
        final ThrottlePlan plan;
        try {
            plan = resolveThrottlePlan(task.getWorkloadGroupId(), task.getThrottlePrincipal());
        } catch (Exception e) {
            logger.debug(() -> "Skipping throttle for workload group [" + task.getWorkloadGroupId() + "] due to an error", e);
            listener.onResponse(null);
            return;
        }
        if (plan == null) {
            listener.onResponse(null);
            return;
        }
        final boolean inherited;
        try {
            inherited = parentAlreadyCounted.getAsBoolean();
        } catch (Exception e) {
            logger.debug(() -> "Skipping throttle for workload group [" + task.getWorkloadGroupId() + "] due to an error", e);
            listener.onResponse(null);
            return;
        }
        if (inherited) {
            task.setThrottleCounted(true);
            listener.onResponse(null);
            return;
        }

        if (plan.nodeLimit() > 0) {
            final Releasable localPermit;
            try {
                localPermit = acquireNodePermit(plan.workloadGroup().get_id(), plan.bucketKey(), plan.nodeLimit());
            } catch (Exception e) {
                logger.debug(
                    () -> "Skipping node-level throttle for workload group [" + task.getWorkloadGroupId() + "] due to an error",
                    e
                );
                listener.onResponse(null);
                return;
            }
            if (localPermit != null) {
                task.setThrottleCounted(true);
                listener.onResponse(localPermit);
                return;
            }
        }

        if (plan.sharedLimit() < 1) {
            // Node-only config with the local allowance exhausted: park the request until a node permit frees, if the
            // group queues and has room, else reject (or observe, in MONITOR).
            if (mayQueue == false || tryParkNodeTierBreach(plan, task, listener) == false) {
                deliverThrottleBreach(plan, false, task, listener);
            }
            return;
        }
        WorkloadGroupSharedThrottleService sharedService = sharedThrottleService;
        if (sharedService == null) {
            // Defensive (only test-built instances lack it): fail closed like any unanswerable shared acquire.
            deliverThrottleBreach(plan, true, task, listener);
            return;
        }
        final WorkloadGroupQueueService qs = queueService;
        if (mayQueue && qs != null && plan.queueSizePerBucket() > 0 && isMonitor(plan) == false) {
            acquireSharedPermitQueued(plan, task, sharedService, qs, listener);
            return;
        }
        acquireSharedPermitUnqueued(plan, task, sharedService, null, listener);
    }

    // Shared-tier admission without retaining the request: a grant admits, a denial rejects (or observes, in MONITOR).
    // onDenial, if set, runs before the breach is delivered.
    private void acquireSharedPermitUnqueued(
        ThrottlePlan plan,
        WorkloadGroupTask task,
        WorkloadGroupSharedThrottleService sharedService,
        @Nullable Runnable onDenial,
        ActionListener<Releasable> listener
    ) {
        final boolean monitor = isMonitor(plan);
        // MONITOR proceeds on every outcome, so the shared tier never delivers a search-pool rejection for it.
        sharedService.acquireAsync(plan.bucketKey(), plan.sharedLimit(), monitor, ActionListener.wrap(permit -> {
            task.setThrottleCounted(true);
            listener.onResponse(permit);
        }, e -> {
            if (WorkloadGroupSharedThrottleService.isDenial(e)) {
                if (onDenial != null) {
                    onDenial.run();
                }
                deliverThrottleBreach(plan, false, task, listener);
            } else if (e instanceof OpenSearchRejectedExecutionException && monitor == false) {
                // The search pool rejected the hand-off (permit already released): a real 429, not a throttle breach.
                listener.onFailure(e);
            } else {
                // The shared tier could not answer: fail closed so shared_limit is never silently exceeded (MONITOR admits).
                if (WorkloadGroupSharedThrottleService.isUnavailable(e) == false) {
                    logger.debug(() -> "Shared throttle for workload group [" + task.getWorkloadGroupId() + "] failed", e);
                }
                deliverThrottleBreach(plan, true, task, listener);
            }
        }));
    }

    /**
     * Enqueue-first shared-tier admission. The exact request is registered as PENDING_ACQUIRE before the owner RPC, which
     * closes the race where a pushed grant arrives before the request is retained locally, without consuming
     * {@code queue.size_per_bucket}: only a denial the owner registered, and that fits the bucket's waiting cap, moves it
     * to WAITING. Every outcome targets that exact entry, so a delayed outcome is a no-op once an early pushed grant, a
     * node-permit handoff, or cancellation has already claimed it, and can never complete a later request.
     * <p>
     * Fail-closed: an unavailable shared tier (and any unexpected acquire failure) takes the entry back and rejects with
     * the unavailable 429 — it is never admitted untracked and never parked. A search-pool rejection of the hand-off is
     * passed through as-is, like on the non-queueing path. The caller has already excluded MONITOR, which never parks.
     */
    private void acquireSharedPermitQueued(
        ThrottlePlan plan,
        WorkloadGroupTask task,
        WorkloadGroupSharedThrottleService sharedService,
        WorkloadGroupQueueService qs,
        ActionListener<Releasable> listener
    ) {
        final WorkloadGroupQueue.QueuedRequest pending = qs.tryRegisterPendingAcquire(
            plan.workloadGroup().get_id(),
            plan.bucketKey(),
            task,
            listener
        );
        if (pending == null) {
            // The group's fixed retained-request ceiling is full, so this request cannot wait. Still ask the owner: its
            // own bucket may have shared capacity, and a full queue must never reject a request the non-queueing path
            // would admit. Only a real denial is a 429, counted as a queue rejection too since it could not be queued.
            final String groupId = plan.workloadGroup().get_id();
            acquireSharedPermitUnqueued(plan, task, sharedService, () -> qs.recordQueueRejection(groupId), listener);
            return;
        }
        // proceedsOnDenial is false: MONITOR never takes this path. A denial (registered or not) is therefore delivered
        // inline, on the network thread for a remote owner; that only moves or rejects this entry. Every admission that
        // follows from it (a pushed grant, a node-permit handoff) is dispatched on an executor by the queue service, and a
        // direct grant arrives on the search pool.
        sharedService.acquireAsync(plan.bucketKey(), plan.sharedLimit(), false, ActionListener.wrap(permit -> {
            // Direct grant: admit this exact request (the queue marks it counted). If another path already claimed it,
            // return the now-unused permit.
            if (qs.admitPendingAcquire(pending, permit) == false) {
                permit.close();
            }
        }, e -> {
            if (WorkloadGroupSharedThrottleService.isDenial(e)) {
                // Denied and registered at the owner: the only point that consumes queue.size_per_bucket. If the bucket's
                // WAITING budget filled while the RPC was in flight, reject this exact request and leave the existing
                // waiters (and the owner's one idempotent registration for this coordinator) untouched.
                if (qs.transitionPendingAcquireToWaiting(
                    pending,
                    plan.queueSizePerBucket()
                ) == WorkloadGroupQueueService.PendingAcquireTransition.QUEUE_FULL) {
                    deliverThrottleBreach(plan, false, task, listener);
                }
            } else if (qs.claimPendingAcquire(pending)) {
                if (WorkloadGroupSharedThrottleService.isUnavailable(e)) {
                    deliverThrottleBreach(plan, true, task, listener);
                } else if (e instanceof OpenSearchRejectedExecutionException) {
                    // The search pool rejected the hand-off (permit already released): a real 429, not a throttle breach.
                    listener.onFailure(e);
                } else {
                    logger.debug(() -> "Shared throttle for workload group [" + task.getWorkloadGroupId() + "] failed", e);
                    deliverThrottleBreach(plan, true, task, listener);
                }
            }
        }), () -> {
            // Denied and not registered at the owner: no grant will come, so reject this exact request. A stale outcome
            // whose entry an early grant or cancellation already removed is a no-op and must not count a throttle.
            if (qs.claimPendingAcquire(pending)) {
                deliverThrottleBreach(plan, false, task, listener);
            }
        });
    }

    /**
     * Wraps {@code listener} so the request's throttle permit is released <em>before</em> the listener is notified: a
     * completion listener may synchronously start new work in the same bucket (an {@code _msearch} dispatches its next
     * sub-search from the previous one's response handler), so releasing after would spuriously 429 a request the
     * coordinator deliberately serialized. Accepted: for a remote shared permit this only sends the fire-and-forget
     * RELEASE, so a follow-up acquire can reach the owner before that release and see the bucket full, a rare false
     * 429 under saturation. The close is guarded so a failed release can never turn a successful search into an error.
     *
     * @param listener       the listener to notify once the permit has been released (or, if remote, its release sent)
     * @param throttlePermit the permit acquired by
     *                       {@link #acquireThrottlePermit(WorkloadGroupTask, BooleanSupplier, ActionListener)}
     */
    public static <T> ActionListener<T> releaseThrottlePermitBeforeCompletion(
        final ActionListener<T> listener,
        final Releasable throttlePermit
    ) {
        return ActionListener.runBefore(listener, () -> {
            try {
                throttlePermit.close();
            } catch (Exception e) {
                logger.warn("Failed to release WLM throttle permit", e);
            }
        });
    }

    private void deliverThrottleBreach(
        ThrottlePlan plan,
        boolean sharedUnavailable,
        WorkloadGroupTask task,
        ActionListener<Releasable> listener
    ) {
        try {
            onThrottleBreach(plan, sharedUnavailable, task);
        } catch (OpenSearchRejectedExecutionException e) {
            listener.onFailure(e);
            return;
        }
        listener.onResponse(null);
    }

    // Node-tier queue-then-reject: holds the request instead of rejecting if the group queues and both the request's
    // bucket and the group have room. A retained request holds no thread — only its listener and open connection — and is
    // admitted later by a node-permit handoff, the recovery sweep, or a live-configuration drain, or evicted by task
    // cancellation (client disconnect / cancel_after_time_interval). There is no queue timeout. Not counted yet:
    // total_queued is taken when WAITING ends, so it and total_queue_wait_millis are written by the same call.
    // MONITOR never parks (it observes and admits through deliverThrottleBreach).
    private boolean tryParkNodeTierBreach(ThrottlePlan plan, WorkloadGroupTask task, ActionListener<Releasable> listener) {
        final WorkloadGroupQueueService qs = queueService;
        return qs != null
            && plan.queueSizePerBucket() > 0
            && isMonitor(plan) == false
            && qs.tryEnqueue(plan.workloadGroup().get_id(), plan.bucketKey(), task, plan.queueSizePerBucket(), listener);
    }

    private static boolean isMonitor(ThrottlePlan plan) {
        return plan.workloadGroup().getResiliencyMode() == MutableWorkloadGroupFragment.ResiliencyMode.MONITOR;
    }

    // Acquires a node-local permit for admission, wrapped so its close() can hand the slot to a queued request.
    private Releasable acquireNodePermit(String groupId, String bucketKey, int nodeLimit) {
        return wrapNodePermit(throttleTracker.tryAcquire(bucketKey, nodeLimit), groupId, bucketKey);
    }

    /**
     * The group's <em>current</em> {@code node_limit} from cluster state, or {@link WorkloadGroupThrottleSettings#UNSET_LIMIT}
     * if the group is gone, the node tier is not configured, or its config cannot be read. Read fresh (not captured)
     * everywhere a drain re-acquires, so a live {@code node_limit} update takes effect on the very next drain.
     */
    private int currentNodeLimit(String groupId) {
        try {
            WorkloadGroup workloadGroup = getWorkloadGroupById(groupId);
            if (workloadGroup == null) {
                return WorkloadGroupThrottleSettings.UNSET_LIMIT;
            }
            return WorkloadGroupThrottleSettings.NODE_LIMIT.get(workloadGroup.getMutableWorkloadGroupFragment().getThrottling());
        } catch (Exception e) {
            return WorkloadGroupThrottleSettings.UNSET_LIMIT;
        }
    }

    // Wraps a raw node permit so its close() atomically hands the still-held tracker slot to the oldest retained request
    // for this bucket. The tracker count remains occupied while the queue lock is held, so a racing new arrival cannot
    // barge ahead in a release-then-reacquire window. Only when there is no retained request is the raw slot released.
    // Every handed-off request receives a fresh one-shot wrapper around the same raw permit, so the chain can continue
    // until the queue empties, at which point the underlying tracker permit is closed exactly once.
    //
    // node_limit is re-read on every handoff. If a live decrease leaves the bucket above its new limit (or the node tier
    // is no longer active), this completion releases instead of transferring; the reacquire path then admits only after
    // the count falls below the new ceiling. This preserves dynamic-limit enforcement while keeping the normal
    // steady-limit path atomic.
    private Releasable wrapNodePermit(Releasable raw, String groupId, String bucketKey) {
        if (raw == null) {
            return null;
        }
        final WorkloadGroupQueueService qs = queueService;
        if (qs == null) {
            return raw;
        }
        final AtomicBoolean closed = new AtomicBoolean(false);
        return () -> {
            if (closed.compareAndSet(false, true) == false) {
                return;
            }

            // Keep the raw tracker slot occupied while selecting the queue head. At/under the current limit it is safe
            // to transfer an existing slot; above a lowered limit, release it so concurrency can converge downward.
            if (qs.retainedDepth(groupId) > 0) {
                final int liveNodeLimit = currentNodeLimit(groupId);
                if (liveNodeLimit > 0
                    && throttleTracker.inFlight(bucketKey) <= liveNodeLimit
                    && qs.handoffNodePermit(groupId, bucketKey, wrapNodePermit(raw, groupId, bucketKey))) {
                    return;
                }
            }

            raw.close();

            // Recovery path for a concurrent enqueue or a live limit decrease. The normal steady-state queue handoff
            // returned above without exposing a free slot, so only these uncommon cases retain the reacquire race.
            if (qs.retainedDepth(groupId) > 0) {
                final int liveNodeLimit = currentNodeLimit(groupId);
                if (liveNodeLimit > 0) {
                    qs.drainNode(groupId, bucketKey, key -> acquireNodePermit(groupId, key, liveNodeLimit));
                }
            }
        };
    }

    /**
     * Node-tier backstop drain for the sweep: admit the oldest retained request for {@code bucketKey} against a freshly
     * acquired node permit if one is free; a no-op otherwise. Wired into {@link WorkloadGroupQueueService#sweep}. This
     * recovers a request the node-completion chain missed without re-running full admission or re-contacting the shared
     * owner (the owner recovers its own lost grants and reservations), and without dequeuing-and-re-parking, so a
     * still-waiting request stays parked in place.
     */
    private void sweepDrainNode(String groupId, String bucketKey) {
        // Read the group's CURRENT node_limit (the same live read the completion drain uses). Group deleted or node tier
        // inactive: nothing to admit here — owner-push handles the shared tier, and the live-config queue drain releases
        // a backlog whose last tier was removed (see requiresQueueDrain).
        final int nodeLimit = currentNodeLimit(groupId);
        if (nodeLimit < 1) {
            return;
        }
        final WorkloadGroupQueueService qs = queueService;
        if (qs != null) {
            qs.drainNode(groupId, bucketKey, key -> acquireNodePermit(groupId, key, nodeLimit));
        }
    }

    private void onThrottleBreach(ThrottlePlan plan, boolean sharedUnavailable, WorkloadGroupTask task) {
        if (plan.workloadGroup().getResiliencyMode() == MutableWorkloadGroupFragment.ResiliencyMode.MONITOR) {
            if (logger.isDebugEnabled()) {
                logger.debug("Request would be throttled (monitor mode, not rejected): {}.", throttleDescription(plan, sharedUnavailable));
            }
            recordThrottleStat(plan.workloadGroup().get_id(), true);
            task.setThrottleCounted(true);
            return;
        }
        recordThrottleStat(plan.workloadGroup().get_id(), false);
        throw new OpenSearchRejectedExecutionException("Request throttled: " + throttleDescription(plan, sharedUnavailable) + ".");
    }

    private static String throttleDescription(ThrottlePlan plan, boolean sharedUnavailable) {
        String target = "workload group [" + plan.workloadGroup().getName() + "]";
        if (WorkloadGroupThrottleSettings.GROUP_SCOPE.equals(plan.by()) == false) {
            target += " for " + plan.by() + " [" + plan.byValue() + "]";
        }
        // A breach means every configured tier is exhausted, so name each one; an unavailable shared tier was not checked.
        if (sharedUnavailable) {
            String sharedUnchecked = "could not check its shared limit of "
                + plan.sharedLimit()
                + " concurrent requests (cluster-wide throttle unavailable)";
            return plan.nodeLimit() > 0
                ? target + " reached its per-node limit of " + plan.nodeLimit() + " concurrent requests and " + sharedUnchecked
                : target + " " + sharedUnchecked;
        } else if (plan.nodeLimit() > 0 && plan.sharedLimit() > 0) {
            return target
                + " reached its per-node limit of "
                + plan.nodeLimit()
                + " and shared limit of "
                + plan.sharedLimit()
                + " concurrent requests";
        } else if (plan.nodeLimit() > 0) {
            return target + " reached its per-node limit of " + plan.nodeLimit() + " concurrent requests";
        }
        return target + " reached its shared limit of " + plan.sharedLimit() + " concurrent requests";
    }

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
        if (throttling == null || throttling.isEmpty()) {
            return null;
        }
        int nodeLimit = WorkloadGroupThrottleSettings.NODE_LIMIT.get(throttling);
        int sharedLimit = WorkloadGroupThrottleSettings.SHARED_LIMIT.get(throttling);
        if (nodeLimit < 1 && sharedLimit < 1) {
            return null;
        }
        String by = WorkloadGroupThrottleSettings.getEffectiveBy(throttling);
        String byValue = resolveThrottleByValue(by, principal);
        if (byValue == null) {
            return null;
        }
        String bucketKey = workloadGroupId + BUCKET_KEY_DELIMITER + by + BUCKET_KEY_DELIMITER + byValue;
        int queueSizePerBucket = queueSizePerBucket(workloadGroup.getMutableWorkloadGroupFragment().getQueue());
        return new ThrottlePlan(workloadGroup, bucketKey, by, byValue, nodeLimit, sharedLimit, queueSizePerBucket);
    }

    // queue.size_per_bucket, or 0 (queueing disabled, reject immediately) when unset or unreadable. A config this node
    // considers invalid was accepted leniently on deserialization; it must disable queueing, not throttling.
    private static int queueSizePerBucket(Settings queue) {
        if (queue == null || queue.isEmpty()) {
            return 0;
        }
        try {
            WorkloadGroupQueueSettings.validate(queue);
            return WorkloadGroupQueueSettings.SIZE_PER_BUCKET.get(queue);
        } catch (IllegalArgumentException e) {
            return 0;
        }
    }

    private record ThrottlePlan(WorkloadGroup workloadGroup, String bucketKey, String by, String byValue, int nodeLimit, int sharedLimit,
        int queueSizePerBucket) {
    }

    /**
     * Bumps {@code total_would_throttle} ({@code wouldThrottleOnly == true}, MONITOR observed) or {@code total_throttled}
     * (actual rejection). Swallows stats failures so they can't mask the request's outcome, and uses the raw state map so a
     * not-yet-registered group isn't misattributed to DEFAULT.
     */
    private void recordThrottleStat(String workloadGroupId, boolean wouldThrottleOnly) {
        try {
            WorkloadGroupState workloadGroupState = workloadGroupsStateAccessor.getWorkloadGroupStateMap().get(workloadGroupId);
            if (workloadGroupState != null) {
                if (wouldThrottleOnly) {
                    workloadGroupState.totalWouldThrottle.inc();
                } else {
                    workloadGroupState.totalThrottled.inc();
                }
            }
        } catch (Exception statsException) {
            logger.warn("Failed to record throttle stat for workload group [" + workloadGroupId + "]", statsException);
        }
    }

    /**
     * Resolves the bucket key value: {@link WorkloadGroupThrottleSettings#GROUP_SCOPE} for whole-group throttling, else the
     * principal's {@code username}/{@code role} value. When a principal carries several values for the subfield (a user in
     * many roles), the lexicographically smallest is chosen so the same caller lands in a stable bucket.
     *
     * @return the bucket dimension value, or {@code null} to fail open when the principal has no usable value
     */
    private String resolveThrottleByValue(String by, String principal) {
        if (WorkloadGroupThrottleSettings.GROUP_SCOPE.equals(by)) {
            return WorkloadGroupThrottleSettings.GROUP_SCOPE;
        }
        if (principal == null || principal.isEmpty()) {
            return null;
        }
        // Trim the token, not the value: trimming past the delimiter would fold "username|alice " into alice's bucket.
        String subfieldPrefix = by + WorkloadGroupTask.WORKLOAD_GROUP_PRINCIPAL_SUBFIELD_DELIMITER;
        String selected = null;
        for (String token : principal.split(WorkloadGroupTask.WORKLOAD_GROUP_PRINCIPAL_VALUE_DELIMITER)) {
            String trimmed = token.trim();
            if (trimmed.startsWith(subfieldPrefix)) {
                String value = trimmed.substring(subfieldPrefix.length());
                if (value.isEmpty() == false && (selected == null || value.compareTo(selected) < 0)) {
                    selected = value;
                }
            }
        }
        return selected;
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
