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
import org.opensearch.action.search.SearchExecutionGate;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.Setting;
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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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

    // Cadence of the recovery-only queue backstop pass in doRun: a node-tier re-acquire plus a shared-tier waiter
    // re-registration.
    static final long QUEUE_BACKSTOP_SWEEP_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(5);

    /**
     * The workload group id: the bucket key's first segment. Group ids are base64 UUIDs, so they never contain the
     * delimiter; the key is built in {@link #resolveThrottlePlan}.
     */
    static String groupIdOf(String bucketKey) {
        final int idx = bucketKey.indexOf(BUCKET_KEY_DELIMITER);
        return idx < 0 ? bucketKey : bucketKey.substring(0, idx);
    }

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
    // Whether any workload group in the applied cluster state configures throttling; see isThrottlingActive.
    private volatile boolean throttlingConfigured;
    // Coordinator-local request queues; late-bound. Null => no queueing (throttle denial rejects immediately).
    private volatile WorkloadGroupQueueService queueService;
    // The service loop also drives node-duress cancellation, so keep that cadence and gate only the cheaper,
    // recovery-only queue sweep independently.
    // Only doRun touches these: its single fixed-delay task never overlaps itself and each run happens-after the last.
    private boolean queueBackstopSwept;
    private long lastQueueBackstopSweepNanos;

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
        final WlmMode wlmMode = workloadManagementSettings.getWlmMode();
        if (wlmMode != WlmMode.ENABLED) {
            releaseRetainedRequestsWhileNotEnabled();
            if (wlmMode == WlmMode.DISABLED) {
                return;
            }
        }
        taskCancellationService.cancelTasks(nodeDuressTrackers::isNodeInDuress, activeWorkloadGroups, deletedWorkloadGroups);
        taskCancellationService.pruneDeletedWorkloadGroups(deletedWorkloadGroups);
        // Recovery-only queue backstop for both tiers: a node-tier re-acquire for each retained bucket, then a shared-tier
        // waiter re-registration for each bucket holding WAITING requests (one that cannot be confirmed leaves them parked
        // for the next pass). The primary paths (completion hands its still-held node permit to the queue, the owner pushes
        // a grant when a shared slot frees, cancellation evicts through its per-request callback) never wait on it, so it
        // can run less frequently. Keep it separate from the WLM enforcement cadence above.
        final WorkloadGroupQueueService qs = queueService;
        if (qs != null && queueBackstopSweepDue(threadPool.relativeTimeInNanos())) {
            qs.sweep(this::sweepDrainNode);
            sweepSharedWaiters(qs);
        }
    }

    // WLM is not ENABLED, so no request is throttled, and onWlmModeChanged drained the queue when WLM left ENABLED. A
    // request that resolved its throttle plan before that switch can still park just after the drain, and nothing is
    // bound to admit it: while DISABLED the queue backstop in doRun never runs, and in MONITOR_ONLY it would wait for a
    // permit under limits that no longer apply. So every run releases such a straggler untracked, as the drain would
    // have. With the queue empty this is one counter read and no allocation, and it runs none of the enforcement work
    // DISABLED skips.
    // Accepted: doRun reads the mode once per run, so a request that parks just after WLM is re-enabled, before this run
    // has released its group, is released untracked too: the same best-effort cutover as the mode-change drain (see
    // requiresQueueDrain).
    private void releaseRetainedRequestsWhileNotEnabled() {
        final WorkloadGroupQueueService qs = queueService;
        if (qs == null || qs.hasRetainedRequests() == false) {
            return;
        }
        for (String groupId : qs.queuedGroupIds()) {
            if (qs.retainedDepth(groupId) == 0) {
                continue;
            }
            try {
                final int released = qs.admitAllUntracked(groupId);
                if (released > 0) {
                    logger.info("Released {} queued request(s) for workload group [{}]: WLM is not enabled.", released, groupId);
                }
            } catch (Exception e) {
                logger.warn("Failed to release the queued backlog for workload group [" + groupId + "]", e);
            }
        }
    }

    // Shared-tier backstop: re-confirm this coordinator's waiter registration for every bucket that holds WAITING
    // shared-tier requests (see WorkloadGroupSharedThrottleService#reconcileRetainedWaiters). WAITING has no deadline, and
    // owner-push is a shared-tier request's only wake-up: this pass restores it when it was lost, and provides it for a
    // request that parked without a registration (a denial its owner could not register, or an acquire the shared tier
    // could not answer). A registration that cannot be confirmed rejects nothing: the requests wait for the next pass.
    // Intentional: the reconcile runs unconditionally instead of first detecting a lost wake-up. Each sweep sends at most
    // one REGISTER_WAITER per bucket holding WAITING shared-tier requests (none for a bucket this node owns), so at most
    // WorkloadGroupQueueSettings.MAX_GROUP_QUEUE_DEPTH per throttled group; a high-cardinality by=username/role group is
    // the worst case. The owner handles each idempotently, and on a still-full bucket its offer stops at the first failed
    // reservation.
    private void sweepSharedWaiters(WorkloadGroupQueueService qs) {
        final WorkloadGroupSharedThrottleService sharedService = sharedThrottleService;
        if (sharedService == null) {
            return;
        }
        for (String bucketKey : qs.bucketKeysWithWaiting()) {
            try {
                if (currentLimit(groupIdOf(bucketKey), WorkloadGroupThrottleSettings.SHARED_LIMIT) >= 1) {
                    sharedService.reconcileRetainedWaiters(bucketKey);
                }
            } catch (Exception e) {
                logger.debug("Failed to reconcile shared-throttle waiters for bucket [" + bucketKey + "]", e);
            }
        }
    }

    /**
     * The ring has no eligible owner for {@code bucketKey}, so no node counts the group's {@code shared_limit} and
     * admission treats it as unset (see {@link #resolveThrottlePlan}). The bucket's retained requests are never rejected
     * for that: they stay where they are and the node tier decides at once. With {@code node_limit}, free node permits
     * admit them oldest first, and a completing request hands its permit to the next; without it they are admitted
     * untracked (see {@link #sweepDrainNode}). Called on the generic pool, from an owner-ring change or the backstop pass.
     */
    private void onNoEligibleOwner(String bucketKey) {
        sweepDrainNode(groupIdOf(bucketKey), bucketKey);
    }

    /**
     * The group's <em>current</em> value of a throttle limit ({@code node_limit} or {@code shared_limit}) from cluster
     * state, or {@link WorkloadGroupThrottleSettings#UNSET_LIMIT} if the group is gone, the tier is not configured, or its
     * config cannot be read. Read fresh (not captured) everywhere a drain re-acquires, so a live limit update takes effect
     * on the very next drain.
     */
    private int currentLimit(String groupId, Setting<Integer> limit) {
        try {
            WorkloadGroup workloadGroup = getWorkloadGroupById(groupId);
            if (workloadGroup == null) {
                return WorkloadGroupThrottleSettings.UNSET_LIMIT;
            }
            return limit.get(workloadGroup.getMutableWorkloadGroupFragment().getThrottling());
        } catch (Exception e) {
            return WorkloadGroupThrottleSettings.UNSET_LIMIT;
        }
    }

    // Relative-clock readings are compared by difference only: the clock's origin is arbitrary and may be negative.
    private boolean queueBackstopSweepDue(long nowNanos) {
        if (queueBackstopSwept && nowNanos - lastQueueBackstopSweepNanos < QUEUE_BACKSTOP_SWEEP_INTERVAL_NANOS) {
            return false;
        }
        queueBackstopSwept = true;
        lastQueueBackstopSweepNanos = nowNanos;
        return true;
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
        this.throttlingConfigured = anyGroupConfiguresThrottling(currentWorkloadGroups.values());
        drainQueuedRequestsUntracked(groupIdsToDrain, "workload group admission policy changed");
    }

    private static boolean anyGroupConfiguresThrottling(Iterable<WorkloadGroup> workloadGroups) {
        for (WorkloadGroup workloadGroup : workloadGroups) {
            Settings throttling = workloadGroup.getMutableWorkloadGroupFragment().getThrottling();
            if (throttling != null && throttling.isEmpty() == false) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether any request can currently be throttled: WLM is {@link WlmMode#ENABLED} and some workload group configures
     * throttling. Backs {@link WorkloadGroupSearchAdmission#isActive()}: with the feature off, searches skip admission entirely
     * (no admission work or listener wrapping). Like every admission input, a group's throttling takes effect once its
     * cluster state is applied on this node.
     */
    public boolean isThrottlingActive() {
        return throttlingConfigured && workloadManagementSettings.getWlmMode() == WlmMode.ENABLED;
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
     * <p>
     * Accepted: after a {@code by} or tier-set change that leaves the group throttled, the released backlog (at most
     * {@link WorkloadGroupQueueSettings#MAX_GROUP_QUEUE_DEPTH} requests per group on each coordinator) starts at once with
     * no permit, and new arrivals are admitted up to the new limits on top of it. When the change shrinks the allowance
     * (for example dropping one of two tiers, or moving {@code by} from a principal to the whole group), that briefly runs
     * against the operator's intent. Re-admitting each request under the new policy would need re-queueing machinery for
     * a rare operator action; lowering a numeric limit, the usual way to tighten, does not drain.
     * <p>
     * A numeric {@code node_limit} increase is not a drain: the next backstop sweep fills the added slots, tracked and
     * oldest first (see {@link #sweepDrainNode}). Accepted: until that sweep, up to
     * {@link #QUEUE_BACKSTOP_SWEEP_INTERVAL_NANOS}, new arrivals can take added slots ahead of retained requests. The
     * shared tier offers an increase at once ({@link WorkloadGroupSharedThrottleService}'s cluster-change hook).
     * <p>
     * {@code previousGroup} must be non-null. Package-private so that hook stays disjoint from this policy.
     */
    static boolean requiresQueueDrain(WorkloadGroup previousGroup, WorkloadGroup currentGroup) {
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
        // One-shot: a request that parks after this drain is released by doRun (see releaseRetainedRequestsWhileNotEnabled).
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
                        long queuedCurrent = qs == null ? 0L : qs.waitingDepth(workloadGroupId);
                        long queuedPeak = qs == null ? 0L : qs.peakWaitingDepth(workloadGroupId);
                        statsHolderMap.put(workloadGroupId, WorkloadGroupStatsHolder.from(currentState, queuedCurrent, queuedPeak));
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

        // Rejections do not happen for MONITOR mode and only happen for SOFT mode when the node is in duress.
        Optional<WorkloadGroup> optionalWorkloadGroup = activeWorkloadGroups.stream()
            .filter(x -> x.get_id().equals(workloadGroupId))
            .findFirst();

        if (optionalWorkloadGroup.isPresent()) {
            MutableWorkloadGroupFragment.ResiliencyMode resiliencyMode = optionalWorkloadGroup.get().getResiliencyMode();
            if (resiliencyMode == MutableWorkloadGroupFragment.ResiliencyMode.MONITOR
                || (resiliencyMode == MutableWorkloadGroupFragment.ResiliencyMode.SOFT && !nodeDuressTrackers.isNodeInDuress())) {
                return;
            }
        }

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
        sharedThrottleService.setNoEligibleOwnerHandler(this::onNoEligibleOwner);
    }

    /**
     * Late-binds the coordinator-local retained-request service. When unset, a throttle denial rejects immediately (the
     * pre-queueing behavior). When set and the group's {@code queue.size_per_bucket} is positive, a node-tier denial can
     * enter WAITING, while a shared-tier request is retained as PENDING_ACQUIRE before the owner verdict and transitions
     * to WAITING on a denial or when the shared tier could not answer (see {@code acquireSharedPermitQueued}). MONITOR
     * groups never park a request.
     */
    public void setQueueService(WorkloadGroupQueueService queueService) {
        this.queueService = queueService;
    }

    /**
     * Admits a search against the node-local allowance ({@code node_limit}), then the shared allowance
     * ({@code shared_limit}, a cluster-wide overflow pool on top of every node's local allowance) if the local bucket is
     * full. While no node can own a shared bucket ({@link WorkloadGroupSharedThrottleService#hasEligibleOwner}),
     * {@code shared_limit} is treated as unset. Only shared admission and a queued request complete asynchronously. A
     * counted parent exempts nested searches at both tiers.
     * MONITOR records would-be throttling without rejecting; SOFT and ENFORCED reject at the effective limit, unless the
     * group's {@code queue.size_per_bucket} lets the request wait for a permit instead (see {@link #setQueueService}).
     * Search actions reach this through {@link SearchExecutionGate#execute} ({@link WorkloadGroupSearchAdmission}),
     * which owns releasing the permit.
     *
     * @param task the search task to mark as counted when admitted against a throttle
     * @param parentAlreadyCounted supplies whether this task inherits an existing throttle charge
     * @param listener receives a permit to release on completion, {@code null} when no permit is needed, or a 429
     */
    public void acquireThrottlePermit(WorkloadGroupTask task, BooleanSupplier parentAlreadyCounted, ActionListener<Releasable> listener) {
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
            parkOrRejectNodeTierBreach(plan, task, listener);
            return;
        }
        WorkloadGroupSharedThrottleService sharedService = sharedThrottleService;
        if (sharedService == null) {
            // Defensive (only test-built instances lack it): reject with the unavailable 429, even if the group queues.
            // Without the shared service no registration or grant could ever wake a parked request.
            deliverThrottleBreach(plan, true, false, task, listener);
            return;
        }
        final WorkloadGroupQueueService qs = queueService;
        // Intentional: MONITOR never parks (see parkOrRejectNodeTierBreach), so it takes the unqueued path.
        if (qs != null && plan.queueSizePerBucket() > 0 && isMonitor(plan) == false) {
            acquireSharedPermitQueued(plan, task, sharedService, qs, listener);
            return;
        }
        acquireSharedPermitUnqueued(plan, task, sharedService, null, listener);
    }

    // Shared-tier admission without retaining the request: a grant admits, while a denial or an acquire the shared tier
    // could not answer rejects (or observes, in MONITOR). onQueueFull is set only when the group's queue had no room for
    // this request: it runs before either breach is delivered, and the 429 then says the queue is full.
    private void acquireSharedPermitUnqueued(
        ThrottlePlan plan,
        WorkloadGroupTask task,
        WorkloadGroupSharedThrottleService sharedService,
        @Nullable Runnable onQueueFull,
        ActionListener<Releasable> listener
    ) {
        final boolean monitor = isMonitor(plan);
        final boolean queueFull = onQueueFull != null;
        // MONITOR proceeds on every outcome, so the shared tier never delivers a search-pool rejection for it.
        sharedService.acquireAsync(plan.bucketKey(), plan.sharedLimit(), task.getId(), monitor, ActionListener.wrap(permit -> {
            task.setThrottleCounted(true);
            listener.onResponse(permit);
        }, e -> {
            if (WorkloadGroupSharedThrottleService.isDenial(e)) {
                if (queueFull) {
                    onQueueFull.run();
                }
                deliverThrottleBreach(plan, false, queueFull, task, listener);
            } else if (e instanceof OpenSearchRejectedExecutionException && monitor == false) {
                // The search pool rejected the hand-off (permit already released): a real 429, not a throttle breach.
                listener.onFailure(e);
            } else {
                // The shared tier could not answer, and this path cannot park the request: fail closed so shared_limit is
                // never silently exceeded (MONITOR admits).
                if (WorkloadGroupSharedThrottleService.isUnavailable(e) == false) {
                    logger.debug(() -> "Shared throttle for workload group [" + task.getWorkloadGroupId() + "] failed", e);
                }
                if (queueFull) {
                    onQueueFull.run();
                }
                deliverThrottleBreach(plan, true, queueFull, task, listener);
            }
        }));
    }

    /**
     * Enqueue-first shared-tier admission. The exact request is registered as PENDING_ACQUIRE before the owner RPC, which
     * closes the race where a pushed grant arrives before the request is retained locally, without consuming
     * {@code queue.size_per_bucket}. A direct grant admits it. Every refusal moves it to WAITING if it fits the bucket's
     * waiting cap: a denial, whether or not the owner registered this coordinator as a waiter, and an acquire the shared
     * tier could not answer (owner not connected, acquire timeout, not_owner, transport failure, or any unexpected
     * failure). A registered denial is woken by the owner's pushed grant; for the others the next backstop pass registers
     * this coordinator with the bucket's owner (see {@link #sweepSharedWaiters}), which then offers any capacity already
     * free. A refusal that does not fit gets the 429 it would get without a queue (the unavailable wording for an
     * unanswered acquire, the normal breach wording for a denial), which also says the queue is full. Every outcome
     * targets that exact entry, so a delayed outcome is a no-op once an early pushed grant, a node-permit handoff, or
     * cancellation has already claimed it, and can never complete a later request.
     * <p>
     * Fail-closed: a request is never admitted untracked because the shared tier could not answer. A search-pool rejection
     * of the hand-off is passed through as-is, like on the non-queueing path. The caller has already excluded MONITOR,
     * which never parks.
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
            // would admit. A denial, or an acquire the shared tier could not answer, is then a 429, counted as a queue
            // rejection too since either would have parked had the request fit.
            final String groupId = plan.workloadGroup().get_id();
            acquireSharedPermitUnqueued(plan, task, sharedService, () -> qs.recordQueueRejection(groupId), listener);
            return;
        }
        // proceedsOnDenial is false: MONITOR never takes this path. Every refusal (a denial, registered or not, or an
        // unanswered acquire) is therefore delivered inline, on the network or timer thread for a remote owner; that only
        // moves or rejects this entry. Every admission that follows from it (a pushed grant, a node-permit handoff) is
        // dispatched on an executor by the queue service, and a direct grant arrives on the search pool.
        sharedService.acquireAsync(plan.bucketKey(), plan.sharedLimit(), task.getId(), false, ActionListener.wrap(permit -> {
            // Direct grant: admit this exact request (the queue marks it counted). If another path already claimed it,
            // return the now-unused permit.
            if (qs.admitPendingAcquire(pending, permit) == false) {
                permit.close();
            }
        }, e -> {
            if (WorkloadGroupSharedThrottleService.isDenial(e)) {
                // Denied and registered at the owner, which pushes this coordinator a grant when a slot frees.
                parkPendingAcquire(plan, task, qs, pending, false, listener);
            } else if (e instanceof OpenSearchRejectedExecutionException) {
                // The search pool rejected the hand-off (permit already released): a real 429, not a throttle breach.
                if (qs.claimPendingAcquire(pending)) {
                    listener.onFailure(e);
                }
            } else {
                // The shared tier could not answer (an unexpected failure is treated the same): park the request without a
                // registration, which the next backstop pass makes once the owner can be reached.
                if (WorkloadGroupSharedThrottleService.isUnavailable(e) == false) {
                    logger.debug(() -> "Shared throttle for workload group [" + task.getWorkloadGroupId() + "] failed", e);
                }
                parkPendingAcquire(plan, task, qs, pending, true, listener);
            }
        }), () -> {
            // Denied and not registered at the owner, so no grant follows from this denial: park the request anyway; the next
            // backstop pass registers this coordinator once the owner can resolve and reach it.
            parkPendingAcquire(plan, task, qs, pending, false, listener);
        });
    }

    // Moves the exact provisional request to WAITING: the only step that consumes queue.size_per_bucket, and the one that
    // counts it in total_queued. If the bucket's WAITING budget filled while the acquire was in flight, rejects this exact
    // request with the queue-full 429 (also counted as a queue rejection, not in total_queued), leaving
    // the existing waiters, and any owner registration of this coordinator, untouched. A stale outcome whose entry an
    // early pushed grant, a node-permit handoff or cancellation already removed is a no-op and must not count a throttle.
    private void parkPendingAcquire(
        ThrottlePlan plan,
        WorkloadGroupTask task,
        WorkloadGroupQueueService qs,
        WorkloadGroupQueue.QueuedRequest pending,
        boolean sharedUnavailable,
        ActionListener<Releasable> listener
    ) {
        if (qs.transitionPendingAcquireToWaiting(pending, plan.queueSizePerBucket()) == WorkloadGroupQueue.WaitingTransition.BUCKET_FULL) {
            deliverThrottleBreach(plan, sharedUnavailable, true, task, listener);
        }
    }

    private void deliverThrottleBreach(
        ThrottlePlan plan,
        boolean sharedUnavailable,
        boolean queueFull,
        WorkloadGroupTask task,
        ActionListener<Releasable> listener
    ) {
        try {
            onThrottleBreach(plan, sharedUnavailable, queueFull, task);
        } catch (OpenSearchRejectedExecutionException e) {
            listener.onFailure(e);
            return;
        }
        listener.onResponse(null);
    }

    // Node-tier queue-then-reject: holds the request instead of rejecting if the group queues and both the request's
    // bucket and the group have room. A retained request holds no thread — only its listener and open connection — and is
    // admitted at once if a slot freed while it was being parked, or later by a node-permit handoff, the recovery sweep,
    // or a live-configuration drain, or evicted by task cancellation: a search's on client disconnect or its
    // cancel_after_time_interval, a scroll page's only through the tasks API (see WorkloadGroupSearchAdmission#admit).
    // There is no queue timeout. Parking counts the request in total_queued at once; its wait is recorded in
    // total_queue_wait_millis when it leaves the queue, admitted or cancelled.
    // Intentional: MONITOR never parks or queues a request (a shared-tier one still waits for the owner's answer); it
    // observes (total_would_throttle) and
    // admits through deliverThrottleBreach, so the queue stats stay 0.
    // A request that does not park gets the breach here: the queue-full wording if the group queues but had no room.
    private void parkOrRejectNodeTierBreach(ThrottlePlan plan, WorkloadGroupTask task, ActionListener<Releasable> listener) {
        final WorkloadGroupQueueService qs = queueService;
        final String groupId = plan.workloadGroup().get_id();
        if (qs == null || plan.queueSizePerBucket() <= 0 || isMonitor(plan)) {
            deliverThrottleBreach(plan, false, false, task, listener);
            return;
        }
        if (qs.tryEnqueue(groupId, plan.bucketKey(), task, plan.queueSizePerBucket(), listener) == false) {
            // Queueing is enabled (checked above), so a refusal means the bucket or group queue is full.
            deliverThrottleBreach(plan, false, true, task, listener);
            return;
        }
        // Closes the deny-then-park race, the node-tier twin of the shared owner's offerIfReleasedBeforeRegistration: a
        // completion that freed its slot after this request's acquire failed, but before the request was retained, saw an
        // empty queue and released the slot to nobody. Retry once now that the request is visible; like the sweep, this
        // admits the bucket head only against a slot it actually acquires, so it can never over-admit.
        sweepDrainNode(groupId, plan.bucketKey());
    }

    private static boolean isMonitor(ThrottlePlan plan) {
        return plan.workloadGroup().getResiliencyMode() == MutableWorkloadGroupFragment.ResiliencyMode.MONITOR;
    }

    // Acquires a node-local permit for admission, wrapped so its close() can hand the slot to a queued request.
    private Releasable acquireNodePermit(String groupId, String bucketKey, int nodeLimit) {
        return wrapNodePermit(throttleTracker.tryAcquire(bucketKey, nodeLimit), groupId, bucketKey);
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
            // Checked per bucket: a node permit can only admit a request retained on its own bucket.
            if (qs.hasRetained(groupId, bucketKey)) {
                final int liveNodeLimit = currentLimit(groupId, WorkloadGroupThrottleSettings.NODE_LIMIT);
                if (liveNodeLimit > 0
                    && throttleTracker.inFlight(bucketKey) <= liveNodeLimit
                    && qs.handoffNodePermit(groupId, bucketKey, wrapNodePermit(raw, groupId, bucketKey))) {
                    return;
                }
            }

            raw.close();

            // Recovery path for a concurrent enqueue or a live limit decrease. The normal steady-state queue handoff
            // returned above without exposing a free slot, so only these uncommon cases retain the reacquire race.
            if (qs.hasRetained(groupId, bucketKey)) {
                final int liveNodeLimit = currentLimit(groupId, WorkloadGroupThrottleSettings.NODE_LIMIT);
                if (liveNodeLimit > 0) {
                    qs.drainNode(groupId, bucketKey, key -> acquireNodePermit(groupId, key, liveNodeLimit));
                }
            }
        };
    }

    /**
     * Node-tier backstop drain, run by the sweep, once right after a node-tier park, and when the ring has no eligible
     * owner for the bucket: admit retained requests for {@code bucketKey}, oldest first, while node permits are free; a
     * no-op otherwise. Wired into {@link WorkloadGroupQueueService#sweep}. This recovers a request the node-completion chain
     * missed, and fills slots a live {@code node_limit} increase added, without re-running full admission or contacting the
     * shared owner (sweepSharedWaiters re-confirms shared-tier wake-ups on the same pass), and without
     * dequeuing-and-re-parking, so a still-waiting request stays parked in place. A deleted group, or one with no tier left
     * that applies (no positive limit, or only a {@code shared_limit} that no node can own), has no permit that could admit
     * its retained requests, so they are released untracked, as the live-config drain and a new arrival would be.
     */
    private void sweepDrainNode(String groupId, String bucketKey) {
        final WorkloadGroupQueueService qs = queueService;
        if (qs == null) {
            return;
        }
        // Read the group's CURRENT limits (the same live reads the completion drain and owner push use).
        final int nodeLimit = currentLimit(groupId, WorkloadGroupThrottleSettings.NODE_LIMIT);
        if (nodeLimit >= 1) {
            // Fill every free slot, oldest first, not just one: a live node_limit increase adds several slots that no
            // completion will hand to the backlog. tryAcquire against the live limit stops the loop; the count cap is only
            // a backstop.
            int admitted = 0;
            while (admitted < nodeLimit && qs.drainNode(groupId, bucketKey, key -> acquireNodePermit(groupId, key, nodeLimit))) {
                admitted++;
            }
        } else if (effectiveSharedLimit(currentLimit(groupId, WorkloadGroupThrottleSettings.SHARED_LIMIT)) < 1) {
            // Group deleted, no positive tier left, or a shared_limit no node can own: no permit can admit this bucket. A
            // request that resolved an older plan can park just after the one-shot live-config drain, or a shared-only
            // bucket's owners can all leave, so release it untracked, as that drain and a new arrival would.
            final int released = qs.admitAllUntracked(groupId, bucketKey);
            if (released > 0) {
                logger.info("Released {} queued request(s) for workload group [{}]: no throttle tier applies.", released, groupId);
            }
        }
        // Otherwise shared tier only: owner push, re-confirmed by sweepSharedWaiters, admits the bucket.
    }

    // queueFull: the group queues, but its queue had no room for this request (never true in MONITOR, which never parks).
    private void onThrottleBreach(ThrottlePlan plan, boolean sharedUnavailable, boolean queueFull, WorkloadGroupTask task) {
        if (plan.workloadGroup().getResiliencyMode() == MutableWorkloadGroupFragment.ResiliencyMode.MONITOR) {
            if (logger.isDebugEnabled()) {
                logger.debug("Request would be throttled (monitor mode, not rejected): {}.", throttleDescription(plan, sharedUnavailable));
            }
            recordThrottleStat(plan.workloadGroup().get_id(), true);
            task.setThrottleCounted(true);
            return;
        }
        recordThrottleStat(plan.workloadGroup().get_id(), false);
        final String queueSuffix = queueFull ? ", and its request queue is full" : "";
        throw new OpenSearchRejectedExecutionException(
            "Request throttled: " + throttleDescription(plan, sharedUnavailable) + queueSuffix + "."
        );
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
        int sharedLimit = effectiveSharedLimit(WorkloadGroupThrottleSettings.SHARED_LIMIT.get(throttling));
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

    // The shared_limit admission applies: with no eligible owner, no node counts it, so it is unset and node_limit alone
    // decides.
    private int effectiveSharedLimit(int sharedLimit) {
        final WorkloadGroupSharedThrottleService sharedService = sharedThrottleService;
        if (sharedLimit >= 1 && sharedService != null && sharedService.hasEligibleOwner() == false) {
            return WorkloadGroupThrottleSettings.UNSET_LIMIT;
        }
        return sharedLimit;
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
