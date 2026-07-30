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
        if (existingStateMap != null) {
            existingStateMap.forEach((workloadGroupId, currentState) -> {
                boolean shouldInclude = workloadGroupIds.contains("_all") || workloadGroupIds.contains(workloadGroupId);
                if (shouldInclude) {
                    if (requestedBreached == null || requestedBreached == resourceLimitBreached(workloadGroupId, currentState)) {
                        statsHolderMap.put(workloadGroupId, WorkloadGroupStatsHolder.from(currentState));
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
     * @param workloadGroupId the workload group the request is assigned to
     * @param principal       the raw {@code WORKLOAD_GROUP_PRINCIPAL_HEADER} value, or {@code null}
     * @param listener        receives the permit / null / 429
     */
    public void acquireThrottlePermit(String workloadGroupId, String principal, ActionListener<Releasable> listener) {
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
            Releasable localPermit = throttleTracker.tryAcquire(plan.bucketKey, plan.nodeLimit);
            if (localPermit != null) {
                listener.onResponse(localPermit);
                return;
            }
            // Local allowance exhausted -> fall through to the shared pool if one is configured.
        }

        // Cluster-level shared tier (asynchronous owner round-trip).
        if (plan.sharedLimit != WorkloadGroupThrottleSettings.UNSET_LIMIT && sharedThrottleService != null) {
            sharedThrottleService.acquireAsync(plan.bucketKey, plan.sharedLimit, ActionListener.wrap(listener::onResponse, e -> {
                if (e instanceof OpenSearchRejectedExecutionException) {
                    // Recompose the denial with the human-readable group name/attribute (the shared tier only has the
                    // opaque bucket key), and count it.
                    incrementThrottled(workloadGroupId);
                    listener.onFailure(sharedTierRejection(plan));
                } else {
                    listener.onFailure(e);
                }
            }));
            return;
        }

        if (plan.nodeLimit != WorkloadGroupThrottleSettings.UNSET_LIMIT && plan.sharedLimit == WorkloadGroupThrottleSettings.UNSET_LIMIT) {
            // Node-only config with the local allowance exhausted and no shared tier to overflow to -> reject.
            incrementThrottled(workloadGroupId);
            listener.onFailure(nodeTierRejection(plan));
        } else {
            // Either a shared tier was configured but is unavailable (not yet wired), or a shared-only config with no
            // wired tier. Fail open rather than reject, consistent with every other shared-tier-unavailable path.
            listener.onResponse(null);
        }
    }

    // User-facing 429 for the node-local tier. Names the group (and attribute, if keyed) and the per-node limit knob.
    private static OpenSearchRejectedExecutionException nodeTierRejection(ThrottlePlan plan) {
        return new OpenSearchRejectedExecutionException(
            "Request throttled: " + plan.describeTarget() + " reached its per-node limit of " + plan.nodeLimit + " concurrent requests."
        );
    }

    // User-facing 429 for the cluster-level shared tier. Same shape as the node message, naming the cluster-wide limit.
    private static OpenSearchRejectedExecutionException sharedTierRejection(ThrottlePlan plan) {
        return new OpenSearchRejectedExecutionException(
            "Request throttled: "
                + plan.describeTarget()
                + " reached its cluster-wide limit of "
                + plan.sharedLimit
                + " concurrent requests."
        );
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
        return new ThrottlePlan(bucketKey, nodeLimit, sharedLimit, workloadGroup.getName(), attribute, value);
    }

    // The resolved throttle configuration for a single request. Carries the human-readable group name and the throttle
    // dimension (attribute + resolved value) purely so a rejection message can name them; the opaque bucketKey remains
    // the identity used by both tracker tiers.
    private static class ThrottlePlan {
        final String bucketKey;
        final int nodeLimit;
        final int sharedLimit;
        final String groupName;
        final String attribute;   // "group" | "username" | "role"
        final String value;       // the resolved principal value for username/role; null for whole-group throttling

        ThrottlePlan(String bucketKey, int nodeLimit, int sharedLimit, String groupName, String attribute, String value) {
            this.bucketKey = bucketKey;
            this.nodeLimit = nodeLimit;
            this.sharedLimit = sharedLimit;
            this.groupName = groupName;
            this.attribute = attribute;
            this.value = value;
        }

        // "workload group [analytics]" or "workload group [analytics] for username [alice]".
        String describeTarget() {
            String base = "workload group [" + groupName + "]";
            if ("group".equals(attribute) || value == null) {
                return base;
            }
            return base + " for " + attribute + " [" + value + "]";
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
