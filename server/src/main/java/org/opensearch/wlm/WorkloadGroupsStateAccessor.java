/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.wlm.stats.WorkloadGroupState;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is used to decouple {@link WorkloadGroupService} and {@link org.opensearch.wlm.cancellation.WorkloadGroupTaskCancellationService} to share the
 * {@link WorkloadGroupState}s
 */
public class WorkloadGroupsStateAccessor {
    // Concurrent: structural updates happen on the cluster-applier thread while request threads read concurrently
    // (throttle admission, stat updates, cancellation). WorkloadGroupState is itself thread safe.
    private final Map<String, WorkloadGroupState> workloadGroupStateMap;

    public WorkloadGroupsStateAccessor() {
        this(new ConcurrentHashMap<>());
    }

    public WorkloadGroupsStateAccessor(Map<String, WorkloadGroupState> workloadGroupStateMap) {
        this.workloadGroupStateMap = workloadGroupStateMap;
    }

    /**
     * returns the workload groups state
     */
    public Map<String, WorkloadGroupState> getWorkloadGroupStateMap() {
        return workloadGroupStateMap;
    }

    /**
     * return WorkloadGroupState for the given workloadGroupId
     * @param workloadGroupId
     * @return WorkloadGroupState for the given workloadGroupId, if id is invalid return default workload group state
     */
    public WorkloadGroupState getWorkloadGroupState(String workloadGroupId) {
        return workloadGroupStateMap.getOrDefault(
            workloadGroupId,
            workloadGroupStateMap.get(WorkloadGroupTask.DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get())
        );
    }

    /**
     * adds new WorkloadGroupState against given workloadGroupId
     * @param workloadGroupId
     */
    public void addNewWorkloadGroup(String workloadGroupId) {
        this.workloadGroupStateMap.putIfAbsent(workloadGroupId, new WorkloadGroupState());
    }

    /**
     * removes WorkloadGroupState against given workloadGroupId
     * @param workloadGroupId
     */
    public void removeWorkloadGroup(String workloadGroupId) {
        this.workloadGroupStateMap.remove(workloadGroupId);
    }
}
