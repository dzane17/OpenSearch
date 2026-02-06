/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.listeners;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchRequestContext;
import org.opensearch.action.search.SearchRequestOperationsListener;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.cluster.metadata.WorkloadGroupMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.WorkloadGroupSearchSettings;
import org.opensearch.wlm.WorkloadGroupService;
import org.opensearch.wlm.WorkloadGroupTask;

import java.util.Map;

import static java.lang.Integer.min;
import static org.opensearch.wlm.WorkloadGroupSearchSettings.WlmSearchSetting.MAX_BUCKET;

/**
 * This listener is used to listen for request lifecycle events for a workloadGroup
 */
public class WorkloadGroupRequestOperationListener extends SearchRequestOperationsListener {

    private static final Logger logger = LogManager.getLogger(WorkloadGroupRequestOperationListener.class);
    private final WorkloadGroupService workloadGroupService;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;

    public WorkloadGroupRequestOperationListener(
        WorkloadGroupService workloadGroupService,
        ThreadPool threadPool,
        ClusterService clusterService
    ) {
        this.workloadGroupService = workloadGroupService;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
    }

    /**
     * This method assumes that the workloadGroupId is already populated in the thread context
     * @param searchRequestContext SearchRequestContext instance
     */
    @Override
    protected void onRequestStart(SearchRequestContext searchRequestContext) {
        final String workloadGroupId = threadPool.getThreadContext().getHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER);
        workloadGroupService.rejectIfNeeded(workloadGroupId);
        applyWorkloadGroupSearchSettings(workloadGroupId, searchRequestContext.getRequest());
    }

    @Override
    protected void onRequestFailure(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
        final String workloadGroupId = threadPool.getThreadContext().getHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER);
        workloadGroupService.incrementFailuresFor(workloadGroupId);
    }

    /**
     * Applies workload group-specific search settings to the search request.
     * Settings are only applied for workload groups that exist in cluster state.
     *
     * @param workloadGroupId the workload group identifier from thread context
     * @param searchRequest the search request to modify
     */
    private void applyWorkloadGroupSearchSettings(String workloadGroupId, SearchRequest searchRequest) {
        // Skip if no workload group ID (default group is added later)
        if (workloadGroupId == null) {
            return;
        }

        WorkloadGroupMetadata metadata = clusterService.state().metadata().custom(WorkloadGroupMetadata.TYPE);
        if (metadata == null) {
            return;
        }

        // Get the workload group object by ID
        WorkloadGroup workloadGroup = metadata.workloadGroups().get(workloadGroupId);
        if (workloadGroup == null) {
            return;
        }

        // Loop through WLM group search settings and apply them as needed
        // WLM settings are applied only if the corresponding setting is not already set in the request
        for (Map.Entry<String, String> entry : workloadGroup.getSearchSettings().entrySet()) {
            try {
                WorkloadGroupSearchSettings.WlmSearchSetting settingKey = WorkloadGroupSearchSettings.WlmSearchSetting.fromKey(
                    entry.getKey()
                );
                if (settingKey == null) continue;

                switch (settingKey) {
                    case BATCHED_REDUCE_SIZE:
                        // Raw value 0 means not explicitly set; WLM overrides default, else use more restrictive
                        int wlmBatchedReduceSize = Integer.parseInt(entry.getValue());
                        int requestBatchedReduceSize = searchRequest.getBatchedReduceSize();
                        searchRequest.setBatchedReduceSize(min(wlmBatchedReduceSize, requestBatchedReduceSize));
                        break;
                    case CANCEL_AFTER_TIME_INTERVAL:
                        // Null means not explicitly set; WLM overrides default, else use more restrictive
                        TimeValue wlmValue = TimeValue.parseTimeValue(
                            entry.getValue(),
                            WorkloadGroupSearchSettings.WlmSearchSetting.CANCEL_AFTER_TIME_INTERVAL.getSettingName()
                        );
                        TimeValue requestValue = searchRequest.getCancelAfterTimeInterval();

                        if (requestValue == null) {
                            searchRequest.setCancelAfterTimeInterval(wlmValue);
                        } else {
                            searchRequest.setCancelAfterTimeInterval(requestValue.millis() <= wlmValue.millis() ? requestValue : wlmValue);
                        }
                        break;
                    case MAX_CONCURRENT_SHARD_REQUESTS:
                        // Raw value 0 means not explicitly set; WLM overrides default, else use more restrictive
                        int wlmMaxConcurrent = Integer.parseInt(entry.getValue());
                        int requestMaxConcurrent = searchRequest.getMaxConcurrentShardRequestsRaw();
                        if (requestMaxConcurrent == 0) {
                            searchRequest.setMaxConcurrentShardRequests(wlmMaxConcurrent);
                        } else {
                            searchRequest.setMaxConcurrentShardRequests(min(wlmMaxConcurrent, requestMaxConcurrent));
                        }
                        break;
                    case PHASE_TOOK:
                        searchRequest.setPhaseTook(Boolean.parseBoolean(entry.getValue()));
                        break;
                    case TIMEOUT:
                        // Null means not explicitly set; WLM overrides default, else use more restrictive
                        TimeValue wlmTimeout = TimeValue.parseTimeValue(
                            entry.getValue(),
                            WorkloadGroupSearchSettings.WlmSearchSetting.TIMEOUT.getSettingName()
                        );

                        if (searchRequest.source() == null) {
                            break;
                        }
                        TimeValue requestTimeout = searchRequest.source().timeout();

                        if (requestTimeout == null) {
                            searchRequest.source().timeout(wlmTimeout);
                        } else {
                            searchRequest.source().timeout(requestTimeout.millis() <= wlmTimeout.millis() ? requestTimeout : wlmTimeout);
                        }
                        break;
                    case MAX_BUCKET:
                        // Store max_bucket as a request header so it propagates to data nodes
                        threadPool.getThreadContext().putHeader(MAX_BUCKET.getSettingName(), entry.getValue());
                        break;
                }
            } catch (Exception e) {
                logger.error("Failed to apply workload group setting [{}={}]: {}", entry.getKey(), entry.getValue(), e);
            }
        }
    }
}
