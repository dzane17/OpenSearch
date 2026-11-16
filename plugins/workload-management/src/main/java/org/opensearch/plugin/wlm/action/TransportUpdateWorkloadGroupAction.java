/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.plugin.wlm.service.WorkloadGroupPersistenceService;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.wlm.MutableWorkloadGroupFragment;
import org.opensearch.wlm.WorkloadGroupThrottleSettings;

import java.io.IOException;

import static org.opensearch.threadpool.ThreadPool.Names.SAME;

/**
 * Transport action to update WorkloadGroup
 *
 * @opensearch.experimental
 */
public class TransportUpdateWorkloadGroupAction extends TransportClusterManagerNodeAction<
    UpdateWorkloadGroupRequest,
    UpdateWorkloadGroupResponse> {

    private final WorkloadGroupPersistenceService workloadGroupPersistenceService;

    /**
     * Constructor for TransportUpdateWorkloadGroupAction
     *
     * @param threadPool - {@link ThreadPool} object
     * @param transportService - a {@link TransportService} object
     * @param actionFilters - a {@link ActionFilters} object
     * @param indexNameExpressionResolver - {@link IndexNameExpressionResolver} object
     * @param workloadGroupPersistenceService - a {@link WorkloadGroupPersistenceService} object
     */
    @Inject
    public TransportUpdateWorkloadGroupAction(
        ThreadPool threadPool,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        WorkloadGroupPersistenceService workloadGroupPersistenceService
    ) {
        super(
            UpdateWorkloadGroupAction.NAME,
            transportService,
            workloadGroupPersistenceService.getClusterService(),
            threadPool,
            actionFilters,
            UpdateWorkloadGroupRequest::new,
            indexNameExpressionResolver
        );
        this.workloadGroupPersistenceService = workloadGroupPersistenceService;
    }

    @Override
    protected void doExecute(Task task, UpdateWorkloadGroupRequest request, ActionListener<UpdateWorkloadGroupResponse> listener) {
        final Settings requested = request.getmMutableWorkloadGroupFragment().getThrottling();
        final UpdateWorkloadGroupRequest toSend;
        try {
            toSend = forElectedClusterManager(request, clusterService.state());
        } catch (IllegalArgumentException e) {
            listener.onFailure(e);
            return;
        }
        // An older cluster manager (e.g. after a failover past the translation above) acknowledges some keyed changes
        // without applying them. Check the result and fail loudly rather than report success.
        super.doExecute(task, toSend, ActionListener.wrap(response -> {
            WorkloadGroupThrottleSettings.verifyUpdateApplied(
                requested,
                response.getWorkloadGroup().getMutableWorkloadGroupFragment().getThrottling()
            );
            listener.onResponse(response);
        }, listener::onFailure));
    }

    // While the elected cluster manager runs a build that predates multi-attribute throttling, send it a throttling change
    // in the flat form it applies (see WorkloadGroupThrottleSettings#toFlatUpdate); otherwise send the request as is.
    static UpdateWorkloadGroupRequest forElectedClusterManager(UpdateWorkloadGroupRequest request, ClusterState state) {
        final MutableWorkloadGroupFragment fragment = request.getmMutableWorkloadGroupFragment();
        if (fragment.getThrottling() == null
            || WorkloadGroupThrottleSettings.clusterManagerPredatesMultiAttributeThrottling(state.nodes()) == false) {
            return request;
        }
        final Settings existing = state.metadata()
            .workloadGroups()
            .values()
            .stream()
            .filter(group -> group.getName().equals(request.getName()))
            .findFirst()
            .map(group -> group.getMutableWorkloadGroupFragment().getThrottling())
            .orElse(null);
        final Settings flat = WorkloadGroupThrottleSettings.toFlatUpdate(fragment.getThrottling(), existing);
        // Copy with only throttling replaced: every other field (and a null "keep" throttling) must reach the cluster
        // manager unchanged, or it would read null-turned-empty as "clear".
        return new UpdateWorkloadGroupRequest(request.getName(), fragment.withThrottling(flat));
    }

    @Override
    protected void clusterManagerOperation(
        UpdateWorkloadGroupRequest request,
        ClusterState clusterState,
        ActionListener<UpdateWorkloadGroupResponse> listener
    ) {
        workloadGroupPersistenceService.updateInClusterStateMetadata(request, listener);
    }

    @Override
    protected String executor() {
        return SAME;
    }

    @Override
    protected UpdateWorkloadGroupResponse read(StreamInput in) throws IOException {
        return new UpdateWorkloadGroupResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(UpdateWorkloadGroupRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
