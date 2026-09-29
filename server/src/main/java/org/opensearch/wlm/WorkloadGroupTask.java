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
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.CancellableTask;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.opensearch.search.SearchService.NO_TIMEOUT;

/**
 * Base class to define WorkloadGroup tasks
 */
@PublicApi(since = "2.18.0")
public class WorkloadGroupTask extends CancellableTask {

    private static final Logger logger = LogManager.getLogger(WorkloadGroupTask.class);
    public static final String WORKLOAD_GROUP_ID_HEADER = "workloadGroupId";
    /**
     * Carries the request's principal for username/role throttling: {@code subfield|value} tokens
     * (e.g. {@code username|alice}) joined by {@link #WORKLOAD_GROUP_PRINCIPAL_VALUE_DELIMITER}. Only set when the
     * security plugin's principal extractor is installed; absent otherwise (username/role throttling then fails open).
     * Consumed only on the origin coordinator (synchronously, before shard fan-out), so it is not propagated cross-node.
     */
    public static final String WORKLOAD_GROUP_PRINCIPAL_HEADER = "workloadGroupPrincipal";
    public static final String WORKLOAD_GROUP_PRINCIPAL_VALUE_DELIMITER = "\u001F";
    public static final Supplier<String> DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER = () -> "DEFAULT_WORKLOAD_GROUP";
    private final LongSupplier nanoTimeSupplier;
    private String workloadGroupId;
    private boolean isWorkloadGroupSet = false;

    // Cancellation callbacks for a request parked in the WLM request queue. A queued request holds no thread; when the
    // task is cancelled (client disconnect or cancel_after timer, both of which cancel the task rather than completing
    // the parked listener), the queue must evict the entry and fail the listener. Guarded by its own lock and fired at
    // most once: once cancelled, a later register runs immediately so there is no lost-cancellation race.
    private final Object cancelCallbackLock = new Object();
    private List<Runnable> onCancelledCallbacks = new ArrayList<>();
    private boolean cancellationNotified = false;

    public WorkloadGroupTask(long id, String type, String action, String description, TaskId parentTaskId, Map<String, String> headers) {
        this(id, type, action, description, parentTaskId, headers, NO_TIMEOUT, System::nanoTime);
    }

    public WorkloadGroupTask(
        long id,
        String type,
        String action,
        String description,
        TaskId parentTaskId,
        Map<String, String> headers,
        TimeValue cancelAfterTimeInterval
    ) {
        this(id, type, action, description, parentTaskId, headers, cancelAfterTimeInterval, System::nanoTime);
    }

    public WorkloadGroupTask(
        long id,
        String type,
        String action,
        String description,
        TaskId parentTaskId,
        Map<String, String> headers,
        TimeValue cancelAfterTimeInterval,
        LongSupplier nanoTimeSupplier
    ) {
        super(id, type, action, description, parentTaskId, headers, cancelAfterTimeInterval);
        this.nanoTimeSupplier = nanoTimeSupplier;
    }

    /**
     * This method should always be called after calling setWorkloadGroupId at least once on this object
     * @return task workloadGroupId
     */
    public final String getWorkloadGroupId() {
        if (workloadGroupId == null) {
            logger.warn("WorkloadGroup _id can't be null, It should be set before accessing it. This is abnormal behaviour ");
        }
        return workloadGroupId;
    }

    /**
     * sets the workloadGroupId from threadContext into the task itself,
     * This method was defined since the workloadGroupId can only be evaluated after task creation
     * @param threadContext current threadContext
     */
    public final void setWorkloadGroupId(final ThreadContext threadContext) {
        isWorkloadGroupSet = true;
        if (threadContext != null && threadContext.getHeader(WORKLOAD_GROUP_ID_HEADER) != null) {
            this.workloadGroupId = threadContext.getHeader(WORKLOAD_GROUP_ID_HEADER);
        } else {
            this.workloadGroupId = DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get();
        }
    }

    public long getElapsedTime() {
        return nanoTimeSupplier.getAsLong() - getStartTimeNanos();
    }

    public boolean isWorkloadGroupSet() {
        return isWorkloadGroupSet;
    }

    @Override
    public boolean shouldCancelChildrenOnCancellation() {
        return false;
    }

    /**
     * Registers a callback invoked once if this task is cancelled, and returns a {@link Releasable} that deregisters it.
     * If the task is already cancelled the callback runs immediately (so there is no window where a cancellation between
     * the cancel check and registration is lost). Used by the WLM request queue to evict and fail a parked request whose
     * client disconnected or whose {@code cancel_after} timer fired — both cancel the task rather than completing the
     * parked listener, so the queue observes cancellation here.
     *
     * @param callback run at most once on cancellation
     * @return a {@link Releasable} that removes the callback if the request is admitted/drained before any cancellation
     */
    public final Releasable addOnCancelledCallback(Runnable callback) {
        synchronized (cancelCallbackLock) {
            if (cancellationNotified || isCancelled()) {
                // Already cancelled: run now rather than register, so a cancellation that landed before registration
                // is never dropped. Nothing to deregister.
                callback.run();
                return () -> {};
            }
            onCancelledCallbacks.add(callback);
        }
        return () -> {
            synchronized (cancelCallbackLock) {
                if (onCancelledCallbacks != null) {
                    onCancelledCallbacks.remove(callback);
                }
            }
        };
    }

    @Override
    protected void onCancelled() {
        final List<Runnable> callbacks;
        synchronized (cancelCallbackLock) {
            if (cancellationNotified) {
                return;
            }
            cancellationNotified = true;
            callbacks = onCancelledCallbacks;
            onCancelledCallbacks = null;
        }
        for (Runnable callback : callbacks) {
            try {
                callback.run();
            } catch (Exception e) {
                logger.warn("WorkloadGroupTask onCancelled callback failed", e);
            }
        }
    }
}
