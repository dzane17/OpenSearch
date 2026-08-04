/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

import java.util.Map;

/**
 * Registry of valid workload group request-queue settings with their validators. Queue config is a nested
 * {@code queue} object (a {@link Settings} bag) alongside {@code throttling}, so per-key null clears a field and an
 * absent key keeps the existing value with no extra bookkeeping.
 * <p>
 * When a request is denied by a throttle limit, instead of an immediate 429 the coordinator may park it in a bounded
 * per-group queue and admit it once a permit frees. {@code size} bounds the queue (per coordinator, per group);
 * {@code 0} disables queueing (immediate reject preserved). {@code timeout} bounds how long a request waits before it
 * is rejected — a server-owned backstop so a client that never sets a deadline cannot hold a slot indefinitely.
 */
@ExperimentalApi
public class WorkloadGroupQueueSettings {

    /** Default queue depth: {@code 0} disables queueing (over-limit requests are rejected immediately, as before). */
    public static final int DEFAULT_SIZE = 0;

    /** Default max wait before a queued request is rejected. A backstop; queued requests usually drain or cancel sooner. */
    public static final TimeValue DEFAULT_TIMEOUT = TimeValue.timeValueSeconds(30);

    /** Maximum queue depth. Guards against an unreasonable per-node queued-request (and open-connection) footprint. */
    public static final int MAX_SIZE = 100_000;

    /** Per-coordinator bounded queue depth per group. {@code 0} = queueing disabled. */
    public static final Setting<Integer> SIZE = Setting.intSetting("size", DEFAULT_SIZE, DEFAULT_SIZE);

    /** Max time a request waits in the queue before it is rejected (429). */
    public static final Setting<TimeValue> TIMEOUT = Setting.timeSetting("timeout", DEFAULT_TIMEOUT, TimeValue.ZERO);

    private static final Map<String, Setting<?>> REGISTERED_SETTINGS = Map.of(SIZE.getKey(), SIZE, TIMEOUT.getKey(), TIMEOUT);

    private WorkloadGroupQueueSettings() {
        throw new UnsupportedOperationException("Utility class");
    }

    /**
     * Per-key validation: every key must be registered, {@code size} must be a non-negative integer no greater than
     * {@link #MAX_SIZE}, and {@code timeout} must be a non-negative time value. Safe to run on a partial fragment from
     * an update request; cross-field checks live in {@link #validateMergedConfig(Settings)}.
     *
     * @param queue the queue settings to validate
     * @throws IllegalArgumentException if any key is unknown or any value is invalid
     */
    public static void validate(Settings queue) {
        if (queue == null) {
            return;
        }
        for (String key : queue.keySet()) {
            String value = queue.get(key);
            if (REGISTERED_SETTINGS.containsKey(key) == false) {
                throw new IllegalArgumentException("Unknown queue setting: " + key);
            }
            // null value means "clear this key" — skip value validation
            if (value == null) {
                continue;
            }
            if (SIZE.getKey().equals(key)) {
                validateSize(value);
            } else if (TIMEOUT.getKey().equals(key)) {
                // Delegates to TimeValue parsing; a negative or malformed value throws with a clear message.
                TIMEOUT.get(queue);
            }
        }
    }

    // Rejects a size that is not a non-negative integer in [0, MAX_SIZE].
    private static void validateSize(String value) {
        final int parsed;
        try {
            parsed = Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("queue.size must be an integer but was '" + value + "'");
        }
        if (parsed < 0) {
            throw new IllegalArgumentException("queue.size must be non-negative but was " + parsed);
        }
        if (parsed > MAX_SIZE) {
            throw new IllegalArgumentException("queue.size must not exceed " + MAX_SIZE + " but was " + parsed);
        }
    }

    /**
     * Cross-field validation on a fully-merged queue config. A non-zero {@code timeout} is only meaningful with a
     * non-zero {@code size} (there is nothing to time out when queueing is disabled). Must be called on the merged
     * result, not a partial update fragment. The requirement that queueing coexist with a throttle limit is enforced
     * at the workload-group level (a queue without a throttle limit has nothing to queue), not here.
     *
     * @param queue the merged queue settings
     * @throws IllegalArgumentException if a non-zero timeout is set without a positive size
     */
    public static void validateMergedConfig(Settings queue) {
        if (queue == null || queue.isEmpty()) {
            return;
        }
        int size = SIZE.get(queue);
        TimeValue timeout = TIMEOUT.get(queue);
        if (size <= 0 && timeout.nanos() > 0 && queue.hasValue(TIMEOUT.getKey())) {
            throw new IllegalArgumentException("queue.timeout requires queue.size > 0 (queueing is disabled when size is 0)");
        }
    }
}
