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

import java.util.Map;

/**
 * Registry of valid workload group request-queue settings with their validators. Queue config is a nested
 * {@code queue} object (a {@link Settings} bag) alongside {@code throttling}, so per-key null clears a field and an
 * absent key keeps the existing value with no extra bookkeeping.
 * <p>
 * When a request is denied by a throttle limit, instead of an immediate 429 the coordinator may park it in a bounded
 * queue and admit it once a permit frees. The single user-facing knob is {@code size_per_bucket}: the maximum
 * <em>denied and waiting</em> requests per throttle bucket (per coordinator), mirroring how the throttle limits
 * ({@code node_limit}, {@code shared_limit}) are themselves per-bucket. {@code 0} disables queueing (immediate reject
 * preserved). A shared-tier request is provisionally retained as {@code PENDING_ACQUIRE} before the owner RPC so an
 * early pushed grant cannot race ahead of local registration; that provisional state does not consume this configured
 * waiting budget. A registered owner denial attempts to transition the exact request to {@code WAITING} and reserve a
 * bucket slot; an unregistered denial or full waiting bucket rejects that exact request instead.
 * Keying the cap per bucket gives fairness for {@code attribute=username}/{@code role}: one principal's denied backlog
 * cannot consume another principal's per-bucket allowance. That fairness is bounded rather than absolute — see the
 * group ceiling below. For {@code attribute=group} there is a single bucket, so this is the group's waiting depth.
 * <p>
 * Above the per-bucket cap sits a fixed, non-configurable per-group ceiling ({@link #MAX_GROUP_QUEUE_DEPTH}) on every
 * retained request across all of a group's buckets on one coordinator:
 * {@code PENDING_ACQUIRE[group] + WAITING[group] <= MAX_GROUP_QUEUE_DEPTH}. It is a safety backstop, not a fairness
 * knob. It bounds both an extreme burst of in-flight shared-owner acquires and the denied backlog created by
 * attacker-controlled bucket cardinality. There is deliberately no node-wide retained-request cap.
 * {@link #MAX_SIZE_PER_BUCKET} is pinned to the same value, so validation never accepts a per-bucket waiting depth the
 * group ceiling could never honour.
 * <p>
 * There is deliberately <b>no wall-clock timeout for WAITING entries</b>. Legitimate queue wait is unbounded — it grows
 * with backlog depth over drain throughput — so any fixed cap would eventually cancel healthy, still-connected
 * requests under a large slow burst. {@code PENDING_ACQUIRE} remains subject to the shared-owner acquire timeout. A
 * client bounds its own queue wait with
 * {@code cancel_after_time_interval} (per request, or the {@code search.cancel_after_time_interval} cluster setting):
 * its cancellation timer is armed before throttle admission, so it fires while the request is parked and evicts the
 * entry promptly. A parked request is therefore bounded only by task cancellation (client disconnect or
 * {@code cancel_after_time_interval}); the queue itself never expires an entry by time. Cancellation callbacks remove
 * cancelled entries, and admission defensively re-checks cancellation after dequeueing.
 */
@ExperimentalApi
public class WorkloadGroupQueueSettings {

    /** Default per-bucket queue depth: {@code 0} disables queueing (over-limit requests are rejected immediately). */
    public static final int DEFAULT_SIZE_PER_BUCKET = 0;

    /**
     * Fixed, non-configurable ceiling on the TOTAL retained requests for one group across all its buckets on a single
     * coordinator. Counts both provisional shared-owner acquires and denied waiters. This is a footprint backstop
     * against both request bursts and attacker-controlled bucket cardinality, not a latency or fairness knob.
     */
    public static final int MAX_GROUP_QUEUE_DEPTH = 1_000;

    /**
     * Maximum configurable per-bucket queue depth. Pinned to {@link #MAX_GROUP_QUEUE_DEPTH}: the group ceiling caps the
     * total across all of a group's buckets, so a larger per-bucket value could never be honoured and accepting one
     * would silently mislead. A single-bucket group ({@code attribute=group}) can therefore still queue the entire group
     * budget in its one bucket.
     */
    public static final int MAX_SIZE_PER_BUCKET = MAX_GROUP_QUEUE_DEPTH;

    /** Per-coordinator bounded queue depth per throttle bucket. {@code 0} = queueing disabled. */
    public static final Setting<Integer> SIZE_PER_BUCKET = Setting.intSetting(
        "size_per_bucket",
        DEFAULT_SIZE_PER_BUCKET,
        DEFAULT_SIZE_PER_BUCKET
    );

    private static final Map<String, Setting<?>> REGISTERED_SETTINGS = Map.of(SIZE_PER_BUCKET.getKey(), SIZE_PER_BUCKET);

    private WorkloadGroupQueueSettings() {
        throw new UnsupportedOperationException("Utility class");
    }

    /**
     * Per-key validation: every key must be registered and {@code size_per_bucket} must be a non-negative integer no
     * greater than {@link #MAX_SIZE_PER_BUCKET}. Safe to run on a partial fragment from an update request.
     * {@code size_per_bucket} is currently the only queue key, so there are no cross-field rules and
     * therefore no merged-config validator (unlike {@link WorkloadGroupThrottleSettings}, which has real cross-field rules).
     *
     * @param queue the queue settings to validate
     * @throws IllegalArgumentException if any key is unknown or {@code size_per_bucket} is invalid
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
            if (SIZE_PER_BUCKET.getKey().equals(key)) {
                validateSizePerBucket(value);
            }
        }
    }

    // Rejects a size_per_bucket that is not a non-negative integer in [0, MAX_SIZE_PER_BUCKET].
    private static void validateSizePerBucket(String value) {
        final int parsed;
        try {
            parsed = Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("queue.size_per_bucket must be an integer but was '" + value + "'");
        }
        if (parsed < 0) {
            throw new IllegalArgumentException("queue.size_per_bucket must be non-negative but was " + parsed);
        }
        if (parsed > MAX_SIZE_PER_BUCKET) {
            throw new IllegalArgumentException("queue.size_per_bucket must not exceed " + MAX_SIZE_PER_BUCKET + " but was " + parsed);
        }
    }

}
