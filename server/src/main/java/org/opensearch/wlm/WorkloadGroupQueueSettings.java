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
 * queue and admit it once a permit frees. The single user-facing knob is {@code size_per_bucket}: the maximum parked
 * requests <em>per throttle bucket</em> (per coordinator), mirroring how the throttle limits ({@code node_limit},
 * {@code shared_limit}) are themselves per-bucket. {@code 0} disables queueing (immediate reject preserved). Keying the
 * cap per bucket gives fairness for {@code attribute=username}/{@code role}: one principal's flood cannot consume
 * another principal's per-bucket allowance. That fairness is bounded rather than absolute — see the group ceiling below,
 * at which admission reverts to first-come-first-served across buckets. For {@code attribute=group} there is a single
 * bucket, so it is simply the group's queue depth.
 * <p>
 * Above the per-bucket cap sits a fixed, non-configurable per-group ceiling ({@link #MAX_GROUP_QUEUE_DEPTH}) on the
 * <em>total</em> parked requests across all of a group's buckets on one coordinator. It is a safety backstop, not a
 * fairness knob: because bucket keys for {@code username}/{@code role} are attacker-controlled (derived from the request
 * principal), a purely per-bucket cap would let unbounded distinct principals each allocate {@code size_per_bucket}
 * slots, so the group ceiling bounds the coordinator's parked footprint (heap + open connections) regardless of bucket
 * cardinality. {@link #MAX_SIZE_PER_BUCKET} is pinned to the same value, so validation never accepts a per-bucket depth
 * the ceiling could not honour: a single-bucket group ({@code attribute=group}) may queue the whole group budget in its
 * one bucket, while a many-bucket group reaches the ceiling first — the intended shed point, where a 429 is the correct
 * response. See {@code WorkloadGroupQueue} for enforcement (a request is admitted only if both its bucket is under
 * {@code size_per_bucket} AND the group total is under this ceiling).
 * <p>
 * There is deliberately <b>no user-facing queue timeout</b>, and no timeout of any kind. Legitimate queue wait is
 * unbounded — it grows with backlog depth over drain throughput — so any fixed wall-clock cap would eventually cancel
 * healthy, still-connected requests under a large slow burst. A client bounds its own wait with
 * {@code cancel_after_time_interval} (per request, or the {@code search.cancel_after_time_interval} cluster setting):
 * its cancellation timer is armed before throttle admission, so it fires while the request is parked and evicts the
 * entry promptly. A parked request is therefore bounded only by task cancellation (client disconnect or
 * {@code cancel_after_time_interval}); the queue itself never expires an entry by time — the backstop sweep only
 * removes entries whose task is already cancelled.
 */
@ExperimentalApi
public class WorkloadGroupQueueSettings {

    /** Default per-bucket queue depth: {@code 0} disables queueing (over-limit requests are rejected immediately). */
    public static final int DEFAULT_SIZE_PER_BUCKET = 0;

    /**
     * Fixed, non-configurable ceiling on the TOTAL parked requests for one group across all its buckets on a single
     * coordinator. A pure OOM/footprint backstop against attacker-controlled bucket cardinality (username/role buckets
     * come from the request principal), NOT a latency or fairness knob. Grounded in the retained heap of a parked
     * request (its held {@code SearchRequest} + open channel + task ~ tens of KB): 10,000 parked ~= a few hundred MB of
     * pinned request memory, a high-but-acceptable backstop on a small heap and negligible on a large one.
     */
    public static final int MAX_GROUP_QUEUE_DEPTH = 10_000;

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
