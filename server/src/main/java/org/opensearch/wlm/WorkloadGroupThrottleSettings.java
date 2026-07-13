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
import java.util.Set;

/**
 * Registry of valid workload group throttle settings with their validators. Throttle config is a nested
 * {@code throttling} object (a {@link Settings} bag) like {@code settings}, so per-key null clears a field and an
 * absent key keeps the existing value with no extra bookkeeping.
 */
@ExperimentalApi
public class WorkloadGroupThrottleSettings {

    /** Sentinel for an unset limit, matching the {@code -1 = not set} convention of {@code WLM_SEARCH_TIMEOUT}. */
    public static final int UNSET_LIMIT = -1;

    /** Dimension the limit is keyed by: {@code group} (default, whole group) or per {@code username} / {@code role}. */
    public static final Setting<String> ATTRIBUTE = Setting.simpleString("attribute", "group");

    /** Per-node in-flight allowance admitted locally with no coordination. {@code -1} means unset. */
    public static final Setting<Integer> NODE_LIMIT = Setting.intSetting("node_limit", UNSET_LIMIT, UNSET_LIMIT);

    /** Central in-flight pool drawn on after a node exhausts its local allowance. {@code -1} means unset. */
    public static final Setting<Integer> SHARED_LIMIT = Setting.intSetting("shared_limit", UNSET_LIMIT, UNSET_LIMIT);

    /** Allowed attribute values; {@code username} / {@code role} map to the security {@code principal.*} attributes at enforcement. */
    public static final Set<String> ALLOWED_ATTRIBUTES = Set.of("group", "username", "role");

    private static final Map<String, Setting<?>> REGISTERED_SETTINGS = Map.of(
        ATTRIBUTE.getKey(),
        ATTRIBUTE,
        NODE_LIMIT.getKey(),
        NODE_LIMIT,
        SHARED_LIMIT.getKey(),
        SHARED_LIMIT
    );

    private WorkloadGroupThrottleSettings() {
        throw new UnsupportedOperationException("Utility class");
    }

    /**
     * Per-key validation: every key must be registered, {@code attribute} must be an allowed value, and each limit
     * must be a non-negative integer ({@code -1} is the internal "unset" sentinel and is not user-settable). Safe to
     * run on a partial fragment from an update request; the cross-field ceiling check lives in
     * {@link #validateCeiling(Settings)}.
     *
     * @param throttling the throttling settings to validate
     * @throws IllegalArgumentException if any key is unknown or any value is invalid
     */
    public static void validate(Settings throttling) {
        if (throttling == null) {
            return;
        }
        for (String key : throttling.keySet()) {
            String value = throttling.get(key);
            if (REGISTERED_SETTINGS.containsKey(key) == false) {
                throw new IllegalArgumentException("Unknown throttle setting: " + key);
            }
            // null value means "clear this key" — skip value validation
            if (value == null) {
                continue;
            }
            // Limits are stored with -1 as the internal "unset" sentinel, but a user may only send a non-negative integer.
            if (NODE_LIMIT.getKey().equals(key) || SHARED_LIMIT.getKey().equals(key)) {
                validateUserLimit(key, value);
            }
        }
        String attribute = throttling.get(ATTRIBUTE.getKey());
        if (attribute != null && ALLOWED_ATTRIBUTES.contains(attribute) == false) {
            throw new IllegalArgumentException(
                "throttling.attribute must be one of " + ALLOWED_ATTRIBUTES + " but was '" + attribute + "'"
            );
        }
    }

    // Rejects a user-supplied limit that is not a non-negative integer; -1 is reserved as the internal "unset" sentinel.
    private static void validateUserLimit(String key, String value) {
        int parsed;
        try {
            parsed = Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("throttling." + key + " must be a non-negative integer but was '" + value + "'");
        }
        if (parsed < 0) {
            throw new IllegalArgumentException("throttling." + key + " must be non-negative but was " + parsed);
        }
    }

    /**
     * Cross-field validation on a fully-merged throttling config: if throttling is configured, the effective ceiling
     * {@code max(0, node_limit) + max(0, shared_limit)} must be at least 1, since a ceiling of 0 rejects every request.
     * Must be called on the merged result, not a partial update fragment.
     *
     * @param throttling the merged throttling settings
     * @throws IllegalArgumentException if throttling is configured but its effective ceiling is 0
     */
    public static void validateCeiling(Settings throttling) {
        if (throttling == null || throttling.isEmpty()) {
            return;
        }
        int node = NODE_LIMIT.get(throttling);
        int shared = SHARED_LIMIT.get(throttling);
        if (Math.max(0, node) + Math.max(0, shared) < 1) {
            throw new IllegalArgumentException(
                "Effective throttle ceiling is 0 (node_limit="
                    + node
                    + ", shared_limit="
                    + shared
                    + "); this would reject all requests. "
                    + "Set at least one limit to a positive value, or send \"throttling\": null to disable throttling"
            );
        }
    }
}
