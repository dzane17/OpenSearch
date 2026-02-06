/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.unit.TimeValue;

import java.util.Map;
import java.util.function.Function;

/**
 * Registry of valid workload group search settings with their validators
 */
public class WorkloadGroupSearchSettings {

    /**
     * Private constructor to prevent instantiation of this utility class.
     */
    private WorkloadGroupSearchSettings() {
        throw new UnsupportedOperationException("Utility class");
    }

    /**
     * Enum defining valid workload group search settings with their validation logic.
     * Settings are categorized as either query parameters or cluster settings.
     */
    public enum WlmSearchSetting {
        // Query parameters (applied to SearchRequest)
        /** Setting for batched reduce size */
        BATCHED_REDUCE_SIZE("batched_reduce_size", WorkloadGroupSearchSettings::validateBatchedReduceSize),
        /** Setting for canceling search requests after a time interval */
        CANCEL_AFTER_TIME_INTERVAL("cancel_after_time_interval", WorkloadGroupSearchSettings::validateTimeValue),
        /** Setting for maximum concurrent shard requests */
        MAX_CONCURRENT_SHARD_REQUESTS("max_concurrent_shard_requests", WorkloadGroupSearchSettings::validatePositiveInt),
        /** Setting for including phase timing information */
        PHASE_TOOK("phase_took", WorkloadGroupSearchSettings::validateBoolean),
        /** Setting for search request timeout */
        TIMEOUT("timeout", WorkloadGroupSearchSettings::validateTimeValue);

        private final String settingName;
        private final Function<String, String> validator;

        WlmSearchSetting(String settingName, Function<String, String> validator) {
            this.settingName = settingName;
            this.validator = validator;
        }

        /**
         * Returns the setting name.
         * @return the setting name
         */
        public String getSettingName() {
            return settingName;
        }

        /**
         * Validates the given value for this setting.
         * @param value the value to validate
         * @throws IllegalArgumentException if the value is invalid
         */
        void validate(String value) {
            String error = validator.apply(value);
            if (error != null) {
                throw new IllegalArgumentException("Invalid value for " + settingName + ": " + error);
            }
        }

        /**
         * Finds a setting by its name.
         * @param settingName the setting name
         * @return the setting or null if not found
         */
        public static WlmSearchSetting fromKey(String settingName) {
            for (WlmSearchSetting setting : values()) {
                if (setting.settingName.equals(settingName)) {
                    return setting;
                }
            }
            return null;
        }
    }

    /**
     * Validates all search settings in the provided map.
     * @param searchSettings map of setting names to values
     * @throws IllegalArgumentException if any setting is unknown or invalid
     */
    public static void validateSearchSettings(Map<String, String> searchSettings) {
        if (searchSettings == null) {
            return;
        }
        for (Map.Entry<String, String> entry : searchSettings.entrySet()) {
            WlmSearchSetting setting = WlmSearchSetting.fromKey(entry.getKey());
            if (setting == null) {
                throw new IllegalArgumentException("Unknown search setting: " + entry.getKey());
            }
            setting.validate(entry.getValue());
        }
    }

    /**
     * Validates a time value string.
     * @param value the string to validate
     * @return null if valid, error message if invalid
     */
    private static String validateTimeValue(String value) {
        try {
            TimeValue.parseTimeValue(value, "validation");
            return null;
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    /**
     * Validates a positive integer string.
     * @param value the string to validate
     * @return null if valid, error message if invalid
     */
    private static String validatePositiveInt(String value) {
        try {
            int intValue = Integer.parseInt(value);
            SearchRequest.validatePositiveInteger(intValue, "value");
            return null;
        } catch (NumberFormatException e) {
            return "must be a valid integer";
        } catch (IllegalArgumentException e) {
            return "must be positive";
        }
    }

    /**
     * Validates a boolean string.
     * @param value the string to validate
     * @return null if valid, error message if invalid
     */
    private static String validateBoolean(String value) {
        if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
            return "must be true or false";
        }
        return null;
    }

    /**
     * Validates batched reduce size (must be >= 2).
     * @param value the string to validate
     * @return null if valid, error message if invalid
     */
    private static String validateBatchedReduceSize(String value) {
        try {
            int intValue = Integer.parseInt(value);
            if (intValue < 2) {
                return "must be >= 2";
            }
            return null;
        } catch (NumberFormatException e) {
            return "must be a valid integer";
        }
    }
}
