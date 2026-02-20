/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

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
        /** Setting for including phase timing information */
        PHASE_TOOK("phase_took", WorkloadGroupSearchSettings::validateBoolean);

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
}
