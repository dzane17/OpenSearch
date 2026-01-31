/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.common.unit.TimeValue;

import java.util.Map;
import java.util.function.Function;

/**
 * Registry of valid workload group search settings with their validators
 */
public class WorkloadGroupSearchSettings {

    public enum Setting {
        CANCEL_AFTER_TIME_INTERVAL("cancel_after_time_interval", WorkloadGroupSearchSettings::validateTimeValue),
        MAX_CONCURRENT_SHARD_REQUESTS("max_concurrent_shard_requests", WorkloadGroupSearchSettings::validatePositiveInt),
        PHASE_TOOK("phase_took", WorkloadGroupSearchSettings::validateBoolean),
        TIMEOUT("timeout", WorkloadGroupSearchSettings::validateTimeValue);

        private final String key;
        private final Function<String, String> validator;

        Setting(String key, Function<String, String> validator) {
            this.key = key;
            this.validator = validator;
        }

        public String getKey() {
            return key;
        }

        public void validate(String value) {
            String error = validator.apply(value);
            if (error != null) {
                throw new IllegalArgumentException("Invalid value for " + key + ": " + error);
            }
        }

        public static Setting fromKey(String key) {
            for (Setting setting : values()) {
                if (setting.key.equals(key)) {
                    return setting;
                }
            }
            return null;
        }
    }

    public static void validateSearchSettings(Map<String, String> searchSettings) {
        for (Map.Entry<String, String> entry : searchSettings.entrySet()) {
            Setting setting = Setting.fromKey(entry.getKey());
            if (setting == null) {
                throw new IllegalArgumentException("Unknown search setting: " + entry.getKey());
            }
            setting.validate(entry.getValue());
        }
    }

    private static String validateTimeValue(String value) {
        try {
            TimeValue.parseTimeValue(value, "validation");
            return null;
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    private static String validatePositiveInt(String value) {
        try {
            int intValue = Integer.parseInt(value);
            if (intValue <= 0) {
                return "must be positive";
            }
            return null;
        } catch (NumberFormatException e) {
            return "must be a valid integer";
        }
    }

    private static String validateBoolean(String value) {
        if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
            return "must be true or false";
        }
        return null;
    }
}
