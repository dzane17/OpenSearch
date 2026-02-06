/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

public class WorkloadGroupSearchSettingsTests extends OpenSearchTestCase {

    public void testEnumSettingNames() {
        assertEquals("phase_took", WorkloadGroupSearchSettings.WlmSearchSetting.PHASE_TOOK.getSettingName());
    }

    public void testFromKeyValidSettings() {
        assertEquals(
            WorkloadGroupSearchSettings.WlmSearchSetting.PHASE_TOOK,
            WorkloadGroupSearchSettings.WlmSearchSetting.fromKey("phase_took")
        );
    }

    public void testFromKeyInvalidSetting() {
        assertNull(WorkloadGroupSearchSettings.WlmSearchSetting.fromKey("invalid_setting"));
        assertNull(WorkloadGroupSearchSettings.WlmSearchSetting.fromKey(""));
        assertNull(WorkloadGroupSearchSettings.WlmSearchSetting.fromKey(null));
    }

    public void testValidateBoolean() {
        WorkloadGroupSearchSettings.WlmSearchSetting.PHASE_TOOK.validate("true");
        WorkloadGroupSearchSettings.WlmSearchSetting.PHASE_TOOK.validate("false");
        WorkloadGroupSearchSettings.WlmSearchSetting.PHASE_TOOK.validate("TRUE");
        WorkloadGroupSearchSettings.WlmSearchSetting.PHASE_TOOK.validate("FALSE");
    }

    public void testValidateInvalidBoolean() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> WorkloadGroupSearchSettings.WlmSearchSetting.PHASE_TOOK.validate("yes")
        );
        assertTrue(exception.getMessage().contains("must be true or false"));
    }

    public void testValidateSearchSettingsValid() {
        Map<String, String> settings = new HashMap<>();
        settings.put("phase_took", "true");

        // Should not throw exception
        WorkloadGroupSearchSettings.validateSearchSettings(settings);
    }

    public void testValidateSearchSettingsUnknownSetting() {
        Map<String, String> settings = new HashMap<>();
        settings.put("unknown_setting", "true");

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> WorkloadGroupSearchSettings.validateSearchSettings(settings)
        );
        assertTrue(exception.getMessage().contains("Unknown search setting: unknown_setting"));
    }

    public void testValidateSearchSettingsEmpty() {
        Map<String, String> settings = new HashMap<>();

        // Should not throw exception for empty map
        WorkloadGroupSearchSettings.validateSearchSettings(settings);
    }
}
