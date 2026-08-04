/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;

public class WorkloadGroupQueueSettingsTests extends OpenSearchTestCase {

    public void testDefaults() {
        assertEquals(0, WorkloadGroupQueueSettings.SIZE.get(Settings.EMPTY).intValue());
        assertEquals(TimeValue.timeValueSeconds(30), WorkloadGroupQueueSettings.TIMEOUT.get(Settings.EMPTY));
    }

    public void testValidAcceptsSizeAndTimeout() {
        Settings queue = Settings.builder().put("size", 200).put("timeout", "45s").build();
        WorkloadGroupQueueSettings.validate(queue); // no throw
        assertEquals(200, WorkloadGroupQueueSettings.SIZE.get(queue).intValue());
        assertEquals(TimeValue.timeValueSeconds(45), WorkloadGroupQueueSettings.TIMEOUT.get(queue));
    }

    public void testValidNullIsNoOp() {
        WorkloadGroupQueueSettings.validate(null);
    }

    public void testValidRejectsUnknownKey() {
        Settings queue = Settings.builder().put("bogus", 1).build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> WorkloadGroupQueueSettings.validate(queue));
        assertTrue(e.getMessage(), e.getMessage().contains("Unknown queue setting"));
    }

    public void testValidRejectsNegativeSize() {
        Settings queue = Settings.builder().put("size", -5).build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> WorkloadGroupQueueSettings.validate(queue));
        assertTrue(e.getMessage(), e.getMessage().contains("non-negative"));
    }

    public void testValidRejectsNonIntegerSize() {
        Settings queue = Settings.builder().put("size", "abc").build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> WorkloadGroupQueueSettings.validate(queue));
        assertTrue(e.getMessage(), e.getMessage().contains("must be an integer"));
    }

    public void testValidRejectsSizeAboveMax() {
        Settings queue = Settings.builder().put("size", WorkloadGroupQueueSettings.MAX_SIZE + 1).build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> WorkloadGroupQueueSettings.validate(queue));
        assertTrue(e.getMessage(), e.getMessage().contains("must not exceed"));
    }

    public void testValidRejectsNegativeTimeout() {
        Settings queue = Settings.builder().put("timeout", "-1s").build();
        expectThrows(IllegalArgumentException.class, () -> WorkloadGroupQueueSettings.validate(queue));
    }

    public void testMergedConfigNullOrEmptyIsNoOp() {
        WorkloadGroupQueueSettings.validateMergedConfig(null);
        WorkloadGroupQueueSettings.validateMergedConfig(Settings.EMPTY);
    }

    public void testMergedConfigTimeoutRequiresSize() {
        Settings queue = Settings.builder().put("timeout", "30s").build(); // size defaults to 0
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> WorkloadGroupQueueSettings.validateMergedConfig(queue)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("queue.timeout requires queue.size > 0"));
    }

    public void testMergedConfigSizeWithTimeoutOk() {
        Settings queue = Settings.builder().put("size", 100).put("timeout", "30s").build();
        WorkloadGroupQueueSettings.validateMergedConfig(queue); // no throw
    }

    public void testMergedConfigSizeOnlyOk() {
        Settings queue = Settings.builder().put("size", 100).build();
        WorkloadGroupQueueSettings.validateMergedConfig(queue); // no throw
    }
}
