/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

public class WorkloadGroupQueueSettingsTests extends OpenSearchTestCase {

    public void testDefaults() {
        assertEquals(0, WorkloadGroupQueueSettings.SIZE_PER_BUCKET.get(Settings.EMPTY).intValue());
    }

    public void testValidAcceptsSizePerBucket() {
        Settings queue = Settings.builder().put("size_per_bucket", 200).build();
        WorkloadGroupQueueSettings.validate(queue); // no throw
        assertEquals(200, WorkloadGroupQueueSettings.SIZE_PER_BUCKET.get(queue).intValue());
    }

    public void testValidRejectsTimeoutKey() {
        // queue.timeout is no longer a setting: it must be rejected as an unknown key so a stale config surfaces clearly.
        Settings queue = Settings.builder().put("timeout", "30s").build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> WorkloadGroupQueueSettings.validate(queue));
        assertTrue(e.getMessage(), e.getMessage().contains("Unknown queue setting"));
    }

    public void testValidRejectsOldSizeKey() {
        // queue.size was renamed to queue.size_per_bucket (the cap is per throttle bucket, not per group). The old key
        // must be rejected as unknown rather than silently ignored, so a stale config is not read as "queueing enabled".
        Settings queue = Settings.builder().put("size", 100).build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> WorkloadGroupQueueSettings.validate(queue));
        assertTrue(e.getMessage(), e.getMessage().contains("Unknown queue setting"));
    }

    public void testValidNullIsNoOp() {
        WorkloadGroupQueueSettings.validate(null);
    }

    public void testValidRejectsUnknownKey() {
        Settings queue = Settings.builder().put("bogus", 1).build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> WorkloadGroupQueueSettings.validate(queue));
        assertTrue(e.getMessage(), e.getMessage().contains("Unknown queue setting"));
    }

    public void testValidRejectsNegativeSizePerBucket() {
        Settings queue = Settings.builder().put("size_per_bucket", -5).build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> WorkloadGroupQueueSettings.validate(queue));
        assertTrue(e.getMessage(), e.getMessage().contains("non-negative"));
    }

    public void testValidRejectsNonIntegerSizePerBucket() {
        Settings queue = Settings.builder().put("size_per_bucket", "abc").build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> WorkloadGroupQueueSettings.validate(queue));
        assertTrue(e.getMessage(), e.getMessage().contains("must be an integer"));
    }

    public void testValidRejectsSizePerBucketAboveMax() {
        Settings queue = Settings.builder().put("size_per_bucket", WorkloadGroupQueueSettings.MAX_SIZE_PER_BUCKET + 1).build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> WorkloadGroupQueueSettings.validate(queue));
        assertTrue(e.getMessage(), e.getMessage().contains("must not exceed"));
    }

    public void testValidAcceptsSizePerBucketAtMax() {
        // The boundary itself is legal: a single-bucket group (attribute=group) may queue the entire group budget.
        Settings queue = Settings.builder().put("size_per_bucket", WorkloadGroupQueueSettings.MAX_SIZE_PER_BUCKET).build();
        WorkloadGroupQueueSettings.validate(queue); // no throw
    }

    public void testMaxSizePerBucketIsPinnedToGroupCeiling() {
        // Invariant: the configurable per-bucket cap can never exceed the fixed per-group ceiling, otherwise validation
        // would accept a depth the group total could never honour.
        assertEquals(WorkloadGroupQueueSettings.MAX_GROUP_QUEUE_DEPTH, WorkloadGroupQueueSettings.MAX_SIZE_PER_BUCKET);
    }

}
