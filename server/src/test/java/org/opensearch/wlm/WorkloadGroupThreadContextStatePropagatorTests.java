/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class WorkloadGroupThreadContextStatePropagatorTests extends OpenSearchTestCase {

    public void testTransients() {
        WorkloadGroupThreadContextStatePropagator sut = new WorkloadGroupThreadContextStatePropagator();
        Map<String, Object> source = Map.of("workloadGroupId", "adgarja0r235te");
        Map<String, Object> transients = sut.transients(source);
        assertEquals("adgarja0r235te", transients.get("workloadGroupId"));
    }

    public void testHeaders() {
        WorkloadGroupThreadContextStatePropagator sut = new WorkloadGroupThreadContextStatePropagator();
        Map<String, Object> source = Map.of("workloadGroupId", "adgarja0r235te");
        Map<String, String> headers = sut.headers(source);
        assertEquals("adgarja0r235te", headers.get("workloadGroupId"));
    }

    public void testPrincipalHeaderIsPropagated() {
        WorkloadGroupThreadContextStatePropagator sut = new WorkloadGroupThreadContextStatePropagator();
        Map<String, Object> source = Map.of(
            WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER,
            "adgarja0r235te",
            WorkloadGroupTask.WORKLOAD_GROUP_PRINCIPAL_HEADER,
            "username|alice"
        );
        assertEquals("username|alice", sut.transients(source).get(WorkloadGroupTask.WORKLOAD_GROUP_PRINCIPAL_HEADER));
        assertEquals("username|alice", sut.headers(source).get(WorkloadGroupTask.WORKLOAD_GROUP_PRINCIPAL_HEADER));
    }
}
