/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * The dimensions a workload group throttle rule can be keyed by. Each names one rule in the group's {@code throttling}
 * config ({@code throttling.<key>.node_limit} / {@code throttling.<key>.shared_limit}). Declaration order is the
 * acquisition order across a request's rules: any fixed order keeps multi-rule acquisition deadlock-free, and listing
 * narrower attributes first means a request waiting on its own narrow bucket never holds a permit in a broader one that
 * other callers share. Adding an attribute is one constant here.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public enum ThrottleAttribute {
    /** One bucket per username (the principal's {@code username|...} token). */
    USERNAME("username", "username"),
    /**
     * One bucket per role (the principal's {@code role|...} tokens). When the principal carries several roles, the
     * lexicographically smallest is used, so the same caller consistently lands in one bucket. Consequently a
     * {@code role} rule partitions callers by their alphabetically first role; it does not cap concurrency for a role.
     * Two callers who both hold {@code readall} draw separate allowances whenever their other roles differ, and a caller
     * holding {@code [all_access, readall]} never draws on {@code readall}'s allowance.
     */
    ROLE("role", "role"),
    /** One bucket for the whole workload group. */
    GROUP("group", null);

    private static final String SUBFIELD_SEPARATOR = "|";

    private final String key;
    private final String principalSubfield;

    ThrottleAttribute(String key, @Nullable String principalSubfield) {
        this.key = key;
        this.principalSubfield = principalSubfield;
    }

    /** The attribute's key under {@code throttling} and its segment in the bucket key. */
    public String key() {
        return key;
    }

    /**
     * Whether the attribute partitions the group into per-value buckets ({@code <groupId>:<key>:<value>}). An unkeyed
     * attribute has exactly one bucket, {@code <groupId>:<key>}.
     */
    public boolean keyed() {
        return principalSubfield != null;
    }

    /**
     * The request's value for this keyed attribute: the lexicographically smallest non-empty {@code <subfield>|<value>}
     * token in the raw {@link WorkloadGroupTask#WORKLOAD_GROUP_PRINCIPAL_HEADER} value. {@code null} means the request
     * can't be attributed to a bucket for this rule, so the rule is skipped for it (fail open for that rule only).
     *
     * @param principal the raw principal header value, or {@code null}
     */
    @Nullable
    public String resolve(@Nullable String principal) {
        if (principalSubfield == null || principal == null || principal.isEmpty()) {
            return null;
        }
        final String prefix = principalSubfield + SUBFIELD_SEPARATOR;
        String smallest = null;
        for (String token : principal.split(WorkloadGroupTask.WORKLOAD_GROUP_PRINCIPAL_VALUE_DELIMITER)) {
            String trimmed = token.trim();
            if (trimmed.startsWith(prefix) && trimmed.length() > prefix.length()) {
                String value = trimmed.substring(prefix.length());
                if (smallest == null || value.compareTo(smallest) < 0) {
                    smallest = value;
                }
            }
        }
        return smallest;
    }

    @Override
    public String toString() {
        return key;
    }
}
