/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Registry of valid workload group throttle settings with their validators. Throttle config is a nested
 * {@code throttling} object (a {@link Settings} bag) keyed by attribute, one independent rule per attribute:
 * <pre>
 * "throttling": {
 *   "username": { "node_limit": 5, "shared_limit": 10 },
 *   "role":     { "shared_limit": 50 }
 * }
 * </pre>
 * stored flat as {@code username.node_limit}, {@code role.shared_limit}, ... A request needs a permit from every rule
 * that applies to it. A caller with several roles is charged to its lexicographically smallest role only, so a
 * {@code role} rule partitions callers by their first role rather than capping each role (see
 * {@link ThrottleAttribute#ROLE}). Per-key null clears a field, a null attribute clears its whole rule, and an
 * absent key keeps the existing value.
 */
@ExperimentalApi
public class WorkloadGroupThrottleSettings {

    private static final Logger logger = LogManager.getLogger(WorkloadGroupThrottleSettings.class);

    /** Sentinel for an unset limit, matching the {@code -1 = not set} convention of {@code WLM_SEARCH_TIMEOUT}. */
    public static final int UNSET_LIMIT = -1;

    /** Upper bound for a limit. Limits are stored in an int-backed setting, so a larger value would overflow the {@code int}. */
    public static final int MAX_LIMIT = Integer.MAX_VALUE;

    /** Per-node in-flight allowance for one rule bucket, admitted locally with no coordination. {@code -1} means unset. */
    public static final Setting<Integer> NODE_LIMIT = Setting.intSetting("node_limit", UNSET_LIMIT, UNSET_LIMIT);

    /** Central in-flight pool for one rule bucket, drawn on after a node exhausts its local allowance. {@code -1} means unset. */
    public static final Setting<Integer> SHARED_LIMIT = Setting.intSetting("shared_limit", UNSET_LIMIT, UNSET_LIMIT);

    // Keys of the old flat form: rejected in user input, translated on read (see normalizeStored).
    private static final String LEGACY_ATTRIBUTE = "attribute";
    private static final Set<String> LEGACY_KEYS = Set.of(LEGACY_ATTRIBUTE, NODE_LIMIT.getKey(), SHARED_LIMIT.getKey());

    /**
     * Throttle attributes a rule can be keyed by. {@code username} / {@code role} map to the security {@code principal.*}
     * subfields at enforcement. This is the only list validation and enforcement consult; register new attributes here.
     */
    public static final Map<String, ThrottleAttribute> ATTRIBUTES = Arrays.stream(ThrottleAttribute.values())
        .collect(Collectors.toUnmodifiableMap(ThrottleAttribute::key, attribute -> attribute));

    private static final Set<String> RULE_FIELDS = Set.of(NODE_LIMIT.getKey(), SHARED_LIMIT.getKey());

    /**
     * Node attribute ({@code node.attr.wlm_multi_attribute_throttling: true}) set automatically by every build that
     * supports throttling rules for several attributes, i.e. reads the throttling format keyed by attribute; see
     * {@code Node}. Never set by users. Rolling a node back in place to such a build is only safe while every group has
     * at most one rule: a multi-rule group is persisted in the keyed form, which that build cannot load from disk. Earlier builds of this feature know only the flat {@code {attribute, node_limit,
     * shared_limit}} form, cannot deserialize a keyed config, and report the same {@link org.opensearch.Version}, so the
     * attribute is the only way to tell them apart. Until every node has it: single-rule groups go to the wire and to disk
     * in the flat form (see {@link #toLegacyFlat}), multi-rule groups are rejected (see {@link #validateSupportedByCluster}),
     * and {@code role} resolves to the caller's first role as on those builds (see {@link ThrottleAttribute#resolve}).
     */
    public static final String MULTI_ATTRIBUTE_THROTTLING_NODE_ATTRIBUTE = "wlm_multi_attribute_throttling";

    // Bad stored rules already warned about (see rule); bounded so it can't grow without limit.
    private static final Set<String> UNREADABLE_RULES_WARNED = ConcurrentHashMap.newKeySet();

    private WorkloadGroupThrottleSettings() {
        throw new UnsupportedOperationException("Utility class");
    }

    /**
     * One configured throttle rule: an attribute and its two tiers.
     */
    @ExperimentalApi
    public static final class Rule {
        private final ThrottleAttribute attribute;
        private final int nodeLimit;
        private final int sharedLimit;

        Rule(ThrottleAttribute attribute, int nodeLimit, int sharedLimit) {
            this.attribute = attribute;
            this.nodeLimit = nodeLimit;
            this.sharedLimit = sharedLimit;
        }

        public ThrottleAttribute attribute() {
            return attribute;
        }

        public int nodeLimit() {
            return nodeLimit;
        }

        public int sharedLimit() {
            return sharedLimit;
        }

        public boolean hasNodeTier() {
            return nodeLimit != UNSET_LIMIT;
        }

        public boolean hasSharedTier() {
            return sharedLimit != UNSET_LIMIT;
        }

    }

    /**
     * The configured rules of a throttling config, in acquisition order (rank, then name). Rules for attributes this
     * node doesn't know and rules with neither limit set are omitted.
     *
     * @param throttling the stored throttling settings (may be null)
     */
    public static List<Rule> rules(@Nullable Settings throttling) {
        if (throttling == null || throttling.isEmpty()) {
            return List.of();
        }
        List<Rule> rules = new ArrayList<>();
        for (String name : attributeNames(throttling)) {
            Rule rule = rule(throttling, name);
            if (rule != null) {
                rules.add(rule);
            }
        }
        rules.sort(Comparator.comparingInt(r -> r.attribute().ordinal()));
        return rules;
    }

    /**
     * Which rules a config has and which tiers each configures, e.g. {@code {username=[true, true], group=[true, false]}}
     * (node, shared); limit values are deliberately excluded. Two configs with the same shape admit requests through the
     * same set of buckets and tiers, so a numeric change keeps queued requests while a shape change re-runs them.
     */
    public static Map<String, List<Boolean>> tierShape(List<Rule> rules) {
        Map<String, List<Boolean>> shape = new LinkedHashMap<>();
        for (Rule rule : rules) {
            shape.put(rule.attribute().key(), List.of(rule.hasNodeTier(), rule.hasSharedTier()));
        }
        return shape;
    }

    /** Whether {@code node} supports multi-attribute throttling (see {@link #MULTI_ATTRIBUTE_THROTTLING_NODE_ATTRIBUTE}). */
    public static boolean supportsMultiAttributeThrottling(DiscoveryNode node) {
        return Boolean.parseBoolean(node.getAttributes().get(MULTI_ATTRIBUTE_THROTTLING_NODE_ATTRIBUTE));
    }

    /** Whether every node in {@code nodes} supports multi-attribute throttling. */
    public static boolean allSupportMultiAttributeThrottling(DiscoveryNodes nodes) {
        for (DiscoveryNode node : nodes) {
            if (supportsMultiAttributeThrottling(node) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether a config needs every node to support multi-attribute throttling: it is non-empty and has no flat
     * single-rule form ({@link #toLegacyFlat}) that nodes on an older build could read, e.g. it has several rules. The one test the write gate, the
     * join check and the stored form all use, so they cannot disagree.
     */
    public static boolean requiresMultiAttributeSupport(@Nullable Settings throttling) {
        return throttling != null && throttling.isEmpty() == false && toLegacyFlat(throttling) == null;
    }

    /**
     * Rejects a config that nodes in {@code nodes} could not read (see {@link #requiresMultiAttributeSupport}), such as one with
     * several rules, while any node runs a build that only knows the flat single-rule form. A single-rule config is
     * always accepted, since it is sent to those nodes in the flat form.
     *
     * @throws IllegalArgumentException if the config needs the keyed form and some node does not support it
     */
    public static void validateSupportedByCluster(@Nullable Settings throttling, DiscoveryNodes nodes) {
        if (requiresMultiAttributeSupport(throttling) == false) {
            return;
        }
        List<String> unsupported = new ArrayList<>();
        for (DiscoveryNode node : nodes) {
            if (supportsMultiAttributeThrottling(node) == false) {
                unsupported.add(node.getName());
            }
        }
        if (unsupported.isEmpty() == false) {
            Collections.sort(unsupported);
            throw new IllegalArgumentException(
                "this throttling config (for example one with more than one rule) needs every node to run a build that supports it; "
                    + unsupported.size()
                    + " node(s) do not: "
                    + (unsupported.size() > 10 ? unsupported.subList(0, 10) + " ..." : unsupported)
                    + ". Finish upgrading those nodes, or use a single rule until then"
            );
        }
    }

    /** Whether the elected cluster manager in {@code nodes} runs a build that predates multi-attribute throttling. */
    public static boolean clusterManagerPredatesMultiAttributeThrottling(DiscoveryNodes nodes) {
        final DiscoveryNode clusterManager = nodes.getClusterManagerNode();
        return clusterManager != null && supportsMultiAttributeThrottling(clusterManager) == false;
    }

    /**
     * Coordinator-side translation of a keyed throttling update for a cluster manager on a build that predates
     * multi-attribute throttling, which only applies the flat form (and silently ignores a keyed change that removes or
     * clears something). While such a node is present every group has at most one rule, so the update's effect on the
     * group's current config ({@code existing}, from this node's cluster state) can be expressed as flat keys that build
     * overwrites or clears: the remaining rule's attribute plus its changed fields, or every flat key cleared when the
     * update leaves no rule. {@code "throttling": null} (empty) is sent as is; that build also reads it as "clear all".
     * A race with a concurrent update is caught afterwards by {@link #verifyUpdateApplied}.
     *
     * @return the flat-form update to send, or {@code null} if it leaves throttling unchanged
     * @throws IllegalArgumentException if the update is invalid or would leave the group with more than one rule
     */
    @Nullable
    public static Settings toFlatUpdate(@Nullable Settings update, @Nullable Settings existing) {
        if (update == null || update.isEmpty()) {
            return update;
        }
        validate(update);
        final Settings current = existing == null ? Settings.EMPTY : existing;
        final Settings merged = merge(current, update);
        if (merged.equals(current)) {
            return null; // e.g. removing a rule the group doesn't have
        }
        final Settings flatResult = toLegacyFlat(merged);
        if (merged.isEmpty() == false && flatResult == null) {
            throw new IllegalArgumentException(
                "this throttling update would give the workload group more than one rule, which needs every node to run a "
                    + "build that supports it; finish the upgrade first"
            );
        }
        final Settings.Builder flat = Settings.builder();
        if (flatResult == null) {
            flat.putNull(LEGACY_ATTRIBUTE); // no rule left: clear every flat key
            for (String field : RULE_FIELDS) {
                flat.putNull(field);
            }
            return flat.build();
        }
        // One rule left: its attribute, and each field as it should end up. Fields are written explicitly (set, or cleared
        // when absent) so the result doesn't depend on what the older build merges them onto.
        flat.put(LEGACY_ATTRIBUTE, flatResult.get(LEGACY_ATTRIBUTE));
        for (String field : RULE_FIELDS) {
            if (flatResult.get(field) != null) {
                flat.put(field, flatResult.get(field));
            } else {
                flat.putNull(field);
            }
        }
        return flat.build();
    }

    /**
     * Join validator: refuses a node on a build that predates keyed rules while any workload group has several rules,
     * since it could not deserialize that group and would fail to apply the cluster state.
     *
     * @throws IllegalStateException if {@code joiningNode} can't read the current workload groups
     */
    public static void validateJoiningNode(DiscoveryNode joiningNode, ClusterState state) {
        if (supportsMultiAttributeThrottling(joiningNode) || state.metadata() == null) {
            return;
        }
        for (WorkloadGroup group : state.metadata().workloadGroups().values()) {
            if (requiresMultiAttributeSupport(group.getMutableWorkloadGroupFragment().getThrottling())) {
                throw new IllegalStateException(
                    "node ["
                        + joiningNode
                        + "] runs a build that cannot read the throttling config of workload group ["
                        + group.getName()
                        + "] (for example several rules); upgrade the node, or reduce every workload group to a single "
                        + "throttle rule first"
                );
            }
        }
    }

    /**
     * The flat form {@code {attribute, node_limit, shared_limit}} of {@code throttling} for nodes on a build that predates
     * keyed rules, or {@code null} if it has no such form: it is not exactly one known rule with set limits and nothing
     * else. {@link #normalizeStored} translates the flat form back, so writing this and reading it is a round trip.
     */
    @Nullable
    public static Settings toLegacyFlat(@Nullable Settings throttling) {
        if (throttling == null || throttling.isEmpty() || throttling.names().size() != 1) {
            return null;
        }
        final String attribute = throttling.names().iterator().next();
        if (ATTRIBUTES.containsKey(attribute) == false) {
            return null;
        }
        Settings.Builder flat = Settings.builder().put(LEGACY_ATTRIBUTE, attribute);
        for (String key : throttling.keySet()) {
            final String value = throttling.get(key);
            final String field = key.startsWith(attribute + ".") ? key.substring(attribute.length() + 1) : null;
            if (field == null || RULE_FIELDS.contains(field) == false || value == null) {
                return null;
            }
            flat.put(field, value);
        }
        return flat.build();
    }

    /**
     * Attributes whose rule was added, removed, or had its tier set changed between two configs (see {@link #tierShape});
     * a rule whose only change is a limit value is not included.
     */
    public static Set<String> rulesWithChangedTiers(List<Rule> previous, List<Rule> current) {
        Map<String, List<Boolean>> before = tierShape(previous);
        Map<String, List<Boolean>> after = tierShape(current);
        Set<String> changed = new HashSet<>();
        for (String attribute : before.keySet()) {
            if (before.get(attribute).equals(after.get(attribute)) == false) {
                changed.add(attribute);
            }
        }
        for (String attribute : after.keySet()) {
            if (before.containsKey(attribute) == false) {
                changed.add(attribute);
            }
        }
        return changed;
    }

    /**
     * The rule for {@code attributeName}, or {@code null} if the attribute is unknown or has neither limit set.
     *
     * @param throttling    the stored throttling settings (may be null)
     * @param attributeName the attribute name
     */
    @Nullable
    public static Rule rule(@Nullable Settings throttling, String attributeName) {
        ThrottleAttribute attribute = ATTRIBUTES.get(attributeName);
        if (throttling == null || attribute == null) {
            return null;
        }
        Settings ruleSettings = throttling.getByPrefix(attributeName + ".");
        final int nodeLimit;
        final int sharedLimit;
        try {
            nodeLimit = NODE_LIMIT.get(ruleSettings);
            sharedLimit = SHARED_LIMIT.get(ruleSettings);
        } catch (IllegalArgumentException e) {
            // Only stored config that bypassed validation gets here; make the rule inert rather than throw on every use.
            // WARN once per distinct bad rule: the rule stops being enforced. Fragments (and their cached rules) are
            // rebuilt whenever a group is deserialized, so an unguarded warning would repeat on every cluster-state apply.
            final String warnKey = attributeName + "=" + ruleSettings;
            if (UNREADABLE_RULES_WARNED.size() < 1000 && UNREADABLE_RULES_WARNED.add(warnKey)) {
                logger.warn(
                    "Ignoring throttling." + attributeName + " with an unreadable limit, so it is not enforced: " + ruleSettings,
                    e
                );
            } else {
                logger.debug("Ignoring throttling." + attributeName + " with an unreadable limit: " + ruleSettings, e);
            }
            return null;
        }
        if (nodeLimit == UNSET_LIMIT && sharedLimit == UNSET_LIMIT) {
            return null;
        }
        return new Rule(attribute, nodeLimit, sharedLimit);
    }

    /**
     * Assembles the opaque bucket key used by both tracker tiers: {@code <groupId>:<attribute>} for an unkeyed attribute
     * (e.g. {@code group}) or {@code <groupId>:<attribute>:<value>}.
     */
    public static String bucketKey(String workloadGroupId, ThrottleAttribute attribute, @Nullable String value) {
        if (attribute.keyed() == false) {
            return workloadGroupId + ":" + attribute.key();
        }
        return workloadGroupId + ":" + attribute.key() + ":" + value;
    }

    /** The attribute segment of a bucket key (values may contain {@code ':'}; group ids and attribute names don't). */
    public static String attributeOf(String bucketKey) {
        int start = bucketKey.indexOf(':');
        if (start < 0) {
            return "";
        }
        int end = bucketKey.indexOf(':', start + 1);
        return end < 0 ? bucketKey.substring(start + 1) : bucketKey.substring(start + 1, end);
    }

    /**
     * The rule {@code bucketKey} belongs to in {@code workloadGroup}'s live config, or {@code null} if the group is gone
     * or no longer has that rule. The one place a bucket is mapped to its rule.
     */
    @Nullable
    public static Rule ruleForBucket(@Nullable WorkloadGroup workloadGroup, String bucketKey) {
        return workloadGroup == null ? null : workloadGroup.getMutableWorkloadGroupFragment().getThrottleRule(attributeOf(bucketKey));
    }

    /** {@code <groupId>:<attribute>} — identifies the rule a bucket belongs to, across all its values. */
    public static String ruleKeyOf(String bucketKey) {
        int start = bucketKey.indexOf(':');
        if (start < 0) {
            return bucketKey;
        }
        int end = bucketKey.indexOf(':', start + 1);
        return end < 0 ? bucketKey : bucketKey.substring(0, end);
    }

    /** {@code <groupId>:<attribute>} for a rule, matching {@link #ruleKeyOf} of its buckets. */
    public static String ruleKey(String workloadGroupId, String attributeName) {
        return workloadGroupId + ":" + attributeName;
    }

    /**
     * Strict parser for user input: {@code {"<attribute>": {"node_limit": n, "shared_limit": n} | null, ...}}. Unlike the
     * generic {@link Settings#fromXContent}, an empty object is rejected rather than silently flattened to nothing (which
     * an update would read as "clear every rule"), and dotted keys are rejected so a rule can only be spelled one way.
     * The result is then checked with {@link #validate}.
     *
     * @param parser positioned on the {@code throttling} object's START_OBJECT
     */
    public static Settings fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("throttling must be an object keyed by attribute, or null");
        }
        Settings.Builder builder = Settings.builder();
        boolean empty = true;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            empty = false;
            String attribute = parser.currentName();
            token = parser.nextToken();
            requireUndotted(attribute, "throttling." + attribute);
            if (token == XContentParser.Token.VALUE_NULL) {
                builder.putNull(attribute);
            } else if (token == XContentParser.Token.START_OBJECT) {
                parseRule(parser, attribute, builder);
            } else if (token.isValue()) {
                // Not a rule object: a legacy flat key or a scalar under an attribute. validate() names which.
                builder.put(attribute, parser.text());
            } else {
                throw new IllegalArgumentException("throttling." + attribute + " must be an object with node_limit and/or shared_limit");
            }
        }
        if (empty) {
            throw new IllegalArgumentException("throttling must not be empty; set it to null to clear every rule");
        }
        Settings throttling = builder.build();
        validate(throttling);
        return throttling;
    }

    private static void parseRule(XContentParser parser, String attribute, Settings.Builder builder) throws IOException {
        boolean empty = true;
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            empty = false;
            String field = parser.currentName();
            String key = attribute + "." + field;
            requireUndotted(field, "throttling." + key);
            XContentParser.Token token = parser.nextToken();
            if (token == XContentParser.Token.VALUE_NULL) {
                builder.putNull(key);
            } else if (token == XContentParser.Token.VALUE_NUMBER || token == XContentParser.Token.VALUE_STRING) {
                builder.put(key, parser.text());
            } else {
                // Still reject unknown fields by name first, so a typo is reported as such.
                if (RULE_FIELDS.contains(field) == false) {
                    throw new IllegalArgumentException("Unknown throttle setting: throttling." + key);
                }
                throw new IllegalArgumentException("throttling." + key + " must be an integer");
            }
        }
        if (empty) {
            throw new IllegalArgumentException(
                "throttling." + attribute + " must not be empty; set it to null to remove the rule, or set node_limit and/or shared_limit"
            );
        }
    }

    private static void requireUndotted(String name, String path) {
        if (name.indexOf('.') >= 0) {
            throw new IllegalArgumentException(
                "Invalid throttle setting [" + path + "]: use nested objects, e.g. {\"username\": {\"node_limit\": 5}}"
            );
        }
    }

    /**
     * Read-path normalization of a stored throttling bag (cluster-state stream or persisted metadata). Never throws: a
     * node must always be able to load its own state. The pre-multi-rule flat form {@code {attribute, node_limit,
     * shared_limit}} is translated to the keyed form; every other key (including bare null "remove this rule" markers of
     * an update fragment, and unknown attributes or fields) is kept untouched so it survives a round trip. Enforcement
     * ignores anything that is not a known rule.
     *
     * @param stored the stored throttling settings (may be null)
     * @return the normalized settings; {@code null} for {@code null}, which in an update fragment means "field absent,
     *         keep the existing config" and must not turn into an empty bag ("clear every rule")
     */
    @Nullable
    public static Settings normalizeStored(@Nullable Settings stored) {
        if (stored == null || stored.get(LEGACY_ATTRIBUTE) == null) {
            return stored;
        }
        String legacyAttribute = stored.get(LEGACY_ATTRIBUTE);
        Settings.Builder builder = Settings.builder();
        for (String key : stored.keySet()) {
            if (LEGACY_KEYS.contains(key) == false) {
                builder.put(key, stored.get(key));
            }
        }
        if (ATTRIBUTES.containsKey(legacyAttribute)) {
            for (String field : RULE_FIELDS) {
                if (stored.get(field) != null) {
                    builder.put(legacyAttribute + "." + field, stored.get(field));
                }
            }
        }
        Settings normalized = builder.build();
        logger.debug("Normalized stored workload group throttling {} to {}", stored, normalized);
        return normalized;
    }

    // Attribute names in a throttling bag, including bare null "remove this rule" markers.
    private static Set<String> attributeNames(Settings throttling) {
        return new TreeSet<>(throttling.names()); // sorted, so iteration order is deterministic
    }

    /**
     * Per-key validation: every key must be {@code <attribute>.<field>} with a registered attribute and field, or a bare
     * {@code <attribute>} set to null (clears that rule in an update), and each limit must be a non-negative integer no
     * greater than {@link #MAX_LIMIT} ({@code -1} is the internal "unset" sentinel and is not user-settable). Safe to run
     * on a partial fragment from an update request; the per-rule ceiling check lives in {@link #validateMergedConfig}.
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
            int dot = key.indexOf('.');
            String attributeName = dot < 0 ? key : key.substring(0, dot);
            if (ATTRIBUTES.containsKey(attributeName) == false) {
                if (dot < 0 && LEGACY_KEYS.contains(key)) {
                    throw new IllegalArgumentException(
                        "throttling must be keyed by attribute, e.g. {\"username\": {\"node_limit\": 5, \"shared_limit\": 10}}; "
                            + "found top-level key ["
                            + key
                            + "]"
                    );
                }
                throw new IllegalArgumentException(
                    "throttling attribute ["
                        + attributeName
                        + "] is not supported; supported attributes are "
                        + new TreeSet<>(ATTRIBUTES.keySet())
                );
            }
            if (dot < 0) {
                // A bare attribute is only meaningful as null ("remove this rule"); a scalar has no rule fields.
                if (value != null) {
                    throw new IllegalArgumentException(
                        "throttling." + key + " must be an object with node_limit and/or shared_limit but was '" + value + "'"
                    );
                }
                continue;
            }
            String field = key.substring(dot + 1);
            if (RULE_FIELDS.contains(field) == false) {
                throw new IllegalArgumentException("Unknown throttle setting: throttling." + key);
            }
            // null value means "clear this key" — skip value validation
            if (value != null) {
                validateUserLimit(key, value);
            }
        }
    }

    // Rejects a user-supplied limit that is not a non-negative integer in [0, MAX_LIMIT]; -1 is reserved as the internal
    // "unset" sentinel. Parsed as a BigInteger so a value that is a well-formed integer but too large for the int-backed
    // setting (e.g. Integer.MAX_VALUE + 1) reports an overflow instead of being mislabelled as "not an integer".
    private static void validateUserLimit(String key, String value) {
        final BigInteger parsed;
        try {
            parsed = new BigInteger(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("throttling." + key + " must be an integer but was '" + value + "'");
        }
        if (parsed.signum() < 0) {
            throw new IllegalArgumentException("throttling." + key + " must be non-negative but was " + parsed);
        }
        if (parsed.compareTo(BigInteger.valueOf(MAX_LIMIT)) > 0) {
            throw new IllegalArgumentException("throttling." + key + " must not exceed " + MAX_LIMIT + " but was " + parsed);
        }
    }

    /**
     * Per-rule validation on a fully-merged throttling config: each rule's effective ceiling
     * {@code max(0, node_limit) + max(0, shared_limit)} must be at least 1, since a ceiling of 0 rejects every request
     * the rule applies to. Must be called on the merged result, not a partial update fragment.
     *
     * @param throttling the merged throttling settings
     * @throws IllegalArgumentException if any rule's effective ceiling is 0
     */
    public static void validateMergedConfig(Settings throttling) {
        for (Rule rule : rules(throttling)) {
            int node = rule.nodeLimit();
            int shared = rule.sharedLimit();
            // Sum as long: each limit can be up to Integer.MAX_VALUE, so an int sum would overflow to a negative ceiling.
            if ((long) Math.max(0, node) + Math.max(0, shared) < 1) {
                throw new IllegalArgumentException(
                    "Effective throttle ceiling for throttling."
                        + rule.attribute().key()
                        + " is 0 (node_limit="
                        + (node == UNSET_LIMIT ? "unset" : node)
                        + ", shared_limit="
                        + (shared == UNSET_LIMIT ? "unset" : shared)
                        + "); this would reject all requests. Set at least one limit to a positive value, or set throttling."
                        + rule.attribute().key()
                        + " to null to remove the rule"
                );
            }
        }
    }

    /**
     * All user-input checks for a complete throttling config about to be written: per-key ({@link #validate}) and per-rule
     * ({@link #validateMergedConfig}). For the write paths that receive their input over the transport layer (and so
     * cannot assume it went through the strict REST parser).
     */
    public static void validateForWrite(Settings throttling) {
        validate(throttling);
        validateMergedConfig(throttling);
    }

    /** Whether {@code fragment} is in the flat form a node on an older build sends: any top-level legacy key. */
    private static boolean isLegacyFlat(@Nullable Settings fragment) {
        if (fragment == null) {
            return false;
        }
        for (String key : fragment.keySet()) {
            if (LEGACY_KEYS.contains(key)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Applies an update fragment received from the transport layer onto stored throttling. A keyed fragment is
     * validated and merged ({@link #merge}). A flat-form fragment, sent by a coordinating node on a build that predates
     * multi-attribute rules, is applied with that build's semantics instead: its keys overwrite (or, when null, clear)
     * the flat form of the stored config, so e.g. {@code {attribute: role}} switches the rule's attribute and keeps its
     * limits. That is well defined because the stored config has at most one rule while such nodes are present (see
     * {@link #validateSupportedByCluster}); a multi-rule config can't be updated that way and is rejected.
     *
     * @throws IllegalArgumentException if the fragment is invalid or can't be applied
     */
    public static Settings mergeUpdate(Settings existing, @Nullable Settings incoming) {
        if (isLegacyFlat(incoming) == false) {
            if (incoming != null) {
                validate(incoming);
            }
            return merge(existing, incoming);
        }
        final Settings flatExisting = existing == null || existing.isEmpty() ? Settings.EMPTY : toLegacyFlat(existing);
        if (flatExisting == null) {
            throw new IllegalArgumentException(
                "this workload group's throttling has several rules, which a node on an older build can't update; "
                    + "send the request to an upgraded node"
            );
        }
        Settings.Builder merged = Settings.builder().put(flatExisting);
        for (String key : incoming.keySet()) {
            if (LEGACY_KEYS.contains(key) == false) {
                throw new IllegalArgumentException("Unknown throttle setting: " + key);
            }
            final String value = incoming.get(key);
            if (value == null) {
                merged.remove(key);
            } else {
                merged.put(key, value);
            }
        }
        final Settings flat = merged.build();
        if (flat.isEmpty()) {
            return Settings.EMPTY;
        }
        final String attribute = flat.get(LEGACY_ATTRIBUTE);
        if (attribute == null) {
            throw new IllegalArgumentException("throttling.attribute is required when a throttle limit is set");
        }
        if (ATTRIBUTES.containsKey(attribute) == false) {
            throw new IllegalArgumentException(
                "throttling attribute [" + attribute + "] is not supported; supported attributes are " + new TreeSet<>(ATTRIBUTES.keySet())
            );
        }
        // Back to the keyed form, then the same checks as any keyed update. A flat config left with an attribute but no
        // limit has no keyed form (no rule); the older build rejects it (its ceiling would be 0), so this does too.
        final Settings keyed = normalizeStored(flat);
        if (keyed.isEmpty()) {
            throw new IllegalArgumentException(
                "Effective throttle ceiling for throttling."
                    + attribute
                    + " is 0 (node_limit=unset, shared_limit=unset); set at "
                    + "least one limit, or set throttling as null to disable throttling"
            );
        }
        validateForWrite(keyed);
        return keyed;
    }

    /**
     * Coordinator-side check of an update's result: whether the cluster manager actually applied every change in the
     * keyed {@code update} to {@code result}. A cluster manager on an older build acknowledges a keyed change that only
     * removes or clears a rule without applying it (its flat-form merge finds no matching key); this turns that silent
     * no-op into an error. A flat-form update is not checked here.
     *
     * @throws OpenSearchStatusException (409) if some change in {@code update} is not reflected in {@code result}
     */
    public static void verifyUpdateApplied(@Nullable Settings update, @Nullable Settings result) {
        if (update == null || isLegacyFlat(update)) {
            return;
        }
        final Settings applied = result == null ? Settings.EMPTY : result;
        boolean ok = true;
        if (update.isEmpty()) {
            ok = applied.isEmpty(); // "throttling": null clears every rule
        }
        for (String key : update.keySet()) {
            final String value = update.get(key);
            if (value != null) {
                ok &= value.equals(applied.get(key));
            } else if (key.indexOf('.') < 0) {
                ok &= applied.getByPrefix(key + ".").isEmpty() && applied.get(key) == null; // rule removed
            } else {
                ok &= applied.get(key) == null; // field cleared
            }
        }
        if (ok == false) {
            throw new OpenSearchStatusException(
                "the cluster manager acknowledged the update but did not apply its throttling change (it may run an older "
                    + "build that ignores this change); the update's other fields may have been applied. Resulting "
                    + "throttling is "
                    + applied
                    + ". Retry once the cluster manager is upgraded",
                RestStatus.CONFLICT
            );
        }
    }

    /**
     * Applies an update fragment onto stored throttling: a null fragment (field absent) keeps the existing config, an
     * empty fragment ({@code "throttling": null}) clears every rule, {@code <attribute>: null} removes that whole rule,
     * {@code <attribute>.<field>: null} clears one field, and any other key overwrites. A rule left with no fields is
     * thereby gone.
     *
     * @param existing the stored throttling settings
     * @param incoming the update fragment (may be null)
     */
    public static Settings merge(Settings existing, @Nullable Settings incoming) {
        if (incoming == null) {
            return Settings.builder().put(existing).build();
        }
        if (incoming.isEmpty()) {
            return Settings.EMPTY;
        }
        Settings.Builder builder = Settings.builder().put(existing);
        for (String key : incoming.keySet()) {
            String value = incoming.get(key);
            if (value != null) {
                builder.put(key, value);
            } else if (key.indexOf('.') < 0) {
                String prefix = key + ".";
                builder.keys().removeIf(k -> k.equals(key) || k.startsWith(prefix));
            } else {
                builder.remove(key);
            }
        }
        return builder.build();
    }
}
