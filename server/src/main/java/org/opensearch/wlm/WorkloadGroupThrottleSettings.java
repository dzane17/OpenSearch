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
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
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
            logger.debug("Ignoring throttling." + attributeName + " with an unreadable limit: " + ruleSettings, e);
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
        return throttling.keySet().stream().map(key -> {
            int dot = key.indexOf('.');
            return dot < 0 ? key : key.substring(0, dot);
        }).collect(Collectors.toCollection(TreeSet::new));
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
