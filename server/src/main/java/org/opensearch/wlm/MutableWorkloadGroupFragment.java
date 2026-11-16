/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.Version;
import org.opensearch.cluster.AbstractDiffable;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;

/**
 * Class to hold the fields that can be updated in a WorkloadGroup.
 */
@ExperimentalApi
public class MutableWorkloadGroupFragment extends AbstractDiffable<MutableWorkloadGroupFragment> {

    public static final String RESILIENCY_MODE_STRING = "resiliency_mode";
    public static final String RESOURCE_LIMITS_STRING = "resource_limits";
    public static final String SETTINGS_STRING = "settings";
    public static final String THROTTLING_STRING = "throttling";
    public static final String QUEUE_STRING = "queue";
    private ResiliencyMode resiliencyMode;
    private Map<ResourceType, Double> resourceLimits;
    private Settings settings;
    private Settings throttling;
    // Lazily parsed from `throttling` for the hot path; reset when it is replaced. Not part of equals/hashCode/wire.
    private volatile List<WorkloadGroupThrottleSettings.Rule> throttleRules;
    // Lazily computed throttling in the form written for stored groups (see storedFormThrottling); reset with throttling.
    private volatile Settings storedFormThrottling;
    private Settings queue;

    public static final List<String> acceptedFieldNames = List.of(
        RESILIENCY_MODE_STRING,
        RESOURCE_LIMITS_STRING,
        SETTINGS_STRING,
        THROTTLING_STRING,
        QUEUE_STRING
    );

    public MutableWorkloadGroupFragment() {}

    /**
     * Constructor for tests only. Production code should use the full constructor below.
     */
    public MutableWorkloadGroupFragment(ResiliencyMode resiliencyMode, Map<ResourceType, Double> resourceLimits) {
        this(resiliencyMode, resourceLimits, Settings.EMPTY);
    }

    public MutableWorkloadGroupFragment(ResiliencyMode resiliencyMode, Map<ResourceType, Double> resourceLimits, Settings settings) {
        this(resiliencyMode, resourceLimits, settings, Settings.EMPTY);
    }

    public MutableWorkloadGroupFragment(
        ResiliencyMode resiliencyMode,
        Map<ResourceType, Double> resourceLimits,
        Settings settings,
        Settings throttling
    ) {
        this(resiliencyMode, resourceLimits, settings, throttling, Settings.EMPTY);
    }

    public MutableWorkloadGroupFragment(
        ResiliencyMode resiliencyMode,
        Map<ResourceType, Double> resourceLimits,
        Settings settings,
        Settings throttling,
        Settings queue
    ) {
        this(resiliencyMode, resourceLimits, settings, throttling, queue, true);
    }

    private MutableWorkloadGroupFragment(
        ResiliencyMode resiliencyMode,
        Map<ResourceType, Double> resourceLimits,
        Settings settings,
        Settings throttling,
        Settings queue,
        boolean validateThrottling
    ) {
        validateResourceLimits(resourceLimits);
        WorkloadGroupSearchSettings.validate(settings);
        if (validateThrottling) {
            WorkloadGroupThrottleSettings.validate(throttling);
        }
        WorkloadGroupQueueSettings.validate(queue);
        this.resiliencyMode = resiliencyMode;
        this.resourceLimits = resourceLimits;
        this.settings = settings != null ? settings : Settings.EMPTY;
        this.throttling = throttling != null ? throttling : Settings.EMPTY;
        this.queue = queue != null ? queue : Settings.EMPTY;
    }

    /**
     * Builds a fragment whose throttling config is already validated (e.g. the merge of a stored config and a validated
     * update) or was read back from stored metadata. Skips the per-key user-input checks on throttling, which would
     * otherwise reject state this build cannot fully interpret (e.g. a rule for an attribute added by a newer build)
     * instead of leaving it inert.
     */
    public static MutableWorkloadGroupFragment ofStoredThrottling(
        ResiliencyMode resiliencyMode,
        Map<ResourceType, Double> resourceLimits,
        Settings settings,
        Settings throttling,
        Settings queue
    ) {
        return new MutableWorkloadGroupFragment(resiliencyMode, resourceLimits, settings, throttling, queue, false);
    }

    /**
     * Reads an update fragment (an update request's body) from the transport layer. Throttling is kept as sent: a
     * flat-form fragment from a node on an older build has update semantics (it overwrites keys of the stored flat
     * form), which {@link WorkloadGroupThrottleSettings#mergeUpdate} applies against the stored config; translating it
     * here, without that config, could not.
     */
    public MutableWorkloadGroupFragment(StreamInput in) throws IOException {
        this(in, false);
    }

    /**
     * Reads a stored group's fragment (cluster state, or a create request's full group) from the transport layer,
     * translating a flat-form throttling config (see {@link WorkloadGroupThrottleSettings#normalizeStored}).
     */
    public static MutableWorkloadGroupFragment readStored(StreamInput in) throws IOException {
        return new MutableWorkloadGroupFragment(in, true);
    }

    private MutableWorkloadGroupFragment(StreamInput in, boolean stored) throws IOException {
        if (in.readBoolean()) {
            resourceLimits = in.readMap((i) -> ResourceType.fromName(i.readString()), StreamInput::readDouble);
        } else {
            resourceLimits = new HashMap<>();
        }
        String updatedResiliencyMode = in.readOptionalString();
        resiliencyMode = updatedResiliencyMode == null ? null : ResiliencyMode.fromName(updatedResiliencyMode);
        // TODO(merge): this block gained a third read (queue) on the queueing branch while staying gated at V_3_7_0, so
        // it is shape-compatible only while queueing ships in the SAME release as throttling. If they split, give queue
        // its own version gate — see the matching note on the write path below.
        if (in.getVersion().onOrAfter(Version.V_3_7_0)) {
            settings = Settings.readOptionalSettingsFromStream(in);
            final Settings sent = Settings.readOptionalSettingsFromStream(in);
            throttling = stored ? WorkloadGroupThrottleSettings.normalizeStored(sent) : sent;
            queue = Settings.readOptionalSettingsFromStream(in);
        } else if (in.getVersion().onOrAfter(Version.V_3_6_0)) {
            // Legacy 3.6 format: read and discard (experimental API, no backward compat guarantee)
            boolean isNull = in.readBoolean();
            if (isNull == false) {
                in.readMap(StreamInput::readString, StreamInput::readString);
            }
            settings = Settings.EMPTY;
            throttling = Settings.EMPTY;
            queue = Settings.EMPTY;
        } else {
            settings = Settings.EMPTY;
            throttling = Settings.EMPTY;
            queue = Settings.EMPTY;
        }
    }

    interface FieldParser<T> {
        T parseField(XContentParser parser) throws IOException;
    }

    static class ResiliencyModeParser implements FieldParser<ResiliencyMode> {
        public ResiliencyMode parseField(XContentParser parser) throws IOException {
            return ResiliencyMode.fromName(parser.text());
        }
    }

    static class ResourceLimitsParser implements FieldParser<Map<ResourceType, Double>> {
        public Map<ResourceType, Double> parseField(XContentParser parser) throws IOException {
            String fieldName = "";
            XContentParser.Token token;
            final Map<ResourceType, Double> resourceLimits = new HashMap<>();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    fieldName = parser.currentName();
                } else {
                    resourceLimits.put(ResourceType.fromName(fieldName), parser.doubleValue());
                }
            }
            return resourceLimits;
        }
    }

    static class SearchSettingsParser implements FieldParser<Settings> {
        public Settings parseField(XContentParser parser) throws IOException {
            // "settings": null means clear all settings
            if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
                return Settings.EMPTY;
            }
            Settings settings = Settings.fromXContent(parser);
            WorkloadGroupSearchSettings.validate(settings);
            return settings;
        }
    }

    static class ThrottlingParser implements FieldParser<Settings> {
        public Settings parseField(XContentParser parser) throws IOException {
            // "throttling": null means clear all throttling (disable)
            if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
                return Settings.EMPTY;
            }
            return WorkloadGroupThrottleSettings.fromXContent(parser);
        }
    }

    // Read-path counterpart of ThrottlingParser for stored metadata: never rejects what an earlier or later build wrote.
    static class StoredThrottlingParser implements FieldParser<Settings> {
        public Settings parseField(XContentParser parser) throws IOException {
            if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
                return Settings.EMPTY;
            }
            return WorkloadGroupThrottleSettings.normalizeStored(Settings.fromXContent(parser));
        }
    }

    static class QueueParser implements FieldParser<Settings> {
        public Settings parseField(XContentParser parser) throws IOException {
            // "queue": null means clear all queue config (disable queueing)
            if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
                return Settings.EMPTY;
            }
            Settings queue = Settings.fromXContent(parser);
            WorkloadGroupQueueSettings.validate(queue);
            return queue;
        }
    }

    static class FieldParserFactory {
        static Optional<FieldParser<?>> fieldParserFor(String fieldName) {
            return switch (fieldName) {
                case RESILIENCY_MODE_STRING -> Optional.of(new ResiliencyModeParser());
                case RESOURCE_LIMITS_STRING -> Optional.of(new ResourceLimitsParser());
                case SETTINGS_STRING -> Optional.of(new SearchSettingsParser());
                case THROTTLING_STRING -> Optional.of(new ThrottlingParser());
                case QUEUE_STRING -> Optional.of(new QueueParser());
                default -> Optional.empty();
            };
        }
    }

    private final Map<String, Function<XContentBuilder, Void>> toXContentMap = Map.of(RESILIENCY_MODE_STRING, (builder) -> {
        try {
            builder.field(RESILIENCY_MODE_STRING, resiliencyMode.getName());
            return null;
        } catch (IOException e) {
            throw new IllegalStateException("writing error encountered for the field " + RESILIENCY_MODE_STRING);
        }
    }, RESOURCE_LIMITS_STRING, (builder) -> {
        try {
            builder.startObject(RESOURCE_LIMITS_STRING);
            for (ResourceType resourceType : ResourceType.values()) {
                if (resourceLimits.containsKey(resourceType)) {
                    builder.field(resourceType.getName(), resourceLimits.get(resourceType));
                }
            }
            builder.endObject();
            return null;
        } catch (IOException e) {
            throw new IllegalStateException("writing error encountered for the field " + RESOURCE_LIMITS_STRING);
        }
    }, SETTINGS_STRING, (builder) -> {
        try {
            builder.startObject(SETTINGS_STRING);
            writeSettingsFields(builder, settings);
            builder.endObject();
            return null;
        } catch (IOException e) {
            throw new IllegalStateException("writing error encountered for the field " + SETTINGS_STRING);
        }
    }, THROTTLING_STRING, (builder) -> {
        writeThrottling(builder, throttling);
        return null;
    }, QUEUE_STRING, (builder) -> {
        try {
            // Like throttling, queue config is omitted entirely when unset.
            Settings q = queue != null ? queue : Settings.EMPTY;
            if (q.isEmpty() == false) {
                builder.startObject(QUEUE_STRING);
                if (q.hasValue(WorkloadGroupQueueSettings.SIZE_PER_BUCKET.getKey())) {
                    builder.field(WorkloadGroupQueueSettings.SIZE_PER_BUCKET.getKey(), WorkloadGroupQueueSettings.SIZE_PER_BUCKET.get(q));
                }
                builder.endObject();
            }
            return null;
        } catch (IOException e) {
            throw new IllegalStateException("writing error encountered for the field " + QUEUE_STRING);
        }
    });

    private static void writeThrottling(XContentBuilder builder, Settings source) {
        try {
            // Unlike settings (always emitted as {}), throttling is omitted entirely when unset.
            Settings t = source != null ? source : Settings.EMPTY;
            if (t.isEmpty() == false) {
                builder.startObject(THROTTLING_STRING);
                // A bare attribute is an update's null "remove this rule" marker.
                for (String attribute : new TreeSet<>(t.names())) {
                    Settings r = t.getByPrefix(attribute + ".");
                    if (r.isEmpty()) {
                        // e.g. an update's null marker, or node_limit in the flat stored form
                        writeThrottlingValue(builder, attribute, t.get(attribute));
                        continue;
                    }
                    builder.startObject(attribute);
                    // All fields, including ones this build doesn't know, so they survive persistence.
                    for (String field : new TreeSet<>(r.keySet())) {
                        writeThrottlingValue(builder, field, r.get(field));
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
        } catch (IOException e) {
            throw new IllegalStateException("writing error encountered for the field " + THROTTLING_STRING);
        }
    }

    // A throttling value: null as null (a clear marker), an integer as a number, anything else as the string it is.
    private static void writeThrottlingValue(XContentBuilder builder, String name, @Nullable String value) throws IOException {
        Integer limit = value == null ? null : parseIntOrNull(value);
        if (value == null) {
            builder.nullField(name);
        } else if (limit != null) {
            builder.field(name, limit.intValue());
        } else {
            builder.field(name, value);
        }
    }

    private static Integer parseIntOrNull(String value) {
        try {
            return Integer.valueOf(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static void writeSettingsFields(XContentBuilder builder, Settings s) throws IOException {
        Settings source = s != null ? s : Settings.EMPTY;
        Map<String, String> sorted = new TreeMap<>();
        for (String key : source.keySet()) {
            sorted.put(key, source.get(key));
        }
        for (Map.Entry<String, String> e : sorted.entrySet()) {
            builder.field(e.getKey(), e.getValue());
        }
    }

    /**
     * A copy of this update fragment with {@code throttling} in place of its throttling, every other field exactly as
     * it is. Unlike the constructors, a null field stays null: in an update fragment null means "keep the existing
     * value" and empty means "clear it", so turning one into the other would change what the update does.
     */
    public MutableWorkloadGroupFragment withThrottling(@Nullable Settings throttling) {
        MutableWorkloadGroupFragment copy = new MutableWorkloadGroupFragment();
        copy.resiliencyMode = resiliencyMode;
        copy.resourceLimits = resourceLimits;
        copy.settings = settings;
        copy.throttling = throttling;
        copy.queue = queue;
        return copy;
    }

    /**
     * Throttling as written for a stored group: a single-rule config in the flat form that builds predating keyed rules
     * read (see {@link WorkloadGroupThrottleSettings#MULTI_ATTRIBUTE_THROTTLING_NODE_ATTRIBUTE}), otherwise unchanged. Readers of a
     * stored group translate it back ({@link WorkloadGroupThrottleSettings#normalizeStored}). Cached: stored groups are
     * written on every cluster-state publication.
     */
    private Settings storedFormThrottling() {
        Settings stored = storedFormThrottling;
        if (stored == null) {
            final Settings flat = WorkloadGroupThrottleSettings.toLegacyFlat(throttling);
            stored = flat != null ? flat : throttling; // null stays null, so the bytes match a plain write
            storedFormThrottling = stored;
        }
        return stored;
    }

    /** Writes this fragment as part of a stored group (see {@link #storedFormThrottling()}); see {@link #writeTo}. */
    public void writeStoredTo(StreamOutput out) throws IOException {
        writeTo(out, storedFormThrottling());
    }

    /** As {@link #writeField(XContentBuilder, String)}, writing throttling in the stored form when {@code storedForm}. */
    public void writeField(XContentBuilder builder, String field, boolean storedForm) {
        if (storedForm && THROTTLING_STRING.equals(field)) {
            writeThrottling(builder, storedFormThrottling());
        } else {
            writeField(builder, field);
        }
    }

    public static boolean shouldParse(String field) {
        return FieldParserFactory.fieldParserFor(field).isPresent();
    }

    public void parseField(XContentParser parser, String field) {
        parseField(parser, field, false);
    }

    /**
     * Parses one field. {@code stored} selects the lenient read path for persisted metadata: throttling is normalized
     * rather than validated, so metadata this build cannot fully interpret still loads (and is ignored by enforcement)
     * instead of making the node unable to read its own state. User input must use the strict path.
     */
    @SuppressWarnings("unchecked")
    public void parseField(XContentParser parser, String field, boolean stored) {
        Optional<FieldParser<?>> parserForField = stored && THROTTLING_STRING.equals(field)
            ? Optional.of(new StoredThrottlingParser())
            : FieldParserFactory.fieldParserFor(field);
        parserForField.ifPresent(fieldParser -> {
            try {
                Object value = fieldParser.parseField(parser);
                switch (field) {
                    case RESILIENCY_MODE_STRING -> setResiliencyMode((ResiliencyMode) value);
                    case RESOURCE_LIMITS_STRING -> setResourceLimits((Map<ResourceType, Double>) value);
                    case SETTINGS_STRING -> setSettings((Settings) value);
                    case THROTTLING_STRING -> {
                        if (stored) {
                            setStoredThrottling((Settings) value);
                        } else {
                            setThrottling((Settings) value);
                        }
                    }
                    case QUEUE_STRING -> setQueue((Settings) value);
                }
            } catch (IllegalArgumentException e) {
                throw e;
            } catch (IOException e) {
                throw new IllegalArgumentException(String.format(Locale.ROOT, "parsing error encountered for the field '%s'", field));
            }
        });
    }

    public void writeField(XContentBuilder builder, String field) {
        toXContentMap.get(field).apply(builder);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writeTo(out, throttling);
    }

    private void writeTo(StreamOutput out, Settings throttlingToWrite) throws IOException {
        if (resourceLimits == null || resourceLimits.isEmpty()) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeMap(resourceLimits, ResourceType::writeTo, StreamOutput::writeDouble);
        }
        out.writeOptionalString(resiliencyMode == null ? null : resiliencyMode.getName());
        if (out.getVersion().onOrAfter(Version.V_3_7_0)) {
            Settings.writeOptionalSettingsToStream(settings, out);
            // TODO(merge): when upstreamed, gate throttling/queue behind their own release version to support mixed-build
            // clusters. Today both ride the V_3_7_0 gate, which only works because they ship in the same release.
            Settings.writeOptionalSettingsToStream(throttlingToWrite, out);
            Settings.writeOptionalSettingsToStream(queue, out);
        } else if (out.getVersion().onOrAfter(Version.V_3_6_0)) {
            // Legacy 3.6 format: write empty map (experimental API, settings not preserved across versions)
            out.writeBoolean(false);
            out.writeMap(Map.of(), StreamOutput::writeString, StreamOutput::writeString);
        }
    }

    public static void validateResourceLimits(Map<ResourceType, Double> resourceLimits) {
        if (resourceLimits == null) {
            return;
        }
        for (Map.Entry<ResourceType, Double> resource : resourceLimits.entrySet()) {
            Double threshold = resource.getValue();
            Objects.requireNonNull(resource.getKey(), "resourceName can't be null");
            Objects.requireNonNull(threshold, "resource limit threshold for" + resource.getKey().getName() + " : can't be null");

            if (Double.compare(threshold, 0.0) <= 0 || Double.compare(threshold, 1.0) > 0) {
                throw new IllegalArgumentException("resource value should be greater than 0 and less or equal to 1.0");
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MutableWorkloadGroupFragment that = (MutableWorkloadGroupFragment) o;
        return Objects.equals(resiliencyMode, that.resiliencyMode)
            && Objects.equals(resourceLimits, that.resourceLimits)
            && Objects.equals(settings, that.settings)
            && Objects.equals(throttling, that.throttling)
            && Objects.equals(queue, that.queue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resiliencyMode, resourceLimits, settings, throttling, queue);
    }

    public ResiliencyMode getResiliencyMode() {
        return resiliencyMode;
    }

    public Map<ResourceType, Double> getResourceLimits() {
        return resourceLimits;
    }

    public Settings getSettings() {
        return settings;
    }

    public Settings getThrottling() {
        return throttling;
    }

    /** The configured throttle rules in acquisition order (see {@link WorkloadGroupThrottleSettings#rules}); cached. */
    public List<WorkloadGroupThrottleSettings.Rule> getThrottleRules() {
        List<WorkloadGroupThrottleSettings.Rule> rules = throttleRules;
        if (rules == null) {
            rules = List.copyOf(WorkloadGroupThrottleSettings.rules(throttling));
            throttleRules = rules;
        }
        return rules;
    }

    /** The configured rule for {@code attribute}, or {@code null} if there is none. */
    public WorkloadGroupThrottleSettings.Rule getThrottleRule(String attribute) {
        for (WorkloadGroupThrottleSettings.Rule rule : getThrottleRules()) {
            if (rule.attribute().key().equals(attribute)) {
                return rule;
            }
        }
        return null;
    }

    public Settings getQueue() {
        return queue;
    }

    /**
     * This enum models the different WorkloadGroup resiliency modes
     * SOFT - means that this workload group can consume more than workload group resource limits if node is not in duress
     * ENFORCED - means that it will never breach the assigned limits and will cancel as soon as the limits are breached
     * MONITOR - it will not cause any cancellation but just log the eligible task cancellations
     */
    @ExperimentalApi
    public enum ResiliencyMode {
        SOFT("soft"),
        ENFORCED("enforced"),
        MONITOR("monitor");

        private final String name;

        ResiliencyMode(String mode) {
            this.name = mode;
        }

        public String getName() {
            return name;
        }

        public static ResiliencyMode fromName(String s) {
            for (ResiliencyMode mode : values()) {
                if (mode.getName().equalsIgnoreCase(s)) return mode;

            }
            throw new IllegalArgumentException("Invalid value for WorkloadGroupMode: " + s);
        }
    }

    void setResiliencyMode(ResiliencyMode resiliencyMode) {
        this.resiliencyMode = resiliencyMode;
    }

    void setResourceLimits(Map<ResourceType, Double> resourceLimits) {
        validateResourceLimits(resourceLimits);
        this.resourceLimits = resourceLimits;
    }

    void setSettings(Settings settings) {
        WorkloadGroupSearchSettings.validate(settings);
        this.settings = settings != null ? settings : Settings.EMPTY;
    }

    void setThrottling(Settings throttling) {
        WorkloadGroupThrottleSettings.validate(throttling);
        setStoredThrottling(throttling);
    }

    private void setStoredThrottling(Settings throttling) {
        this.throttling = throttling != null ? throttling : Settings.EMPTY;
        this.throttleRules = null;
        this.storedFormThrottling = null;
    }

    void setQueue(Settings queue) {
        WorkloadGroupQueueSettings.validate(queue);
        this.queue = queue != null ? queue : Settings.EMPTY;
    }

}
