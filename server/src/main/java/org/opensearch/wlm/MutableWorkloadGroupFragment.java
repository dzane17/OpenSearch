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
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * Class to hold the fields that can be updated in a WorkloadGroup.
 */
@ExperimentalApi
public class MutableWorkloadGroupFragment extends AbstractDiffable<MutableWorkloadGroupFragment> {

    public static final String RESILIENCY_MODE_STRING = "resiliency_mode";
    public static final String RESOURCE_LIMITS_STRING = "resource_limits";
    public static final String SETTINGS_STRING = "settings";
    public static final String THROTTLE_ATTRIBUTE_STRING = "throttle_attribute";
    public static final String NODE_THROTTLE_LIMIT_STRING = "node_throttle_limit";
    public static final String SHARED_THROTTLE_LIMIT_STRING = "shared_throttle_limit";
    private ResiliencyMode resiliencyMode;
    private Map<ResourceType, Double> resourceLimits;
    private Settings settings;
    private String throttleAttribute;
    private Integer nodeThrottleLimit;
    private Integer sharedThrottleLimit;
    // Throttle field names that arrived as an explicit JSON null (clear) so the update-merge can tell "clear" from "absent".                                      
    private final Set<String> explicitlyNulledThrottleFields = new HashSet<>();

    public static final List<String> acceptedFieldNames = List.of(
        RESILIENCY_MODE_STRING,
        RESOURCE_LIMITS_STRING,
        SETTINGS_STRING,
        THROTTLE_ATTRIBUTE_STRING,
        NODE_THROTTLE_LIMIT_STRING,
        SHARED_THROTTLE_LIMIT_STRING
    );

    public MutableWorkloadGroupFragment() {}

    /**
     * Constructor for tests only. Production code should use the full constructor below.
     */
    public MutableWorkloadGroupFragment(ResiliencyMode resiliencyMode, Map<ResourceType, Double> resourceLimits) {
        this(resiliencyMode, resourceLimits, Settings.EMPTY);
    }

    public MutableWorkloadGroupFragment(ResiliencyMode resiliencyMode, Map<ResourceType, Double> resourceLimits, Settings settings) {
        this(resiliencyMode, resourceLimits, settings, null, null, null);
    }

    public MutableWorkloadGroupFragment(
        ResiliencyMode resiliencyMode,
        Map<ResourceType, Double> resourceLimits,
        Settings settings,
        String throttleAttribute,
        Integer nodeThrottleLimit,
        Integer sharedThrottleLimit
    ) {
        validateResourceLimits(resourceLimits);
        WorkloadGroupSearchSettings.validate(settings);
        validateThrottleAttribute(throttleAttribute);
        validateThrottleLimit(NODE_THROTTLE_LIMIT_STRING, nodeThrottleLimit);
        validateThrottleLimit(SHARED_THROTTLE_LIMIT_STRING, sharedThrottleLimit);
        this.resiliencyMode = resiliencyMode;
        this.resourceLimits = resourceLimits;
        this.settings = settings != null ? settings : Settings.EMPTY;
        this.throttleAttribute = throttleAttribute;
        this.nodeThrottleLimit = nodeThrottleLimit;
        this.sharedThrottleLimit = sharedThrottleLimit;
    }

    public MutableWorkloadGroupFragment(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            resourceLimits = in.readMap((i) -> ResourceType.fromName(i.readString()), StreamInput::readDouble);
        } else {
            resourceLimits = new HashMap<>();
        }
        String updatedResiliencyMode = in.readOptionalString();
        resiliencyMode = updatedResiliencyMode == null ? null : ResiliencyMode.fromName(updatedResiliencyMode);
        if (in.getVersion().onOrAfter(Version.V_3_7_0)) {
            settings = Settings.readOptionalSettingsFromStream(in);
            throttleAttribute = in.readOptionalString();
            nodeThrottleLimit = in.readOptionalVInt();
            sharedThrottleLimit = in.readOptionalVInt();
            explicitlyNulledThrottleFields.addAll(in.readStringList());
        } else if (in.getVersion().onOrAfter(Version.V_3_6_0)) {
            // Legacy 3.6 format: read and discard (experimental API, no backward compat guarantee)
            boolean isNull = in.readBoolean();
            if (isNull == false) {
                in.readMap(StreamInput::readString, StreamInput::readString);
            }
            settings = Settings.EMPTY;
        } else {
            settings = Settings.EMPTY;
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

    static class ThrottleAttributeParser implements FieldParser<String> {
        public String parseField(XContentParser parser) throws IOException {
            if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
                return null;
            }
            return parser.text();
        }
    }

    static class ThrottleLimitParser implements FieldParser<Integer> {
        public Integer parseField(XContentParser parser) throws IOException {
            if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
                return null;
            }
            return parser.intValue();
        }
    }

    static class FieldParserFactory {
        static Optional<FieldParser<?>> fieldParserFor(String fieldName) {
            return switch (fieldName) {
                case RESILIENCY_MODE_STRING -> Optional.of(new ResiliencyModeParser());
                case RESOURCE_LIMITS_STRING -> Optional.of(new ResourceLimitsParser());
                case SETTINGS_STRING -> Optional.of(new SearchSettingsParser());
                case THROTTLE_ATTRIBUTE_STRING -> Optional.of(new ThrottleAttributeParser());
                case NODE_THROTTLE_LIMIT_STRING, SHARED_THROTTLE_LIMIT_STRING -> Optional.of(new ThrottleLimitParser());
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
            Settings s = settings != null ? settings : Settings.EMPTY;
            Map<String, String> sortedSettingsMap = new TreeMap<>();
            for (String key : s.keySet()) {
                sortedSettingsMap.put(key, s.get(key));
            }
            for (Map.Entry<String, String> e : sortedSettingsMap.entrySet()) {
                builder.field(e.getKey(), e.getValue());
            }
            builder.endObject();
            return null;
        } catch (IOException e) {
            throw new IllegalStateException("writing error encountered for the field " + SETTINGS_STRING);
        }
    }, THROTTLE_ATTRIBUTE_STRING, (builder) -> {
        try {
            if (throttleAttribute != null) {
                builder.field(THROTTLE_ATTRIBUTE_STRING, throttleAttribute);
            }
            return null;
        } catch (IOException e) {
            throw new IllegalStateException("writing error encountered for the field " + THROTTLE_ATTRIBUTE_STRING);
        }
    }, NODE_THROTTLE_LIMIT_STRING, (builder) -> {
        try {
            if (nodeThrottleLimit != null) {
                builder.field(NODE_THROTTLE_LIMIT_STRING, nodeThrottleLimit);
            }
            return null;
        } catch (IOException e) {
            throw new IllegalStateException("writing error encountered for the field " + NODE_THROTTLE_LIMIT_STRING);
        }
    }, SHARED_THROTTLE_LIMIT_STRING, (builder) -> {
        try {
            if (sharedThrottleLimit != null) {
                builder.field(SHARED_THROTTLE_LIMIT_STRING, sharedThrottleLimit);
            }
            return null;
        } catch (IOException e) {
            throw new IllegalStateException("writing error encountered for the field " + SHARED_THROTTLE_LIMIT_STRING);
        }
    });

    public static boolean shouldParse(String field) {
        return FieldParserFactory.fieldParserFor(field).isPresent();
    }

    @SuppressWarnings("unchecked")
    public void parseField(XContentParser parser, String field) {
        FieldParserFactory.fieldParserFor(field).ifPresent(fieldParser -> {
            try {
                Object value = fieldParser.parseField(parser);
                switch (field) {
                    case RESILIENCY_MODE_STRING -> setResiliencyMode((ResiliencyMode) value);
                    case RESOURCE_LIMITS_STRING -> setResourceLimits((Map<ResourceType, Double>) value);
                    case SETTINGS_STRING -> setSettings((Settings) value);
                    case THROTTLE_ATTRIBUTE_STRING -> setThrottleAttribute((String) value);
                    case NODE_THROTTLE_LIMIT_STRING -> setNodeThrottleLimit((Integer) value);
                    case SHARED_THROTTLE_LIMIT_STRING -> setSharedThrottleLimit((Integer) value);
                }
                if (isThrottleField(field)) {
                    if (value == null) {
                        explicitlyNulledThrottleFields.add(field);
                    } else {
                        explicitlyNulledThrottleFields.remove(field);
                    }
                }
            } catch (IllegalArgumentException e) {
                throw e;
            } catch (IOException e) {
                throw new IllegalArgumentException(String.format(Locale.ROOT, "parsing error encountered for the field '%s'", field));
            }
        });
    }

    private static boolean isThrottleField(String field) {
        return THROTTLE_ATTRIBUTE_STRING.equals(field)
            || NODE_THROTTLE_LIMIT_STRING.equals(field)
            || SHARED_THROTTLE_LIMIT_STRING.equals(field);
    }

    public boolean isThrottleFieldExplicitlyNulled(String field) {
        return explicitlyNulledThrottleFields.contains(field);
    }

    public void clearExplicitlyNulledThrottleFields() {
        explicitlyNulledThrottleFields.clear();
    }

    public void writeField(XContentBuilder builder, String field) {
        toXContentMap.get(field).apply(builder);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (resourceLimits == null || resourceLimits.isEmpty()) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeMap(resourceLimits, ResourceType::writeTo, StreamOutput::writeDouble);
        }
        out.writeOptionalString(resiliencyMode == null ? null : resiliencyMode.getName());
        if (out.getVersion().onOrAfter(Version.V_3_7_0)) {
            Settings.writeOptionalSettingsToStream(settings, out);
            out.writeOptionalString(throttleAttribute);
            out.writeOptionalVInt(nodeThrottleLimit);
            out.writeOptionalVInt(sharedThrottleLimit);
            out.writeStringCollection(explicitlyNulledThrottleFields);
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

    public static void validateThrottleAttribute(String throttleAttribute) {
        if (throttleAttribute != null && throttleAttribute.isBlank()) {
            throw new IllegalArgumentException(THROTTLE_ATTRIBUTE_STRING + " can't be empty when set");
        }
    }

    public static void validateThrottleLimit(String fieldName, Integer limit) {
        if (limit != null && limit < 0) {
            throw new IllegalArgumentException(fieldName + " must be non-negative");
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
            && Objects.equals(throttleAttribute, that.throttleAttribute)
            && Objects.equals(nodeThrottleLimit, that.nodeThrottleLimit)
            && Objects.equals(sharedThrottleLimit, that.sharedThrottleLimit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resiliencyMode, resourceLimits, settings, throttleAttribute, nodeThrottleLimit, sharedThrottleLimit);
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

    public String getThrottleAttribute() {
        return throttleAttribute;
    }

    public Integer getNodeThrottleLimit() {
        return nodeThrottleLimit;
    }

    public Integer getSharedThrottleLimit() {
        return sharedThrottleLimit;
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

    void setThrottleAttribute(String throttleAttribute) {
        validateThrottleAttribute(throttleAttribute);
        this.throttleAttribute = throttleAttribute;
    }

    void setNodeThrottleLimit(Integer nodeThrottleLimit) {
        validateThrottleLimit(NODE_THROTTLE_LIMIT_STRING, nodeThrottleLimit);
        this.nodeThrottleLimit = nodeThrottleLimit;
    }

    void setSharedThrottleLimit(Integer sharedThrottleLimit) {
        validateThrottleLimit(SHARED_THROTTLE_LIMIT_STRING, sharedThrottleLimit);
        this.sharedThrottleLimit = sharedThrottleLimit;
    }

}
