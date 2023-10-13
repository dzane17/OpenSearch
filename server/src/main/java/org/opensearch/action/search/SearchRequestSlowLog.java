/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.search;

import com.fasterxml.jackson.core.io.JsonStringEncoder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.logging.OpenSearchLogMessage;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.index.SlowLogLevel;
import org.opensearch.tasks.Task;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * The search time slow log implementation
 *
 * @opensearch.internal
 */
public final class SearchRequestSlowLog implements SearchRequestOperationsListener {
    private static final Charset UTF_8 = Charset.forName("UTF-8");

    private long absoluteStartNanos;

    private long warnThreshold;
    private long infoThreshold;
    private long debugThreshold;
    private long traceThreshold;
    Map<String, Long> phaseTookMap;

    private final Logger logger;

    static final String CLUSTER_SEARCH_REQUEST_SLOWLOG_PREFIX = "cluster.search.request.slowlog.threshold";

    public static final Setting<TimeValue> CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_WARN_SETTING = Setting.timeSetting(
        CLUSTER_SEARCH_REQUEST_SLOWLOG_PREFIX + ".warn",
        TimeValue.timeValueNanos(-1),
        TimeValue.timeValueMillis(-1),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_INFO_SETTING = Setting.timeSetting(
        CLUSTER_SEARCH_REQUEST_SLOWLOG_PREFIX + ".info",
        TimeValue.timeValueNanos(-1),
        TimeValue.timeValueMillis(-1),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_DEBUG_SETTING = Setting.timeSetting(
        CLUSTER_SEARCH_REQUEST_SLOWLOG_PREFIX + ".debug",
        TimeValue.timeValueNanos(-1),
        TimeValue.timeValueMillis(-1),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_TRACE_SETTING = Setting.timeSetting(
        CLUSTER_SEARCH_REQUEST_SLOWLOG_PREFIX + ".trace",
        TimeValue.timeValueNanos(-1),
        TimeValue.timeValueMillis(-1),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<SlowLogLevel> CLUSTER_SEARCH_REQUEST_SLOWLOG_LEVEL = new Setting<>(
        CLUSTER_SEARCH_REQUEST_SLOWLOG_PREFIX + ".level",
        SlowLogLevel.TRACE.name(),
        SlowLogLevel::parse,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private static final ToXContent.Params FORMAT_PARAMS = new ToXContent.MapParams(Collections.singletonMap("pretty", "false"));
    private SlowLogLevel level;

    public SearchRequestSlowLog(ClusterService clusterService) {
        this.logger = LogManager.getLogger(CLUSTER_SEARCH_REQUEST_SLOWLOG_PREFIX + ".SearchRequestSlowLog");
        Loggers.setLevel(this.logger, SlowLogLevel.TRACE.name());

        this.warnThreshold = clusterService.getClusterSettings().get(CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_WARN_SETTING).nanos();
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_WARN_SETTING, this::setWarnThreshold);
        this.infoThreshold = clusterService.getClusterSettings().get(CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_INFO_SETTING).nanos();
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_INFO_SETTING, this::setInfoThreshold);
        this.debugThreshold = clusterService.getClusterSettings().get(CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_DEBUG_SETTING).nanos();
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_DEBUG_SETTING, this::setDebugThreshold);
        this.traceThreshold = clusterService.getClusterSettings().get(CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_TRACE_SETTING).nanos();
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_TRACE_SETTING, this::setTraceThreshold);

        this.level = clusterService.getClusterSettings().get(CLUSTER_SEARCH_REQUEST_SLOWLOG_LEVEL);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(CLUSTER_SEARCH_REQUEST_SLOWLOG_LEVEL, this::setLevel);

        this.phaseTookMap = new HashMap<>();
    }

    private void setLevel(SlowLogLevel level) {
        this.level = level;
    }

    @Override
    public void onPhaseStart(SearchPhaseContext context) {}

    @Override
    public void onPhaseEnd(SearchPhaseContext context) {
        long tookInNanos = System.nanoTime() - context.getCurrentPhase().getStartTimeInNanos();
        phaseTookMap.put(context.getCurrentPhase().getName(), TimeUnit.NANOSECONDS.toMillis(tookInNanos));
    }

    @Override
    public void onPhaseFailure(SearchPhaseContext context) {}

    @Override
    public void onRequestStart(long startTimeNanos) {
        this.absoluteStartNanos = startTimeNanos;
    }

    private long getRequestStart() {
        return absoluteStartNanos;
    }

    @Override
    public void onRequestEnd(SearchPhaseContext context) {
        long tookInNanos = System.nanoTime() - absoluteStartNanos;

        if (warnThreshold >= 0 && tookInNanos > warnThreshold && level.isLevelEnabledFor(SlowLogLevel.WARN)) {
            logger.warn(new SearchRequestSlowLogMessage(context, tookInNanos, phaseTookMap));
        } else if (infoThreshold >= 0 && tookInNanos > infoThreshold && level.isLevelEnabledFor(SlowLogLevel.INFO)) {
            logger.info(new SearchRequestSlowLogMessage(context, tookInNanos, phaseTookMap));
        } else if (debugThreshold >= 0 && tookInNanos > debugThreshold && level.isLevelEnabledFor(SlowLogLevel.DEBUG)) {
            logger.debug(new SearchRequestSlowLogMessage(context, tookInNanos, phaseTookMap));
        } else if (traceThreshold >= 0 && tookInNanos > traceThreshold && level.isLevelEnabledFor(SlowLogLevel.TRACE)) {
            logger.trace(new SearchRequestSlowLogMessage(context, tookInNanos, phaseTookMap));
        }
    };

    /**
     * Search slow log message
     *
     * @opensearch.internal
     */
    static final class SearchRequestSlowLogMessage extends OpenSearchLogMessage {

        SearchRequestSlowLogMessage(SearchPhaseContext context, long tookInNanos, Map<String, Long> phaseTookMap) {
            super(prepareMap(context, tookInNanos, phaseTookMap), message(context, tookInNanos, phaseTookMap));
        }

        private static Map<String, Object> prepareMap(SearchPhaseContext context, long tookInNanos, Map<String, Long> phaseTookMap) {
            Map<String, Object> messageFields = new HashMap<>();
            messageFields.put("took", TimeValue.timeValueNanos(tookInNanos));
            messageFields.put("took_millis", TimeUnit.NANOSECONDS.toMillis(tookInNanos));
            messageFields.put("phase_took", phaseTookMap);
            messageFields.put("search_type", context.getRequest().searchType());
            messageFields.put("total_shards", context.getNumShards());

            if (context.getRequest().source() != null) {
                String source = escapeJson(context.getRequest().source().toString(FORMAT_PARAMS));

                messageFields.put("source", source);
            } else {
                messageFields.put("source", "{}");
            }

            messageFields.put("id", context.getTask().getHeader(Task.X_OPAQUE_ID));
            return messageFields;
        }

        // Message will be used in plaintext logs
        private static String message(SearchPhaseContext context, long tookInNanos, Map<String, Long> phaseTookMap) {
            StringBuilder sb = new StringBuilder();
            sb.append("took[").append(TimeValue.timeValueNanos(tookInNanos)).append("], ");
            sb.append("took_millis[").append(TimeUnit.NANOSECONDS.toMillis(tookInNanos)).append("], ");
            sb.append("phase_took_millis[").append(phaseTookMap.toString()).append("], ");
            sb.append("search_type[").append(context.getRequest().searchType()).append("], ");
            sb.append("total_shards[").append(context.getNumShards()).append("], ");
            if (context.getRequest().source() != null) {
                sb.append("source[").append(context.getRequest().source().toString(FORMAT_PARAMS)).append("], ");
            } else {
                sb.append("source[], ");
            }
            if (context.getTask().getHeader(Task.X_OPAQUE_ID) != null) {
                sb.append("id[").append(context.getTask().getHeader(Task.X_OPAQUE_ID)).append("], ");
            } else {
                sb.append("id[]");
            }
            return sb.toString();
        }

        private static String escapeJson(String text) {
            byte[] sourceEscaped = JsonStringEncoder.getInstance().quoteAsUTF8(text);
            return new String(sourceEscaped, UTF_8);
        }
    }

    private void setWarnThreshold(TimeValue warnThreshold) {
        this.warnThreshold = warnThreshold.nanos();
    }

    private void setInfoThreshold(TimeValue infoThreshold) {
        this.infoThreshold = infoThreshold.nanos();
    }

    private void setDebugThreshold(TimeValue debugThreshold) {
        this.debugThreshold = debugThreshold.nanos();
    }

    private void setTraceThreshold(TimeValue traceThreshold) {
        this.traceThreshold = traceThreshold.nanos();
    }

    protected long getWarnThreshold() {
        return warnThreshold;
    }

    protected long getInfoThreshold() {
        return infoThreshold;
    }

    protected long getDebugThreshold() {
        return debugThreshold;
    }

    protected long getTraceThreshold() {
        return traceThreshold;
    }

    SlowLogLevel getLevel() {
        return level;
    }
}
