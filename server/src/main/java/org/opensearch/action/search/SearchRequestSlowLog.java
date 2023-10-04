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

    private long overallWarnThreshold;
    private long overallInfoThreshold;
    private long overallDebugThreshold;
    private long overallTraceThreshold;

    private long queryWarnThreshold;
    private long queryInfoThreshold;
    private long queryDebugThreshold;
    private long queryTraceThreshold;

    private long fetchWarnThreshold;
    private long fetchInfoThreshold;
    private long fetchDebugThreshold;
    private long fetchTraceThreshold;

    private final Logger overallLogger;
    private final Logger queryLogger;
    private final Logger fetchLogger;

    static final String CLUSTER_SEARCH_REQUEST_SLOWLOG_PREFIX = "cluster.search.request.slowlog.threshold";

    // overall settings
    public static final Setting<TimeValue> CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_OVERALL_WARN_SETTING = Setting.timeSetting(
        CLUSTER_SEARCH_REQUEST_SLOWLOG_PREFIX + ".overall.warn",
        TimeValue.timeValueNanos(-1),
        TimeValue.timeValueMillis(-1),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_OVERALL_INFO_SETTING = Setting.timeSetting(
        CLUSTER_SEARCH_REQUEST_SLOWLOG_PREFIX + ".overall.info",
        TimeValue.timeValueNanos(-1),
        TimeValue.timeValueMillis(-1),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_OVERALL_DEBUG_SETTING = Setting.timeSetting(
        CLUSTER_SEARCH_REQUEST_SLOWLOG_PREFIX + ".overall.debug",
        TimeValue.timeValueNanos(-1),
        TimeValue.timeValueMillis(-1),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_OVERALL_TRACE_SETTING = Setting.timeSetting(
        CLUSTER_SEARCH_REQUEST_SLOWLOG_PREFIX + ".overall.trace",
        TimeValue.timeValueNanos(-1),
        TimeValue.timeValueMillis(-1),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    // query settings
    public static final Setting<TimeValue> CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING = Setting.timeSetting(
        CLUSTER_SEARCH_REQUEST_SLOWLOG_PREFIX + ".query.warn",
        TimeValue.timeValueNanos(-1),
        TimeValue.timeValueMillis(-1),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING = Setting.timeSetting(
        CLUSTER_SEARCH_REQUEST_SLOWLOG_PREFIX + ".query.info",
        TimeValue.timeValueNanos(-1),
        TimeValue.timeValueMillis(-1),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING = Setting.timeSetting(
        CLUSTER_SEARCH_REQUEST_SLOWLOG_PREFIX + ".query.debug",
        TimeValue.timeValueNanos(-1),
        TimeValue.timeValueMillis(-1),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING = Setting.timeSetting(
        CLUSTER_SEARCH_REQUEST_SLOWLOG_PREFIX + ".query.trace",
        TimeValue.timeValueNanos(-1),
        TimeValue.timeValueMillis(-1),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    // fetch settings
    public static final Setting<TimeValue> CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_FETCH_WARN_SETTING = Setting.timeSetting(
        CLUSTER_SEARCH_REQUEST_SLOWLOG_PREFIX + ".fetch.warn",
        TimeValue.timeValueNanos(-1),
        TimeValue.timeValueMillis(-1),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_FETCH_INFO_SETTING = Setting.timeSetting(
        CLUSTER_SEARCH_REQUEST_SLOWLOG_PREFIX + ".fetch.info",
        TimeValue.timeValueNanos(-1),
        TimeValue.timeValueMillis(-1),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_FETCH_DEBUG_SETTING = Setting.timeSetting(
        CLUSTER_SEARCH_REQUEST_SLOWLOG_PREFIX + ".fetch.debug",
        TimeValue.timeValueNanos(-1),
        TimeValue.timeValueMillis(-1),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_FETCH_TRACE_SETTING = Setting.timeSetting(
        CLUSTER_SEARCH_REQUEST_SLOWLOG_PREFIX + ".fetch.trace",
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
        this.overallLogger = LogManager.getLogger(CLUSTER_SEARCH_REQUEST_SLOWLOG_PREFIX + ".overall");
        this.queryLogger = LogManager.getLogger(CLUSTER_SEARCH_REQUEST_SLOWLOG_PREFIX + ".query");
        this.fetchLogger = LogManager.getLogger(CLUSTER_SEARCH_REQUEST_SLOWLOG_PREFIX + ".fetch");
        Loggers.setLevel(this.overallLogger, SlowLogLevel.TRACE.name());
        Loggers.setLevel(this.queryLogger, SlowLogLevel.TRACE.name());
        Loggers.setLevel(this.fetchLogger, SlowLogLevel.TRACE.name());

        this.overallWarnThreshold = clusterService.getClusterSettings().get(CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_OVERALL_WARN_SETTING).nanos();
        clusterService.getClusterSettings().addSettingsUpdateConsumer(CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_OVERALL_WARN_SETTING, this::setOverallWarnThreshold);
        this.overallInfoThreshold = clusterService.getClusterSettings().get(CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_OVERALL_INFO_SETTING).nanos();
        clusterService.getClusterSettings().addSettingsUpdateConsumer(CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_OVERALL_INFO_SETTING, this::setOverallInfoThreshold);
        this.overallDebugThreshold = clusterService.getClusterSettings().get(CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_OVERALL_DEBUG_SETTING).nanos();
        clusterService.getClusterSettings().addSettingsUpdateConsumer(CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_OVERALL_DEBUG_SETTING, this::setOverallDebugThreshold);
        this.overallTraceThreshold = clusterService.getClusterSettings().get(CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_OVERALL_TRACE_SETTING).nanos();
        clusterService.getClusterSettings().addSettingsUpdateConsumer(CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_OVERALL_TRACE_SETTING, this::setOverallDebugThreshold);

        this.queryWarnThreshold = clusterService.getClusterSettings().get(CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING).nanos();
        clusterService.getClusterSettings().addSettingsUpdateConsumer(CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING, this::setQueryWarnThreshold);
        this.queryInfoThreshold = clusterService.getClusterSettings().get(CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING).nanos();
        clusterService.getClusterSettings().addSettingsUpdateConsumer(CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING, this::setQueryInfoThreshold);
        this.queryDebugThreshold = clusterService.getClusterSettings().get(CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING).nanos();
        clusterService.getClusterSettings().addSettingsUpdateConsumer(CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING, this::setQueryDebugThreshold);
        this.queryTraceThreshold = clusterService.getClusterSettings().get(CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING).nanos();
        clusterService.getClusterSettings().addSettingsUpdateConsumer(CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING, this::setQueryTraceThreshold);

        this.fetchWarnThreshold = clusterService.getClusterSettings().get(CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_FETCH_WARN_SETTING).nanos();
        clusterService.getClusterSettings().addSettingsUpdateConsumer(CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_FETCH_WARN_SETTING, this::setFetchWarnThreshold);
        this.fetchInfoThreshold = clusterService.getClusterSettings().get(CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_FETCH_INFO_SETTING).nanos();
        clusterService.getClusterSettings().addSettingsUpdateConsumer(CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_FETCH_INFO_SETTING, this::setFetchInfoThreshold);
        this.fetchDebugThreshold = clusterService.getClusterSettings().get(CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_FETCH_DEBUG_SETTING).nanos();
        clusterService.getClusterSettings().addSettingsUpdateConsumer(CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_FETCH_DEBUG_SETTING, this::setFetchDebugThreshold);
        this.fetchTraceThreshold = clusterService.getClusterSettings().get(CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_FETCH_TRACE_SETTING).nanos();
        clusterService.getClusterSettings().addSettingsUpdateConsumer(CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_FETCH_TRACE_SETTING, this::setFetchTraceThreshold);

        this.level = clusterService.getClusterSettings().get(CLUSTER_SEARCH_REQUEST_SLOWLOG_LEVEL);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(CLUSTER_SEARCH_REQUEST_SLOWLOG_LEVEL, this::setLevel);
    }

    private void setLevel(SlowLogLevel level) {
        this.level = level;
    }

    public void onQueryPhaseEnd(SearchPhaseContext context, long tookInNanos) {
        if (queryWarnThreshold >= 0 && tookInNanos > queryWarnThreshold && level.isLevelEnabledFor(SlowLogLevel.WARN)) {
            queryLogger.warn(new SearchRequestSlowLogMessage(context, tookInNanos));
        } else if (queryInfoThreshold >= 0 && tookInNanos > queryInfoThreshold && level.isLevelEnabledFor(SlowLogLevel.INFO)) {
            queryLogger.info(new SearchRequestSlowLogMessage(context, tookInNanos));
        } else if (queryDebugThreshold >= 0 && tookInNanos > queryDebugThreshold && level.isLevelEnabledFor(SlowLogLevel.DEBUG)) {
            queryLogger.debug(new SearchRequestSlowLogMessage(context, tookInNanos));
        } else if (queryTraceThreshold >= 0 && tookInNanos > queryTraceThreshold && level.isLevelEnabledFor(SlowLogLevel.TRACE)) {
            queryLogger.trace(new SearchRequestSlowLogMessage(context, tookInNanos));
        }
    }

    public void onFetchPhaseEnd(SearchPhaseContext context, long tookInNanos) {
        if (fetchWarnThreshold >= 0 && tookInNanos > fetchWarnThreshold && level.isLevelEnabledFor(SlowLogLevel.WARN)) {
            fetchLogger.warn(new SearchRequestSlowLogMessage(context, tookInNanos));
        } else if (fetchInfoThreshold >= 0 && tookInNanos > fetchInfoThreshold && level.isLevelEnabledFor(SlowLogLevel.INFO)) {
            fetchLogger.info(new SearchRequestSlowLogMessage(context, tookInNanos));
        } else if (fetchDebugThreshold >= 0 && tookInNanos > fetchDebugThreshold && level.isLevelEnabledFor(SlowLogLevel.DEBUG)) {
            fetchLogger.debug(new SearchRequestSlowLogMessage(context, tookInNanos));
        } else if (fetchTraceThreshold >= 0 && tookInNanos > fetchTraceThreshold && level.isLevelEnabledFor(SlowLogLevel.TRACE)) {
            fetchLogger.trace(new SearchRequestSlowLogMessage(context, tookInNanos));
        }
    }

    @Override
    public void onPhaseStart(SearchPhaseContext context) {}

    @Override
    public void onPhaseEnd(SearchPhaseContext context) {
        long tookInNanos = System.nanoTime() - context.getCurrentPhase().getStartTimeInNanos();

        switch (context.getCurrentPhase().getSearchPhaseName()) {
            case QUERY:
                onQueryPhaseEnd(context, tookInNanos);
                break;
            case FETCH:
                onFetchPhaseEnd(context, tookInNanos);
                break;
        }
    }

    @Override
    public void onPhaseFailure(SearchPhaseContext context) {}

    @Override
    public void onRequestStart() {
        this.absoluteStartNanos = System.nanoTime();
    }

    @Override
    public void onRequestEnd(SearchPhaseContext context) {
        long tookInNanos = System.nanoTime() - absoluteStartNanos;

        if (overallWarnThreshold >= 0 && tookInNanos > overallWarnThreshold && level.isLevelEnabledFor(SlowLogLevel.WARN)) {
            overallLogger.warn(new SearchRequestSlowLogMessage(context, tookInNanos));
        } else if (overallInfoThreshold >= 0 && tookInNanos > overallInfoThreshold && level.isLevelEnabledFor(SlowLogLevel.INFO)) {
            overallLogger.info(new SearchRequestSlowLogMessage(context, tookInNanos));
        } else if (overallDebugThreshold >= 0 && tookInNanos > overallDebugThreshold && level.isLevelEnabledFor(SlowLogLevel.DEBUG)) {
            overallLogger.debug(new SearchRequestSlowLogMessage(context, tookInNanos));
        } else if (overallTraceThreshold >= 0 && tookInNanos > overallTraceThreshold && level.isLevelEnabledFor(SlowLogLevel.TRACE)) {
            overallLogger.trace(new SearchRequestSlowLogMessage(context, tookInNanos));
        }
    };

    /**
     * Search slow log message
     *
     * @opensearch.internal
     */
    static final class SearchRequestSlowLogMessage extends OpenSearchLogMessage {

        SearchRequestSlowLogMessage(SearchPhaseContext context, long tookInNanos) {
            super(prepareMap(context, tookInNanos), message(context, tookInNanos));
        }

        private static Map<String, Object> prepareMap(SearchPhaseContext context, long tookInNanos) {
            Map<String, Object> messageFields = new HashMap<>();
            messageFields.put("took", TimeValue.timeValueNanos(tookInNanos));
            messageFields.put("took_millis", TimeUnit.NANOSECONDS.toMillis(tookInNanos));
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
        private static String message(SearchPhaseContext context, long tookInNanos) {
            StringBuilder sb = new StringBuilder();
            sb.append("took[")
                .append(TimeValue.timeValueNanos(tookInNanos))
                .append("], ");
            sb.append("took_millis[")
                .append(TimeUnit.NANOSECONDS.toMillis(tookInNanos))
                .append("], ");
            sb.append("search_type[")
                .append(context.getRequest().searchType())
                .append("], ");
            sb.append("total_shards[")
                .append(context.getNumShards())
                .append("], ");
            if (context.getRequest().source() != null) {
                sb.append("source[").append(context.getRequest().source().toString(FORMAT_PARAMS)).append("], ");
            } else {
                sb.append("source[], ");
            }
            if (context.getTask().getHeader(Task.X_OPAQUE_ID) != null) {
                sb.append("id[").append(context.getTask().getHeader(Task.X_OPAQUE_ID)).append("], ");
            } else {
                sb.append("id[], ");
            }
            return sb.toString();
        }

        private static String escapeJson(String text) {
            byte[] sourceEscaped = JsonStringEncoder.getInstance().quoteAsUTF8(text);
            return new String(sourceEscaped, UTF_8);
        }
    }

    private void setOverallWarnThreshold(TimeValue warnThreshold) {
        this.overallWarnThreshold = warnThreshold.nanos();
    }

    private void setOverallInfoThreshold(TimeValue infoThreshold) {
        this.overallInfoThreshold = infoThreshold.nanos();
    }

    private void setOverallDebugThreshold(TimeValue debugThreshold) {
        this.overallDebugThreshold = debugThreshold.nanos();
    }

    private void setOverallTraceThreshold(TimeValue traceThreshold) {
        this.overallTraceThreshold = traceThreshold.nanos();
    }

    private void setQueryWarnThreshold(TimeValue warnThreshold) {
        this.queryWarnThreshold = warnThreshold.nanos();
    }

    private void setQueryInfoThreshold(TimeValue infoThreshold) {
        this.queryInfoThreshold = infoThreshold.nanos();
    }

    private void setQueryDebugThreshold(TimeValue debugThreshold) {
        this.queryDebugThreshold = debugThreshold.nanos();
    }

    private void setQueryTraceThreshold(TimeValue traceThreshold) {
        this.queryTraceThreshold = traceThreshold.nanos();
    }

    private void setFetchWarnThreshold(TimeValue warnThreshold) {
        this.fetchWarnThreshold = warnThreshold.nanos();
    }

    private void setFetchInfoThreshold(TimeValue infoThreshold) {
        this.fetchInfoThreshold = infoThreshold.nanos();
    }

    private void setFetchDebugThreshold(TimeValue debugThreshold) {
        this.fetchDebugThreshold = debugThreshold.nanos();
    }

    private void setFetchTraceThreshold(TimeValue traceThreshold) {
        this.fetchTraceThreshold = traceThreshold.nanos();
    }

    long getQueryWarnThreshold() {
        return queryWarnThreshold;
    }

    long getQueryInfoThreshold() {
        return queryInfoThreshold;
    }

    long getQueryDebugThreshold() {
        return queryDebugThreshold;
    }

    long getQueryTraceThreshold() {
        return queryTraceThreshold;
    }

    long getFetchWarnThreshold() {
        return fetchWarnThreshold;
    }

    long getFetchInfoThreshold() {
        return fetchInfoThreshold;
    }

    long getFetchDebugThreshold() {
        return fetchDebugThreshold;
    }

    long getFetchTraceThreshold() {
        return fetchTraceThreshold;
    }

    SlowLogLevel getLevel() {
        return level;
    }
}
