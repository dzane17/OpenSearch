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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.logging.MockAppender;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.SlowLogLevel;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class SearchRequestSlowLogTests extends OpenSearchSingleNodeTestCase {
    static MockAppender appender;
    static Logger logger = LogManager.getLogger(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_PREFIX + ".SearchRequestSlowLog");

    @BeforeClass
    public static void init() throws IllegalAccessException {
        appender = new MockAppender("trace_appender");
        appender.start();
        Loggers.addAppender(logger, appender);
    }

    @AfterClass
    public static void cleanup() {
        Loggers.removeAppender(logger, appender);
        appender.stop();
    }

    public void testMultipleSlowLoggersUseSingleLog4jLogger() {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);

        SearchPhaseContext searchPhaseContext1 = new MockSearchPhaseContext(1);
        ClusterService clusterService1 = new ClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null
        );
        SearchRequestSlowLog searchRequestSlowLog1 = new SearchRequestSlowLog(clusterService1);
        int numberOfLoggersBefore = context.getLoggers().size();

        SearchPhaseContext searchPhaseContext2 = new MockSearchPhaseContext(1);
        ClusterService clusterService2 = new ClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null
        );
        SearchRequestSlowLog searchRequestSlowLog2 = new SearchRequestSlowLog(clusterService2);

        int numberOfLoggersAfter = context.getLoggers().size();
        assertThat(numberOfLoggersAfter, equalTo(numberOfLoggersBefore));
    }

    public void testSearchRequestSlowLogHasJsonFields() throws IOException {
        SearchSourceBuilder source = SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery());
        SearchRequest searchRequest = new SearchRequest().source(source);
        SearchPhaseContext searchPhaseContext = new MockSearchPhaseContext(1, searchRequest);
        Map<String, Long> phaseTookMap = new HashMap<>();
        phaseTookMap.put(SearchPhaseName.FETCH.getName(), 10L);
        phaseTookMap.put(SearchPhaseName.QUERY.getName(), 50L);
        phaseTookMap.put(SearchPhaseName.EXPAND.getName(), 5L);
        SearchRequestSlowLog.SearchRequestSlowLogMessage p = new SearchRequestSlowLog.SearchRequestSlowLogMessage(
            searchPhaseContext,
            10,
            phaseTookMap
        );

        assertThat(p.getValueFor("took"), equalTo("10nanos"));
        assertThat(p.getValueFor("took_millis"), equalTo("0"));
        assertThat(p.getValueFor("phase_took"), equalTo(phaseTookMap.toString()));
        assertThat(p.getValueFor("search_type"), equalTo("QUERY_THEN_FETCH"));
        assertThat(p.getValueFor("total_shards"), equalTo("1"));
        assertThat(p.getValueFor("source"), equalTo("{\\\"query\\\":{\\\"match_all\\\":{\\\"boost\\\":1.0}}}"));
        assertThat(p.getValueFor("id"), equalTo(null));
    }

    public void testSearchRequestSlowLogSearchContextPrinterToLog() throws IOException {
        SearchSourceBuilder source = SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery());
        SearchRequest searchRequest = new SearchRequest().source(source);
        SearchPhaseContext searchPhaseContext = new MockSearchPhaseContext(1, searchRequest);
        Map<String, Long> phaseTookMap = new HashMap<>();
        phaseTookMap.put(SearchPhaseName.FETCH.getName(), 10L);
        phaseTookMap.put(SearchPhaseName.QUERY.getName(), 50L);
        phaseTookMap.put(SearchPhaseName.EXPAND.getName(), 5L);
        SearchRequestSlowLog.SearchRequestSlowLogMessage p = new SearchRequestSlowLog.SearchRequestSlowLogMessage(
            searchPhaseContext,
            100000,
            phaseTookMap
        );

        assertThat(p.getFormattedMessage(), startsWith("took[100micros]"));
        // Makes sure that output doesn't contain any new lines
        assertThat(p.getFormattedMessage(), not(containsString("\n")));
        assertThat(p.getFormattedMessage(), endsWith("id[]"));
    }

    public void testLevelSettingWarn() {
        SlowLogLevel level = SlowLogLevel.WARN;
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_LEVEL.getKey(), level);
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterService clusterService = new ClusterService(settings, clusterSettings, null);
        SearchRequestSlowLog searchRequestSlowLog = new SearchRequestSlowLog(clusterService);
        assertEquals(level, searchRequestSlowLog.getLevel());
    }

    public void testLevelSettingDebug() {
        SlowLogLevel level = SlowLogLevel.DEBUG;
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_LEVEL.getKey(), level);
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterService clusterService = new ClusterService(settings, clusterSettings, null);
        SearchRequestSlowLog searchRequestSlowLog = new SearchRequestSlowLog(clusterService);
        assertEquals(level, searchRequestSlowLog.getLevel());
    }

    public void testLevelSettingFail() {
        String level = "NOT A LEVEL";
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_LEVEL.getKey(), level);
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterService clusterService = new ClusterService(settings, clusterSettings, null);

        try {
            new SearchRequestSlowLog(clusterService);
            fail();
        } catch (IllegalArgumentException ex) {
            final String expected = "No enum constant org.opensearch.index.SlowLogLevel.NOT A LEVEL";
            assertThat(ex, hasToString(containsString(expected)));
            assertThat(ex, instanceOf(IllegalArgumentException.class));
        }
    }

    public void testSetThresholds() {
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_WARN_SETTING.getKey(), "400ms");
        settingsBuilder.put(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_INFO_SETTING.getKey(), "300ms");
        settingsBuilder.put(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_DEBUG_SETTING.getKey(), "200ms");
        settingsBuilder.put(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_TRACE_SETTING.getKey(), "100ms");
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterService clusterService = new ClusterService(settings, clusterSettings, null);
        SearchRequestSlowLog searchRequestSlowLog = new SearchRequestSlowLog(clusterService);
        assertEquals(TimeValue.timeValueMillis(400).nanos(), searchRequestSlowLog.getWarnThreshold());
        assertEquals(TimeValue.timeValueMillis(300).nanos(), searchRequestSlowLog.getInfoThreshold());
        assertEquals(TimeValue.timeValueMillis(200).nanos(), searchRequestSlowLog.getDebugThreshold());
        assertEquals(TimeValue.timeValueMillis(100).nanos(), searchRequestSlowLog.getTraceThreshold());
        assertEquals(SlowLogLevel.TRACE, searchRequestSlowLog.getLevel());
    }

    public void testSetThresholdsUnits() {
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_WARN_SETTING.getKey(), "400s");
        settingsBuilder.put(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_INFO_SETTING.getKey(), "300ms");
        settingsBuilder.put(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_DEBUG_SETTING.getKey(), "200micros");
        settingsBuilder.put(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_TRACE_SETTING.getKey(), "100nanos");
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterService clusterService = new ClusterService(settings, clusterSettings, null);
        SearchRequestSlowLog searchRequestSlowLog = new SearchRequestSlowLog(clusterService);
        assertEquals(TimeValue.timeValueSeconds(400).nanos(), searchRequestSlowLog.getWarnThreshold());
        assertEquals(TimeValue.timeValueMillis(300).nanos(), searchRequestSlowLog.getInfoThreshold());
        assertEquals(TimeValue.timeValueNanos(200000).nanos(), searchRequestSlowLog.getDebugThreshold());
        assertEquals(TimeValue.timeValueNanos(100).nanos(), searchRequestSlowLog.getTraceThreshold());
        assertEquals(SlowLogLevel.TRACE, searchRequestSlowLog.getLevel());
    }

    public void testSetThresholdsDefaults() {
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_WARN_SETTING.getKey(), "400ms");
        settingsBuilder.put(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_DEBUG_SETTING.getKey(), "200ms");
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterService clusterService = new ClusterService(settings, clusterSettings, null);
        SearchRequestSlowLog searchRequestSlowLog = new SearchRequestSlowLog(clusterService);
        assertEquals(TimeValue.timeValueMillis(400).nanos(), searchRequestSlowLog.getWarnThreshold());
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), searchRequestSlowLog.getInfoThreshold());
        assertEquals(TimeValue.timeValueMillis(200).nanos(), searchRequestSlowLog.getDebugThreshold());
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), searchRequestSlowLog.getTraceThreshold());
        assertEquals(SlowLogLevel.TRACE, searchRequestSlowLog.getLevel());
    }

    public void testSetThresholdsError() {
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_WARN_SETTING.getKey(), "NOT A TIME VALUE");
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterService clusterService = new ClusterService(settings, clusterSettings, null);

        try {
            new SearchRequestSlowLog(clusterService);
            fail();
        } catch (IllegalArgumentException ex) {
            final String expected =
                "failed to parse setting [cluster.search.request.slowlog.threshold.warn] with value [NOT A TIME VALUE] as a time value";
            assertThat(ex, hasToString(containsString(expected)));
            assertThat(ex, instanceOf(IllegalArgumentException.class));
        }
    }
}
