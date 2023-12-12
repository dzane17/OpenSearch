/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.telemetry.tracing.AttributeNames;
import org.opensearch.telemetry.tracing.SpanBuilder;
import org.opensearch.telemetry.tracing.SpanContext;
import org.opensearch.telemetry.tracing.SpanScope;
import org.opensearch.telemetry.tracing.Tracer;

import static org.opensearch.core.common.Strings.capitalize;

/**
 * Listener for search request tracing on the coordinator node
 *
 * @opensearch.internal
 */
public final class SearchRequestCoordinatorTrace extends SearchRequestOperationsListener {
    private final Tracer tracer;

    public SearchRequestCoordinatorTrace(Tracer tracer) {
        this.tracer = tracer;
    }

    @Override
    void onPhaseStart(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
        searchRequestContext.setPhaseSpan(
            tracer.startSpan(
                SpanBuilder.from(
                    "coordinator" + capitalize(context.getCurrentPhase().getName()),
                    new SpanContext(searchRequestContext.getRequestSpan())
                )
            )
        );
        SpanScope spanScope = tracer.withSpanInScope(searchRequestContext.getPhaseSpan());
    }

    @Override
    void onPhaseEnd(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
        searchRequestContext.getPhaseSpan().endSpan();
    }

    @Override
    void onPhaseFailure(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
        searchRequestContext.getPhaseSpan().endSpan();
    }

    @Override
    void onRequestStart(SearchRequestContext searchRequestContext) {
        searchRequestContext.setRequestSpan(tracer.startSpan(SpanBuilder.from("coordinatorRequest")));
    }

    @Override
    void onRequestEnd(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
        // add response-related attributes on request end
        if (searchRequestContext.totalHits() != null) {
            searchRequestContext.getRequestSpan().addAttribute(AttributeNames.TOTAL_HITS, searchRequestContext.totalHits().toString());
        }
        if (!searchRequestContext.formattedShardStats().equals("")) {
            searchRequestContext.getRequestSpan().addAttribute(AttributeNames.SHARDS, searchRequestContext.formattedShardStats());
        }
        searchRequestContext.getRequestSpan().addAttribute(AttributeNames.SOURCE, context.getRequest().source().toString());
        searchRequestContext.getRequestSpan().endSpan();
    }
}
