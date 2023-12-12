/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.opensearch.search.internal.SearchContext;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.SpanBuilder;
import org.opensearch.telemetry.tracing.Tracer;

/**
 * Tracer listener for shard-level search requests
 *
 * @opensearch.internal
 */
public class SearchShardTrace implements SearchOperationListener {
    private final Tracer tracer;
    private Span querySpan;
    private Span fetchSpan;

    SearchShardTrace(Tracer tracer) {
        this.tracer = tracer;
    }

    @Override
    public void onPreQueryPhase(SearchContext searchContext) {
        SearchOperationListener.super.onPreQueryPhase(searchContext);
        querySpan = tracer.startSpan(SpanBuilder.from("shardQuery", searchContext));
    }

    @Override
    public void onQueryPhase(SearchContext searchContext, long tookInNanos) {
        SearchOperationListener.super.onQueryPhase(searchContext, tookInNanos);
        querySpan.endSpan();
    }

    @Override
    public void onPreFetchPhase(SearchContext searchContext) {
        SearchOperationListener.super.onPreFetchPhase(searchContext);
        fetchSpan = tracer.startSpan(SpanBuilder.from("shardFetch", searchContext));
    }

    @Override
    public void onFetchPhase(SearchContext searchContext, long tookInNanos) {
        SearchOperationListener.super.onFetchPhase(searchContext, tookInNanos);
        fetchSpan.endSpan();
    }
}
