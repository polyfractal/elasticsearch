/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.search.aggregations;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.lucene.search.XCollector;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;
import org.elasticsearch.common.lucene.search.XFilteredQuery;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchPhase;
import org.elasticsearch.search.aggregations.bucket.single.global.GlobalAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.ValueSpace;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.query.QueryPhaseExecutionException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class AggregationPhase implements SearchPhase {

    private final AggregationParseElement parseElement;

    private final AggregationBinaryParseElement binaryParseElement;

    @Inject
    public AggregationPhase(AggregationParseElement parseElement, AggregationBinaryParseElement binaryParseElement) {
        this.parseElement = parseElement;
        this.binaryParseElement = binaryParseElement;
    }

    @Override
    public Map<String, ? extends SearchParseElement> parseElements() {
        return ImmutableMap.<String, SearchParseElement>builder()
                .put("aggregations", parseElement)
                .put("aggs", parseElement)
                .put("aggregations_binary", binaryParseElement)
                .put("aggregationsBinary", binaryParseElement)
                .put("aggs_binary", binaryParseElement)
                .put("aggsBinary", binaryParseElement)
                .build();
    }

    @Override
    public void preProcess(SearchContext context) {
        if (context.aggregations() != null) {
            AggregationContext aggregationContext = new AggregationContext(context);
            context.aggregations().aggregationContext(aggregationContext);
            List<Aggregator.Collector> collectors = new ArrayList<Aggregator.Collector>();
            List<Aggregator> aggregators = new ArrayList<Aggregator>(context.aggregations().factories().size());
            for (Aggregator.Factory factory : context.aggregations().factories()) {
                Aggregator aggregator = factory.create(aggregationContext, null);
                aggregators.add(aggregator);
                if (!(aggregator instanceof GlobalAggregator)) {
                    Aggregator.Collector collector = aggregator.collector();
                    if (collector != null) {
                        collectors.add(aggregator.collector());
                    }
                }
            }
            context.aggregations().aggregators(aggregators);
            if (!collectors.isEmpty()) {
                context.searcher().addMainQueryCollector(new AggregationsCollector(collectors, aggregationContext));
            }
        }
    }

    @Override
    public void execute(SearchContext context) throws ElasticSearchException {
        if (context.aggregations() == null) {
            return;
        }

        if (context.queryResult().aggregations() != null) {
            // no need to compute the facets twice, they should be computed on a per context basis
            return;
        }

        List<Aggregator.Collector> globals = new ArrayList<Aggregator.Collector>();
        for (Aggregator aggregator : context.aggregations().aggregators()) {
            if (aggregator instanceof GlobalAggregator) {
                globals.add(aggregator.collector());
            }
        }

        // optimize the global collector based execution
        if (!globals.isEmpty()) {
            AggregationsCollector collector = new AggregationsCollector(globals, context.aggregations().aggregationContext());
            Query query = new XConstantScoreQuery(Queries.MATCH_ALL_FILTER);
            Filter searchFilter = context.searchFilter(context.types());
            if (searchFilter != null) {
                query = new XFilteredQuery(query, searchFilter);
            }
            try {
                context.searcher().search(query, collector);
            } catch (Exception e) {
                throw new QueryPhaseExecutionException(context, "Failed to execute global aggregators", e);
            }
            collector.postCollection();
        }

        List<InternalAggregation> aggregations = new ArrayList<InternalAggregation>(context.aggregations().aggregators().size());
        for (Aggregator aggregator : context.aggregations().aggregators()) {
            aggregations.add(aggregator.buildAggregation());
        }
        context.queryResult().aggregations(new InternalAggregations(aggregations));

    }


    static class AggregationsCollector extends XCollector {

        private final AggregationContext aggregationContext;
        private final List<Aggregator.Collector> collectors;

        AggregationsCollector(List<Aggregator.Collector> collectors, AggregationContext aggregationContext) {
            this.collectors = collectors;
            this.aggregationContext = aggregationContext;
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            aggregationContext.setScorer(scorer);
        }

        @Override
        public void collect(int doc) throws IOException {
            for (int i = 0; i < collectors.size(); i++) {
                collectors.get(i).collect(doc, ValueSpace.DEFAULT);
            }
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            aggregationContext.setNextReader(context);
        }

        @Override
        public boolean acceptsDocsOutOfOrder() {
            return true;
        }

        @Override
        public void postCollection() {
            for (int i = 0; i < collectors.size(); i++) {
                collectors.get(i).postCollection();
            }
        }
    }
}
