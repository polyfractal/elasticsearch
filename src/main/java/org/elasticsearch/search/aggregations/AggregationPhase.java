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
import com.google.common.collect.Maps;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.*;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.lucene.search.XCollector;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;
import org.elasticsearch.common.lucene.search.XFilteredQuery;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchPhase;
import org.elasticsearch.search.aggregations.bucket.single.global.GlobalAggregator;
import org.elasticsearch.search.aggregations.context.DefaultAggregationContext;
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
        return ImmutableMap.of(
                "aggregations", parseElement,
                "aggs", parseElement,
                "aggregations_binary", binaryParseElement,
                "aggregationsBinary", binaryParseElement);
    }

    @Override
    public void preProcess(SearchContext context) {
        if (context.aggregations() != null) {
            List<Aggregator> aggregators = new ArrayList<Aggregator>(context.aggregations().factories().size());
            for (Aggregator.Factory factory : context.aggregations().factories()) {
                Aggregator aggregator = factory.create(context, null);
                aggregators.add(aggregator);
                if (!(aggregator instanceof GlobalAggregator)) {
                    Aggregator.Collector collector = aggregator.collector();
                    if (collector != null) {
                        context.searcher().addMainQueryCollector(new AggregatorCollectorWrapper(collector));
                    }
                }
            }
            context.aggregations().aggregators(aggregators);
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

        Map<Filter, List<Collector>> filtersByCollector = null;
        for (Aggregator aggregator : context.aggregations().aggregators()) {
            if (aggregator instanceof GlobalAggregator) {
                Filter filter = Queries.MATCH_ALL_FILTER;
                Collector collector = new AggregatorCollectorWrapper(aggregator.collector());
                if (filtersByCollector == null) {
                    filtersByCollector = Maps.newHashMap();
                }
                List<Collector> list = filtersByCollector.get(filter);
                if (list == null) {
                    list = new ArrayList<Collector>();
                    filtersByCollector.put(filter, list);
                }
                list.add(collector);
            }
        }

        // optimize the global collector based execution
        if (filtersByCollector != null) {
            // now, go and execute the filters->collector ones
            for (Map.Entry<Filter, List<Collector>> entry : filtersByCollector.entrySet()) {
                Filter filter = entry.getKey();
                Query query = new XConstantScoreQuery(filter);
                Filter searchFilter = context.searchFilter(context.types());
                if (searchFilter != null) {
                    query = new XFilteredQuery(query, searchFilter);
                }
                try {
                    context.searcher().search(query, MultiCollector.wrap(entry.getValue().toArray(new Collector[entry.getValue().size()])));
                } catch (Exception e) {
                    throw new QueryPhaseExecutionException(context, "Failed to execute global aggregators", e);
                }
                for (Collector collector : entry.getValue()) {
                    if (collector instanceof XCollector) {
                        ((XCollector) collector).postCollection();
                    }
                }
            }
        }

        List<InternalAggregation> aggregations = new ArrayList<InternalAggregation>(context.aggregations().aggregators().size());
        for (Aggregator aggregator : context.aggregations().aggregators()) {
            aggregations.add(aggregator.buildAggregation());
        }
        context.queryResult().aggregations(new InternalAggregations(aggregations));

    }


    static class AggregatorCollectorWrapper extends XCollector {

        private final Aggregator.Collector collector;

        AggregatorCollectorWrapper(Aggregator.Collector collector) {
            this.collector = collector;
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            collector.setScorer(scorer);
        }

        @Override
        public void collect(int doc) throws IOException {
            collector.collect(doc);
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            collector.setNextReader(context, DefaultAggregationContext.INSTANCE);
        }

        @Override
        public boolean acceptsDocsOutOfOrder() {
            return true;
        }

        @Override
        public void postCollection() {
            collector.postCollection();
        }
    }
}
