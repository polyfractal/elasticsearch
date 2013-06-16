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

package org.elasticsearch.search.aggregations.bucket.single.filter;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.single.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.ReaderBasedDataSource;
import org.elasticsearch.search.aggregations.context.ValuesSourceFactory;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;

/**
 * Aggregate all docs that match a filter.
 */
public class FilterAggregator extends SingleBucketAggregator implements ReaderBasedDataSource<Bits> {

    private final Filter filter;

    long docCount;

    private Bits bits;

    public FilterAggregator(String name,
                            org.apache.lucene.search.Filter filter,
                            List<Aggregator.Factory> factories,
                            SearchContext searchContext,
                            ValuesSourceFactory valuesSourceFactory,
                            Aggregator parent) {

        super(name, factories, searchContext, valuesSourceFactory, parent);
        this.filter = filter;
    }

    @Override
    public Collector collector(Aggregator[] aggregators) {
        return new Collector(aggregators);
    }

    @Override
    protected InternalAggregation buildAggregation(InternalAggregations aggregations) {
        return new InternalFilter(name, docCount, aggregations);
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) throws IOException {
        bits = DocIdSets.toSafeBits(reader.reader(), filter.getDocIdSet(reader, reader.reader().getLiveDocs()));
    }

    class Collector extends SingleBucketAggregator.BucketCollector {

        private Bits bits;
        private long docCount;

        Collector(Aggregator[] subAggregators) {
            super(subAggregators, FilterAggregator.this);
        }

        @Override
        protected AggregationContext setAndGetContext(AggregationContext context) throws IOException {
            return context;
        }

        @Override
        protected boolean onDoc(int doc) throws IOException {
            if (bits.get(doc)) {
                docCount++;
                return true;
            }
            return false;
        }

        @Override
        protected void postCollection(Aggregator[] aggregators) {
            FilterAggregator.this.docCount = docCount;
        }

    }

    public static class Factory extends Aggregator.CompoundFactory<FilterAggregator> {

        private org.apache.lucene.search.Filter filter;

        public Factory(String name, org.apache.lucene.search.Filter filter) {
            super(name);
            this.filter = filter;
        }

        @Override
        public FilterAggregator create(SearchContext searchContext, ValuesSourceFactory valuesSourceFactory, Aggregator parent) {
            return new FilterAggregator(name, filter, factories, searchContext, valuesSourceFactory, parent);
        }

    }
}


