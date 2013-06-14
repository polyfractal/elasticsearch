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

package org.elasticsearch.search.aggregations.bucket.single.missing;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.single.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class UnmappedMissingAggregator extends SingleBucketAggregator {

    long docCount;

    UnmappedMissingAggregator(String name, List<Aggregator.Factory> factories, SearchContext searchContext, Aggregator parent) {
        super(name, factories, searchContext, parent);
    }

    @Override
    protected InternalAggregation buildAggregation(InternalAggregations aggregations) {
        return new InternalMissing(name, docCount, aggregations);
    }

    @Override
    protected Aggregator.Collector collector(Aggregator[] aggregators) {
        return new Collector(aggregators);
    }

    class Collector extends BucketCollector {

        private long docCount;

        Collector(Aggregator[] aggregators) {
            super(aggregators, UnmappedMissingAggregator.this);
        }

        @Override
        protected boolean onDoc(int doc) throws IOException {
            docCount++;
            return true;
        }

        @Override
        protected AggregationContext setReaderAngGetContext(AtomicReaderContext reader, AggregationContext context) throws IOException {
            return context;
        }

        @Override
        protected void postCollection(Aggregator[] aggregators) {
            UnmappedMissingAggregator.this.docCount = docCount;
        }

    }

    public static class Factory extends Aggregator.CompoundFactory<UnmappedMissingAggregator> {

        public Factory(String name) {
            super(name);
        }

        @Override
        public UnmappedMissingAggregator create(SearchContext searchContext, Aggregator parent) {
            return new UnmappedMissingAggregator(name, factories, searchContext, parent);
        }
    }

}
