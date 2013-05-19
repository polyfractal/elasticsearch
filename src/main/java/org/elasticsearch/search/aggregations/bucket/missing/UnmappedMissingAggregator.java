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

package org.elasticsearch.search.aggregations.bucket.missing;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.BucketAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class UnmappedMissingAggregator extends BucketAggregator {

    long docCount;
    List<Aggregator> aggregators;

    UnmappedMissingAggregator(String name, List<Aggregator.Factory> factories, Aggregator parent) {
        super(name, factories, parent);
    }

    @Override
    public Collector collector() {
        return new Collector();
    }

    @Override
    public InternalMissing buildAggregation() {
        List<InternalAggregation> aggregationResults = new ArrayList<InternalAggregation>(aggregators.size());
        for (Aggregator aggregator : aggregators) {
            aggregationResults.add(aggregator.buildAggregation());
        }
        return new InternalMissing(name, docCount, aggregationResults);
    }

    class Collector extends BucketCollector {

        private long count;

        Collector() {
            super(UnmappedMissingAggregator.this);
        }

        @Override
        protected AggregationContext onDoc(int doc, AggregationContext context) throws IOException {
            count++;
            return context;
        }

        @Override
        protected void postCollection(List<Aggregator> aggregators) {
            UnmappedMissingAggregator.this.docCount = count;
            UnmappedMissingAggregator.this.aggregators = aggregators;
        }

    }

    public static class Factory extends BucketAggregator.Factory<UnmappedMissingAggregator, Factory> {

        public Factory(String name) {
            super(name);
        }

        @Override
        public UnmappedMissingAggregator create(Aggregator parent) {
            return new UnmappedMissingAggregator(name, factories, parent);
        }
    }

}
