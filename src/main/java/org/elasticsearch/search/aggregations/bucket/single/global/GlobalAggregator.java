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

package org.elasticsearch.search.aggregations.bucket.single.global;

import com.google.common.collect.Lists;
import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.single.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class GlobalAggregator extends SingleBucketAggregator {

    long docCount;

    public GlobalAggregator(String name, List<Aggregator.Factory> factories) {
        super(name, factories, null);
    }

    @Override
    public Collector collector(Aggregator[] aggregators) {
        return new Collector(aggregators);
    }

    @Override
    public InternalGlobal buildAggregation(Aggregator[] aggregators) {
        List<InternalAggregation> aggregations = Lists.newArrayListWithCapacity(aggregators.length);
        for (Aggregator aggregator : aggregators) {
            aggregations.add(aggregator.buildAggregation());
        }
        return new InternalGlobal(name, docCount, aggregations);
    }

    class Collector extends BucketCollector {

        long docCount;

        Collector(Aggregator[] aggregators) {
            super(aggregators);
        }

        @Override
        protected AggregationContext setReaderAngGetContext(AtomicReaderContext reader, AggregationContext context) throws IOException {
            return context;
        }

        @Override
        protected boolean onDoc(int doc) throws IOException {
            docCount++;
            return true;
        }

        @Override
        protected void postCollection(Aggregator[] aggregators) {
            GlobalAggregator.this.docCount = docCount;
        }
    }

    public static class Factory extends SingleBucketAggregator.Factory<GlobalAggregator, Factory> {

        public Factory(String name) {
            super(name);
        }

        @Override
        public GlobalAggregator create(Aggregator parent) {
            if (parent != null) {
                throw new AggregationExecutionException("Aggregation [" + parent.name() + "] cannot have a global " +
                        "sub-aggregation [" + name + "].Global aggregations can only be defined as top level aggregations");
            }
            return new GlobalAggregator(name, factories);
        }
    }
}
