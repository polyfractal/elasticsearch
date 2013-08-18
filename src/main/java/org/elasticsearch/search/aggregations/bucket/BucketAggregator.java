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

package org.elasticsearch.search.aggregations.bucket;

import com.google.common.collect.Lists;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.ValueSpace;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public abstract class BucketAggregator extends Aggregator {

    protected BucketAggregator(String name, AggregationContext aggregationContext, Aggregator parent) {
        super(name, aggregationContext, parent);
    }

    public static Aggregator[] createAggregators(List<Aggregator.Factory> factories, Aggregator aggregator) {
        int i = 0;
        Aggregator[] aggregators = new Aggregator[factories.size()];
        for (Aggregator.Factory factory : factories) {
            aggregators[i++] = factory.create(aggregator.aggregationContext(), aggregator);
        }
        return aggregators;
    }

    public static InternalAggregations buildAggregations(Aggregator[] aggregators) {
        List<InternalAggregation> aggregations = Lists.newArrayListWithCapacity(aggregators.length);
        for (int i = 0; i < aggregators.length; i++) {
            aggregations.add(aggregators[i].buildAggregation());
        }
        return new InternalAggregations(aggregations);
    }

    public abstract static class BucketCollector implements Collector {

        protected final Aggregator aggregator;
        public final Aggregator[] subAggregators;
        public final Collector[] collectors;

        public BucketCollector(Aggregator[] subAggregators, Aggregator aggregator) {
            this.aggregator = aggregator;
            this.subAggregators = subAggregators;
            this.collectors = new Collector[subAggregators.length];
            for (int i = 0; i < subAggregators.length; i++) {
                collectors[i] = subAggregators[i].collector();
            }
        }

        public BucketCollector(List<Aggregator.Factory> factories, Aggregator aggregator) {
            this.aggregator = aggregator;
            this.subAggregators = new Aggregator[factories.size()];
            this.collectors = new Collector[subAggregators.length];
            for (int i = 0; i < factories.size(); i++) {
                subAggregators[i] = factories.get(i).create(aggregator.aggregationContext(), aggregator);
                collectors[i] = subAggregators[i].collector();
            }
        }

        @Override
        public final void postCollection() {
            for (int i = 0; i < collectors.length; i++) {
                if (collectors[i] != null) {
                    collectors[i].postCollection();
                }
            }
            postCollection(subAggregators);
        }

        @Override
        public void collect(int doc, ValueSpace valueSpace) throws IOException {
            valueSpace = onDoc(doc, valueSpace);
            if (valueSpace != null) {
                for (int i = 0; i < collectors.length; i++) {
                    if (collectors[i] != null) {
                        collectors[i].collect(doc, valueSpace);
                    }
                }
            }
        }

        /**
         * Called to aggregate the data in the given doc and returns whether the value space that should be used for all sub-aggregators
         * of this bucket.
         *
         * @param doc   The doc to aggregate
         * @return      The value space for all the sub-aggregator of this bucket. If the doc doesn't "fall" within this bucket, this
         *              method <strong>must</strong> return {@code null} (in which case, the sub-aggregators will not be asked to collect
         *              the doc)
         * @throws IOException
         */
        protected abstract ValueSpace onDoc(int doc, ValueSpace valueSpace) throws IOException;

        /**
         * Called when collection is finished
         *
         * @param aggregators   The sub aggregators of this bucket
         */
        protected abstract void postCollection(Aggregator[] aggregators);

    }

}
