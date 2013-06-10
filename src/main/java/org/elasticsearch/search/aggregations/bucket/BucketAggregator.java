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
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.search.aggregations.AbstractAggregator;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.context.AggregationContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public abstract class BucketAggregator extends AbstractAggregator {

    protected BucketAggregator(String name, Aggregator parent) {
        super(name, parent);
    }


    public abstract static class BucketCollector implements Collector {

        public final Aggregator[] aggregators;
        public final Collector[] collectors;

        public BucketCollector(Aggregator[] aggregators) {
            this.aggregators = aggregators;
            this.collectors = new Collector[aggregators.length];
            for (int i = 0; i < aggregators.length; i++) {
                collectors[i] = aggregators[i].collector();
            }
        }

        public BucketCollector(Aggregator[] aggregators, AtomicReaderContext reader, Scorer scorer, AggregationContext context) {
            this.aggregators = aggregators;
            this.collectors = new Collector[aggregators.length];
            for (int i = 0; i < aggregators.length; i++) {
                collectors[i] = aggregators[i].collector();
                if (reader != null) {
                    collectors[i].setNextReader(reader, context);
                }
            }
        }

        @Override
        public final void postCollection() {
            for (int i = 0; i < collectors.length; i++) {
                if (collectors[i] != null) {
                    collectors[i].postCollection();
                }
            }
            postCollection(aggregators);
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            for (int i = 0; i < collectors.length; i++) {
                if (collectors[i] != null) {
                    collectors[i].setScorer(scorer);
                }
            }
        }

        @Override
        public final void setNextReader(AtomicReaderContext reader, AggregationContext context) throws IOException {
            context = setReaderAngGetContext(reader, context);
            for (int i = 0; i < collectors.length; i++) {
                if (collectors[i] != null) {
                    collectors[i].setNextReader(reader, context);
                }
            }
        }


        @Override
        public final void collect(int doc) throws IOException {
            if (onDoc(doc)) {
                for (int i = 0; i < collectors.length; i++) {
                    if (collectors[i] != null) {
                        collectors[i].collect(doc);
                    }
                }
            }
        }

        /**
         * Called to aggregate the data in the given doc and returns whether the given doc falls within this bucket.
         *
         * @param doc   The doc to aggregate
         * @return      {@code true} if the given doc matched this bucket, {@code false} otherwise.
         * @throws IOException
         */
        protected abstract boolean onDoc(int doc) throws IOException;


        /**
         * Called when the parent aggregation context is set for this bucket, and returns the aggregation context of this bucket (which will
         * be propagated to all sub aggregators/collectors)
         *
         * @param context   The parent context of this bucket
         * @return          The bucket context
         */
        protected abstract AggregationContext setReaderAngGetContext(AtomicReaderContext reader, AggregationContext context) throws IOException;

        /**
         * Called when collection is finished
         *
         * @param aggregators   The sub aggregators of this bucket
         */
        protected abstract void postCollection(Aggregator[] aggregators);

        protected InternalAggregations buildAggregations() {
            List<InternalAggregation> aggregations = Lists.newArrayListWithCapacity(aggregators.length);
            for (Aggregator aggregator : aggregators) {
                aggregations.add(aggregator.buildAggregation());
            }
            return new InternalAggregations(aggregations);
        }
    }


    public static abstract class Factory<A extends Aggregator, F extends Factory<A, F>> extends Aggregator.Factory<A> {

        protected List<Aggregator.Factory> factories = new ArrayList<Aggregator.Factory>();

        protected Factory(String name) {
            super(name);
        }

        @SuppressWarnings("unchecked")
        public F set(List<Aggregator.Factory> factories) {
            this.factories = factories;
            return (F) this;
        }
    }
}
