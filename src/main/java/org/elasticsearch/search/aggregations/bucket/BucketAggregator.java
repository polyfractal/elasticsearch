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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.context.AggregationContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public abstract class BucketAggregator extends AbstractAggregator {

    protected final List<Aggregator.Factory> factories;

    protected BucketAggregator(String name, List<Aggregator.Factory> factories, Aggregator parent) {
        super(name, parent);
        this.factories = factories;
    }

    public abstract static class BucketCollector implements Collector {

        public final List<Aggregator> aggregators;
        public final List<Collector> collectors;

        public BucketCollector(BucketAggregator parent) {
            this(parent, null, null, null);
        }

        public BucketCollector(BucketAggregator parent, Scorer scorer, AtomicReaderContext reader, AggregationContext context) {
            aggregators = new ArrayList<Aggregator>(parent.factories.size());
            collectors = new ArrayList<Collector>(aggregators.size());
            for (Aggregator.Factory factory : parent.factories) {
                Aggregator aggregator = factory.create(parent);
                aggregators.add(aggregator);
                Collector collector = aggregator.collector();
                try {
                    if (scorer != null) {
                        collector.setScorer(scorer);
                    }
                    if (reader != null) {
                        collector.setNextReader(reader, context);
                    }
                } catch (IOException ioe) {
                    throw  new AggregationExecutionException("Failed to aggregate bucket", ioe);
                }
                collectors.add(collector);
            }
        }

        public BucketCollector(List<Aggregator> aggregators) {
            this.aggregators = aggregators;
            this.collectors = new ArrayList<Collector>(aggregators.size());
            for (Aggregator aggregator : aggregators) {
                Collector collector = aggregator.collector();
                if (collector != null) {
                    collectors.add(collector);
                }
            }
        }

        @Override
        public final void postCollection() {
            for (Collector collector : collectors) {
                collector.postCollection();
            }
            postCollection(aggregators);
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            for (Collector collector : collectors) {
                collector.setScorer(scorer);
            }
        }

        @Override
        public final void setNextReader(AtomicReaderContext reader, AggregationContext context) throws IOException {
            context = setReaderAngGetContext(reader, context);
            for (Collector collector : collectors) {
                collector.setNextReader(reader, context);
            }
        }

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
        protected abstract void postCollection(List<Aggregator> aggregators);

        protected InternalAggregations buildAggregations() {
            List<InternalAggregation> aggregations = new ArrayList<InternalAggregation>(aggregators.size());
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
