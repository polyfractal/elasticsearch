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


    public static abstract class Factory<A extends Aggregator, F extends Factory<A, F>> implements Aggregator.Factory<A> {

        protected String name;
        protected List<Aggregator.Factory> factories = new ArrayList<Aggregator.Factory>();

        protected Factory(String name) {
            this.name = name;
        }

        public F add(Aggregator.Factory factory) {
            factories.add(factory);
            return (F) this;
        }

        public F add(List<Aggregator.Factory> factories) {
            this.factories.addAll(factories);
            return (F) this;
        }

        public F set(List<Aggregator.Factory> factories) {
            this.factories = factories;
            return (F) this;
        }
    }

    public abstract static class BucketCollector extends Collector {

        public final List<Aggregator> aggregators;
        public final List<Collector> collectors;


        public BucketCollector(BucketAggregator parent) {
            this(parent, null, null);
        }

        public BucketCollector(BucketAggregator parent, Scorer scorer, AtomicReaderContext context) {
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
                    if (context != null) {
                        collector.setNextReader(context);
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

        protected abstract void postCollection(List<Aggregator> aggregators);

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            for (Collector collector : collectors) {
                collector.setScorer(scorer);
            }
        }

        @Override
        public void collect(int doc, AggregationContext context) throws IOException {
            context = onDoc(doc, context);
            if (context != null) {
                for (Collector collector : collectors) {
                    collector.collect(doc, context);
                }
            }
        }

        /**
         * Returns {@code true} if the given doc falls in this bucket. {@code false} otherwise, which means the sub
         * aggregations will not be calculated (in other words, the sub aggregations are only calculated for the
         * documents that fall in this bucket).
         */
        protected abstract AggregationContext onDoc(int doc, AggregationContext context) throws IOException;

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            for (Collector collector : collectors) {
                collector.setNextReader(context);
            }
        }

        protected InternalAggregations buildAggregations() {
            List<InternalAggregation> aggregations = new ArrayList<InternalAggregation>(aggregators.size());
            for (Aggregator aggregator : aggregators) {
                aggregations.add(aggregator.buildAggregation());
            }
            return new InternalAggregations(aggregations);
        }
    }

}
