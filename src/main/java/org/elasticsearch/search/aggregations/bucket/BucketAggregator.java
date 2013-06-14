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
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public abstract class BucketAggregator extends Aggregator {

    protected BucketAggregator(String name, SearchContext searchContext, Aggregator parent) {
        super(name, searchContext, parent);
    }

    public static Aggregator[] createAggregators(List<Aggregator.Factory> factories, Aggregator aggregator) {
        int i = 0;
        Aggregator[] aggregators = new Aggregator[factories.size()];
        for (Aggregator.Factory factory : factories) {
            aggregators[i++] = factory.create(aggregator.searchContext(), aggregator);
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

        public BucketCollector(List<Aggregator.Factory> factories, AtomicReaderContext reader, Scorer scorer,
                               AggregationContext context, Aggregator aggregator) {

            this.aggregator = aggregator;
            this.subAggregators = new Aggregator[factories.size()];
            this.collectors = new Collector[subAggregators.length];
            int i = 0;
            for (Aggregator.Factory factory : factories) {
                subAggregators[i] = factory.create(aggregator.searchContext(), aggregator);
                collectors[i] = subAggregators[i].collector();
                try {
                    if (reader != null) {
                        collectors[i].setNextReader(reader, context);
                    }
                    if (scorer != null) {
                        collectors[i].setScorer(scorer);
                    }
                } catch (IOException ioe) {
                    throw new AggregationExecutionException("Failed to aggregate [" + aggregator.name() + "]", ioe);
                }
                i++;
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

    }

}
