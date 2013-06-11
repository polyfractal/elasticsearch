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
import org.elasticsearch.search.aggregations.ValuesSourceAggregator;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.ValuesSource;
import org.elasticsearch.search.aggregations.context.ValuesSourceBased;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public abstract class ValuesSourceBucketAggregator<VS extends ValuesSource> extends ValuesSourceAggregator<VS> implements ValuesSourceBased {

    public ValuesSourceBucketAggregator(String name, VS valuesSource, Class<VS> valuesSourceType, Aggregator parent) {
        super(name, valuesSource, valuesSourceType, parent);
    }

    @Override
    public ValuesSource valuesSource() {
        return valuesSource;
    }

    protected static abstract class BucketCollector<VS extends ValuesSource> implements Collector {

        protected final VS valuesSource;
        protected final String aggregationName;
        public final Aggregator[] aggregators;
        public final Collector[] collectors;

        protected AggregationContext parentContext;

        public BucketCollector(String aggregatorName, VS valuesSource, Aggregator[] aggregators) {
            this.valuesSource = valuesSource;
            this.aggregationName = aggregatorName;
            this.aggregators = aggregators;
            this.collectors = new Collector[aggregators.length];
            for (int i = 0; i < aggregators.length; i++) {
                collectors[i] = aggregators[i].collector();
            }
        }

        public BucketCollector(String aggregationName, VS valuesSource, List<Aggregator.Factory> factories, AtomicReaderContext reader,
                               Scorer scorer, AggregationContext context, Aggregator parent) {
            this.valuesSource = valuesSource;
            this.aggregationName = aggregationName;
            this.aggregators = new Aggregator[factories.size()];
            this.collectors = new Collector[aggregators.length];
            int i = 0;
            for (Aggregator.Factory factory : factories) {
                aggregators[i] = factory.create(parent);
                collectors[i] = aggregators[i].collector();
                try {
                    if (reader != null) {
                        collectors[i].setNextReader(reader, context);
                    }
                    if (scorer != null) {
                        collectors[i].setScorer(scorer);
                    }
                } catch (IOException ioe) {
                    throw new AggregationExecutionException("Failed to aggregate [" + aggregationName + "]", ioe);
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
            this.parentContext = context;
            valuesSource.setNextReader(reader);
            context = setNextValues(valuesSource, context);
            for (int i = 0; i < collectors.length; i++) {
                if (collectors[i] != null) {
                    collectors[i].setNextReader(reader, context);
                }
            }
        }


        @Override
        public final void collect(int doc) throws IOException {
            if (onDoc(doc, parentContext)) {
                for (int i = 0; i < collectors.length; i++) {
                    if (collectors[i] != null) {
                        collectors[i].collect(doc);
                    }
                }
            }
        }

        protected abstract AggregationContext setNextValues(VS valuesSource, AggregationContext context) throws IOException;

        protected abstract boolean onDoc(int doc, AggregationContext context) throws IOException;

        protected abstract void postCollection(Aggregator[] aggregators);

    }

}
