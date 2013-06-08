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
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.values.ValuesSource;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public abstract class ValuesSourceBucketAggregator extends BucketAggregator {

    private final ValuesSource valuesSource;

    public ValuesSourceBucketAggregator(String name, List<Aggregator.Factory> factories, ValuesSource valuesSource, Aggregator parent) {
        super(name, factories, parent);
        this.valuesSource = valuesSource;
    }

    public abstract static class BucketCollector extends BucketAggregator.BucketCollector {

        protected final ValuesSource valuesSource;

        public BucketCollector(BucketAggregator parent, ValuesSource valuesSource) {
            super(parent);
            this.valuesSource = valuesSource;
        }

        public BucketCollector(BucketAggregator parent, ValuesSource valuesSource, Scorer scorer, AtomicReaderContext reader, AggregationContext context) {
            super(parent, scorer, reader, context);
            this.valuesSource = valuesSource;
        }

        public BucketCollector(List<Aggregator> aggregators, ValuesSource valuesSource) {
            super(aggregators);
            this.valuesSource = valuesSource;
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            super.setScorer(scorer);
            valuesSource.setNextScorer(scorer);
        }

        @Override
        protected final AggregationContext setReaderAngGetContext(AtomicReaderContext reader, AggregationContext context) throws IOException {
            valuesSource.setNextReader(reader);
            return setNextValues(valuesSource, context);
        }

        protected abstract AggregationContext setNextValues(ValuesSource valuesSource, AggregationContext context) throws IOException;

    }
}
