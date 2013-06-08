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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.GeoPointValues;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.values.ValuesSource;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public abstract class NumericBucketAggregator extends ValuesSourceBucketAggregator {

    public NumericBucketAggregator(String name, List<Aggregator.Factory> factories, ValuesSource valuesSource, Aggregator parent) {
        super(name, factories, valuesSource, parent);
    }

    public abstract class BucketCollector extends ValuesSourceBucketAggregator.BucketCollector implements AggregationContext {

        private final String aggregatorName;

        private AggregationContext parentContext;
        private DoubleValues values;

        protected BucketCollector(String aggregatorName, BucketAggregator parent, ValuesSource valuesSource) {
            super(parent, valuesSource);
            this.aggregatorName = aggregatorName;
        }

        protected BucketCollector(String aggregatorName, BucketAggregator parent, ValuesSource valuesSource, Scorer scorer, AtomicReaderContext reader, AggregationContext context) {
            super(parent, valuesSource, scorer, reader, context);
            this.aggregatorName = aggregatorName;
        }

        protected BucketCollector(String aggregatorName, List<Aggregator> aggregators, ValuesSource valuesSource) {
            super(aggregators, valuesSource);
            this.aggregatorName = aggregatorName;
        }

        @Override
        protected AggregationContext setNextValues(ValuesSource valuesSource, AggregationContext context) throws IOException {
            values = valuesSource.doubleValues();
            if (values == null) {
                values = context.doubleValues();
            }
            if (values == null) {
                throw new AggregationExecutionException("Missing numeric values in aggregation context for aggregator [" + aggregatorName + "]");
            }
            if (!values.isMultiValued()) {
                return context;
            }
            this.parentContext = context;
            return this;
        }

        @Override
        public DoubleValues doubleValues() {
            return values;
        }

        @Override
        public LongValues longValues() {
            return null;
        }

        @Override
        public BytesValues bytesValues() {
            return null;
        }

        @Override
        public GeoPointValues geoPointValues() {
            return null;
        }

        @Override
        public boolean accept(int doc, String valueSourceId, long value) {
            return parentContext.accept(doc, valueSourceId, value);
        }

        @Override
        public boolean accept(int doc, String valueSourceId, BytesRef value) {
            return parentContext.accept(doc, valueSourceId, value);
        }

        @Override
        public boolean accept(int doc, String valueSourceId, GeoPoint value) {
            return parentContext.accept(doc, valueSourceId, value);
        }
    }
}
