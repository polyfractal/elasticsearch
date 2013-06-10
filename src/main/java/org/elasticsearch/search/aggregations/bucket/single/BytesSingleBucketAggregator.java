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

package org.elasticsearch.search.aggregations.bucket.single;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.bytes.BytesValuesSource;
import org.elasticsearch.search.aggregations.context.doubles.DoubleValuesSource;
import org.elasticsearch.search.aggregations.context.geopoints.GeoPointValuesSource;
import org.elasticsearch.search.aggregations.context.longs.LongValuesSource;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public abstract class BytesSingleBucketAggregator extends ValuesSourceSingleBucketAggregator<BytesValuesSource> {

    public BytesSingleBucketAggregator(String name, List<Aggregator.Factory> factories, Aggregator parent) {
        super(name, factories, parent);
    }

    public BytesSingleBucketAggregator(String name, List<Aggregator.Factory> factories, BytesValuesSource valuesSource, Aggregator parent) {
        super(name, factories, valuesSource, parent);
    }

    public static abstract class BucketCollector extends ValuesSourceSingleBucketAggregator.BucketCollector<BytesValuesSource> implements AggregationContext {

        private final String aggregatorName;

        private BytesValues values;
        private AggregationContext parentContext;

        public BucketCollector(String aggregatorName, Aggregator[] aggregators, BytesValuesSource valuesSource) {
            super(aggregators, valuesSource);
            this.aggregatorName = aggregatorName;
        }

        @Override
        protected AggregationContext setNextValues(BytesValuesSource valuesSource, AggregationContext context) throws IOException {
            values = valuesSource.values();
            if (!values.isMultiValued()) {
                return context;
            }
            parentContext = context;
            return this;
        }

        @Override
        protected BytesValuesSource extractValuesSourceFromContext(AggregationContext context) {
            BytesValuesSource valuesSource = context.bytesValuesSource();
            if (valuesSource == null) {
                throw new AggregationExecutionException("Missing numeric values in aggregation context for aggregator [" + aggregatorName + "]");
            }
            return valuesSource;
        }

        @Override
        protected final boolean onDoc(int doc) throws IOException {
            return onDoc(doc, values);
        }

        protected abstract boolean onDoc(int doc, BytesValues values) throws IOException;

        @Override
        public DoubleValuesSource doubleValuesSource() {
            return parentContext.doubleValuesSource();
        }

        @Override
        public LongValuesSource longValuesSource() {
            return parentContext.longValuesSource();
        }

        @Override
        public BytesValuesSource bytesValuesSource() {
            return valuesSource;
        }

        @Override
        public GeoPointValuesSource geoPointValuesSource() {
            return parentContext.geoPointValuesSource();
        }

        @Override
        public boolean accept(int doc, String valueSourceKey, double value) {
            return parentContext.accept(doc, valueSourceKey, value);
        }

        @Override
        public boolean accept(int doc, String valueSourceKey, long value) {
            return parentContext.accept(doc, valueSourceKey, value);
        }

        @Override
        public boolean accept(int doc, String valueSourceKey, GeoPoint value) {
            return parentContext.accept(doc, valueSourceKey, value);
        }

        @Override
        public boolean accept(int doc, String valueSourceKey, BytesRef value) {
            if (valuesSource.key().equals(valueSourceKey)) {
                return accept(doc, value, values);
            }
            return true;
        }

        public abstract boolean accept(int doc, BytesRef value, BytesValues values);
    }
}
