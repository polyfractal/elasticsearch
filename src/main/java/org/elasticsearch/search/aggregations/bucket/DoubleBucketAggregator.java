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
import org.elasticsearch.index.fielddata.DoubleValues;
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
public abstract class DoubleBucketAggregator extends ValuesSourceBucketAggregator<DoubleValuesSource> {

    protected DoubleBucketAggregator(String name, DoubleValuesSource valuesSource, Aggregator parent) {
        super(name, valuesSource, DoubleValuesSource.class, parent);
    }

    public static abstract class BucketCollector extends ValuesSourceBucketAggregator.BucketCollector<DoubleValuesSource> implements AggregationContext {

        private DoubleValues values;

        protected BucketCollector(String aggregationName, DoubleValuesSource valuesSource, Aggregator[] aggregators) {
            super(aggregationName, valuesSource, aggregators);
        }

        protected BucketCollector(String aggregationName, DoubleValuesSource valuesSource, List<Aggregator.Factory> factories,
                                  AtomicReaderContext reader, Scorer scorer, AggregationContext context, Aggregator parent) {
            super(aggregationName, valuesSource, factories, reader, scorer, context, parent);
        }

        @Override
        protected AggregationContext setNextValues(DoubleValuesSource valuesSource, AggregationContext context) throws IOException {
            values = valuesSource.values();
            if (!values.isMultiValued()) {
                return context;
            }
            return this;
        }

        @Override
        protected final boolean onDoc(int doc, AggregationContext context) throws IOException {
            return onDoc(doc, values, context);
        }

        protected abstract boolean onDoc(int doc, DoubleValues values, AggregationContext context) throws IOException;

        @Override
        public DoubleValuesSource doubleValuesSource() {
            return valuesSource;
        }

        @Override
        public LongValuesSource longValuesSource() {
            return parentContext.longValuesSource();
        }

        @Override
        public BytesValuesSource bytesValuesSource() {
            return parentContext.bytesValuesSource();
        }

        @Override
        public GeoPointValuesSource geoPointValuesSource() {
            return parentContext.geoPointValuesSource();
        }

        @Override
        public boolean accept(int doc, String valueSourceKey, double value) {
            if (!parentContext.accept(doc, valueSourceKey, value)) {
                return false;
            }
            if (valuesSource.key().equals(valueSourceKey)) {
                return accept(doc, value, values);
            }
            return true;
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
            return parentContext.accept(doc, valueSourceKey, value);
        }

        public abstract boolean accept(int doc, double value, DoubleValues values);
    }
}
