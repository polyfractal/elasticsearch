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
import org.elasticsearch.index.fielddata.GeoPointValues;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.FieldDataContext;
import org.elasticsearch.search.aggregations.context.bytes.BytesValuesSource;
import org.elasticsearch.search.aggregations.context.doubles.DoubleValuesSource;
import org.elasticsearch.search.aggregations.context.geopoints.GeoPointValuesSource;
import org.elasticsearch.search.aggregations.context.longs.LongValuesSource;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public abstract class GeoPointBucketAggregator extends ValuesSourceBucketAggregator<GeoPointValuesSource> {

    protected GeoPointBucketAggregator(String name, GeoPointValuesSource valuesSource, Aggregator parent) {
        super(name, valuesSource, GeoPointValuesSource.class, parent);
    }

    public static abstract class BucketCollector extends ValuesSourceBucketAggregator.BucketCollector<GeoPointValuesSource> implements AggregationContext {

        private GeoPointValues values;

        protected BucketCollector(String aggregationName, GeoPointValuesSource valuesSource, Aggregator[] aggregators) {
            super(aggregationName, valuesSource, aggregators);
        }

        protected BucketCollector(String aggregationName, GeoPointValuesSource valuesSource, List<Aggregator.Factory> factories,
                                  AtomicReaderContext reader, Scorer scorer, AggregationContext context, Aggregator parent) {
            super(aggregationName, valuesSource, factories, reader, scorer, context, parent);
        }

        @Override
        protected AggregationContext setNextValues(GeoPointValuesSource valuesSource, AggregationContext context) throws IOException {
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

        protected abstract boolean onDoc(int doc, GeoPointValues values, AggregationContext context) throws IOException;

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
            return parentContext.bytesValuesSource();
        }

        @Override
        public GeoPointValuesSource geoPointValuesSource() {
            return valuesSource;
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
            if (!parentContext.accept(doc, valueSourceKey, value)) {
                return false;
            }
            if (valuesSource.key().equals(valueSourceKey)) {
                return accept(doc, value, values);
            }
            return true;
        }

        @Override
        public boolean accept(int doc, String valueSourceKey, BytesRef value) {
            return parentContext.accept(doc, valueSourceKey, value);
        }

        public abstract boolean accept(int doc, GeoPoint value, GeoPointValues values);
    }

    protected abstract static class FieldDataFactory<A extends GeoPointBucketAggregator> extends CompoundFactory<A> {

        private final FieldDataContext fieldDataContext;
        private final SearchScript valueScript;

        public FieldDataFactory(String name, FieldDataContext fieldDataContext) {
            this(name, fieldDataContext, null);
        }

        public FieldDataFactory(String name, FieldDataContext fieldDataContext, SearchScript valueScript) {
            super(name);
            this.fieldDataContext = fieldDataContext;
            this.valueScript = valueScript;
        }

        @Override
        public final A create(Aggregator parent) {
            if (valueScript != null) {
                return create(new GeoPointValuesSource.FieldData(fieldDataContext.field(), fieldDataContext.indexFieldData(), valueScript), parent);
            }
            return create(new GeoPointValuesSource.FieldData(fieldDataContext.field(), fieldDataContext.indexFieldData()), parent);
        }

        protected abstract A create(GeoPointValuesSource source, Aggregator parent);
    }

    protected abstract static class ScriptFactory<A extends GeoPointBucketAggregator> extends CompoundFactory<A> {

        private final SearchScript script;

        protected ScriptFactory(String name, SearchScript script) {
            super(name);
            this.script = script;
        }

        @Override
        public final A create(Aggregator parent) {
            return create(new GeoPointValuesSource.Script(script), parent);
        }

        protected abstract A create(GeoPointValuesSource source, Aggregator parent);
    }

    protected abstract static class ContextBasedFactory<A extends GeoPointBucketAggregator> extends CompoundFactory<A> {

        protected ContextBasedFactory(String name) {
            super(name);
        }

    }
}
