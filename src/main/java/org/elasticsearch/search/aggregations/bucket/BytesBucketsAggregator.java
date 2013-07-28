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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.FieldContext;
import org.elasticsearch.search.aggregations.context.ValueSpace;
import org.elasticsearch.search.aggregations.context.ValuesSource;
import org.elasticsearch.search.aggregations.context.bytes.BytesValuesSource;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public abstract class BytesBucketsAggregator extends ValuesSourceBucketsAggregator<ValuesSource> {

    protected BytesBucketsAggregator(String name,
                                     ValuesSource valuesSource,
                                     AggregationContext aggregationContext,
                                     Aggregator parent) {

        super(name, valuesSource, ValuesSource.class, aggregationContext, parent);
    }

    public static abstract class BucketCollector extends ValuesSourceBucketsAggregator.BucketCollector<ValuesSource> implements ValueSpace {

        private ValueSpace parentContext;

        protected BucketCollector(ValuesSource valuesSource, Aggregator[] subAggregators, Aggregator aggregator) {
            super(valuesSource, subAggregators, aggregator);
        }

        protected BucketCollector(ValuesSource valuesSource, List<Aggregator.Factory> factories,Aggregator aggregator) {
            super(valuesSource, factories, aggregator);
        }

        @Override
        protected final ValueSpace onDoc(int doc, ValueSpace context) throws IOException {
            BytesValues values = valuesSource.bytesValues();
            if (!onDoc(doc, values, context)) {
                return null;
            }
            if (values.isMultiValued()) {
                parentContext = context;
                return this;
            }
            return context;
        }

        protected abstract boolean onDoc(int doc, BytesValues values, ValueSpace context) throws IOException;

        @Override
        public boolean accept(Object valueSourceKey, double value) {
            return parentContext.accept(valueSourceKey, value);
        }

        @Override
        public boolean accept(Object valueSourceKey, long value) {
            return parentContext.accept(valueSourceKey, value);
        }

        @Override
        public boolean accept(Object valueSourceKey, GeoPoint value) {
            return parentContext.accept(valueSourceKey, value);
        }

        @Override
        public boolean accept(Object valueSourceKey, BytesRef value) {
            if (valuesSource.key().equals(valueSourceKey)) {
                return accept(value);
            }
            return true;
        }

        public abstract boolean accept(BytesRef value);
    }

    protected abstract static class FieldDataFactory<A extends BytesBucketsAggregator> extends CompoundFactory<A> {

        private final FieldContext fieldContext;
        private final SearchScript valueScript;

        public FieldDataFactory(String name, FieldContext fieldContext, SearchScript valueScript) {
            super(name);
            this.fieldContext = fieldContext;
            this.valueScript = valueScript;
        }

        @Override
        public A create(AggregationContext aggregationContext, Aggregator parent) {
            BytesValuesSource source = aggregationContext.bytesField(fieldContext, valueScript);
            return create(source, parent);
        }

        protected abstract A create(BytesValuesSource source, Aggregator parent);
    }

    protected abstract static class ScriptFactory<A extends BytesBucketsAggregator> extends CompoundFactory<A> {

        private final SearchScript script;
        private final boolean multiValued;

        protected ScriptFactory(String name, SearchScript script, boolean multiValued) {
            super(name);
            this.script = script;
            this.multiValued = multiValued;
        }

        @Override
        public A create(AggregationContext aggregationContext, Aggregator parent) {
            return create(aggregationContext.bytesScript(script, multiValued), parent);
        }

        protected abstract A create(BytesValuesSource source, Aggregator parent);
    }

    protected abstract static class ContextBasedFactory<A extends BytesBucketsAggregator> extends CompoundFactory<A> {

        protected ContextBasedFactory(String name) {
            super(name);
        }

    }
}
