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
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.FieldContext;
import org.elasticsearch.search.aggregations.context.ValueSpace;
import org.elasticsearch.search.aggregations.context.numeric.NumericValuesSource;
import org.elasticsearch.search.aggregations.context.numeric.ValueFormatter;
import org.elasticsearch.search.aggregations.context.numeric.ValueParser;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public abstract class LongBucketsAggregator extends ValuesSourceBucketsAggregator<NumericValuesSource> {

    public LongBucketsAggregator(String name,
                                 NumericValuesSource valuesSource,
                                 AggregationContext aggregationContext,
                                 Aggregator parent) {

        super(name, valuesSource, NumericValuesSource.class, aggregationContext, parent);
    }

    public static abstract class BucketCollector extends ValuesSourceBucketsAggregator.BucketCollector<NumericValuesSource> implements ValueSpace {

        private ValueSpace parentContext;

        protected BucketCollector(NumericValuesSource valuesSource, Aggregator[] subAggregators, Aggregator aggregator) {
            super(valuesSource, subAggregators, aggregator);
        }

        protected BucketCollector(NumericValuesSource valuesSource, List<Factory> factories, Aggregator parent) {
            super(valuesSource, factories, parent);
        }

        @Override
        protected final ValueSpace onDoc(int doc, ValueSpace context) throws IOException {
            LongValues values = valuesSource.longValues();
            if (!onDoc(doc, values, context)) {
                return null;
            }
            if (values.isMultiValued()) {
                parentContext = context;
                return this;
            }
            return context;
        }

        protected abstract boolean onDoc(int doc, LongValues values, ValueSpace context) throws IOException;

        @Override
        public boolean accept(Object valueSourceKey, double value) {
            if (!parentContext.accept(valueSourceKey, value)) {
                return false;
            }
            if (valuesSource.key().equals(valueSourceKey)) {
                return accept((long) value);
            }
            return true;
        }

        @Override
        public boolean accept(Object valueSourceKey, long value) {
            if (!parentContext.accept(valueSourceKey, value)) {
                return false;
            }
            if (valuesSource.key().equals(valueSourceKey)) {
                return accept(value);
            }
            return true;
        }

        @Override
        public boolean accept(Object valueSourceKey, GeoPoint value) {
            return parentContext.accept(valueSourceKey, value);
        }

        @Override
        public boolean accept(Object valueSourceKey, BytesRef value) {
            return parentContext.accept(valueSourceKey, value);
        }

        public abstract boolean accept(long value);
    }

    protected abstract static class FieldDataFactory<A extends LongBucketsAggregator> extends CompoundFactory<A> {

        private final FieldContext fieldContext;
        private final SearchScript valueScript;
        private final ValueFormatter formatter;
        private final ValueParser parser;

        public FieldDataFactory(String name,
                                FieldContext fieldContext,
                                @Nullable SearchScript valueScript,
                                @Nullable ValueFormatter formatter,
                                @Nullable ValueParser parser) {
            super(name);
            this.fieldContext = fieldContext;
            this.valueScript = valueScript;
            this.formatter = formatter;
            this.parser = parser;
        }

        @Override
        public final A create(AggregationContext aggregationContext, Aggregator parent) {
            NumericValuesSource source = aggregationContext.numericField(fieldContext, valueScript, formatter, parser);
            return create(source, aggregationContext, parent);
        }

        protected abstract A create(NumericValuesSource source, AggregationContext aggregationContext, Aggregator parent);
    }

    protected abstract static class ScriptFactory<A extends LongBucketsAggregator> extends CompoundFactory<A> {

        private final SearchScript script;
        private final boolean multiValued;
        private final ValueFormatter formatter;
        private final ValueParser parser;

        protected ScriptFactory(String name, SearchScript script, boolean multiValued, @Nullable ValueFormatter formatter, @Nullable ValueParser parser) {
            super(name);
            this.script = script;
            this.multiValued = multiValued;
            this.formatter = formatter;
            this.parser = parser;
        }

        @Override
        public A create(AggregationContext aggregationContext, Aggregator parent) {
            return create(aggregationContext.numericScript(script, multiValued, formatter, parser), aggregationContext, parent);
        }

        protected abstract A create(NumericValuesSource source, AggregationContext aggregationContext, Aggregator parent);
    }

    protected abstract static class ContextBasedFactory<A extends LongBucketsAggregator> extends CompoundFactory<A> {

        protected ContextBasedFactory(String name) {
            super(name);
        }

    }
}
