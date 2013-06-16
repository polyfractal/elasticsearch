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
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.FieldContext;
import org.elasticsearch.search.aggregations.context.ValuesSourceFactory;
import org.elasticsearch.search.aggregations.context.numeric.NumericValuesSource;
import org.elasticsearch.search.aggregations.context.numeric.ValueFormatter;
import org.elasticsearch.search.aggregations.context.numeric.ValueParser;
import org.elasticsearch.search.aggregations.context.numeric.longs.LongValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public abstract class LongBucketAggregator extends ValuesSourceBucketAggregator<NumericValuesSource> {

    public LongBucketAggregator(String name,
                                NumericValuesSource valuesSource,
                                SearchContext searchContext,
                                ValuesSourceFactory valuesSourceFactory,
                                Aggregator parent) {

        super(name, valuesSource, NumericValuesSource.class, searchContext, valuesSourceFactory, parent);
    }

    public static abstract class BucketCollector extends ValuesSourceBucketAggregator.BucketCollector<NumericValuesSource> implements AggregationContext {

        private LongValues values;

        protected BucketCollector(NumericValuesSource valuesSource, Aggregator[] subAggregators, Aggregator aggregator) {
            super(valuesSource, subAggregators, aggregator);
        }

        protected BucketCollector(NumericValuesSource valuesSource, List<Factory> factories, AtomicReaderContext reader,
                                  Scorer scorer, AggregationContext context, Aggregator parent) {
            super(valuesSource, factories, reader, scorer, context, parent);
        }

        @Override
        protected AggregationContext setNextValues(NumericValuesSource valuesSource, AggregationContext context) throws IOException {
            values = valuesSource.longValues();
            if (!values.isMultiValued()) {
                return context;
            }
            return this;
        }

        @Override
        protected final boolean onDoc(int doc, AggregationContext context) throws IOException {
            return onDoc(doc, values, context);
        }

        protected abstract boolean onDoc(int doc, LongValues values, AggregationContext context) throws IOException;

        @Override
        public boolean accept(String valueSourceKey, double value) {
            return parentContext.accept(valueSourceKey, value);
        }

        @Override
        public boolean accept(String valueSourceKey, long value) {
            if (!parentContext.accept(valueSourceKey, value)) {
                return false;
            }
            if (valuesSource.key().equals(valueSourceKey)) {
                return accept(value);
            }
            return true;
        }

        @Override
        public boolean accept(String valueSourceKey, GeoPoint value) {
            return parentContext.accept(valueSourceKey, value);
        }

        @Override
        public boolean accept(String valueSourceKey, BytesRef value) {
            return parentContext.accept(valueSourceKey, value);
        }

        public abstract boolean accept(double value);
    }

    protected abstract static class FieldDataFactory<A extends LongBucketAggregator> extends CompoundFactory<A> {

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
        public final A create(SearchContext searchContext, ValuesSourceFactory valuesSourceFactory, Aggregator parent) {
            NumericValuesSource source = valuesSourceFactory.longField(fieldContext, valueScript, formatter, parser);
            return create(source, searchContext, valuesSourceFactory, parent);
        }

        protected abstract A create(NumericValuesSource source, SearchContext searchContext, ValuesSourceFactory valuesSourceFactory, Aggregator parent);
    }

    protected abstract static class ScriptFactory<A extends LongBucketAggregator> extends CompoundFactory<A> {

        private final SearchScript script;
        private final boolean multiValued;
        private final ValueFormatter formatter;

        protected ScriptFactory(String name, SearchScript script, boolean multiValued, @Nullable ValueFormatter formatter) {
            super(name);
            this.script = script;
            this.multiValued = multiValued;
            this.formatter = formatter;
        }

        @Override
        public A create(SearchContext searchContext, ValuesSourceFactory valuesSourceFactory, Aggregator parent) {
            return create(valuesSourceFactory.longScript(script, multiValued, formatter), searchContext, valuesSourceFactory, parent);
        }

        protected abstract A create(LongValuesSource source, SearchContext searchContext, ValuesSourceFactory valuesSourceFactory, Aggregator parent);
    }

    protected abstract static class ContextBasedFactory<A extends LongBucketAggregator> extends CompoundFactory<A> {

        protected ContextBasedFactory(String name) {
            super(name);
        }

    }
}
