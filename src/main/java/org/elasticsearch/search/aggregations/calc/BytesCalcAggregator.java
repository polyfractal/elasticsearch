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

package org.elasticsearch.search.aggregations.calc;

import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.FieldContext;
import org.elasticsearch.search.aggregations.context.ValuesSourceFactory;
import org.elasticsearch.search.aggregations.context.bytes.BytesValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 *
 */
public abstract class BytesCalcAggregator extends ValuesSourceCalcAggregator<BytesValuesSource>  {

    public BytesCalcAggregator(String name, BytesValuesSource valuesSource, SearchContext searchContext, ValuesSourceFactory valuesSourceFactory, Aggregator parent) {
        super(name, valuesSource, BytesValuesSource.class, searchContext, valuesSourceFactory, parent);
    }

    protected static abstract class Collector extends ValuesSourceCalcAggregator.Collector<BytesValuesSource> {

        private BytesValues values;
        private AggregationContext context;

        protected Collector(BytesValuesSource valuesSource, Aggregator aggregator) {
            super(valuesSource, aggregator);
        }

        @Override
        protected void setNextValues(BytesValuesSource valuesSource, AggregationContext context) throws IOException {
            this.context = context;
            values = valuesSource.values();
        }


        @Override
        public final void collect(int doc) throws IOException {
            collect(doc, values, context);
        }

        protected abstract void collect(int doc, BytesValues values, AggregationContext context) throws IOException;
    }

    protected abstract static class FieldDataFactory<A extends BytesCalcAggregator> extends Factory<A> {

        private final FieldContext fieldContext;
        private final SearchScript valueScript;

        public FieldDataFactory(String name, FieldContext fieldContext) {
            this(name, fieldContext, null);
        }

        public FieldDataFactory(String name, FieldContext fieldContext, SearchScript valueScript) {
            super(name);
            this.fieldContext = fieldContext;
            this.valueScript = valueScript;
        }

        @Override
        public A create(SearchContext context, ValuesSourceFactory valuesSourceFactory, Aggregator parent) {
            BytesValuesSource source = valuesSourceFactory.bytesField(fieldContext, valueScript);
            return create(source, context, valuesSourceFactory, parent);
        }

        protected abstract A create(BytesValuesSource source, SearchContext searchContext, ValuesSourceFactory valuesSourceFactory, Aggregator parent);
    }

    protected abstract static class ScriptFactory<A extends BytesCalcAggregator> extends Factory<A> {

        private final SearchScript script;
        private final boolean multiValued;

        protected ScriptFactory(String name, SearchScript script, boolean multiValued) {
            super(name);
            this.script = script;
            this.multiValued = multiValued;
        }

        @Override
        public A create(SearchContext searchContext, ValuesSourceFactory valuesSourceFactory, Aggregator parent) {
            BytesValuesSource source = valuesSourceFactory.bytesScript(script, multiValued);
            return create(source, searchContext, parent);
        }

        protected abstract A create(BytesValuesSource source, SearchContext searchContext, Aggregator parent);
    }

    protected abstract static class ContextBasedFactory<A extends ValuesSourceCalcAggregator> extends Factory<A> {

        protected ContextBasedFactory(String name) {
            super(name);
        }

    }
}
