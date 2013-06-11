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

import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.FieldDataContext;
import org.elasticsearch.search.aggregations.context.doubles.DoubleValuesSource;

import java.io.IOException;

/**
 *
 */
public abstract class DoubleCalcAggregator extends ValuesSourceCalcAggregator<DoubleValuesSource>  {

    public DoubleCalcAggregator(String name, DoubleValuesSource valuesSource, Aggregator parent) {
        super(name, valuesSource, DoubleValuesSource.class, parent);
    }

    protected static abstract class Collector extends ValuesSourceCalcAggregator.Collector<DoubleValuesSource> {

        private final String aggregatorName;
        private DoubleValues values;
        private AggregationContext context;

        protected Collector(String aggregatorName, DoubleValuesSource valuesSource) {
            super(valuesSource);
            this.aggregatorName = aggregatorName;
        }

        @Override
        protected void setNextValues(DoubleValuesSource valuesSource, AggregationContext context) throws IOException {
            this.context = context;
            values = valuesSource.values();
        }


        @Override
        public final void collect(int doc) throws IOException {
            collect(doc, values, context);
        }

        protected abstract void collect(int doc, DoubleValues values, AggregationContext context) throws IOException;
    }


    protected abstract static class FieldDataFactory<A extends DoubleCalcAggregator> extends Factory<A> {

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
                return create(new DoubleValuesSource.FieldData(fieldDataContext.field(), fieldDataContext.indexFieldData(), valueScript), parent);
            }
            return create(new DoubleValuesSource.FieldData(fieldDataContext.field(), fieldDataContext.indexFieldData()), parent);
        }

        protected abstract A create(DoubleValuesSource source, Aggregator parent);
    }

    protected abstract static class ScriptFactory<A extends ValuesSourceCalcAggregator> extends Factory<A> {

        private final SearchScript script;

        protected ScriptFactory(String name, SearchScript script) {
            super(name);
            this.script = script;
        }

        @Override
        public final A create(Aggregator parent) {
            return create(new DoubleValuesSource.Script(script), parent);
        }

        protected abstract A create(DoubleValuesSource source, Aggregator parent);
    }

    protected abstract static class ContextBasedFactory<A extends ValuesSourceCalcAggregator> extends Factory<A> {

        protected ContextBasedFactory(String name) {
            super(name);
        }

        @Override
        public A create(Aggregator parent) {
            return create(null, parent);
        }

        protected abstract A create(DoubleValuesSource source, Aggregator parent);
    }
}
