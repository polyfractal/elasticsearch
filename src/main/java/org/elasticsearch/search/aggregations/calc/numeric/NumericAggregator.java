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

package org.elasticsearch.search.aggregations.calc.numeric;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.calc.ValuesSourceCalcAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.FieldContext;
import org.elasticsearch.search.aggregations.context.ValueSpace;
import org.elasticsearch.search.aggregations.context.numeric.NumericValuesSource;

import java.io.IOException;

/**
 *
 */
public class NumericAggregator<A extends NumericAggregation> extends ValuesSourceCalcAggregator<NumericValuesSource> {

    private final NumericAggregation.Factory<A> aggregationFactory;

    A stats;

    public NumericAggregator(String name,
                             NumericValuesSource valuesSource,
                             NumericAggregation.Factory<A> aggregationFactory,
                             AggregationContext aggregationContext,
                             Aggregator parent) {

        super(name, valuesSource, NumericValuesSource.class, aggregationContext, parent);
        this.aggregationFactory = aggregationFactory;
    }

    @Override
    public Collector collector() {
        A stats = aggregationFactory.create(name);
        return new Collector(valuesSource, stats);
    }

    @Override
    public NumericAggregation buildAggregation() {
        return stats;
    }

    //========================================= Collector ===============================================//

    class Collector extends ValuesSourceCalcAggregator.Collector<NumericValuesSource> {

        private A stats;

        Collector(NumericValuesSource valuesSource, A stats) {
            super(valuesSource, NumericAggregator.this);
            this.stats = stats;
        }

        @Override
        public void collect(int doc, ValueSpace valueSpace) throws IOException {

            DoubleValues values = valuesSource.doubleValues();
            if (values == null) {
                return;
            }

            if (!values.hasValue(doc)) {
                return;
            }

            if (!values.isMultiValued()) {
                double value = values.getValue(doc);
                if (valueSpace.accept(valuesSource.key(), value)) {
                    stats.collect(doc, value);
                }
                return;
            }

            for (DoubleValues.Iter iter = values.getIter(doc); iter.hasNext();) {
                double value = iter.next();
                if (valueSpace.accept(valuesSource.key(), value)) {
                    stats.collect(doc, value);
                }
            }
        }

        @Override
        public void postCollection() {
            NumericAggregator.this.stats = stats;
        }
    }

    //============================================== Factories ===============================================//

    public static class FieldDataFactory<A extends NumericAggregation> extends Factory<NumericAggregator<A>> {

        private final FieldContext fieldContext;
        private final SearchScript valueScript;
        private final NumericAggregation.Factory<A> aggregationFactory;

        public FieldDataFactory(String name,
                                FieldContext fieldContext,
                                @Nullable SearchScript valueScript,
                                NumericAggregation.Factory<A> aggregationFactory) {
            super(name);
            this.fieldContext = fieldContext;
            this.valueScript = valueScript;
            this.aggregationFactory = aggregationFactory;
        }

        @Override
        public NumericAggregator<A> create(AggregationContext aggregationContext, Aggregator parent) {
            NumericValuesSource valuesSource = aggregationContext.numericField(fieldContext, valueScript, null, null);
            return new NumericAggregator<A>(name, valuesSource, aggregationFactory, aggregationContext, parent);
        }

    }

    protected static class ScriptFactory<A extends NumericAggregation> extends Factory<NumericAggregator<A>> {

        private final SearchScript script;
        private final boolean multiValued;
        private final NumericAggregation.Factory<A> aggregationFactory;

        protected ScriptFactory(String name, SearchScript script, boolean multiValued, NumericAggregation.Factory<A> aggregationFactory) {
            super(name);
            this.script = script;
            this.multiValued = multiValued;
            this.aggregationFactory = aggregationFactory;
        }

        @Override
        public NumericAggregator<A> create(AggregationContext aggregationContext, Aggregator parent) {
            NumericValuesSource valuesSource = aggregationContext.numericScript(script, multiValued, null, null);
            return new NumericAggregator<A>(name, valuesSource, aggregationFactory, aggregationContext, parent);
        }
    }

    protected static class ContextBasedFactory<A extends NumericAggregation> extends Factory<NumericAggregator<A>> {

        private final NumericAggregation.Factory<A> aggregationFactory;

        protected ContextBasedFactory(String name, NumericAggregation.Factory<A> aggregationFactory) {
            super(name);
            this.aggregationFactory = aggregationFactory;
        }

        @Override
        public NumericAggregator<A> create(AggregationContext aggregationContext, Aggregator parent) {
            return new NumericAggregator<A>(name, null, aggregationFactory, aggregationContext, parent);
        }
    }


}
