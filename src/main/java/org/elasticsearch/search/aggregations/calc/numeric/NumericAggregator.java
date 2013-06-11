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

import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.calc.DoubleCalcAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.FieldDataContext;
import org.elasticsearch.search.aggregations.context.doubles.DoubleValuesSource;

import java.io.IOException;

/**
 *
 */
public class NumericAggregator<S extends NumericAggregation> extends DoubleCalcAggregator {

    private final NumericAggregation.Factory<S> statsFactory;

    S stats;

    public NumericAggregator(String name, DoubleValuesSource valuesSource, NumericAggregation.Factory<S> statsFactory, Aggregator parent) {
        super(name, valuesSource, parent);
        this.statsFactory = statsFactory;
    }

    @Override
    public Collector collector() {
        S stats = statsFactory.create(name);
        return new Collector(name, valuesSource, stats);
    }

    @Override
    public NumericAggregation buildAggregation() {
        return stats;
    }

    //========================================= Collector ===============================================//

    class Collector extends DoubleCalcAggregator.Collector {

        S stats;

        Collector(String aggregatorName, DoubleValuesSource valuesSource, S stats) {
            super(aggregatorName, valuesSource);
            this.stats = stats;
        }

        @Override
        protected void collect(int doc, DoubleValues values, AggregationContext context) throws IOException {
            if (!values.hasValue(doc)) {
                return;
            }

            if (!values.isMultiValued()) {
                double value = values.getValue(doc);
                if (context.accept(valuesSource.key(), value)) {
                    stats.collect(doc, value);
                }
                return;
            }

            for (DoubleValues.Iter iter = values.getIter(doc); iter.hasNext();) {
                double value = iter.next();
                if (context.accept(valuesSource.key(), value)) {
                    stats.collect(doc, value);
                }
            }
        }

        @Override
        public void postCollection() {
            NumericAggregator.this.stats = stats;
        }
    }

    //========================================== Builders ===============================================//

    public static class FieldDataFactory<S extends NumericAggregation> extends DoubleCalcAggregator.FieldDataFactory<NumericAggregator<S>> {

        private final NumericAggregation.Factory<S> statsFactory;

        public FieldDataFactory(String name, NumericAggregation.Factory<S> statsFactory) {
            super(name, null);
            this.statsFactory = statsFactory;
        }

        public FieldDataFactory(String name, NumericAggregation.Factory<S> statsFactory, FieldDataContext fieldDataContext) {
            super(name, fieldDataContext);
            this.statsFactory = statsFactory;
        }

        public FieldDataFactory(String name, NumericAggregation.Factory<S> statsFactory, FieldDataContext fieldDataContext, SearchScript valueScript) {
            super(name, fieldDataContext, valueScript);
            this.statsFactory = statsFactory;
        }

        @Override
        protected NumericAggregator<S> create(DoubleValuesSource source, Aggregator parent) {
            return new NumericAggregator<S>(name, source, statsFactory, parent);
        }
    }

    public static class ScriptFactory<S extends NumericAggregation> extends DoubleCalcAggregator.ScriptFactory<NumericAggregator<S>> {

        private final NumericAggregation.Factory<S> statsFactory;

        public ScriptFactory(String name, SearchScript script, NumericAggregation.Factory<S> statsFactory) {
            super(name, script);
            this.statsFactory = statsFactory;
        }

        @Override
        protected NumericAggregator<S> create(DoubleValuesSource source, Aggregator parent) {
            return new NumericAggregator<S>(name, source, statsFactory, parent);
        }
    }

    public static class ContextBasedFactory<S extends NumericAggregation> extends DoubleCalcAggregator.ContextBasedFactory<NumericAggregator<S>> {

        private final NumericAggregation.Factory<S> statsFactory;

        public ContextBasedFactory(String name, NumericAggregation.Factory<S> statsFactory) {
            super(name);
            this.statsFactory = statsFactory;
        }

        @Override
        protected NumericAggregator<S> create(DoubleValuesSource source, Aggregator parent) {
            return new NumericAggregator<S>(name, source, statsFactory, parent);
        }
    }

}
