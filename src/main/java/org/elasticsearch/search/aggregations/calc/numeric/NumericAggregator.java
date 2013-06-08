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
import org.elasticsearch.search.aggregations.calc.NumericCalcAggregator;
import org.elasticsearch.search.aggregations.calc.ValuesSourceCalcAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.FieldDataContext;
import org.elasticsearch.search.aggregations.context.values.ValuesSource;

import java.io.IOException;

/**
 *
 */
public class NumericAggregator<S extends Stats> extends NumericCalcAggregator {

    private final Stats.Factory<S> statsFactory;

    S stats;

    public NumericAggregator(String name, ValuesSource valuesSource, Stats.Factory<S> statsFactory, Aggregator parent) {
        super(name, valuesSource, parent);
        this.statsFactory = statsFactory;
    }

    @Override
    public Collector collector() {
        S stats = statsFactory.create(name);
        return new Collector(name, valuesSource, stats);
    }

    @Override
    public Stats buildAggregation() {
        return stats;
    }

    //========================================= Collector ===============================================//

    class Collector extends NumericCalcAggregator.Collector {

        S stats;

        Collector(String aggregatorName, ValuesSource valuesSource, S stats) {
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
                if (context.accept(doc, valuesSource.id(), value)) {
                    stats.collect(doc, value);
                }
                return;
            }

            for (DoubleValues.Iter iter = values.getIter(doc); iter.hasNext();) {
                double value = iter.next();
                if (context.accept(doc, valuesSource.id(), value)) {
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

    public static class FieldDataFactory<S extends Stats> extends ValuesSourceCalcAggregator.FieldDataFactory<NumericAggregator<S>> {

        private final Stats.Factory<S> statsFactory;

        public FieldDataFactory(String name, Stats.Factory<S> statsFactory) {
            super(name, null);
            this.statsFactory = statsFactory;
        }

        public FieldDataFactory(String name, Stats.Factory<S> statsFactory, FieldDataContext fieldDataContext) {
            super(name, fieldDataContext);
            this.statsFactory = statsFactory;
        }

        public FieldDataFactory(String name, Stats.Factory<S> statsFactory, FieldDataContext fieldDataContext, SearchScript valueScript) {
            super(name, fieldDataContext, valueScript);
            this.statsFactory = statsFactory;
        }

        @Override
        protected NumericAggregator<S> create(ValuesSource source, Aggregator parent) {
            return new NumericAggregator<S>(name, source, statsFactory, parent);
        }
    }

    public static class ScriptFactory<S extends Stats> extends ValuesSourceCalcAggregator.ScriptFactory<NumericAggregator<S>> {

        private final Stats.Factory<S> statsFactory;

        public ScriptFactory(String name, SearchScript script, Stats.Factory<S> statsFactory) {
            super(name, script);
            this.statsFactory = statsFactory;
        }

        @Override
        protected NumericAggregator<S> create(ValuesSource source, Aggregator parent) {
            return new NumericAggregator<S>(name, source, statsFactory, parent);
        }
    }

    public static class ContextBasedFactory<S extends Stats> extends ValuesSourceCalcAggregator.ContextBasedFactory<NumericAggregator<S>> {

        private final Stats.Factory<S> statsFactory;

        public ContextBasedFactory(String name, Stats.Factory<S> statsFactory) {
            super(name);
            this.statsFactory = statsFactory;
        }

        @Override
        protected NumericAggregator<S> create(ValuesSource source, Aggregator parent) {
            return new NumericAggregator<S>(name, source, statsFactory, parent);
        }
    }

}
