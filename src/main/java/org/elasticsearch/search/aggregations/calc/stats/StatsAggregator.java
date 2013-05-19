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

package org.elasticsearch.search.aggregations.calc.stats;

import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.calc.FieldDataCalcAggregator;
import org.elasticsearch.search.aggregations.calc.NumericFieldDataCalcAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.FieldDataContext;
import org.elasticsearch.search.aggregations.context.ValueTransformer;
import org.elasticsearch.search.aggregations.format.ValueFormatter;

/**
 *
 */
public class StatsAggregator<S extends Stats> extends NumericFieldDataCalcAggregator {

    private final Stats.Factory<S> statsFactory;
    private final ValueFormatter valueFormatter;

    S stats;

    StatsAggregator(String name, Stats.Factory<S> statsFactory, ValueFormatter valueFormatter, Aggregator parent) {
        super(name, null, null, parent);
        this.statsFactory = statsFactory;
        this.valueFormatter = valueFormatter;
    }

    StatsAggregator(String name, Stats.Factory<S> statsFactory, ValueFormatter valueFormatter, FieldDataContext fieldDataContext, ValueTransformer valueTransformer, Aggregator parent) {
        super(name, fieldDataContext, valueTransformer, parent);
        this.statsFactory = statsFactory;
        this.valueFormatter = valueFormatter;
    }

    @Override
    public Collector collector() {
        if (fieldDataContext == null) {
            return null;
        }
        S stats = statsFactory.create(name);
        stats.valueFormatter = valueFormatter;
        return new Collector(stats, fieldDataContext, valueTransformer);
    }

    @Override
    public Stats buildAggregation() {
        return stats;
    }

    //========================================= Collector ===============================================//

    class Collector extends NumericFieldDataCalcAggregator.Collector {

        S stats;

        public Collector(S stats, FieldDataContext fieldDataContext, ValueTransformer valueTransformer) {
            super(fieldDataContext, valueTransformer);
            this.stats = stats;
        }

        @Override
        protected void collect(int doc, String field, DoubleValues values, AggregationContext context) {

            if (!values.hasValue(doc)) {
                return;
            }

            if (!values.isMultiValued()) {
                double value = valueTransformer.transform(values.getValue(doc));
                if (context.accept(field, value)) {
                    stats.collect(doc, value);
                }
                return;
            }

            for (DoubleValues.Iter iter = values.getIter(doc); iter.hasNext();) {
                double value = valueTransformer.transform(iter.next());
                if (context.accept(field, value)) {
                    stats.collect(doc, value);
                }
            }
        }

        @Override
        public void postCollection() {
            StatsAggregator.this.stats = stats;
        }
    }

    //========================================== Builders ===============================================//

    public static class Factory<S extends Stats> extends FieldDataCalcAggregator.Factory<StatsAggregator<S>> {

        private final Stats.Factory<S> statsFactory;
        private SearchScript script;
        private ValueFormatter formatter;

        /** Used for aggregations that fall back on the ancestor aggregator field context */
        public Factory(String name, ValueFormatter formatter, Stats.Factory<S> statsFactory) {
            super(name, null);
            this.statsFactory = statsFactory;
            this.formatter = formatter;
        }

        public Factory(String name, ValueFormatter formatter, Stats.Factory<S> statsFactory, FieldDataContext fieldDataContext) {
            this(name, formatter, statsFactory, fieldDataContext, ValueTransformer.NONE);
        }

        public Factory(String name, ValueFormatter formatter, Stats.Factory<S> statsFactory, FieldDataContext fieldDataContext, ValueTransformer valueTransformer) {
            super(name, fieldDataContext, valueTransformer);
            this.statsFactory = statsFactory;
            this.formatter = formatter;
        }

        @Override
        public StatsAggregator<S> create(Aggregator parent) {
            return new StatsAggregator<S>(name, statsFactory, formatter, fieldDataContext, valueTransformer, parent);
        }
    }

}
