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

package org.elasticsearch.search.aggregations.calc.count;

import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.calc.BytesCalcAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.FieldContext;
import org.elasticsearch.search.aggregations.context.ValuesSourceFactory;
import org.elasticsearch.search.aggregations.context.bytes.BytesValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * A field data based aggregator that counts the number of values a specific field has within the aggregation context.
 */
public class CountAggregator extends BytesCalcAggregator {

    long value;

    public CountAggregator(String name, BytesValuesSource valuesSource, SearchContext searchContext, ValuesSourceFactory valuesSourceFactory, Aggregator parent) {
        super(name, valuesSource, searchContext, valuesSourceFactory, parent);
    }

    @Override
    public Aggregator.Collector collector() {
        return new Collector(valuesSource);
    }

    @Override
    public InternalAggregation buildAggregation() {
        return new InternalCount(name, value);
    }

    class Collector extends BytesCalcAggregator.Collector {

        long value;

        Collector(BytesValuesSource valuesSource) {
            super(valuesSource, CountAggregator.this);
        }

        @Override
        protected void collect(int doc, BytesValues values, AggregationContext context) throws IOException {
            if (!values.hasValue(doc)) {
                return;
            }
            if (!values.isMultiValued()) {
                if (context.accept(valuesSource.key(), values.getValue(doc))) {
                    value++;
                }
                return;
            }
            for (BytesValues.Iter iter  = values.getIter(doc); iter.hasNext();) {
                if (context.accept(valuesSource.key(), iter.next())) {
                    value++;
                }
            }
        }

        @Override
        public void postCollection() {
            CountAggregator.this.value = value;
        }
    }

    public static class FieldDataFactory extends BytesCalcAggregator.FieldDataFactory<CountAggregator> {

        public FieldDataFactory(String name, FieldContext fieldContext) {
            super(name, fieldContext);
        }

        @Override
        protected CountAggregator create(BytesValuesSource source, SearchContext searchContext, ValuesSourceFactory valuesSourceFactory, Aggregator parent) {
            return new CountAggregator(name, source, searchContext, valuesSourceFactory, parent);
        }
    }

    public static class ContextBasedFactory extends BytesCalcAggregator.ContextBasedFactory<CountAggregator> {

        public ContextBasedFactory(String name) {
            super(name);
        }

        @Override
        public CountAggregator create(SearchContext searchContext, ValuesSourceFactory valuesSourceFactory, Aggregator parent) {
            return new CountAggregator(name, null, searchContext, valuesSourceFactory, parent);
        }

    }
}
