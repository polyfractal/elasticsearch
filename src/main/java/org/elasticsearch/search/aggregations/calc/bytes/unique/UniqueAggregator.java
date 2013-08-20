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

package org.elasticsearch.search.aggregations.calc.bytes.unique;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.ValuesSourceAggregator;
import org.elasticsearch.search.aggregations.calc.bytes.BytesCalcAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.ValueSpace;
import org.elasticsearch.search.aggregations.context.ValuesSource;
import org.elasticsearch.search.aggregations.context.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.context.bytes.BytesValuesSource;

import java.io.IOException;

/**
 * A field data based aggregator that counts the number of unique values a specific field has within the aggregation context.
 */
public class UniqueAggregator extends BytesCalcAggregator {

    private ICardinality hll;

    public UniqueAggregator(String name, BytesValuesSource valuesSource, AggregationContext aggregationContext, Aggregator parent, int precision) {
        super(name, valuesSource, aggregationContext, parent);
        hll = new HyperLogLogPlus(precision);
    }

    @Override
    public Aggregator.Collector collector() {
        return valuesSource != null ? new Collector(valuesSource) : null;
    }

    @Override
    public InternalAggregation buildAggregation() {
        return new InternalUnique(name, hll);
    }

    class Collector extends BytesCalcAggregator.Collector {

        Collector(ValuesSource valuesSource) {
            super(valuesSource, UniqueAggregator.this);
        }

        @Override
        protected void collect(int doc, BytesValues values, ValueSpace valueSpace) throws IOException {
            if (!values.hasValue(doc)) {
                return;
            }
            if (!values.isMultiValued()) {
                if (valueSpace.accept(valuesSource.key(), values.getValue(doc))) {
                    hll.offer(values.getValue(doc));
                }
                return;
            }
            for (BytesValues.Iter iter  = values.getIter(doc); iter.hasNext();) {
                if (valueSpace.accept(valuesSource.key(), iter.next())) {
                    hll.offer(iter.next());
                }
            }
        }

        @Override
        public void postCollection() {
            UniqueAggregator.this.hll = hll;
        }
    }

    public static class Factory extends ValuesSourceAggregator.Factory<BytesValuesSource> {

        private final int precision;

        public Factory(String name, ValuesSourceConfig<BytesValuesSource> valuesSourceBuilder, int precision) {
            super(name, valuesSourceBuilder);
            this.precision = precision;
        }

        @Override
        protected UniqueAggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            return new UniqueAggregator(name, null, aggregationContext, parent, precision);
        }

        @Override
        protected UniqueAggregator create(BytesValuesSource valuesSource, AggregationContext aggregationContext, Aggregator parent) {
            return new UniqueAggregator(name, valuesSource, aggregationContext, parent, precision);
        }
    }

}
