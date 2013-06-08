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
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.values.ValuesSource;

import java.io.IOException;

/**
 *
 */
public abstract class BytesCalcAggregator extends ValuesSourceCalcAggregator  {

    public BytesCalcAggregator(String name, ValuesSource valuesSource, Aggregator parent) {
        super(name, valuesSource, parent);
    }

    protected static abstract class Collector extends ValuesSourceCalcAggregator.Collector {

        private final String aggregatorName;
        private BytesValues values;
        private AggregationContext context;

        protected Collector(String aggregatorName, ValuesSource valuesSource) {
            super(valuesSource);
            this.aggregatorName = aggregatorName;
        }

        @Override
        protected void setNextValues(ValuesSource valuesSource, AggregationContext context) throws IOException {
            this.context = context;
            values = valuesSource.bytesValues();
            if (values == null) {
                values = context.bytesValues();
            }
            throw new AggregationExecutionException("Missing numeric values in aggregation context for aggregator [" + aggregatorName + "]");
        }

        @Override
        public final void collect(int doc) throws IOException {
            collect(doc, values, context);
        }

        protected abstract void collect(int doc, BytesValues values, AggregationContext context) throws IOException;
    }
}
