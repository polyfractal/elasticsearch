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

import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.FieldDataContext;
import org.elasticsearch.search.aggregations.context.ValueTransformer;

import java.io.IOException;

/**
 *
 */
public abstract class NumericFieldDataCalcAggregator extends FieldDataCalcAggregator {

    protected NumericFieldDataCalcAggregator(String name, FieldDataContext fieldDataContext, ValueTransformer valueTransformer, Aggregator parent) {
        super(name, fieldDataContext, valueTransformer, parent, true);
    }

    protected static abstract class Collector extends FieldDataCalcAggregator.Collector {

        protected Collector(FieldDataContext fieldDataContext, ValueTransformer valueTransformer) {
            super(fieldDataContext, valueTransformer);
        }

        @Override
        public void collect(int doc, String field, AtomicFieldData fieldData, AggregationContext context) throws IOException {
            DoubleValues values = ((AtomicNumericFieldData) fieldData).getDoubleValues();
            collect(doc, field, values, context);
        }

        protected abstract void collect(int doc, String field, DoubleValues values, AggregationContext context);
    }

}
