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

package org.elasticsearch.search.aggregations.bucket.multi.terms;

import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.ValuesSourceAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.ValuesSource;
import org.elasticsearch.search.aggregations.context.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.context.bytes.BytesValuesSource;
import org.elasticsearch.search.aggregations.context.numeric.NumericValuesSource;

/**
 *
 */
public class TermsAggregatorFactory extends ValuesSourceAggregator.CompoundFactory {

    private final InternalOrder order;
    private final int requiredSize;

    public TermsAggregatorFactory(String name, ValuesSourceConfig valueSourceConfig, InternalOrder order, int requiredSize) {
        super(name, valueSourceConfig);
        this.order = order;
        this.requiredSize = requiredSize;
    }

    @Override
    protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
        return new UnmappedTermsAggregator(name, order, requiredSize, aggregationContext, parent);
    }

    @Override
    protected Aggregator create(ValuesSource valuesSource, AggregationContext aggregationContext, Aggregator parent) {

        if (valuesSource instanceof BytesValuesSource) {
            return new StringTermsAggregator(name, factories, valuesSource, order, requiredSize, aggregationContext, parent);
        }

        if (valuesSource instanceof NumericValuesSource) {
            if (((NumericValuesSource) valuesSource).isFloatingPoint()) {
                return new DoubleTermsAggregator(name, factories, (NumericValuesSource) valuesSource, order, requiredSize, aggregationContext, parent);
            }
            return new LongTermsAggregator(name, factories, (NumericValuesSource) valuesSource, order, requiredSize, aggregationContext, parent);
        }

        throw new AggregationExecutionException("terms aggregation cannot be applied to field [" + valueSourceConfig.fieldContext().field() +
                "]. It can only be applied to numeric or string fields.");
    }

}
