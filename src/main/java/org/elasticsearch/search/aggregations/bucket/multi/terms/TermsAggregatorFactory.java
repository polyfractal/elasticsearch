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

import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.ip.IpFieldMapper;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.bucket.single.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.bucket.multi.terms.doubles.DoubleTermsAggregator;
import org.elasticsearch.search.aggregations.bucket.multi.terms.longs.LongTermsAggregator;
import org.elasticsearch.search.aggregations.bucket.multi.terms.string.StringTermsAggregator;
import org.elasticsearch.search.aggregations.context.FieldDataBased;
import org.elasticsearch.search.aggregations.context.FieldDataContext;
import org.elasticsearch.search.aggregations.context.ValueTransformer;
import org.elasticsearch.search.aggregations.format.ValueFormatter;

/**
 *
 */
public class TermsAggregatorFactory extends SingleBucketAggregator.Factory<SingleBucketAggregator, TermsAggregatorFactory> {

    private FieldDataContext fieldDataContext;
    private ValueTransformer valueTransformer;
    private final Terms.Order order;
    private final String formatPattern;
    private final int requiredSize;

    /** Used for terms facet that depend on the field context of one of the ancestors in the aggregation hierarchy */
    public TermsAggregatorFactory(String name, Terms.Order order, String formatPattern, int requiredSize) {
        this(name, null, order, formatPattern, requiredSize);
    }

    public TermsAggregatorFactory(String name, FieldDataContext fieldDataContext, Terms.Order order, String formatPattern, int requiredSize) {
        this(name, fieldDataContext, ValueTransformer.NONE, order, formatPattern, requiredSize);
    }

    public TermsAggregatorFactory(String name, FieldDataContext fieldDataContext, ValueTransformer valueTransformer, Terms.Order order, String formatPattern, int requiredSize) {
        super(name);
        this.fieldDataContext = fieldDataContext;
        this.valueTransformer = valueTransformer;
        this.order = order;
        this.formatPattern = formatPattern;
        this.requiredSize = requiredSize;
    }

    @Override
    public SingleBucketAggregator create(Aggregator parent) {
        if (fieldDataContext == null) {
            fieldDataContext = resolveIndexFeildDatasFromContext(parent);
            if (fieldDataContext == null) {
                throw new AggregationExecutionException("Aggregator [" + name + "] requires a field context");
            }
        }

        if (fieldDataContext.indexFieldDatas().length == 0) {
            throw new AggregationExecutionException("Terms aggregation [" + name + "] is missing a field context");
        }

        // handling IPv4 case
        if (fieldDataContext.fieldMapper(0) instanceof IpFieldMapper) {
            ValueFormatter formatter = ValueFormatter.IPv4;
            return new LongTermsAggregator(name, factories, fieldDataContext, valueTransformer, formatter, order, requiredSize, parent);
        }

        //sampling the first index field data to figure out the data type
        if (fieldDataContext.indexFieldDatas()[0] instanceof IndexNumericFieldData) {
            ValueFormatter formatter = formatPattern == null ? null : new ValueFormatter.Number.Pattern(formatPattern);
            if (((IndexNumericFieldData) fieldDataContext.indexFieldDatas()[0]).getNumericType().isFloatingPoint()) {
                return new DoubleTermsAggregator(name, factories, fieldDataContext, valueTransformer, formatter, order, requiredSize, parent);
            } else {
                return new LongTermsAggregator(name, factories, fieldDataContext, valueTransformer, formatter, order, requiredSize, parent);
            }
        } else {
            return new StringTermsAggregator(name, factories, fieldDataContext, valueTransformer, order, requiredSize, parent);
        }
    }

    static FieldDataContext resolveIndexFeildDatasFromContext(Aggregator parent) {
        while (parent != null) {
            if (parent instanceof FieldDataBased) {
                return ((FieldDataBased) parent).fieldDataContext();
            }
            parent = parent.parent();
        }
        return null;
    }
}
