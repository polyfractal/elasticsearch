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
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.index.mapper.ip.IpFieldMapper;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.ValuesSourceAggregator;
import org.elasticsearch.search.aggregations.bucket.multi.terms.doubles.DoubleTermsAggregator;
import org.elasticsearch.search.aggregations.bucket.multi.terms.longs.LongTermsAggregator;
import org.elasticsearch.search.aggregations.bucket.multi.terms.string.StringTermsAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.FieldContext;
import org.elasticsearch.search.aggregations.context.ValuesSource;
import org.elasticsearch.search.aggregations.context.bytes.BytesValuesSource;
import org.elasticsearch.search.aggregations.context.numeric.NumericValuesSource;
import org.elasticsearch.search.aggregations.context.numeric.ValueFormatter;
import org.elasticsearch.search.aggregations.context.numeric.ValueParser;

/**
 *
 */
public class TermsAggregatorFactory extends Aggregator.CompoundFactory<Aggregator> {

    private final FieldContext fieldContext;
    private final SearchScript script;
    private final Terms.Order order;
    private final int requiredSize;
    private final Terms.ValueType valueType;
    private final boolean multiValued;
    private final String format;

    public TermsAggregatorFactory(String name, FieldContext fieldContext, SearchScript script, boolean multiValued, Terms.Order order,
                                  int requiredSize, Terms.ValueType valueType, String format) {
        super(name);
        this.fieldContext = fieldContext;
        this.script = script;
        this.multiValued = multiValued;
        this.order = order;
        this.requiredSize = requiredSize;
        this.valueType = valueType;
        this.format = format;
    }

    @Override
    public Aggregator create(AggregationContext aggregationContext, Aggregator parent) {

        ValuesSource valuesSource = resolveValueSource(aggregationContext, parent);

        if (valuesSource instanceof BytesValuesSource) {
            return new StringTermsAggregator(name, factories, (BytesValuesSource) valuesSource, order, requiredSize, aggregationContext, parent);
        }

        if (valuesSource instanceof NumericValuesSource) {
            if (((NumericValuesSource) valuesSource).isFloatingPoint()) {
                return new DoubleTermsAggregator(name, factories, (NumericValuesSource) valuesSource, order, requiredSize, aggregationContext, parent);
            }
            return new LongTermsAggregator(name, factories, (NumericValuesSource) valuesSource, order, requiredSize, aggregationContext, parent);
        }

        throw new AggregationExecutionException("terms aggregation cannot be applied to field [" + fieldContext.field() +
                "]. It can only be applied to numeric or string fields.");

    }

    private ValuesSource resolveValueSource(AggregationContext aggregationContext, Aggregator parent) {

        if (fieldContext == null) {

            if (script == null) {
                // the user didn't specify a field or a script, so we need to fall back on a value source from the ancestors
                ValuesSource valuesSource = ValuesSourceAggregator.resolveValuesSourceFromAncestors(name, parent, null);
                if (valuesSource instanceof NumericValuesSource) {
                    if (format != null) {
                        // the user defined a format that should be used with the inherited value source... so we wrap it with the new format
                        // but right now we only support configurable date formats
                        ValueFormatter formatter = new ValueFormatter.DateTime(format);

                        return new NumericValuesSource.Delegate((NumericValuesSource) valuesSource, formatter);
                    }
                    return valuesSource;
                }
            }

            // we have a script, so it's a script value source
            switch (valueType) {
                case STRING:
                    return aggregationContext.bytesScript(script, multiValued);
                case LONG:
                case DOUBLE:
                    return aggregationContext.numericScript(script, multiValued, null);
                default:
                    throw new AggregationExecutionException("unknown field type [" + valueType.name() + "]");
            }
        }

        // date field
        if (fieldContext.mapper() instanceof DateFieldMapper) {
            DateFieldMapper mapper = (DateFieldMapper) fieldContext.mapper();
            ValueFormatter formatter = format == null ?
                    new ValueFormatter.DateTime(mapper.dateTimeFormatter()) :
                    new ValueFormatter.DateTime(format);
            ValueParser parser = new ValueParser.DateMath(mapper.dateMathParser());
            return aggregationContext.numericField(fieldContext, script, formatter, parser);
        }

        // ip field
        if (fieldContext.mapper() instanceof IpFieldMapper) {
            return aggregationContext.numericField(fieldContext, script, ValueFormatter.IPv4, ValueParser.IPv4);
        }

        if (fieldContext.indexFieldData() instanceof IndexNumericFieldData) {
            return aggregationContext.numericField(fieldContext, script, null, null);
        }

        return aggregationContext.bytesField(fieldContext, script);
    }

}
