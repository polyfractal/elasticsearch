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
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.ValuesSourceAggregator;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.bucket.multi.terms.doubles.DoubleTermsAggregator;
import org.elasticsearch.search.aggregations.bucket.multi.terms.longs.LongTermsAggregator;
import org.elasticsearch.search.aggregations.bucket.multi.terms.string.StringTermsAggregator;
import org.elasticsearch.search.aggregations.context.FieldDataContext;
import org.elasticsearch.search.aggregations.context.ValuesSource;
import org.elasticsearch.search.aggregations.context.bytes.BytesValuesSource;
import org.elasticsearch.search.aggregations.context.doubles.DoubleValuesSource;
import org.elasticsearch.search.aggregations.context.longs.LongValuesSource;

/**
 *
 */
public class TermsAggregatorFactory extends Aggregator.CompoundFactory<Aggregator> {

    private final FieldDataContext fieldDataContext;
    private final SearchScript script;
    private final Terms.Order order;
    private final int requiredSize;
    private final Terms.ScriptValueType valueType;

    /** Used for terms facet that depend on the field context of one of the ancestors in the aggregation hierarchy */
    public TermsAggregatorFactory(String name, Terms.Order order, int requiredSize) {
        this(name, null, order, requiredSize);
    }

    public TermsAggregatorFactory(String name, FieldDataContext fieldDataContext, Terms.Order order, int requiredSize) {
        this(name, fieldDataContext, null, order, requiredSize);

    }

    public TermsAggregatorFactory(String name, FieldDataContext fieldDataContext, SearchScript valueScript, Terms.Order order, int requiredSize) {
        super(name);
        this.fieldDataContext = fieldDataContext;
        this.script = valueScript;
        this.order = order;
        this.requiredSize = requiredSize;
        this.valueType = Terms.ScriptValueType.STRING;
    }

    public TermsAggregatorFactory(String name, SearchScript script, Terms.Order order, int requiredSize, Terms.ScriptValueType valueType) {
        super(name);
        this.fieldDataContext = null;
        this.script = script;
        this.order = order;
        this.requiredSize = requiredSize;
        this.valueType = valueType;
    }

    @Override
    public Aggregator create(Aggregator parent) {

        ValuesSource valuesSource = resolveValueSource(parent);
        Terms.ScriptValueType type = Terms.ScriptValueType.resolveType(valuesSource);

        switch (type) {
            case STRING:
                return new StringTermsAggregator(name, factories, (BytesValuesSource) valuesSource, order, requiredSize, parent);
            case DOUBLE:
                return new DoubleTermsAggregator(name, factories, (DoubleValuesSource) valuesSource, order, requiredSize, parent);
            case LONG:
                return new LongTermsAggregator(name, factories, (LongValuesSource) valuesSource, order, requiredSize, parent);
            default:
                throw new AggregationExecutionException("Unknown terms type [" + valueType.name() + "]");
        }

    }

    private ValuesSource resolveValueSource(Aggregator parent) {

        if (fieldDataContext == null) {

            if (script == null) {
                // the user didn't specify a field or a script, so we need to fall back on a value source from the ancestors
                return ValuesSourceAggregator.resolveValuesSource(name, parent, null);
            }

            // we have a script, so it's a script value source
            switch (valueType) {
                case STRING:    return new BytesValuesSource.Script(script);
                case LONG:      return new LongValuesSource.Script(script);
                case DOUBLE:    return new DoubleValuesSource.Script(script);
                default:        throw new AggregationExecutionException("unknown field type [" + valueType.name() + "]");
            }
        }

        if (script != null) {

            // we have both a field and a script, so the script is a value script
            if (fieldDataContext.indexFieldData() instanceof IndexNumericFieldData) {
                if (((IndexNumericFieldData) fieldDataContext.indexFieldData()).getNumericType().isFloatingPoint()) {
                    return new DoubleValuesSource.FieldData(fieldDataContext.field(), fieldDataContext.indexFieldData(), script);
                }
                return new LongValuesSource.FieldData(fieldDataContext.field(), fieldDataContext.indexFieldData(), script);
            }
            return new BytesValuesSource.FieldData(fieldDataContext.field(), fieldDataContext.indexFieldData(), script);
        }

        // we only have a field (no script)
        if (fieldDataContext.indexFieldData() instanceof IndexNumericFieldData) {
            if (((IndexNumericFieldData) fieldDataContext.indexFieldData()).getNumericType().isFloatingPoint()) {
                return new DoubleValuesSource.FieldData(fieldDataContext.field(), fieldDataContext.indexFieldData());
            }
            return new LongValuesSource.FieldData(fieldDataContext.field(), fieldDataContext.indexFieldData());
        }

        return new BytesValuesSource.FieldData(fieldDataContext.field(), fieldDataContext.indexFieldData());
    }

}
