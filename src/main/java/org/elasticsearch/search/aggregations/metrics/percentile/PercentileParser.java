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

package org.elasticsearch.search.aggregations.metrics.percentile;

import com.carrotsearch.hppc.DoubleArrayList;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.metrics.ValuesSourceMetricsAggregatorParser;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.numeric.NumericValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class PercentileParser extends ValuesSourceMetricsAggregatorParser<InternalPercentile> {

    /**
     * We must override the parse method because we need to allow custom parameters
     * (execution_hint, etc) which is not possible otherwise
     */
    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        ValuesSourceConfig<NumericValuesSource> config = new ValuesSourceConfig<NumericValuesSource>(NumericValuesSource.class);

        String field = null;
        String script = null;
        String scriptLang = null;
        InternalPercentile.EXECUTION_HINT execution_hint = InternalPercentile.EXECUTION_HINT.TDIGEST;
        double[] percentiles = null;
        Map<String, Object> scriptParams = null;
        boolean assumeSorted = false;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("field".equals(currentFieldName)) {
                    field = parser.text();
                } else if ("script".equals(currentFieldName)) {
                    script = parser.text();
                } else if ("lang".equals(currentFieldName)) {
                    scriptLang = parser.text();
                } else if ("execution_hint".equals(currentFieldName)) {

                    if ("tdigest".equals(parser.text())) {
                        execution_hint = InternalPercentile.EXECUTION_HINT.TDIGEST;
                    } else if ("qdigest".equals(parser.text())) {
                        execution_hint = InternalPercentile.EXECUTION_HINT.QDIGEST;
                    } else if ("frugal".equals(parser.text())) {
                        execution_hint = InternalPercentile.EXECUTION_HINT.FRUGAL;
                    }
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("percentiles".equals(currentFieldName)) {

                    // @TODO better way to do this?
                    DoubleArrayList tPercentiles = new DoubleArrayList(5);
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        tPercentiles.add(parser.doubleValue());
                    }
                    percentiles = tPercentiles.toArray();

                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("params".equals(currentFieldName)) {
                    scriptParams = parser.map();
                }
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if ("script_values_sorted".equals(currentFieldName)) {
                    assumeSorted = parser.booleanValue();
                }
            }
        }

        if (percentiles == null) {
            percentiles = getDefaultPercentiles();
        }

        if (script != null) {
            config.script(context.scriptService().search(context.lookup(), scriptLang, script, scriptParams));
        }

        if (!assumeSorted && requiresSortedValues()) {
            config.ensureSorted(true);
        }

        if (field == null) {
            return createFactoryWithCustom(aggregationName, config, execution_hint, percentiles);
        }

        FieldMapper<?> mapper = context.smartNameFieldMapper(field);
        if (mapper == null) {
            config.unmapped(true);
            return createFactoryWithCustom(aggregationName, config, execution_hint, percentiles);
        }

        IndexFieldData<?> indexFieldData = context.fieldData().getForField(mapper);
        config.fieldContext(new FieldContext(field, indexFieldData));
        return createFactoryWithCustom(aggregationName, config, execution_hint, percentiles);
    }


    @Override
    public String type() {
        return InternalPercentile.TYPE.name();
    }

    /**
     * This is likely never used, but provided to satisfy the abstract parent.  Sets default
     * algorithm to TDigest
     */
    @Override
    protected AggregatorFactory createFactory(String aggregationName, ValuesSourceConfig<NumericValuesSource> config) {
        double[] percentiles = getDefaultPercentiles();
        return createFactoryWithCustom(aggregationName, config, InternalPercentile.EXECUTION_HINT.TDIGEST, percentiles);
    }


    protected AggregatorFactory createFactoryWithCustom(
            String aggregationName,
            ValuesSourceConfig<NumericValuesSource> config,
            InternalPercentile.EXECUTION_HINT execution_hint,
            double[] intervals
    ) {
        return new PercentileAggregator.Factory(aggregationName, type(), config, execution_hint, intervals);
    }


    private double[] getDefaultPercentiles() {
        return new double[] {1, 5, 25, 50, 75, 95, 99};
    }

}
