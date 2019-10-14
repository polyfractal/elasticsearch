/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics.metricselector;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceParseHelper;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class MetricSelectorAggregationBuilder extends MultiValuesSourceAggregationBuilder.LeafOnly<ValuesSource.Numeric, MetricSelectorAggregationBuilder> {
    public static final String NAME = "metric_selector";
    public static final ParseField METRIC_FIELD = new ParseField("metric_field");
    public static final ParseField SELECTOR_FIELD = new ParseField("selector_field");
    public static final ParseField MULTIVALUE_MODE_FIELD = new ParseField("multi_value_mode");

    private SelectionCriteria selectionCriteria = SelectionCriteria.MAX;
    private MultiValueMode multiValueMode = MultiValueMode.AVG;

    private static final ObjectParser<MetricSelectorAggregationBuilder, Void> PARSER;
    static {
        PARSER = new ObjectParser<>(MetricSelectorAggregationBuilder.NAME);
        MultiValuesSourceParseHelper.declareCommon(PARSER, true, ValueType.NUMERIC);
        MultiValuesSourceParseHelper.declareField(METRIC_FIELD.getPreferredName(), PARSER, true, false);
        MultiValuesSourceParseHelper.declareField(SELECTOR_FIELD.getPreferredName(), PARSER, true, false);
        PARSER.declareString(MetricSelectorAggregationBuilder::selectionCriteria, SelectionCriteria.SELECTION_CRITERIA);
        PARSER.declareString(MetricSelectorAggregationBuilder::multiValueMode, MULTIVALUE_MODE_FIELD);
    }

    public static AggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, new MetricSelectorAggregationBuilder(aggregationName), null);
    }

    public MetricSelectorAggregationBuilder(String name) {
        super(name, ValueType.NUMERIC);
    }

    public MetricSelectorAggregationBuilder(MetricSelectorAggregationBuilder clone, AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metaData) {
        super(clone, factoriesBuilder, metaData);
    }

    public MetricSelectorAggregationBuilder metricField(MultiValuesSourceFieldConfig metricConfig) {
        metricConfig = Objects.requireNonNull(metricConfig, "Configuration for field [" + METRIC_FIELD + "] cannot be null");
        field(METRIC_FIELD.getPreferredName(), metricConfig);
        return this;
    }

    public MetricSelectorAggregationBuilder selectorField(MultiValuesSourceFieldConfig selectorConfig) {
        selectorConfig = Objects.requireNonNull(selectorConfig, "Configuration for field [" + SELECTOR_FIELD + "] cannot be null");
        field(SELECTOR_FIELD.getPreferredName(), selectorConfig);
        return this;
    }

    public MetricSelectorAggregationBuilder selectionCriteria(String criteria) {
        this.selectionCriteria = SelectionCriteria.fromString(criteria);
        return this;
    }

    public MetricSelectorAggregationBuilder multiValueMode(String mode) {
        this.multiValueMode = MultiValueMode.fromString(mode);
        return this;
    }

    /**
     * Read from a stream.
     */
    public MetricSelectorAggregationBuilder(StreamInput in) throws IOException {
        super(in, ValueType.NUMERIC);
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metaData) {
        return new MetricSelectorAggregationBuilder(this, factoriesBuilder, metaData);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        multiValueMode.writeTo(out);
        selectionCriteria.writeTo(out);
    }

    @Override
    protected MultiValuesSourceAggregatorFactory<ValuesSource.Numeric> innerBuild(QueryShardContext queryShardContext,
                                                                                  Map<String, ValuesSourceConfig<ValuesSource.Numeric>> configs,
                                                                                  DocValueFormat format,
                                                                                  AggregatorFactory parent,
                                                                                  AggregatorFactories.Builder subFactoriesBuilder) throws IOException {
        return new MetricSelectorAggregatorFactory(name, configs, format, selectionCriteria, multiValueMode,
            queryShardContext, parent, subFactoriesBuilder, metaData);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(MULTIVALUE_MODE_FIELD.getPreferredName(), multiValueMode);
        builder.field(SelectionCriteria.SELECTION_CRITERIA.getPreferredName(), selectionCriteria);
        return builder;
    }

    @Override
    public String getType() {
        return NAME;
    }
}
