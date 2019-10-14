/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics.metricselector;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.WeightedAvg;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalMetricSelector extends InternalNumericMetricsAggregation.SingleValue implements WeightedAvg {
    private final double value;
    private final double sort;
    private final SelectionCriteria selectionCriteria;

    public static final ParseField SORT_FIELD = new ParseField("sort");
    public static final ParseField SORT_FIELD_AS_STRING = new ParseField("sort_as_string");

    InternalMetricSelector(String name, double value, double sort, SelectionCriteria selectionCriteria,
                           DocValueFormat format, List<PipelineAggregator> pipelineAggregators,
                        Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.value = value;
        this.sort = sort;
        this.format = format;
        this.selectionCriteria = selectionCriteria;
    }

    /**
     * Read from a stream.
     */
    public InternalMetricSelector(StreamInput in) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        value = in.readDouble();
        sort = in.readDouble();
        selectionCriteria = SelectionCriteria.fromStream(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeDouble(value);
        out.writeDouble(sort);
        if (selectionCriteria != null) {
            selectionCriteria.writeTo(out);
        }
    }

    @Override
    public double value() {
        return getValue();
    }

    @Override
    public double getValue() {
        return value;
    }

    double getSort() {
        return sort;
    }
    DocValueFormat getFormatter() {
        return format;
    }

    @Override
    public String getWriteableName() {
        return MetricSelectorAggregationBuilder.NAME;
    }

    @Override
    public InternalMetricSelector doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        Double bestValue = null;
        Double bestSort = null;

        for (InternalAggregation aggregation : aggregations) {
            InternalMetricSelector metric = (InternalMetricSelector) aggregation;
            if (bestValue == null) {
                bestValue = metric.getValue();
                bestSort = metric.getSort();
            } else {
                if (metric.getSort() == selectionCriteria.select(bestSort, metric.getSort())) {
                    bestSort = metric.getSort();
                    bestValue = metric.getValue();
                }
            }
        }
        return new InternalMetricSelector(getName(), bestValue == null ?  Double.NaN : bestValue,
            bestSort == null ?  Double.NaN : bestSort, selectionCriteria, format, pipelineAggregators(), getMetaData());
    }
    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonFields.VALUE.getPreferredName(), Double.isNaN(value) ? null : getValue());
        builder.field(SORT_FIELD.getPreferredName(), Double.isNaN(sort) ? null : getSort());

        String keyAsString = format.format(getSort()).toString();
        if (format != DocValueFormat.RAW) {
            builder.field(SORT_FIELD_AS_STRING.getPreferredName(), keyAsString);
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), value, sort, format.getWriteableName());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        InternalMetricSelector other = (InternalMetricSelector) obj;
        return Objects.equals(value, other.value) &&
            Objects.equals(sort, other.sort) &&
            Objects.equals(format.getWriteableName(), other.format.getWriteableName());
    }
}
