/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics.metricselector;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.MultiValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.analytics.metricselector.MetricSelectorAggregationBuilder.METRIC_FIELD;
import static org.elasticsearch.xpack.analytics.metricselector.MetricSelectorAggregationBuilder.SELECTOR_FIELD;

class MetricSelectorAggregator extends NumericMetricsAggregator.SingleValue {


    private final MultiValuesSource.NumericMultiValuesSource valuesSources;
    private DoubleArray values;
    private DoubleArray sorts;
    private DocValueFormat format;
    private final SelectionCriteria selectorCriteria;
    private final MultiValueMode multiValueMode;

    MetricSelectorAggregator(String name, MultiValuesSource.NumericMultiValuesSource valuesSources, DocValueFormat format,
                             SelectionCriteria selectorCriteria, MultiValueMode multiValueMode, SearchContext context, Aggregator parent,
                             List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        super(name, context, parent, pipelineAggregators, metaData);
        this.valuesSources = valuesSources;
        this.format = format;
        if (valuesSources != null) {
            final BigArrays bigArrays = context.bigArrays();
            values = bigArrays.newDoubleArray(1, true);
            sorts = bigArrays.newDoubleArray(1, true);
        }
        this.multiValueMode = multiValueMode;
        this.selectorCriteria = selectorCriteria;
    }

    @Override
    public ScoreMode scoreMode() {
        return valuesSources != null && valuesSources.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
                                                final LeafBucketCollector sub) throws IOException {
        if (valuesSources == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final BigArrays bigArrays = context.bigArrays();
        final NumericDoubleValues docValues = multiValueMode.select(valuesSources.getField(METRIC_FIELD.getPreferredName(), ctx));
        final SortedNumericDoubleValues docSorts = valuesSources.getField(SELECTOR_FIELD.getPreferredName(), ctx);

        return new LeafBucketCollectorBase(sub, docValues) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                values = bigArrays.grow(values, bucket + 1);
                sorts = bigArrays.grow(sorts, bucket + 1);

                if (docValues.advanceExact(doc) && docSorts.advanceExact(doc)) {
                    if (docSorts.docValueCount() > 1) {
                        throw new AggregationExecutionException("Encountered more than one selection criteria for a " +
                            "single document. Use a script to combine multiple selection-criteria per-doc into a single value.");
                    }
                    assert docSorts.docValueCount() == 1;

                    final double sort = docSorts.nextValue();
                    double value = docValues.doubleValue();
                    double bestSort = sorts.get(bucket);

                    if (sort == selectorCriteria.select(sort, bestSort)) {
                        sorts.set(bucket, sort);
                        values.set(bucket, value);
                    }
                }
            }
        };
    }

    @Override
    public double metric(long owningBucketOrd) {
        if (valuesSources == null) {
            return Double.NaN;
        }
        return values.get(owningBucketOrd);
    }

    public double sortValue(long owningBucketOrd) {
        if (valuesSources == null) {
            return Double.NaN;
        }
        return sorts.get(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) throws IOException {
        if (valuesSources == null) {
            return buildEmptyAggregation();
        }
        return new InternalMetricSelector(name, values.get(bucket), sorts.get(bucket), selectorCriteria,
            format, pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalMetricSelector(name, Double.NaN, Double.NaN, null, format, pipelineAggregators(), metaData());
    }

    @Override
    public void doClose() {
        Releasables.close(values, sorts);
    }

}
