/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics.metricselector;

import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.MultiValuesSource;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

class MetricSelectorAggregatorFactory extends MultiValuesSourceAggregatorFactory<ValuesSource.Numeric> {
    private final SelectionCriteria selectorCriteria;
    private final MultiValueMode multiValueMode;

    MetricSelectorAggregatorFactory(String name, Map<String, ValuesSourceConfig<ValuesSource.Numeric>> configs,
                                    DocValueFormat format, SelectionCriteria selectorCriteria, MultiValueMode multiValueMode,
                                    QueryShardContext queryShardContext, AggregatorFactory parent,
                                    AggregatorFactories.Builder subFactoriesBuilder,
                                    Map<String, Object> metaData) throws IOException {
        super(name, configs, format, queryShardContext, parent, subFactoriesBuilder, metaData);
        this.selectorCriteria = selectorCriteria;
        this.multiValueMode = multiValueMode;
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext,
                                        Aggregator parent,
                                        List<PipelineAggregator> pipelineAggregators,
                                        Map<String, Object> metaData) throws IOException {
        return new MetricSelectorAggregator(name, null, format, null, null, searchContext, parent, pipelineAggregators, metaData);
    }

    @Override
    protected Aggregator doCreateInternal(SearchContext searchContext,
                                          Map<String, ValuesSourceConfig<ValuesSource.Numeric>> configs,
                                          DocValueFormat format,
                                          Aggregator parent,
                                          boolean collectsFromSingleBucket,
                                          List<PipelineAggregator> pipelineAggregators,
                                          Map<String, Object> metaData) throws IOException {
        MultiValuesSource.NumericMultiValuesSource numericMultiVS
            = new MultiValuesSource.NumericMultiValuesSource(configs, queryShardContext);
        if (numericMultiVS.areValuesSourcesEmpty()) {
            return createUnmapped(searchContext, parent, pipelineAggregators, metaData);
        }
        return new MetricSelectorAggregator(name, numericMultiVS, format, selectorCriteria, multiValueMode,
            searchContext, parent, pipelineAggregators, metaData);
    }
}
