/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.metrics.PercentilesAggregationBuilder.PercentilesConfig;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregatorSupplier;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * This factory is used to generate both TDigest and HDRHisto aggregators, depending
 * on the selected method
 */
class PercentilesAggregatorFactory extends ValuesSourceAggregatorFactory {

    private final double[] percents;
    private final PercentilesConfig percentilesConfig;
    private final boolean keyed;

    static void registerAggregators(ValuesSourceRegistry valuesSourceRegistry) {
        valuesSourceRegistry.register(PercentilesAggregationBuilder.NAME,
            List.of(CoreValuesSourceType.NUMERIC, CoreValuesSourceType.HISTOGRAM),
            new PercentilesAggregatorSupplier() {
                @Override
                public Aggregator build(String name, ValuesSource valuesSource, SearchContext context, Aggregator parent,
                                        double[] percents, PercentilesConfig percentilesConfig, boolean keyed, DocValueFormat formatter,
                                        List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {

                    if (percentilesConfig.getMethod().equals(PercentilesMethod.TDIGEST)) {
                        double compression = ((PercentilesConfig.TDigestConfig)percentilesConfig).getCompression();
                        return new TDigestPercentilesAggregator(name, valuesSource, context, parent, percents, compression, keyed,
                            formatter, pipelineAggregators, metaData);
                    } else if (percentilesConfig.getMethod().equals(PercentilesMethod.HDR)) {
                        int numSigFig = ((PercentilesConfig.HdrHistoConfig)percentilesConfig).getNumberOfSignificantValueDigits();
                        return new HDRPercentilesAggregator(name, valuesSource, context, parent, percents, numSigFig, keyed,
                            formatter, pipelineAggregators, metaData);
                    }

                    // This should already have thrown but just in case
                    throw new IllegalStateException("Unknown percentiles method: [" + percentilesConfig.getMethod().toString() + "]");
                }
            }
        );
    }

    PercentilesAggregatorFactory(String name, ValuesSourceConfig config, double[] percents,
                                 PercentilesConfig percentilesConfig, boolean keyed, QueryShardContext queryShardContext,
                                 AggregatorFactory parent, AggregatorFactories.Builder subFactoriesBuilder,
                                 Map<String, Object> metaData) throws IOException {
        super(name, config, queryShardContext, parent, subFactoriesBuilder, metaData);
        this.percents = percents;
        this.percentilesConfig = percentilesConfig;
        this.keyed = keyed;
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext,
                                        Aggregator parent,
                                        List<PipelineAggregator> pipelineAggregators,
                                        Map<String, Object> metaData) throws IOException {
        if (percentilesConfig.getMethod().equals(PercentilesMethod.TDIGEST)) {
            double compression = ((PercentilesConfig.TDigestConfig)percentilesConfig).getCompression();
            return new TDigestPercentilesAggregator(name, null, searchContext, parent, percents, compression, keyed, config.format(),
                pipelineAggregators, metaData);
        } else if (percentilesConfig.getMethod().equals(PercentilesMethod.HDR)) {
            int numSigFig = ((PercentilesConfig.HdrHistoConfig)percentilesConfig).getNumberOfSignificantValueDigits();
            return new HDRPercentilesAggregator(name, null, searchContext, parent, percents, numSigFig, keyed,
                config.format(), pipelineAggregators, metaData);
        }

        // This should already have thrown but just in case
        throw new IllegalStateException("Unknown percentiles method: [" + percentilesConfig.getMethod().toString() + "]");
    }

    @Override
    protected Aggregator doCreateInternal(ValuesSource valuesSource,
                                          SearchContext searchContext,
                                          Aggregator parent,
                                          boolean collectsFromSingleBucket,
                                          List<PipelineAggregator> pipelineAggregators,
                                          Map<String, Object> metaData) throws IOException {

        AggregatorSupplier aggregatorSupplier = ValuesSourceRegistry.getInstance().getAggregator(config.valueSourceType(),
            PercentilesAggregationBuilder.NAME);

        if (aggregatorSupplier instanceof PercentilesAggregatorSupplier == false) {
            throw new AggregationExecutionException("Registry miss-match - expected PercentilesAggregatorSupplier, found [" +
                aggregatorSupplier.getClass().toString() + "]");
        }
        PercentilesAggregatorSupplier percentilesAggregatorSupplier = (PercentilesAggregatorSupplier) aggregatorSupplier;
        return percentilesAggregatorSupplier.build(name, valuesSource, searchContext, parent, percents, percentilesConfig, keyed,
            config.format(), pipelineAggregators, metaData);
    }
}
