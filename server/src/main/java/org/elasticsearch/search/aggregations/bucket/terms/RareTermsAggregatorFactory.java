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

package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class RareTermsAggregatorFactory extends ValuesSourceAggregatorFactory<ValuesSource, RareTermsAggregatorFactory> {

    private static final DeprecationLogger DEPRECATION_LOGGER
        = new DeprecationLogger(Loggers.getLogger(TermsAggregatorFactory.class));

    private final IncludeExclude includeExclude;
    private final String executionHint;
    private final SubAggCollectionMode collectMode = SubAggCollectionMode.BREADTH_FIRST;
    private final int maxDocCount;

    public RareTermsAggregatorFactory(String name, ValuesSourceConfig<ValuesSource> config,
                                      IncludeExclude includeExclude, String executionHint,
                                      SearchContext context,
                                      AggregatorFactory<?> parent, AggregatorFactories.Builder subFactoriesBuilder,
                                      Map<String, Object> metaData, int maxDocCount) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metaData);
        this.includeExclude = includeExclude;
        this.executionHint = executionHint;
        this.maxDocCount = maxDocCount;
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
        throws IOException {
        final InternalAggregation aggregation = new UnmappedRareTerms(name, pipelineAggregators, metaData);
        return new NonCollectingAggregator(name, context, parent, factories, pipelineAggregators, metaData) {
            @Override
            public InternalAggregation buildEmptyAggregation() {
                return aggregation;
            }
        };
    }

    @Override
    protected Aggregator doCreateInternal(ValuesSource valuesSource, Aggregator parent, boolean collectsFromSingleBucket,
                                          List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        if (collectsFromSingleBucket == false) {
            return asMultiBucketAggregator(this, context, parent);
        }
        if (valuesSource instanceof ValuesSource.Bytes) {
            ExecutionMode execution = null;
            if (executionHint != null) {
                execution = ExecutionMode.fromString(executionHint, DEPRECATION_LOGGER);
            }
            // In some cases, using ordinals is just not supported: override it
            if (valuesSource instanceof ValuesSource.Bytes.WithOrdinals == false) {
                execution = ExecutionMode.MAP;
            }
            if (execution == null) {
                execution = ExecutionMode.MAP; //TODO global ords not implemented yet
            }

            DocValueFormat format = config.format();
            if ((includeExclude != null) && (includeExclude.isRegexBased()) && format != DocValueFormat.RAW) {
                throw new AggregationExecutionException("Aggregation [" + name + "] cannot support regular expression style include/exclude "
                    + "settings as they can only be applied to string fields. Use an array of values for include/exclude clauses");
            }

            return execution.create(name, factories, valuesSource, format,
                includeExclude, context, parent, pipelineAggregators, metaData, maxDocCount);
        }

        if ((includeExclude != null) && (includeExclude.isRegexBased())) {
            throw new AggregationExecutionException("Aggregation [" + name + "] cannot support regular expression style include/exclude "
                + "settings as they can only be applied to string fields. Use an array of numeric values for include/exclude clauses used to filter numeric fields");
        }

        if (valuesSource instanceof ValuesSource.Numeric) {
            IncludeExclude.LongFilter longFilter = null;
            if (((ValuesSource.Numeric) valuesSource).isFloatingPoint()) {
                if (includeExclude != null) {
                    longFilter = includeExclude.convertToDoubleFilter();
                }
                return new DoubleRareTermsAggregator(name, factories, (ValuesSource.Numeric) valuesSource,
                    config.format(), context, parent, longFilter, maxDocCount, pipelineAggregators, metaData);
            }
            if (includeExclude != null) {
                longFilter = includeExclude.convertToLongFilter(config.format());
            }
            return new LongRareTermsAggregator(name, factories, (ValuesSource.Numeric) valuesSource, config.format(),
                context, parent, longFilter, maxDocCount, pipelineAggregators, metaData);
        }

        throw new AggregationExecutionException("terms aggregation cannot be applied to field [" + config.fieldContext().field()
            + "]. It can only be applied to numeric or string fields.");
    }

    public enum ExecutionMode {

        MAP(new ParseField("map")) {

            @Override
            Aggregator create(String name, AggregatorFactories factories, ValuesSource valuesSource,
                              DocValueFormat format, IncludeExclude includeExclude,
                              SearchContext context, Aggregator parent,
                               List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData, long maxDocCount)
                throws IOException {
                final IncludeExclude.StringFilter filter = includeExclude == null ? null : includeExclude.convertToStringFilter(format);
                return new StringRareTermsAggregator(name, factories, valuesSource, format, filter,
                    context, parent, pipelineAggregators, metaData, maxDocCount);
            }

            @Override
            boolean needsGlobalOrdinals() {
                return false;
            }

        };

        public static ExecutionMode fromString(String value, final DeprecationLogger deprecationLogger) {
            switch (value) {
                case "map":
                    return MAP;
                default:
                    throw new IllegalArgumentException("Unknown `execution_hint`: [" + value + "], expected any of [map]");
            }
        }

        private final ParseField parseField;

        ExecutionMode(ParseField parseField) {
            this.parseField = parseField;
        }

        abstract Aggregator create(String name, AggregatorFactories factories, ValuesSource valuesSource,
                                   DocValueFormat format, IncludeExclude includeExclude,
                                   SearchContext context, Aggregator parent,
                                   List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData,
                                   long maxDocCount)
            throws IOException;

        abstract boolean needsGlobalOrdinals();

        @Override
        public String toString() {
            return parseField.getPreferredName();
        }
    }

}
