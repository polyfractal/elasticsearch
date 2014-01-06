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

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.percentile.providers.FrugalProvider;
import org.elasticsearch.search.aggregations.metrics.percentile.providers.PercentileProvider;
import org.elasticsearch.search.aggregations.metrics.percentile.providers.QDigestProvider;
import org.elasticsearch.search.aggregations.metrics.percentile.providers.TDigestProvider;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValueSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.numeric.NumericValuesSource;

import java.io.IOException;

/**
 *
 */
public class PercentileAggregator extends Aggregator {

    private final NumericValuesSource valuesSource;

    private ObjectArray<PercentileProvider> percentiles;
    private PercentileProvider.Factory percentileFactory;


    public PercentileAggregator(String name, long estimatedBucketsCount, NumericValuesSource valuesSource, AggregationContext context, Aggregator parent, PercentileProvider.Factory percentileFactory) {
        super(name, BucketAggregationMode.MULTI_BUCKETS, AggregatorFactories.EMPTY, estimatedBucketsCount, context, parent);
        this.valuesSource = valuesSource;
        this.percentileFactory = percentileFactory;
        if (valuesSource != null) {
            final long initialSize = estimatedBucketsCount < 2 ? 1 : estimatedBucketsCount;
            percentiles = BigArrays.newObjectArray(initialSize);
            for (long i = 0; i < initialSize; ++i) {
                percentiles.set(i, this.percentileFactory.create());
            }
        }
    }

    @Override
    public boolean shouldCollect() {
        return valuesSource != null;
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        assert valuesSource != null : "if value source is null, collect should never be called";

        DoubleValues values = valuesSource.doubleValues();
        if (values == null) {
            return;
        }

        if (owningBucketOrdinal >= percentiles.size()) {
            long from = percentiles.size();
            percentiles = BigArrays.grow(percentiles, owningBucketOrdinal + 1);
            for (long i = from; i < percentiles.size(); ++i) {
                percentiles.set(i, this.percentileFactory.create());
            }
        }

        final int valueCount = values.setDocument(doc);
        PercentileProvider percentile = percentiles.get(owningBucketOrdinal);
        for (int i = 0; i < valueCount; i++) {
            percentile.offer(values.nextValue());
        }

        percentiles.set(owningBucketOrdinal, percentile);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        if (valuesSource == null || owningBucketOrdinal >= percentiles.size()) {
            return new InternalPercentile(name, percentileFactory.create());
        }
        return new InternalPercentile(name, percentiles.get(owningBucketOrdinal));
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalPercentile(name, percentileFactory.create());
    }

    public static class Factory extends ValueSourceAggregatorFactory.LeafOnly<NumericValuesSource> {

        private final PercentileProvider.Factory percentileFactory;

        public Factory(String name, String type, ValuesSourceConfig<NumericValuesSource> valuesSourceConfig, InternalPercentile.EXECUTION_HINT execution_hint, double[] percentiles) {
            super(name, type, valuesSourceConfig);

            if (execution_hint == InternalPercentile.EXECUTION_HINT.QDIGEST) {
                this.percentileFactory = new QDigestProvider.Factory(percentiles);
            } else if (execution_hint == InternalPercentile.EXECUTION_HINT.FRUGAL) {
                this.percentileFactory = new FrugalProvider.Factory(percentiles);
            } else if (execution_hint == InternalPercentile.EXECUTION_HINT.TDIGEST) {
                this.percentileFactory = new TDigestProvider.Factory(percentiles);
            } else {
                this.percentileFactory = new TDigestProvider.Factory(percentiles);
            }
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            return new PercentileAggregator(name, 0, null, aggregationContext, parent, percentileFactory);
        }

        @Override
        protected Aggregator create(NumericValuesSource valuesSource, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
            return new PercentileAggregator(name, expectedBucketsCount, valuesSource, aggregationContext, parent, percentileFactory);
        }
    }

}
