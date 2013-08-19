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

package org.elasticsearch.search.aggregations.bucket.multi.histogram;

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.LongBucketsAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.context.numeric.NumericValuesSource;
import org.elasticsearch.search.aggregations.context.numeric.ValueFormatter;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class HistogramAggregator extends LongBucketsAggregator {

    private final List<Aggregator.Factory> factories;
    private final Rounding rounding;
    private final InternalOrder order;
    private final boolean keyed;
    private final AbstractHistogramBase.Factory histogramFactory;
    private final Recycler.V<ExtTLongObjectHashMap<HistogramCollector.BucketCollector>> collectors;

    public HistogramAggregator(String name,
                               List<Aggregator.Factory> factories,
                               Rounding rounding,
                               InternalOrder order,
                               boolean keyed,
                               @Nullable NumericValuesSource valuesSource,
                               AbstractHistogramBase.Factory histogramFactory,
                               AggregationContext aggregationContext,
                               Aggregator parent) {

        super(name, valuesSource, aggregationContext, parent);
        this.factories = factories;
        this.rounding = rounding;
        this.order = order;
        this.keyed = keyed;
        this.histogramFactory = histogramFactory;
        this.collectors = aggregationContext.cacheRecycler().longObjectMap(-1);
    }

    @Override
    public Collector collector() {
        return valuesSource != null ? new HistogramCollector(this, factories, valuesSource, rounding, collectors.v()) : null;
    }

    @Override
    public InternalAggregation buildAggregation() {
        List<HistogramBase.Bucket> buckets = new ArrayList<HistogramBase.Bucket>(collectors.v().size());
        for (Object collector : collectors.v().internalValues()) {
            if (collector == null) {
                continue;
            }
            HistogramCollector.BucketCollector bucketCollector = (HistogramCollector.BucketCollector) collector;
            List<InternalAggregation> aggregations = new ArrayList<InternalAggregation>(bucketCollector.subAggregators.length);
            for (int i = 0; i < bucketCollector.subAggregators.length; i++) {
                aggregations.add(bucketCollector.subAggregators[i].buildAggregation());
            }
            buckets.add(histogramFactory.createBucket(bucketCollector.key, bucketCollector.docCount, aggregations));
        }
        collectors.release();
        CollectionUtil.introSort(buckets, order.comparator());

        // value source will be null for unmapped fields
        ValueFormatter formatter = valuesSource != null ? valuesSource.formatter() : null;

        return histogramFactory.create(name, buckets, order, formatter, keyed);
    }

    public static class Factory extends CompoundFactory<NumericValuesSource> {

        private final Rounding rounding;
        private final InternalOrder order;
        private final boolean keyed;
        private final AbstractHistogramBase.Factory histogramFactory;

        public Factory(String name, ValuesSourceConfig<NumericValuesSource> valueSourceConfig,
                       Rounding rounding, InternalOrder order, boolean keyed, AbstractHistogramBase.Factory histogramFactory) {
            super(name, valueSourceConfig);
            this.rounding = rounding;
            this.order = order;
            this.keyed = keyed;
            this.histogramFactory = histogramFactory;
        }

        @Override
        protected HistogramAggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            return new HistogramAggregator(name, factories, rounding, order, keyed, null, histogramFactory, aggregationContext, parent);
        }

        @Override
        protected HistogramAggregator create(NumericValuesSource valuesSource, AggregationContext aggregationContext, Aggregator parent) {
            return new HistogramAggregator(name, factories, rounding, order, keyed, valuesSource, histogramFactory, aggregationContext, parent);
        }
    }
}
