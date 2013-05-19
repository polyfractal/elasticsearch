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

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.BucketAggregator;
import org.elasticsearch.search.aggregations.bucket.FieldDataBucketAggregator;
import org.elasticsearch.search.aggregations.context.FieldDataContext;
import org.elasticsearch.search.aggregations.context.ValueTransformer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class HistogramAggregator extends FieldDataBucketAggregator implements HistogramCollector.Listener {

    private final Rounding rounding;
    private final InternalOrder order;
    private final boolean keyed;

    ExtTLongObjectHashMap<HistogramCollector.BucketCollector> collectors;

    public HistogramAggregator(String name, List<Aggregator.Factory> factories, FieldDataContext fieldDataContext,
                               ValueTransformer valueTransformer, long interval, InternalOrder order, boolean keyed, Aggregator parent) {
        super(name, factories, fieldDataContext, valueTransformer, parent, true);
        this.rounding = new Rounding.Interval(interval);
        this.order = order;
        this.keyed = keyed;
    }

    @Override
    public Collector collector() {
        return new HistogramCollector(this, fieldDataContext, rounding, valueTransformer, this);
    }

    @Override
    public InternalAggregation buildAggregation() {
        List<Histogram.Bucket> buckets = new ArrayList<Histogram.Bucket>(collectors.size());
        for (Object collector : collectors.internalValues()) {
            if (collector == null) {
                continue;
            }
            HistogramCollector.BucketCollector bucketCollector = (HistogramCollector.BucketCollector) collector;
            List<InternalAggregation> aggregations = new ArrayList<InternalAggregation>(bucketCollector.aggregators.size());
            for (Aggregator aggregator : bucketCollector.aggregators) {
                aggregations.add(aggregator.buildAggregation());
            }
            buckets.add(new InternalHistogram.Bucket(bucketCollector.key, bucketCollector.docCount, aggregations));
        }
        Collections.sort(buckets, order.comparator());
        return new InternalHistogram(name, buckets, order, keyed);
    }

    @Override
    public void onFinish(ExtTLongObjectHashMap<HistogramCollector.BucketCollector> collectors) {
        this.collectors = collectors;
    }

    public static class Factory extends BucketAggregator.Factory<HistogramAggregator, Factory> {

        private final FieldDataContext fieldDataContext;
        private final ValueTransformer valueTransformer;
        private final long interval;
        private final InternalOrder order;
        private final boolean keyed;

        public Factory(String name, long interval, InternalOrder order, boolean keyed) {
            this(name, null, ValueTransformer.NONE, interval, order, keyed);
        }

        public Factory(String name, FieldDataContext fieldDataContext, ValueTransformer valueTransformer, long interval, InternalOrder order, boolean keyed) {
            super(name);
            this.fieldDataContext = fieldDataContext;
            this.valueTransformer = valueTransformer;
            this.interval = interval;
            this.order = order;
            this.keyed = keyed;
        }

        @Override
        public HistogramAggregator create(Aggregator parent) {
            return new HistogramAggregator(name, factories, fieldDataContext, valueTransformer, interval, order, keyed, parent);
        }
    }
}
