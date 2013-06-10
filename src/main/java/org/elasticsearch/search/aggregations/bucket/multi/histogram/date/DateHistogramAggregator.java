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

package org.elasticsearch.search.aggregations.bucket.multi.histogram.date;

import org.elasticsearch.common.joda.TimeZoneRounding;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.single.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.format.ValueFormatter;
import org.elasticsearch.search.aggregations.bucket.FieldDataBucketAggregator;
import org.elasticsearch.search.aggregations.bucket.multi.histogram.HistogramCollector;
import org.elasticsearch.search.aggregations.context.FieldDataContext;
import org.elasticsearch.search.aggregations.context.ValueTransformer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class DateHistogramAggregator extends FieldDataBucketAggregator implements HistogramCollector.Listener {

    private final TimeZoneRounding rounding;
    private final InternalDateOrder order;
    private final boolean keyed;
    private final ValueFormatter formatter;

    ExtTLongObjectHashMap<HistogramCollector.BucketCollector> collectors;

    public DateHistogramAggregator(String name, List<Aggregator.Factory> factories, FieldDataContext fieldDataContext,
                                   ValueTransformer valueTransformer, TimeZoneRounding rounding, InternalDateOrder order, boolean keyed,
                                   ValueFormatter formatter, Aggregator parent) {
        super(name, factories, fieldDataContext, valueTransformer, parent, IndexNumericFieldData.class);
        this.rounding = rounding;
        this.order = order;
        this.keyed = keyed;
        this.formatter = formatter;
    }

    @Override
    public Collector collector() {
        return new HistogramCollector(this, fieldDataContext, rounding, valueTransformer, this);
    }

    @Override
    public InternalAggregation buildAggregation() {
        List<DateHistogram.Bucket> buckets = new ArrayList<DateHistogram.Bucket>(collectors.size());
        for (Object collector : collectors.internalValues()) {
            if (collector == null) {
                continue;
            }
            HistogramCollector.BucketCollector bucketCollector = (HistogramCollector.BucketCollector) collector;
            List<InternalAggregation> aggregations = new ArrayList<InternalAggregation>(bucketCollector.aggregators.size());
            for (Aggregator aggregator : bucketCollector.aggregators) {
                aggregations.add(aggregator.buildAggregation());
            }
            buckets.add(new InternalDateHistogram.Bucket(bucketCollector.key, formatter.format(bucketCollector.key), bucketCollector.docCount, aggregations));
        }
        Collections.sort(buckets, order.comparator());
        return new InternalDateHistogram(name, buckets, order, keyed);
    }

    @Override
    public void onFinish(ExtTLongObjectHashMap<HistogramCollector.BucketCollector> collectors) {
        this.collectors = collectors;
    }

    public static class Factory extends SingleBucketAggregator.Factory<DateHistogramAggregator, Factory> {

        private final FieldDataContext fieldDataContext;
        private final ValueTransformer valueTransformer;
        private final TimeZoneRounding rounding;
        private final InternalDateOrder order;
        private final boolean keyed;
        private final ValueFormatter formatter;

        public Factory(String name, TimeZoneRounding rounding, InternalDateOrder order, boolean keyed, ValueFormatter formatter) {
            this(name, null, ValueTransformer.NONE, rounding, order, keyed, formatter);
        }

        public Factory(String name, FieldDataContext fieldDataContext, ValueTransformer valueTransformer, TimeZoneRounding rounding, InternalDateOrder order, boolean keyed, ValueFormatter formatter) {
            super(name);
            this.fieldDataContext = fieldDataContext;
            this.valueTransformer = valueTransformer;
            this.rounding = rounding;
            this.order = order;
            this.keyed = keyed;
            this.formatter = formatter;
        }

        @Override
        public DateHistogramAggregator create(Aggregator parent) {
            return new DateHistogramAggregator(name, factories, fieldDataContext, valueTransformer, rounding, order, keyed, formatter, parent);
        }
    }
}
