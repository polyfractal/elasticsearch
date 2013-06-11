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
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.DoubleBucketAggregator;
import org.elasticsearch.search.aggregations.bucket.multi.histogram.HistogramCollector;
import org.elasticsearch.search.aggregations.context.FieldDataContext;
import org.elasticsearch.search.aggregations.context.doubles.DoubleValuesSource;
import org.elasticsearch.search.aggregations.format.ValueFormatter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class DateHistogramAggregator extends DoubleBucketAggregator implements HistogramCollector.Listener {

    private final List<Aggregator.Factory> factories;
    private final TimeZoneRounding rounding;
    private final InternalDateOrder order;
    private final boolean keyed;
    private final ValueFormatter formatter;

    ExtTLongObjectHashMap<HistogramCollector.BucketCollector> collectors;

    public DateHistogramAggregator(String name, List<Aggregator.Factory> factories, DoubleValuesSource valuesSource,
                                   TimeZoneRounding rounding, InternalDateOrder order, boolean keyed, ValueFormatter formatter,
                                   Aggregator parent) {
        super(name, valuesSource, parent);
        this.factories = factories;
        this.rounding = rounding;
        this.order = order;
        this.keyed = keyed;
        this.formatter = formatter;
    }

    @Override
    public Collector collector() {
        return new HistogramCollector(this, factories, valuesSource, rounding, this);
    }

    @Override
    public InternalAggregation buildAggregation() {
        List<DateHistogram.Bucket> buckets = new ArrayList<DateHistogram.Bucket>(collectors.size());
        for (Object collector : collectors.internalValues()) {
            if (collector == null) {
                continue;
            }
            HistogramCollector.BucketCollector bucketCollector = (HistogramCollector.BucketCollector) collector;
            List<InternalAggregation> aggregations = new ArrayList<InternalAggregation>(bucketCollector.aggregators.length);
            for (int i = 0; i < bucketCollector.aggregators.length; i++) {
                aggregations.add(bucketCollector.aggregators[i].buildAggregation());
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

    public static class FieldDataFactory extends DoubleBucketAggregator.FieldDataFactory<DateHistogramAggregator> {

        private final TimeZoneRounding rounding;
        private final InternalDateOrder order;
        private final boolean keyed;
        private final ValueFormatter formatter;

        public FieldDataFactory(String name, FieldDataContext fieldDataContext, TimeZoneRounding rounding, InternalDateOrder order, boolean keyed, ValueFormatter formatter) {
            super(name, fieldDataContext);
            this.rounding = rounding;
            this.order = order;
            this.keyed = keyed;
            this.formatter = formatter;
        }

        public FieldDataFactory(String name, FieldDataContext fieldDataContext, SearchScript valueScript, TimeZoneRounding rounding, InternalDateOrder order, boolean keyed, ValueFormatter formatter) {
            super(name, fieldDataContext, valueScript);
            this.rounding = rounding;
            this.order = order;
            this.keyed = keyed;
            this.formatter = formatter;
        }

        @Override
        protected DateHistogramAggregator create(DoubleValuesSource source, Aggregator parent) {
            return new DateHistogramAggregator(name, factories, source, rounding, order, keyed, formatter, parent);
        }
    }

    public static class ScriptFactory extends DoubleBucketAggregator.ScriptFactory<DateHistogramAggregator> {

        private final TimeZoneRounding rounding;
        private final InternalDateOrder order;
        private final boolean keyed;
        private final ValueFormatter formatter;

        public ScriptFactory(String name, SearchScript script, TimeZoneRounding rounding, InternalDateOrder order, boolean keyed, ValueFormatter formatter) {
            super(name, script);
            this.rounding = rounding;
            this.order = order;
            this.keyed = keyed;
            this.formatter = formatter;
        }

        @Override
        protected DateHistogramAggregator create(DoubleValuesSource source, Aggregator parent) {
            return new DateHistogramAggregator(name, factories, source, rounding, order, keyed, formatter, parent);
        }
    }

    public static class ContextBasedFactory extends DoubleBucketAggregator.ContextBasedFactory<DateHistogramAggregator> {

        private final TimeZoneRounding rounding;
        private final InternalDateOrder order;
        private final boolean keyed;
        private final ValueFormatter formatter;

        public ContextBasedFactory(String name, TimeZoneRounding rounding, InternalDateOrder order, boolean keyed, ValueFormatter formatter) {
            super(name);
            this.rounding = rounding;
            this.order = order;
            this.keyed = keyed;
            this.formatter = formatter;
        }

        @Override
        public DateHistogramAggregator create(Aggregator parent) {
            return new DateHistogramAggregator(name, factories, null, rounding, order, keyed, formatter, parent);
        }
    }
}
