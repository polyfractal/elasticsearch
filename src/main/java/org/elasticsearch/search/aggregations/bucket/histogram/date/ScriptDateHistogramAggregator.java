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

package org.elasticsearch.search.aggregations.bucket.histogram.date;

import org.elasticsearch.common.joda.TimeZoneRounding;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.format.ValueFormatter;
import org.elasticsearch.search.aggregations.bucket.BucketAggregator;
import org.elasticsearch.search.aggregations.bucket.histogram.ScriptHistogramCollector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class ScriptDateHistogramAggregator extends BucketAggregator implements ScriptHistogramCollector.Listener {

    private final SearchScript script;
    private final TimeZoneRounding rounding;
    private final InternalDateOrder order;
    private final boolean keyed;
    private final ValueFormatter formatter;

    ExtTLongObjectHashMap<ScriptHistogramCollector.BucketCollector> collectors;

    public ScriptDateHistogramAggregator(String name, List<Aggregator.Factory> factories,
                                         SearchScript script, TimeZoneRounding rounding, InternalDateOrder order, boolean keyed,
                                         ValueFormatter formatter, Aggregator parent) {
        super(name, factories, parent);
        this.script = script;
        this.rounding = rounding;
        this.order = order;
        this.keyed = keyed;
        this.formatter = formatter;
    }

    @Override
    public ScriptHistogramCollector collector() {
        return new ScriptHistogramCollector(this, script, rounding, this);
    }

    @Override
    public InternalAggregation buildAggregation() {
        List<DateHistogram.Bucket> buckets = new ArrayList<DateHistogram.Bucket>(collectors.size());
        for (Object collector : collectors.internalValues()) {
            if (collector == null) {
                continue;
            }
            ScriptHistogramCollector.BucketCollector bucketCollector = (ScriptHistogramCollector.BucketCollector) collector;
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
    public void onFinish(ExtTLongObjectHashMap<ScriptHistogramCollector.BucketCollector> collectors) {
        this.collectors = collectors;
    }

    public static class Factory extends BucketAggregator.Factory<ScriptDateHistogramAggregator, Factory> {

        private final SearchScript script;
        private final TimeZoneRounding rounding;
        private final InternalDateOrder order;
        private final boolean keyed;
        private final ValueFormatter formatter;

        public Factory(String name, SearchScript script, TimeZoneRounding rounding, InternalDateOrder order, boolean keyed, ValueFormatter formatter) {
            super(name);
            this.script = script;
            this.rounding = rounding;
            this.order = order;
            this.keyed = keyed;
            this.formatter = formatter;
        }

        @Override
        public ScriptDateHistogramAggregator create(Aggregator parent) {
            return new ScriptDateHistogramAggregator(name, factories, script, rounding, order, keyed, formatter, parent);
        }
    }

}
