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

package org.elasticsearch.search.aggregations.bucket.multi.geo.distance;

import com.google.common.collect.Lists;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.fielddata.GeoPointValues;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.BucketAggregator;
import org.elasticsearch.search.aggregations.bucket.GeoPointBucketAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.FieldContext;
import org.elasticsearch.search.aggregations.context.geopoints.GeoPointValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.search.aggregations.bucket.BucketAggregator.buildAggregations;

/**
 *
 */
public class GeoDistanceAggregator extends GeoPointBucketAggregator {

    static class DistanceRange {

        String key;
        GeoPoint origin;
        double from;
        double to;
        DistanceUnit unit;
        org.elasticsearch.common.geo.GeoDistance distanceType;

        DistanceRange(String key, double from, double to) {
            this.key = key;
            this.from = from;
            this.to = to;
        }

        boolean matches(GeoPoint target) {
            double distance = distanceType.calculate(origin.getLat(), origin.getLon(), target.getLat(), target.getLon(), unit);
            return distance >= from && distance < to;
        }
    }

    private final BucketCollector[] collectors;

    public GeoDistanceAggregator(String name, GeoPointValuesSource valuesSource, List<Aggregator.Factory> factories,
                                 List<DistanceRange> ranges, SearchContext searchContext, Aggregator parent) {
        super(name, valuesSource, searchContext, parent);
        collectors = new BucketCollector[ranges.size()];
        int i = 0;
        for (DistanceRange range : ranges) {
            collectors[i++] = new BucketCollector(range, valuesSource, BucketAggregator.createAggregators(factories, parent), this);
        }
    }

    @Override
    public Collector collector() {
        return new Collector();
    }

    @Override
    public InternalAggregation buildAggregation() {
        List<GeoDistance.Bucket> buckets = Lists.newArrayListWithCapacity(collectors.length);
        for (BucketCollector collector : collectors) {
            buckets.add(collector.buildBucket());
        }
        return new InternalGeoDistance(name, buckets);
    }

    class Collector implements Aggregator.Collector {

        @Override
        public void collect(int doc) throws IOException {
            for (BucketCollector collector : collectors) {
                collector.collect(doc);
            }
        }

        @Override
        public void setNextReader(AtomicReaderContext reader, AggregationContext context) throws IOException {
            for (BucketCollector collector : collectors) {
                collector.setNextReader(reader, context);
            }
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            for (BucketCollector collector : collectors) {
                collector.setScorer(scorer);
            }
        }

        @Override
        public void postCollection() {
            for (BucketCollector collector : collectors) {
                collector.postCollection();
            }
        }
    }

    static class BucketCollector extends GeoPointBucketAggregator.BucketCollector {

        private final DistanceRange range;

        long docCount;

        BucketCollector(DistanceRange range, GeoPointValuesSource valuesSource, Aggregator[] subAggregators, Aggregator aggregator) {
            super(valuesSource, subAggregators, aggregator);
            this.range = range;
        }

        @Override
        protected boolean onDoc(int doc, GeoPointValues values, AggregationContext context) throws IOException {
            if (matches(doc, valuesSource.key(), values, context)) {
                docCount++;
                return true;
            }
            return false;
        }

        private boolean matches(int doc, String valuesSourceKey, GeoPointValues values, AggregationContext context) {
            if (!values.hasValue(doc)) {
                return false;
            }
            if (!values.isMultiValued()) {
                return range.matches(values.getValue(doc)) && context.accept(valuesSourceKey, values.getValue(doc));
            }
            for (GeoPointValues.Iter iter = values.getIter(doc); iter.hasNext();) {
                GeoPoint point = iter.next();
                if (range.matches(point) && context.accept(valuesSourceKey, point)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        protected void postCollection(Aggregator[] aggregators) {
        }

        @Override
        public boolean accept(GeoPoint value) {
            return range.matches(value);
        }

        InternalGeoDistance.Bucket buildBucket() {
            return new InternalGeoDistance.Bucket(range.key, range.unit, range.from, range.to, docCount, buildAggregations(subAggregators));
        }
    }

    public static class FieldDataFactory extends GeoPointBucketAggregator.FieldDataFactory<GeoDistanceAggregator> {

        private final List<DistanceRange> ranges;

        public FieldDataFactory(String name, FieldContext fieldContext, List<DistanceRange> ranges) {
            super(name, fieldContext, null);
            this.ranges = ranges;
        }

        @Override
        protected GeoDistanceAggregator create(GeoPointValuesSource source, SearchContext searchContext, Aggregator parent) {
            return new GeoDistanceAggregator(name, source, factories, ranges, searchContext, parent);
        }
    }

    public static class ScriptFactory extends GeoPointBucketAggregator.ScriptFactory<GeoDistanceAggregator> {

        private final List<DistanceRange> ranges;

        public ScriptFactory(String name, SearchScript script, boolean multiValued, List<DistanceRange> ranges) {
            super(name, script, multiValued);
            this.ranges = ranges;
        }

        @Override
        protected GeoDistanceAggregator create(GeoPointValuesSource source, SearchContext searchContext, Aggregator parent) {
            return new GeoDistanceAggregator(name, source, factories, ranges, searchContext, parent);
        }
    }

    public static class ContextBasedFactory extends GeoPointBucketAggregator.ContextBasedFactory<GeoDistanceAggregator> {

        private final List<DistanceRange> ranges;

        public ContextBasedFactory(String name, List<DistanceRange> ranges) {
            super(name);
            this.ranges = ranges;
        }

        @Override
        public GeoDistanceAggregator create(SearchContext searchContext, Aggregator parent) {
            return new GeoDistanceAggregator(name, null, factories, ranges, searchContext, parent);
        }
    }

}
