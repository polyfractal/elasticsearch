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

package org.elasticsearch.search.aggregations.bucket.geo.distance;

import com.google.common.collect.Lists;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.fielddata.GeoPointValues;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.BucketAggregator;
import org.elasticsearch.search.aggregations.bucket.FieldDataBucketAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.FieldDataContext;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class GeoDistanceAggregator extends FieldDataBucketAggregator {

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

    private final List<BucketCollector> collectors;


    GeoDistanceAggregator(String name, List<DistanceRange> ranges, List<Aggregator.Factory> factories, FieldDataContext fieldDataContext, Aggregator parent) {
        super(name, factories, fieldDataContext, parent, IndexGeoPointFieldData.class);
        collectors = Lists.newArrayListWithCapacity(ranges.size());
        for (DistanceRange range : ranges) {
            List<Aggregator> aggregators = Lists.newArrayListWithCapacity(factories.size());
            for (Aggregator.Factory factory : factories) {
                aggregators.add(factory.create(this));
            }
            collectors.add(new BucketCollector(range, aggregators, fieldDataContext));
        }
    }

    @Override
    public Collector collector() {
        return new Collector();
    }

    @Override
    public InternalAggregation buildAggregation() {
        List<GeoDistance.Bucket> buckets = Lists.newArrayListWithCapacity(collectors.size());
        for (BucketCollector collector : collectors) {
            buckets.add(collector.buildBucket());
        }
        return new InternalGeoDistance(name, buckets);
    }

    class Collector extends Aggregator.Collector {

        @Override
        public void collect(int doc, AggregationContext context) throws IOException {
            for (BucketCollector collector : collectors) {
                collector.collect(doc, context);
            }
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            for (BucketCollector collector : collectors) {
                collector.setScorer(scorer);
            }
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            for (BucketCollector collector : collectors) {
                collector.setNextReader(context);
            }
        }

        @Override
        public void postCollection() {
            for (BucketCollector collector : collectors) {
                collector.postCollection();
            }
        }
    }

    static class BucketCollector extends BucketAggregator.BucketCollector implements AggregationContext {

        private final DistanceRange range;
        private final FieldDataContext fieldDataContext;
        private final GeoPointValues[] values;

        AggregationContext currentParentContext;

        long docCount;

        BucketCollector(DistanceRange range, List<Aggregator> aggregators, FieldDataContext fieldDataContext) {
            super(aggregators);
            this.range = range;
            this.fieldDataContext = fieldDataContext;
            values = new GeoPointValues[fieldDataContext.fieldCount()];
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            super.setNextReader(context);
            fieldDataContext.loadGeoPointValues(context, values);
        }

        @Override
        protected AggregationContext onDoc(int doc, AggregationContext context) throws IOException {
            this.currentParentContext = context;

            if (values.length == 1) {
                if (matches(doc, values[0], fieldDataContext.field(0), context)) {
                    docCount++;
                    return context;
                }
                return null;
            }

            for (int i = 0; i < values.length; i++) {
                if (matches(doc, values[i], fieldDataContext.field(i), context)) {
                    docCount++;
                    return this;
                }
            }

            return null;
        }

        private boolean matches(int doc, GeoPointValues values, String field, AggregationContext context) {
            if (!values.hasValue(doc)) {
                return false;
            }
            if (!values.isMultiValued()) {
                return range.matches(values.getValue(doc)) && context.accept(field, values.getValue(doc));
            }
            for (GeoPointValues.Iter iter = values.getIter(doc); iter.hasNext();) {
                GeoPoint point = iter.next();
                if (range.matches(point) && context.accept(field, point)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        protected void postCollection(List<Aggregator> aggregators) {
        }

        @Override
        public boolean accept(String field, double value) {
            return currentParentContext.accept(field, value);
        }

        @Override
        public boolean accept(String field, long value) {
            return currentParentContext.accept(field, value);
        }

        @Override
        public boolean accept(String field, BytesRef value) {
            return currentParentContext.accept(field, value);
        }

        @Override
        public boolean accept(String field, GeoPoint value) {
            if (!currentParentContext.accept(field, value)) {
                return false;
            }
            if (!fieldDataContext.hasField(field)) {
                return true;
            }
            return range.matches(value);
        }

        InternalGeoDistance.Bucket buildBucket() {
            List<InternalAggregation> aggregations = Lists.newArrayListWithCapacity(aggregators.size());
            for (Aggregator aggregator : aggregators) {
                aggregations.add(aggregator.buildAggregation());
            }
            return new InternalGeoDistance.Bucket(range.key, range.unit, range.from, range.to, docCount, aggregations);
        }
    }

    public static class Factory extends BucketAggregator.Factory<GeoDistanceAggregator, Factory> {

        private final List<DistanceRange> ranges;
        private final FieldDataContext fieldDataContext;

        public Factory(String name, List<DistanceRange> ranges, FieldDataContext fieldDataContext) {
            super(name);
            this.ranges = ranges;
            this.fieldDataContext = fieldDataContext;
        }

        @Override
        public GeoDistanceAggregator create(Aggregator parent) {
            return new GeoDistanceAggregator(name, ranges, factories, fieldDataContext, parent);
        }
    }
}
