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

package org.elasticsearch.search.aggregations.bucket.range;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.BucketAggregator;
import org.elasticsearch.search.aggregations.bucket.FieldDataBucketAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.FieldDataContext;
import org.elasticsearch.search.aggregations.context.ValueTransformer;
import org.elasticsearch.search.aggregations.format.ValueFormatter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class RangeAggregator extends FieldDataBucketAggregator {

    static class Range {

        final String key;
        final double from;
        final double to;

        Range(String key, double from, double to) {
            this.key = key;
            this.from = from;
            this.to = to;
        }

        boolean matches(double value) {
            return value >= from && value < to;
        }

        @Override
        public String toString() {
            return "(" + from + " to " + to + "]";
        }
    }

    private final boolean keyed;

    List<RangeCollector> rangeCollectors;

    RangeAggregator(String name, List<Aggregator.Factory> factories, FieldDataContext fieldDataContext,
                    ValueTransformer valueTransformer, ValueFormatter valueFormatter, List<Range> ranges, boolean keyed, Aggregator parent) {
        super(name, factories, fieldDataContext, valueTransformer, parent, IndexNumericFieldData.class);
        this.keyed = keyed;
        rangeCollectors = new ArrayList<RangeCollector>(ranges.size());
        for (Range range : ranges) {
            List<Aggregator> aggregators = new ArrayList<Aggregator>(factories.size());
            for (Aggregator.Factory factory : factories) {
                aggregators.add(factory.create(this));
            }
            rangeCollectors.add(new RangeCollector(range, aggregators, valueTransformer, valueFormatter, this.fieldDataContext));
        }
    }

    @Override
    public Collector collector() {
        return new Collector(rangeCollectors);
    }

    @Override
    public InternalAggregation buildAggregation() {
        List<InternalRange.Bucket> buckets = new ArrayList<InternalRange.Bucket>(rangeCollectors.size());
        for (RangeCollector collector : rangeCollectors) {
            buckets.add(collector.buildBucket());
        }
        return new InternalRange(name, buckets, keyed);
    }

    class Collector extends Aggregator.Collector {

        private final List<RangeCollector> collectors;

        Collector(List<RangeCollector> collectors) {
            this.collectors = collectors;
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            valueTransformer.setScorer(scorer);
            for (RangeCollector collector : collectors) {
                collector.setScorer(scorer);
            }
        }

        @Override
        public void collect(int doc, AggregationContext context) throws IOException {
            valueTransformer.setNextDocId(doc);
            for (RangeCollector collector : collectors) {
                collector.collect(doc, context);
            }
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            valueTransformer.setNextReader(context);
            for (RangeCollector collector : collectors) {
                collector.setNextReader(context);
            }
        }

        @Override
        public void postCollection() {
            for (RangeCollector collector : collectors) {
                collector.postCollection();
            }
        }
    }

    static class RangeCollector extends BucketCollector implements AggregationContext {

        private final Range range;
        private final FieldDataContext fieldDataContext;
        private final DoubleValues[] values;
        private final ValueTransformer valueTransformer;
        private final ValueFormatter valueFormatter;

        private AggregationContext currentParentContext;

        long docCount;

        RangeCollector(Range range, List<Aggregator> aggregators, ValueTransformer valueTransformer, ValueFormatter valueFormatter, FieldDataContext fieldDataContext) {
            super(aggregators);
            this.range = range;
            this.fieldDataContext = fieldDataContext;
            this.valueTransformer = valueTransformer;
            this.valueFormatter = valueFormatter;
            this.values = new DoubleValues[fieldDataContext.fieldCount()];
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            fieldDataContext.loadDoubleValues(context, values);
            super.setNextReader(context);
        }


        @Override
        protected AggregationContext onDoc(int doc, AggregationContext context) throws IOException {
            this.currentParentContext = context;

            // when aggregating the values of a single field
            if (values.length == 1) {
                String field = fieldDataContext.fields()[0];
                if (matches(doc, field, values[0], context)) {
                    docCount++;
                    if (!values[0].isMultiValued()) {
                        // when aggregating a single field, and it's a single valued field, we can just keep the
                        // the existing aggregation context. The only time we need to change the context is when
                        // a single document may *partially* match a range - either aggregating on a single field which
                        // is multi valued or when aggregating on multiple fields - in either case the document might
                        // match the range based on some values while other field values don't. In the case here we don't
                        // have this problem because if a document matches a range it can only be based on a single value
                        // and we only pass the document down the aggregation hierarchy if it does - thus, not running
                        // the risk that an aggregator down the hierarchy will try to aggregate on this value.
                        return context;
                    }
                    return this;
                }

                return null;
            }

            for (int i = 0; i < values.length; i++) {
                String field = fieldDataContext.field(i);
                if (matches(doc, field, values[i], context)) {
                    docCount++;
                    return this;
                }
            }

            return null;
        }

        private boolean matches(int doc, String field, DoubleValues values, AggregationContext context) {
            if (!values.hasValue(doc)) {
                return false;
            }
            if (!values.isMultiValued()) {
                double value = valueTransformer.transform(values.getValue(doc));
                return context.accept(field, value) && range.matches(value);
            }
            for (DoubleValues.Iter iter = values.getIter(doc); iter.hasNext();) {
                double value = valueTransformer.transform(iter.next());
                if (context.accept(field, value) && range.matches(value)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        protected void postCollection(List<Aggregator> aggregators) {
        }

        InternalRange.Bucket buildBucket() {
            return new InternalRange.Bucket(range.key, range.from, range.to, docCount, valueFormatter, buildAggregations());
        }

        @Override
        public boolean accept(String field, double value) {
            if (!currentParentContext.accept(field, value)) {
                return false;
            }
            if (!fieldDataContext.hasField(field)) {
                return true;
            }
            return range.matches(value);
        }

        @Override
        public boolean accept(String field, long value) {
            return accept(field, (double) value);
        }

        @Override
        public boolean accept(String field, BytesRef value) {
            return currentParentContext.accept(field, value);
        }

        @Override
        public boolean accept(String field, GeoPoint value) {
            return currentParentContext.accept(field, value);
        }
    }

    public static class Factory extends BucketAggregator.Factory<RangeAggregator, Factory> {

        private final FieldDataContext fieldDataContext;
        private final ValueTransformer valueTransformer;
        private final ValueFormatter valueFormatter;
        private final List<Range> ranges;
        private final boolean keyed;

        public Factory(String name, FieldDataContext fieldDataContext, List<Range> ranges, boolean keyed) {
            this(name, fieldDataContext, ValueTransformer.NONE, null, ranges, keyed);
        }

        public Factory(String name, FieldDataContext fieldDataContext, ValueFormatter valueFormatter, List<Range> ranges, boolean keyed) {
            this(name, fieldDataContext, ValueTransformer.NONE, valueFormatter, ranges, keyed);
        }

        public Factory(String name, FieldDataContext fieldDataContext, ValueTransformer valueTransformer, ValueFormatter valueFormatter, List<Range> ranges, boolean keyed) {
            super(name);
            this.fieldDataContext = fieldDataContext;
            this.valueTransformer = valueTransformer;
            this.valueFormatter = valueFormatter;
            this.ranges = ranges;
            this.keyed = keyed;
        }

        @Override
        public RangeAggregator create(Aggregator parent) {
            return new RangeAggregator(name, factories, fieldDataContext, valueTransformer, valueFormatter, ranges, keyed, parent);
        }
    }

}
