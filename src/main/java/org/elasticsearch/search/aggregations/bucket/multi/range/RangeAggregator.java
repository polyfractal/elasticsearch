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

package org.elasticsearch.search.aggregations.bucket.multi.range;

import com.google.common.collect.Lists;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.DoubleBucketAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.FieldContext;
import org.elasticsearch.search.aggregations.context.ValueSpace;
import org.elasticsearch.search.aggregations.context.numeric.NumericValuesSource;
import org.elasticsearch.search.aggregations.context.numeric.ValueFormatter;
import org.elasticsearch.search.aggregations.context.numeric.ValueParser;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.search.aggregations.bucket.BucketAggregator.buildAggregations;
import static org.elasticsearch.search.aggregations.bucket.BucketAggregator.createSubAggregators;

/**
 *
 */
public class RangeAggregator extends DoubleBucketAggregator {

    public static class Range {

        final String key;
        double from = Double.NEGATIVE_INFINITY;
        String fromAsStr;
        double to = Double.POSITIVE_INFINITY;
        String toAsStr;

        public Range(String key, double from, String fromAsStr, double to, String toAsStr) {
            this.key = key;
            this.from = from;
            this.fromAsStr = fromAsStr;
            this.to = to;
            this.toAsStr = toAsStr;
        }

        boolean matches(double value) {
            return value >= from && value < to;
        }

        @Override
        public String toString() {
            return "(" + from + " to " + to + "]";
        }

        void process(ValueParser parser, AggregationContext aggregationContext) {
            if (fromAsStr != null) {
                from = parser != null ? parser.parseDouble(fromAsStr, aggregationContext.searchContext()) : Double.valueOf(fromAsStr);
            }
            if (toAsStr != null) {
                to = parser != null ? parser.parseDouble(toAsStr, aggregationContext.searchContext()) : Double.valueOf(toAsStr);
            }
        }
    }

    private final boolean keyed;
    private final AbstractRangeBase.Factory rangeFactory;
    BucketCollector[] bucketCollectors;

    public RangeAggregator(String name,
                           List<Aggregator.Factory> factories,
                           NumericValuesSource valuesSource,
                           AbstractRangeBase.Factory rangeFactory,
                           List<Range> ranges,
                           boolean keyed,
                           AggregationContext aggregationContext,
                           Aggregator parent) {

        super(name, valuesSource, aggregationContext, parent);
        this.keyed = keyed;
        this.rangeFactory = rangeFactory;
        bucketCollectors = new BucketCollector[ranges.size()];
        int i = 0;
        for (Range range : ranges) {
            range.process(valuesSource.parser(), aggregationContext);
            bucketCollectors[i++] = new BucketCollector(range, valuesSource, createSubAggregators(factories, this), this);
        }
    }

    @Override
    public Collector collector() {
        return new Collector();
    }

    @Override
    public InternalAggregation buildAggregation() {
        List<RangeBase.Bucket> buckets = Lists.newArrayListWithCapacity(bucketCollectors.length);
        for (int i = 0; i < bucketCollectors.length; i++) {
            buckets.add(bucketCollectors[i].buildBucket(rangeFactory));
        }
        return rangeFactory.create(name, buckets, valuesSource.formatter(), keyed);
    }

    class Collector implements Aggregator.Collector {

        @Override
        public void collect(int doc, ValueSpace valueSpace) throws IOException {
            for (int i = 0; i < bucketCollectors.length; i++) {
                bucketCollectors[i].collect(doc, valueSpace);
            }
        }

        @Override
        public void postCollection() {
            for (int i = 0; i < bucketCollectors.length; i++) {
                bucketCollectors[i].postCollection();
            }
        }
    }

    static class BucketCollector extends DoubleBucketAggregator.BucketCollector {

        private final Range range;

        long docCount;

        BucketCollector(Range range, NumericValuesSource valuesSource, Aggregator[] aggregators, Aggregator aggregator) {
            super(valuesSource, aggregators, aggregator);
            this.range = range;
        }

        @Override
        protected boolean onDoc(int doc, DoubleValues values, ValueSpace context) throws IOException {
            if (matches(doc, values, context)) {
                docCount++;
                return true;
            }
            return false;
        }

        private boolean matches(int doc, DoubleValues values, ValueSpace context) {
            if (!values.hasValue(doc)) {
                return false;
            }

            Object valueSourceKey = valuesSource.key();
            if (!values.isMultiValued()) {
                double value = values.getValue(doc);
                return context.accept(valueSourceKey, value) && range.matches(value);
            }

            for (DoubleValues.Iter iter = values.getIter(doc); iter.hasNext();) {
                double value = iter.next();
                if (context.accept(valueSourceKey, value) && range.matches(value)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        protected void postCollection(Aggregator[] aggregators) {
        }

        @Override
        public boolean accept(double value) {
            return range.matches(value);
        }

        RangeBase.Bucket buildBucket(AbstractRangeBase.Factory factory) {
            return factory.createBucket(range.key, range.from, range.to, docCount, buildAggregations(subAggregators), valuesSource.formatter());
        }

    }

    public static class FieldDataFactory extends DoubleBucketAggregator.FieldDataFactory<RangeAggregator> {

        private final AbstractRangeBase.Factory rangeFactory;
        private final List<Range> ranges;
        private final boolean keyed;

        public FieldDataFactory(String name,
                                FieldContext fieldContext,
                                SearchScript valueScript,
                                ValueFormatter formatter,
                                ValueParser parser,
                                AbstractRangeBase.Factory rangeFactory,
                                List<Range> ranges,
                                boolean keyed) {

            super(name, fieldContext, valueScript, formatter, parser);
            this.rangeFactory = rangeFactory;
            this.ranges = ranges;
            this.keyed = keyed;
        }

        @Override
        protected RangeAggregator create(NumericValuesSource source, AggregationContext aggregationContext, Aggregator parent) {
            return new RangeAggregator(name, factories, source, rangeFactory, ranges, keyed, aggregationContext, parent);
        }

    }

    public static class ScriptFactory extends DoubleBucketAggregator.ScriptFactory<RangeAggregator> {

        private final AbstractRangeBase.Factory rangeFactory;
        private final List<Range> ranges;
        private final boolean keyed;

        public ScriptFactory(String name,
                             SearchScript script,
                             boolean multiValued,
                             ValueFormatter formatter,
                             AbstractRangeBase.Factory rangeFactory,
                             List<Range> ranges,
                             boolean keyed) {

            super(name, script, multiValued, formatter);
            this.rangeFactory = rangeFactory;
            this.ranges = ranges;
            this.keyed = keyed;
        }

        @Override
        protected RangeAggregator create(NumericValuesSource source, AggregationContext aggregationContext, Aggregator parent) {
            return new RangeAggregator(name, factories, source, rangeFactory, ranges, keyed, aggregationContext, parent);
        }
    }

    public static class ContextBasedFactory extends DoubleBucketAggregator.ContextBasedFactory<RangeAggregator> {

        private final AbstractRangeBase.Factory rangeFactory;
        private final List<Range> ranges;
        private final boolean keyed;

        public ContextBasedFactory(String name, AbstractRangeBase.Factory rangeFactory, List<Range> ranges, boolean keyed) {
            super(name);
            this.rangeFactory = rangeFactory;
            this.ranges = ranges;
            this.keyed = keyed;
        }

        @Override
        public RangeAggregator create(AggregationContext aggregationContext, Aggregator parent) {
            return new RangeAggregator(name, factories, null, rangeFactory, ranges, keyed, aggregationContext, parent);
        }
    }

}
