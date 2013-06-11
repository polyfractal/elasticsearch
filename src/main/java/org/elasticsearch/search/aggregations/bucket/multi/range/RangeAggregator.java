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
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.DoubleBucketAggregator;
import org.elasticsearch.search.aggregations.bucket.multi.DoubleMultiBucketAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.FieldDataContext;
import org.elasticsearch.search.aggregations.context.doubles.DoubleValuesSource;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.search.aggregations.bucket.BucketAggregator.buildAggregations;
import static org.elasticsearch.search.aggregations.bucket.BucketAggregator.createAggregators;

/**
 *
 */
public class RangeAggregator extends DoubleMultiBucketAggregator {

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

    BucketCollector[] bucketCollectors;

    public RangeAggregator(String name, List<Aggregator.Factory> factories, DoubleValuesSource valuesSource, List<Range> ranges, boolean keyed, Aggregator parent) {
        super(name, valuesSource, parent);
        this.keyed = keyed;
        bucketCollectors = new BucketCollector[ranges.size()];
        int i = 0;
        for (Range range : ranges) {
            bucketCollectors[i++] = new BucketCollector(name, valuesSource, createAggregators(factories, this), range);
        }
    }


    @Override
    public Collector collector() {
        return new Collector();
    }

    @Override
    public InternalAggregation buildAggregation() {
        List<InternalRange.Bucket> buckets = Lists.newArrayListWithCapacity(bucketCollectors.length);
        for (int i = 0; i < bucketCollectors.length; i++) {
            buckets.add(bucketCollectors[i].buildBucket());
        }
        return new InternalRange(name, buckets, keyed);
    }

    class Collector implements Aggregator.Collector {

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            for (int i = 0; i < bucketCollectors.length; i++) {
                bucketCollectors[i].setScorer(scorer);
            }
        }

        @Override
        public void collect(int doc) throws IOException {
            for (int i = 0; i < bucketCollectors.length; i++) {
                bucketCollectors[i].collect(doc);
            }
        }

        @Override
        public void setNextReader(AtomicReaderContext reader, AggregationContext context) throws IOException {
            for (int i = 0; i < bucketCollectors.length; i++) {
                bucketCollectors[i].setNextReader(reader, context);
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

        BucketCollector(String aggregationName, DoubleValuesSource valuesSource, Aggregator[] aggregators, Range range) {
            super(aggregationName, valuesSource, aggregators);
            this.range = range;
        }

        @Override
        protected boolean onDoc(int doc, DoubleValues values, AggregationContext context) throws IOException {
            if (matches(doc, values, context)) {
                docCount++;
                return true;
            }
            return false;
        }

        private boolean matches(int doc, DoubleValues values, AggregationContext context) {
            if (!values.hasValue(doc)) {
                return false;
            }

            String valueSourceKey = valuesSource.key();
            if (!values.isMultiValued()) {
                double value = values.getValue(doc);
                return context.accept(doc, valueSourceKey, value) && range.matches(value);
            }

            for (DoubleValues.Iter iter = values.getIter(doc); iter.hasNext();) {
                double value = iter.next();
                if (context.accept(doc, valueSourceKey, value) && range.matches(value)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        protected void postCollection(Aggregator[] aggregators) {
        }

        @Override
        public boolean accept(int doc, double value, DoubleValues values) {
            return range.matches(value);
        }

        InternalRange.Bucket buildBucket() {
            return new InternalRange.Bucket(range.key, range.from, range.to, docCount, buildAggregations(aggregators));
        }

    }

    public static class FieldDataFactory extends DoubleMultiBucketAggregator.FieldDataFactory<RangeAggregator> {

        private final List<Range> ranges;
        private final boolean keyed;

        public FieldDataFactory(String name, FieldDataContext fieldDataContext, List<Range> ranges, boolean keyed) {
            super(name, fieldDataContext);
            this.ranges = ranges;
            this.keyed = keyed;
        }

        public FieldDataFactory(String name, FieldDataContext fieldDataContext, SearchScript valueScript, List<Range> ranges, boolean keyed) {
            super(name, fieldDataContext, valueScript);
            this.ranges = ranges;
            this.keyed = keyed;
        }

        @Override
        protected RangeAggregator create(DoubleValuesSource source, Aggregator parent) {
            return new RangeAggregator(name, factories, source, ranges, keyed, parent);
        }
    }

    public static class ScriptFactory extends DoubleMultiBucketAggregator.ScriptFactory<RangeAggregator> {

        private final List<Range> ranges;
        private final boolean keyed;

        public ScriptFactory(String name, SearchScript script, List<Range> ranges, boolean keyed) {
            super(name, script);
            this.ranges = ranges;
            this.keyed = keyed;
        }

        @Override
        protected RangeAggregator create(DoubleValuesSource source, Aggregator parent) {
            return new RangeAggregator(name, factories, source, ranges, keyed, parent);
        }
    }

    public static class ContextBasedFactory extends DoubleMultiBucketAggregator.ContextBasedFactory<RangeAggregator> {

        private final List<Range> ranges;
        private final boolean keyed;

        public ContextBasedFactory(String name, List<Range> ranges, boolean keyed) {
            super(name);
            this.ranges = ranges;
            this.keyed = keyed;
        }

        @Override
        protected RangeAggregator create(DoubleValuesSource source, Aggregator parent) {
            return new RangeAggregator(name, factories, source, ranges, keyed, parent);
        }
    }

}
