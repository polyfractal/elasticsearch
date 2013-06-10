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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.single.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.format.ValueFormatter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class ScriptRangeAggregator extends SingleBucketAggregator {

    private final List<RangeAggregator.Range> ranges;
    private final SearchScript script;
    private final boolean keyed;

    List<RangeCollector> rangeCollectors;

    public ScriptRangeAggregator(String name, List<Aggregator.Factory> factories, List<RangeAggregator.Range> ranges,
                                 SearchScript script, ValueFormatter valueFormatter, boolean keyed, Aggregator parent) {
        super(name, factories, parent);
        this.ranges = ranges;
        this.script = script;
        this.keyed = keyed;
        rangeCollectors = new ArrayList<RangeCollector>(ranges.size());
        for (RangeAggregator.Range range : ranges) {
            List<Aggregator> aggregators = new ArrayList<Aggregator>(factories.size());
            for (Aggregator.Factory factory : factories) {
                aggregators.add(factory.create(this));
            }
            rangeCollectors.add(new RangeCollector(range, script, valueFormatter, aggregators));
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
            script.setScorer(scorer);
            for (RangeCollector collector : collectors) {
                collector.setScorer(scorer);
            }
        }

        @Override
        public void collect(int doc, AggregationContext context) throws IOException {
            script.setNextDocId(doc);
            for (RangeCollector collector : collectors) {
                collector.collect(doc, context);
            }
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            script.setNextReader(context);
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

    static class RangeCollector extends BucketCollector {

        private final RangeAggregator.Range range;
        private final SearchScript script;
        private final ValueFormatter valueFormatter;

        long docCount;

        RangeCollector(RangeAggregator.Range range, SearchScript script, ValueFormatter valueFormatter, List<Aggregator> aggregators) {
            super(aggregators);
            this.range = range;
            this.script = script;
            this.valueFormatter = valueFormatter;
        }

        @Override
        protected void postCollection(List<Aggregator> aggregators) {
        }

        @Override
        protected AggregationContext onDoc(int doc, AggregationContext context) throws IOException {
            double value = script.runAsDouble();
            if (range.matches(value)) {
                docCount++;
                return context;
            }
            return null;
        }

        InternalRange.Bucket buildBucket() {
            return new InternalRange.Bucket(range.key, range.from, range.to, docCount, valueFormatter, buildAggregations());
        }
    }

    public static class Factory extends SingleBucketAggregator.Factory<ScriptRangeAggregator, Factory> {

        private final List<RangeAggregator.Range> ranges;
        private final SearchScript script;
        private final ValueFormatter valueFormatter;
        private final boolean keyed;

        public Factory(String name, SearchScript script, List<RangeAggregator.Range> ranges, boolean keyed) {
            this(name, script, null, ranges, keyed);
        }

        public Factory(String name, SearchScript script, ValueFormatter valueFormatter, List<RangeAggregator.Range> ranges, boolean keyed) {
            super(name);
            this.script = script;
            this.ranges = ranges;
            this.valueFormatter = valueFormatter;
            this.keyed = keyed;
        }

        @Override
        public ScriptRangeAggregator create(Aggregator parent) {
            return new ScriptRangeAggregator(name, factories, ranges, script, valueFormatter, keyed, parent);
        }
    }

}
