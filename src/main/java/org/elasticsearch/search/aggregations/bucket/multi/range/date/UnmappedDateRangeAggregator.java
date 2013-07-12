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

package org.elasticsearch.search.aggregations.bucket.multi.range.date;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.multi.range.AbstractRangeBase;
import org.elasticsearch.search.aggregations.bucket.multi.range.RangeAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.numeric.ValueFormatter;
import org.elasticsearch.search.aggregations.context.numeric.ValueParser;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class UnmappedDateRangeAggregator extends Aggregator {

    private final List<RangeAggregator.Range> ranges;
    private final boolean keyed;
    private final ValueFormatter formatter;
    private final ValueParser parser;

    public UnmappedDateRangeAggregator(String name,
                                       List<RangeAggregator.Range> ranges,
                                       boolean keyed,
                                       ValueFormatter formatter,
                                       ValueParser parser,
                                       AggregationContext aggregationContext,
                                       Aggregator parent) {

        super(name, aggregationContext, parent);
        this.ranges = ranges;
        this.keyed = keyed;
        this.formatter = formatter;
        this.parser = parser;
    }

    @Override
    public Collector collector() {
        return null;
    }

    @Override
    public AbstractRangeBase buildAggregation() {
        List<DateRange.Bucket> buckets = new ArrayList<DateRange.Bucket>(ranges.size());
        for (RangeAggregator.Range range : ranges) {
            range.process(parser, aggregationContext) ;
            buckets.add(new InternalDateRange.Bucket(range.key, range.from, range.to, 0, InternalAggregations.EMPTY, formatter));
        }
        return new InternalDateRange(name, buckets, null, keyed);
    }

    public static class Factory extends CompoundFactory<UnmappedDateRangeAggregator> {

        private final List<RangeAggregator.Range> ranges;
        private final boolean keyed;
        private final ValueFormatter formatter;
        private final ValueParser parser;

        public Factory(String name, List<RangeAggregator.Range> ranges, boolean keyed, ValueFormatter formatter, ValueParser parser) {
            super(name);
            this.ranges = ranges;
            this.keyed = keyed;
            this.formatter = formatter;
            this.parser = parser;
        }

        @Override
        public UnmappedDateRangeAggregator create(AggregationContext aggregationContext, Aggregator parent) {
            return new UnmappedDateRangeAggregator(name, ranges, keyed, formatter, parser, aggregationContext, parent);
        }
    }

}
