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

package org.elasticsearch.search.aggregations.bucket.multi.range.ip4v;

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
public class UnmappedIPv4RangeAggregator extends Aggregator {

    private final List<RangeAggregator.Range> ranges;
    private final boolean keyed;

    public UnmappedIPv4RangeAggregator(String name,
                                       List<RangeAggregator.Range> ranges,
                                       boolean keyed,
                                       AggregationContext aggregationContext,
                                       Aggregator parent) {

        super(name, aggregationContext, parent);
        this.ranges = ranges;
        this.keyed = keyed;
    }

    @Override
    public Collector collector() {
        return null;
    }

    @Override
    public AbstractRangeBase buildAggregation() {
        List<IPv4Range.Bucket> buckets = new ArrayList<IPv4Range.Bucket>(ranges.size());
        for (RangeAggregator.Range range : ranges) {
            range.process(ValueParser.IPv4, aggregationContext) ;
            buckets.add(new InternalIPv4Range.Bucket(range.key, range.from, range.to, 0, InternalAggregations.EMPTY, ValueFormatter.IPv4));
        }
        return new InternalIPv4Range(name, buckets, keyed);
    }

    public static class Factory extends CompoundFactory<UnmappedIPv4RangeAggregator> {

        private final List<RangeAggregator.Range> ranges;
        private final boolean keyed;

        public Factory(String name, List<RangeAggregator.Range> ranges, boolean keyed) {
            super(name);
            this.ranges = ranges;
            this.keyed = keyed;
        }

        @Override
        public UnmappedIPv4RangeAggregator create(AggregationContext aggregationContext, Aggregator parent) {
            return new UnmappedIPv4RangeAggregator(name, ranges, keyed, aggregationContext, parent);
        }
    }

}
