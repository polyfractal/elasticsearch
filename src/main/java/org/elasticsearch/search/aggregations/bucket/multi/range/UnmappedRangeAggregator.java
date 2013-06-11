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

import org.elasticsearch.search.aggregations.AbstractAggregator;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class UnmappedRangeAggregator extends AbstractAggregator {

    private final List<RangeAggregator.Range> ranges;
    private final boolean keyed;

    public UnmappedRangeAggregator(String name, List<RangeAggregator.Range> ranges, boolean keyed, Aggregator parent) {
        super(name, parent);
        this.ranges = ranges;
        this.keyed = keyed;
    }

    @Override
    public Collector collector() {
        return null;
    }

    @Override
    public InternalRange buildAggregation() {
        List<InternalRange.Bucket> buckets = new ArrayList<InternalRange.Bucket>(ranges.size());
        for (RangeAggregator.Range range : ranges) {
            buckets.add(new InternalRange.Bucket(range.key, range.from, range.to, 0, InternalAggregations.EMPTY));
        }
        return new InternalRange(name, buckets, keyed);
    }

    public static class Factory extends Aggregator.CompoundFactory<UnmappedRangeAggregator> {

        private final List<RangeAggregator.Range> ranges;
        private final boolean keyed;

        public Factory(String name, List<RangeAggregator.Range> ranges, boolean keyed) {
            super(name);
            this.ranges = ranges;
            this.keyed = keyed;
        }

        @Override
        public UnmappedRangeAggregator create(Aggregator parent) {
            return new UnmappedRangeAggregator(name, ranges, keyed, parent);
        }
    }

}
