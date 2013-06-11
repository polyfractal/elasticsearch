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
import org.elasticsearch.search.aggregations.AbstractAggregator;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.util.List;

/**
 *
 */
public class UnmappedGeoDistanceAggregator extends AbstractAggregator {

    private final List<GeoDistanceAggregator.DistanceRange> ranges;

    public UnmappedGeoDistanceAggregator(String name, List<GeoDistanceAggregator.DistanceRange> ranges, Aggregator parent) {
        super(name, parent);
        this.ranges = ranges;
    }

    @Override
    public Collector collector() {
        return null;
    }

    @Override
    public InternalAggregation buildAggregation() {
        List<GeoDistance.Bucket> buckets = Lists.newArrayListWithCapacity(ranges.size());
        for (GeoDistanceAggregator.DistanceRange range : ranges) {
            buckets.add(new InternalGeoDistance.Bucket(range.key, range.unit, range.from, range.to, 0, InternalAggregations.EMPTY));
        }
        return new InternalGeoDistance(name, buckets);
    }

    public static class Factory extends Aggregator.CompoundFactory<UnmappedGeoDistanceAggregator> {

        private final List<GeoDistanceAggregator.DistanceRange> ranges;

        public Factory(String name, List<GeoDistanceAggregator.DistanceRange> ranges) {
            super(name);
            this.ranges = ranges;
        }

        @Override
        public UnmappedGeoDistanceAggregator create(Aggregator parent) {
            return new UnmappedGeoDistanceAggregator(name, ranges, parent);
        }
    }
}
