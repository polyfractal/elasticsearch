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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.multi.range.InternalRange;
import org.elasticsearch.search.aggregations.context.numeric.ValueFormatter;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class InternalIPv4Range extends InternalRange<InternalIPv4Range.Bucket> implements IPv4Range<InternalIPv4Range.Bucket> {

    public final static Type TYPE = new Type("ip_range", "iprange");

    private final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream<InternalRange>() {
        @Override
        public InternalRange readResult(StreamInput in) throws IOException {
            InternalIPv4Range range = new InternalIPv4Range();
            range.readFrom(in);
            return range;
        }
    };

    public static void registerStream() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    public static final Factory FACTORY = new Factory();

    public static class Bucket extends InternalRange.Bucket implements IPv4Range.Bucket {

        public Bucket(String key, double from, double to, long docCount, List<InternalAggregation> aggregations) {
            super(key, from, to, docCount, new InternalAggregations(aggregations));
        }

        public Bucket(String key, double from, double to, long docCount, InternalAggregations aggregations) {
            super(key, from, to, docCount, aggregations);
        }

        @Override
        public String getFromAsString() {
            return ValueFormatter.IPv4.format(getFrom());
        }

        @Override
        public String getToAsString() {
            return ValueFormatter.IPv4.format(getTo());
        }
    }

    private static class Factory extends InternalRange.Factory<Bucket> {

        @Override
        public InternalIPv4Range create(String name, List<Bucket> buckets, ValueFormatter formatter, boolean keyed) {
            return new InternalIPv4Range(name, buckets, formatter, keyed);
        }

        @Override
        public Bucket createBucket(String key, double from, double to, long docCount, InternalAggregations aggregations) {
            return new Bucket(key, from, to, docCount, aggregations);
        }
    }

    public InternalIPv4Range() {
    }

    public InternalIPv4Range(String name, List<Bucket> ranges, ValueFormatter formatter, boolean keyed) {
        super(name, ranges, formatter, keyed);
    }

    @Override
    protected Bucket createBucket(String key, double from, double to, long docCount, InternalAggregations aggregations) {
        return new Bucket(key, from, to, docCount, aggregations);
    }

    @Override
    public Type type() {
        return TYPE;
    }

}
