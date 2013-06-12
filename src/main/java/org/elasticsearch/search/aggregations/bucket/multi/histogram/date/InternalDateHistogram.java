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

package org.elasticsearch.search.aggregations.bucket.multi.histogram.date;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.bucket.multi.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.multi.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.bucket.multi.histogram.InternalOrder;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class InternalDateHistogram extends InternalHistogram implements DateHistogram {

    public final static Type TYPE = new Type("date_histogram", "dhisto");

    private final static AggregationStreams.Stream<InternalDateHistogram> STREAM = new AggregationStreams.Stream<InternalDateHistogram>() {
        @Override
        public InternalDateHistogram readResult(StreamInput in) throws IOException {
            InternalDateHistogram histogram = new InternalDateHistogram();
            histogram.readFrom(in);
            return histogram;
        }
    };

    public static void registerStream() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    static class Factory extends InternalHistogram.Factory {
        @Override
        public InternalHistogram create(String name, List<Histogram.Bucket> buckets, InternalOrder order, boolean keyed) {
            return new InternalDateHistogram(name, buckets, order, keyed);
        }
    }

    InternalDateHistogram() {}

    InternalDateHistogram(String name, List<Histogram.Bucket> buckets, InternalOrder order, boolean keyed) {
        super(name, buckets, order, keyed);
    }
}
