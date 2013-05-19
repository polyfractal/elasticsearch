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

package org.elasticsearch.search.aggregations.bucket.missing;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class InternalMissing extends SingleBucketAggregation<InternalMissing> implements Missing {

    public final static Type TYPE = new Type("missing");

    public final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream<InternalMissing>() {
        @Override
        public InternalMissing readResult(StreamInput in) throws IOException {
            InternalMissing missing = new InternalMissing();
            missing.readFrom(in);
            return missing;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }


    InternalMissing() {
    }

    InternalMissing(String name, long docCount, List<InternalAggregation> subAggregations) {
        super(name, docCount, subAggregations);
    }

    @Override
    public Type type() {
        return TYPE;
    }

}
