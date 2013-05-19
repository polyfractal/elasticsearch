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

package org.elasticsearch.search.aggregations.calc.stats.count;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.calc.stats.Stats;

import java.io.IOException;
import java.util.List;

/**
*
*/
public class InternalCount extends Stats.SingleValue implements Count {

    public final static Type TYPE = new Type("count");

    public final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream<InternalCount>() {
        @Override
        public InternalCount readResult(StreamInput in) throws IOException {
            InternalCount result = new InternalCount();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    private long count;

    InternalCount() {} // for serialization

    InternalCount(String name) {
        super(name);
    }

    @Override
    public double value() {
        return count;
    }

    public long getValue() {
        return count;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public void collect(int doc, double value) {
        count++;
    }

    @Override
    public InternalCount reduce(List<InternalAggregation> aggregations) {
        if (aggregations.size() == 1) {
            return (InternalCount) aggregations.get(0);
        }
        InternalCount reduced = null;
        for (InternalAggregation aggregation : aggregations) {
            if (reduced == null) {
                reduced = (InternalCount) aggregation;
            } else {
                reduced.count += ((InternalCount) aggregation).count;
            }
        }
        return reduced;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        count = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeVLong(count);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject(name)
                .field(CommonFields.VALUE, count)
                .endObject();
    }

    public static class Factory implements Stats.Factory<InternalCount> {
        @Override
        public InternalCount create(String name) {
            return new InternalCount(name);
        }

        @Override
        public InternalCount createUnmapped(String name) {
            return new InternalCount(name);
        }
    }

}
