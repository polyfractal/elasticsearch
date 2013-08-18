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

package org.elasticsearch.search.aggregations.calc.bytes.count;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;

import java.io.IOException;
import java.util.List;

/**
 * An internal implementation of {@link Count}.
 */
public class InternalCount extends InternalAggregation implements Count {

    public static final Type TYPE = new Type("count");

    private static final AggregationStreams.Stream<InternalCount> STREAM = new AggregationStreams.Stream<InternalCount>() {
        @Override
        public InternalCount readResult(StreamInput in) throws IOException {
            InternalCount count = new InternalCount();
            count.readFrom(in);
            return count;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    private long value;

    InternalCount() {} // for serialization

    public InternalCount(String name, long value) {
        super(name);
        this.value = value;
    }

    @Override
    public long getValue() {
        return value;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public InternalAggregation reduce(ReduceContext reduceContext) {
        List<InternalAggregation> aggregations = reduceContext.aggregations();
        if (aggregations.size() == 1) {
            return aggregations.get(0);
        }
        InternalCount reduced = null;
        for (InternalAggregation aggregation : aggregations) {
            if (reduced == null) {
                reduced = (InternalCount) aggregation;
            } else {
                reduced.value += ((InternalCount) aggregation).value;
            }
        }
        return reduced;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        value = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeVLong(value);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject(name)
                .field(CommonFields.VALUE, value)
                .endObject();
    }

}
