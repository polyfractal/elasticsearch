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

package org.elasticsearch.search.aggregations.calc.stats.max;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.calc.stats.Stats;
import org.elasticsearch.search.aggregations.format.ValueFormatterStreams;

import java.io.IOException;
import java.util.List;

/**
*
*/
public class InternalMax extends Stats.SingleValue implements Max {

    public final static Type TYPE = new Type("max");

    public final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream<InternalMax>() {
        @Override
        public InternalMax readResult(StreamInput in) throws IOException {
            InternalMax result = new InternalMax();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    private double max;
    private boolean set;

    InternalMax() {} // for serialization

    public InternalMax(String name) {
        this(name, true);
    }

    public InternalMax(String name, boolean set) {
        super(name);
        this.max = Double.NEGATIVE_INFINITY;
        this.set = set;
    }

    @Override
    public double value() {
        return max;
    }

    public double getValue() {
        return max;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public void collect(int doc, double value) {
        max = Math.max(value, max);
    }

    @Override
    public InternalMax reduce(List<InternalAggregation> aggregations) {
        if (aggregations.size() == 1) {
            return (InternalMax) aggregations.get(0);
        }
        InternalMax reduced = null;
        for (InternalAggregation aggregation : aggregations) {
            if (reduced == null) {
                if (((InternalMax) aggregation).set) {
                    reduced = (InternalMax) aggregation;
                }
            } else {
                if (((InternalMax) aggregation).set) {
                    reduced.max = Math.max(reduced.max, ((InternalMax) aggregation).max);
                }
            }
        }
        if (reduced != null) {
            return reduced;
        }
        return (InternalMax) aggregations.get(0);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        valueFormatter = ValueFormatterStreams.readOptional(in);
        if (in.readBoolean()) {
            max = in.readDouble();
            set = true;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        ValueFormatterStreams.writeOptional(valueFormatter, out);
        out.writeBoolean(set);
        if (set) {
            out.writeDouble(max);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field(CommonFields.VALUE, set ? max : null);
        if (set && valueFormatter != null) {
            builder.field(CommonFields.VALUE_AS_STRING, valueFormatter.format(max));
        }
        builder.endObject();
        return builder;
    }

    public static class Factory implements Stats.Factory<InternalMax> {
        @Override
        public InternalMax create(String name) {
            return new InternalMax(name);
        }

        @Override
        public InternalMax createUnmapped(String name) {
            return new InternalMax(name, false);
        }
    }

}
