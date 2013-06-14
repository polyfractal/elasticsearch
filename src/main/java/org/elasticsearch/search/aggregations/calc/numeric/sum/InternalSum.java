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

package org.elasticsearch.search.aggregations.calc.numeric.sum;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.calc.numeric.NumericAggregation;
import org.elasticsearch.search.aggregations.context.numeric.ValueFormatterStreams;

import java.io.IOException;
import java.util.List;

/**
*
*/
public class InternalSum extends NumericAggregation.SingleValue implements Sum {

    public final static Type TYPE = new Type("sum");

    public final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream<InternalSum>() {
        @Override
        public InternalSum readResult(StreamInput in) throws IOException {
            InternalSum result = new InternalSum();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }


    private boolean set;
    private double sum;

    InternalSum() {} // for serialization

    InternalSum(String name) {
        this(name, true);
    }

    InternalSum(String name, boolean set) {
        super(name);
        this.set = set;
    }

    @Override
    public double value() {
        return sum;
    }

    public double getValue() {
        return sum;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public void collect(int doc, double value) {
        sum += value;
    }

    @Override
    public InternalSum reduce(List<InternalAggregation> aggregations) {
        if (aggregations.size() == 1) {
            return (InternalSum) aggregations.get(0);
        }
        InternalSum reduced = null;
        for (InternalAggregation aggregation : aggregations) {
            if (reduced == null && ((InternalSum) aggregation).set) {
                reduced = (InternalSum) aggregation;
            } else {
                if (((InternalSum) aggregation).set) {
                    reduced.sum += ((InternalSum) aggregation).sum;
                }
            }
        }
        if (reduced != null) {
            return reduced;
        }
        return (InternalSum) aggregations.get(0);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        valueFormatter = ValueFormatterStreams.readOptional(in);
        if (in.readBoolean()) {
            sum = in.readDouble();
            set = true;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        ValueFormatterStreams.writeOptional(valueFormatter, out);
        out.writeBoolean(set);
        if (set) {
            out.writeDouble(sum);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field(CommonFields.VALUE, set ? sum : null);
        if (set && valueFormatter != null) {
            builder.field(CommonFields.VALUE_AS_STRING, valueFormatter.format(sum));
        }
        builder.endObject();
        return builder;
    }

    public static class Factory implements NumericAggregation.Factory<InternalSum> {
        @Override
        public InternalSum create(String name) {
            return new InternalSum(name);
        }

        @Override
        public InternalSum createUnmapped(String name) {
            return new InternalSum(name, false);
        }
    }

}
