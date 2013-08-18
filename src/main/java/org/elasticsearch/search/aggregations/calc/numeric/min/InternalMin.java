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

package org.elasticsearch.search.aggregations.calc.numeric.min;

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
public class InternalMin extends NumericAggregation.SingleValue implements Min {

    public final static Type TYPE = new Type("min");

    public final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream<InternalMin>() {
        @Override
        public InternalMin readResult(StreamInput in) throws IOException {
            InternalMin result = new InternalMin();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }


    private double min;

    InternalMin() {} // for serialization

    public InternalMin(String name) {
        super(name);
        this.min = Double.POSITIVE_INFINITY;
    }

    @Override
    public double value() {
        return min;
    }

    public double getValue() {
        return min;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public void collect(int doc, double value) {
        min = Math.min(value, min);
    }

    @Override
    public InternalMin reduce(ReduceContext reduceContext) {
        List<InternalAggregation> aggregations = reduceContext.aggregations();
        if (aggregations.size() == 1) {
            return (InternalMin) aggregations.get(0);
        }
        InternalMin reduced = null;
        for (InternalAggregation aggregation : aggregations) {
            if (reduced == null) {
                reduced = (InternalMin) aggregation;
            } else {
                reduced.min = Math.min(reduced.min, ((InternalMin) aggregation).min);
            }
        }
        if (reduced != null) {
            return reduced;
        }
        return (InternalMin) aggregations.get(0);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        valueFormatter = ValueFormatterStreams.readOptional(in);
        min = in.readDouble();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        ValueFormatterStreams.writeOptional(valueFormatter, out);
        out.writeDouble(min);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        boolean hasValue = !Double.isInfinite(min);
        builder.field(CommonFields.VALUE, hasValue ? min : null);
        if (hasValue && valueFormatter != null) {
            builder.field(CommonFields.VALUE_AS_STRING, valueFormatter.format(min));
        }
        builder.endObject();
        return builder;
    }

    public static class Factory implements NumericAggregation.Factory<InternalMin> {
        @Override
        public InternalMin create(String name) {
            return new InternalMin(name);
        }

        @Override
        public InternalMin createUnmapped(String name) {
            return new InternalMin(name);
        }
    }

}
