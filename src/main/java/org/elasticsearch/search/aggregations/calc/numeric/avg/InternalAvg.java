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

package org.elasticsearch.search.aggregations.calc.numeric.avg;

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
public class InternalAvg extends NumericAggregation.SingleValue implements Avg {

    public final static Type TYPE = new Type("avg");

    public final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream<InternalAvg>() {
        @Override
        public InternalAvg readResult(StreamInput in) throws IOException {
            InternalAvg result = new InternalAvg();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    private double sum;
    private long count;

    InternalAvg() {} // for serialization

    public InternalAvg(String name) {
        this(name, 0, 0);
    }

    public InternalAvg(String name, double sum, long count) {
        super(name);
        this.sum = sum;
        this.count = count;
    }

    @Override
    public double value() {
        return getValue();
    }

    public double getValue() {
        return sum / count;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public void collect(int doc, double value) {
        sum += value;
        count++;
    }

    @Override
    public InternalAvg reduce(ReduceContext reduceContext) {
        List<InternalAggregation> aggregations = reduceContext.aggregations();
        if (aggregations.size() == 1) {
            return (InternalAvg) aggregations.get(0);
        }
        InternalAvg reduced = null;
        for (InternalAggregation aggregation : aggregations) {
            if (reduced == null) {
                reduced = (InternalAvg) aggregation;
            } else {
                reduced.count += ((InternalAvg) aggregation).count;
                reduced.sum += ((InternalAvg) aggregation).sum;
            }
        }
        return reduced;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        valueFormatter = ValueFormatterStreams.readOptional(in);
        sum = in.readDouble();
        count = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        ValueFormatterStreams.writeOptional(valueFormatter, out);
        out.writeDouble(sum);
        out.writeVLong(count);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field(CommonFields.VALUE, count != 0 ? getValue() : null);
        if (count != 0 && valueFormatter != null) {
            builder.field(CommonFields.VALUE_AS_STRING, valueFormatter.format(getValue()));
        }
        builder.endObject();
        return builder;
    }

    public static class Factory implements NumericAggregation.Factory<InternalAvg> {
        @Override
        public InternalAvg create(String name) {
            return new InternalAvg(name);
        }

        @Override
        public InternalAvg createUnmapped(String name) {
            return new InternalAvg(name);
        }
    }

}
