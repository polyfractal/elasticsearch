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

package org.elasticsearch.search.aggregations.calc.numeric.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.calc.numeric.NumericAggregation;
import org.elasticsearch.search.aggregations.context.numeric.ValueFormatterStreams;

import java.io.IOException;
import java.util.List;

/**
*
*/
public class InternalStats extends NumericAggregation.MultiValue implements Stats {

    public final static Type TYPE = new Type("stats");

    public final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream<InternalStats>() {
        @Override
        public InternalStats readResult(StreamInput in) throws IOException {
            InternalStats result = new InternalStats();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    protected long count;
    protected double min;
    protected double max;
    protected double sum;

    InternalStats() {} // for serialization

    public InternalStats(String name) {
        super(name);
        this.min = Double.POSITIVE_INFINITY;
        this.max = Double.NEGATIVE_INFINITY;
    }

    @Override
    public long getCount() {
        return count;
    }

    @Override
    public double getMin() {
        return min;
    }

    @Override
    public double getMax() {
        return max;
    }

    @Override
    public double getAvg() {
        return sum / count;
    }

    @Override
    public double getSum() {
        return sum;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public double value(String name) {
        if ("min".equals(name)) {
            return min;
        } else if ("max".equals(name)) {
            return max;
        } else if ("avg".equals(name)) {
            return getAvg();
        } else if ("count".equals(name)) {
            return count;
        } else if ("sum".equals(name)) {
            return sum;
        } else {
            throw new IllegalArgumentException("Unknown value [" + name + "] in common stats aggregation");
        }
    }

    @Override
    public void collect(int doc, double value) {
        count++;
        min = Math.min(value, min);
        max = Math.max(value, max);
        sum += value;
    }

    @Override
    public InternalStats reduce(ReduceContext reduceContext) {
        List<InternalAggregation> aggregations = reduceContext.aggregations();
        if (aggregations.size() == 1) {
            return (InternalStats) aggregations.get(0);
        }
        InternalStats reduced = null;
        for (InternalAggregation aggregation : aggregations) {
            if (reduced == null) {
                if (((InternalStats) aggregation).count != 0) {
                    reduced = (InternalStats) aggregation;
                }
            } else {
                if (((InternalStats) aggregation).count != 0) {
                    reduced.count += ((InternalStats) aggregation).count;
                    reduced.min = Math.min(reduced.min, ((InternalStats) aggregation).min);
                    reduced.max = Math.max(reduced.max, ((InternalStats) aggregation).max);
                    reduced.sum += ((InternalStats) aggregation).sum;
                    mergeOtherStats(reduced, aggregation);
                }
            }
        }
        if (reduced != null) {
            return reduced;
        }
        return (InternalStats) aggregations.get(0);
    }

    protected void mergeOtherStats(InternalStats to, InternalAggregation from) {
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        valueFormatter = ValueFormatterStreams.readOptional(in);
        count = in.readVLong();
        min = in.readDouble();
        max = in.readDouble();
        sum = in.readDouble();
        readOtherStatsFrom(in);
    }

    public void readOtherStatsFrom(StreamInput in) throws IOException {
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        ValueFormatterStreams.writeOptional(valueFormatter, out);
        out.writeVLong(count);
        out.writeDouble(min);
        out.writeDouble(max);
        out.writeDouble(sum);
        writeOtherStatsTo(out);
    }

    protected void writeOtherStatsTo(StreamOutput out) throws IOException {
    }

    static class Fields {
        public static final XContentBuilderString COUNT = new XContentBuilderString("count");
        public static final XContentBuilderString MIN = new XContentBuilderString("min");
        public static final XContentBuilderString MIN_AS_STRING = new XContentBuilderString("min_as_string");
        public static final XContentBuilderString MAX = new XContentBuilderString("max");
        public static final XContentBuilderString MAX_AS_STRING = new XContentBuilderString("max_as_string");
        public static final XContentBuilderString AVG = new XContentBuilderString("avg");
        public static final XContentBuilderString AVG_AS_STRING = new XContentBuilderString("avg_as_string");
        public static final XContentBuilderString SUM = new XContentBuilderString("sum");
        public static final XContentBuilderString SUM_AS_STRING = new XContentBuilderString("sum_as_string");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field(Fields.COUNT, count);
        builder.field(Fields.MIN, count != 0 ? min : null);
        builder.field(Fields.MAX, count != 0 ? max : null);
        builder.field(Fields.AVG, count != 0 ? getAvg() : null);
        builder.field(Fields.SUM, count != 0 ? sum : null);
        if (count != 0 && valueFormatter != null) {
            builder.field(Fields.MIN_AS_STRING, valueFormatter.format(min));
            builder.field(Fields.MAX_AS_STRING, valueFormatter.format(max));
            builder.field(Fields.AVG_AS_STRING, valueFormatter.format(getAvg()));
            builder.field(Fields.SUM_AS_STRING, valueFormatter.format(sum));
        }
        otherStatsToXCotent(builder, params);
        builder.endObject();
        return builder;
    }

    protected XContentBuilder otherStatsToXCotent(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }

    public static class Factory implements NumericAggregation.Factory<InternalStats> {
        @Override
        public InternalStats create(String name) {
            return new InternalStats(name);
        }

        @Override
        public InternalStats createUnmapped(String name) {
            return new InternalStats(name);
        }
    }

}
