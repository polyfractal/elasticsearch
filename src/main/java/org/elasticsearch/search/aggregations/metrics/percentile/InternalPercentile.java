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

package org.elasticsearch.search.aggregations.metrics.percentile;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.percentile.providers.PercentileProvider;
import org.elasticsearch.search.aggregations.support.numeric.ValueFormatterStreams;

import java.io.IOException;
import java.util.List;

/**
*
*/
public class InternalPercentile extends MetricsAggregation.MultiValue implements Percentile {

    public final static Type TYPE = new Type("percentile");
    private static final ESLogger log= Loggers.getLogger(InternalPercentile.class);

    public static enum EXECUTION_HINT {
        FRUGAL, QDIGEST, TDIGEST
    }

    public final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalPercentile readResult(StreamInput in) throws IOException {
            InternalPercentile result = new InternalPercentile();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    private PercentileProvider quantile;

    InternalPercentile() {} // for serialization

    public InternalPercentile(String name, PercentileProvider quantile) {
        super(name);
        this.quantile = quantile;
    }

    @Override
    public double value(String name) {
        if ("min".equals(name)) {
            return 0;
        } else {
            throw new IllegalArgumentException("Unknown value [" + name + "] in percentile aggregation");
        }
    }

    public double getValue(int index) {
        return quantile.getEstimate(index);
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public InternalPercentile reduce(ReduceContext reduceContext) {
        List<InternalAggregation> aggregations = reduceContext.aggregations();
        if (aggregations.size() == 1) {
            return (InternalPercentile) aggregations.get(0);
        }
        InternalPercentile reduced = null;
        int expectedMerges = aggregations.size();

        for (InternalAggregation aggregation : aggregations) {
            if (reduced == null) {
                reduced = (InternalPercentile) aggregation;
            } else {
                reduced.quantile = reduced.quantile.merge(((InternalPercentile) aggregation).quantile, expectedMerges);
            }
        }

        return reduced;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        valueFormatter = ValueFormatterStreams.readOptional(in);
        quantile.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        ValueFormatterStreams.writeOptional(valueFormatter, out);
        quantile.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        double intervals[] = quantile.getPercentiles();


        builder.startObject(name);
        builder.startObject("quantiles");

        for(int i = 0; i < intervals.length; ++i) {

            builder.field(String.valueOf(intervals[i]), getValue(i));

            if (valueFormatter != null) {
                builder.field(String.valueOf(intervals[i]) + "_as_string", valueFormatter.format(getValue(i)));
            }

        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

}
