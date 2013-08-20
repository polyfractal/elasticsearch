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

package org.elasticsearch.search.aggregations.calc.bytes.unique;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;

import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.logging.ESLogger;

import java.io.IOException;
import java.util.List;

/**
 * An internal implementation of {@link Unique}.
 */
public class InternalUnique extends InternalAggregation implements Unique {

    public static final Type TYPE = new Type("unique");
    private ESLogger logger;

    private static final AggregationStreams.Stream<InternalUnique> STREAM = new AggregationStreams.Stream<InternalUnique>() {
        @Override
        public InternalUnique readResult(StreamInput in) throws IOException {
            InternalUnique count = new InternalUnique();
            count.readFrom(in);
            return count;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }


    private ICardinality hll;

    InternalUnique() {} // for serialization

    public InternalUnique(String name, ICardinality hll) {
        super(name);
        this.hll = hll;
        this.logger = Loggers.getLogger("Unique");
    }

    @Override
    public ICardinality getHll() {
        return hll;
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

        InternalUnique merged = null;

        for (InternalAggregation aggregation : aggregations) {
            if (merged == null) {
                merged = (InternalUnique) aggregation;
            } else {
                try {
                    merged.hll = merged.hll.merge(((InternalUnique) aggregation).hll);
                } catch (CardinalityMergeException e) {
                    this.logger.error(e.getMessage());
                }

            }
        }

        return merged;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();

        int length = in.readInt();
        byte[] hllBytes = new byte[length];
        in.readBytes(hllBytes, 0, length);
        hll = HyperLogLogPlus.Builder.build(hllBytes);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        byte[] hllBytes= hll.getBytes();
        out.writeString(name);
        out.writeInt(hllBytes.length);
        out.writeBytes(hllBytes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject(name)
                .field(CommonFields.VALUE, hll.cardinality())
                .endObject();
    }

}
