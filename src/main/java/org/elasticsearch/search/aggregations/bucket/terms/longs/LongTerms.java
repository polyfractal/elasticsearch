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

package org.elasticsearch.search.aggregations.bucket.terms.longs;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.terms.InternalOrder;
import org.elasticsearch.search.aggregations.bucket.terms.InternalTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.format.ValueFormatter;
import org.elasticsearch.search.aggregations.format.ValueFormatterStreams;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *
 */
public class LongTerms extends InternalTerms {

    public static final Type TYPE = new Type("terms", "lterms");

    public static AggregationStreams.Stream STREAM = new AggregationStreams.Stream<LongTerms>() {
        @Override
        public LongTerms readResult(StreamInput in) throws IOException {
            LongTerms buckets = new LongTerms();
            buckets.readFrom(in);
            return buckets;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }


    static class Bucket extends InternalTerms.Bucket {

        private long term;

        public Bucket(long term, long docCount, List<InternalAggregation> aggregations) {
            super(docCount, aggregations);
            this.term = term;
        }

        public Bucket(long term, long docCount, InternalAggregations aggregations) {
            super(docCount, aggregations);
            this.term = term;
        }

        @Override
        public Text getTerm() {
            return new StringText(String.valueOf(term));
        }

        @Override
        public Number getTermAsNumber() {
            return term;
        }

        @Override
        protected int compareTerm(Terms.Bucket other) {
            long otherTerm = other.getTermAsNumber().longValue();
            if (this.term > otherTerm) {
                return 1;
            }
            if (this.term < otherTerm) {
                return -1;
            }
            return 0;
        }
    }

    private ValueFormatter valueFormatter;

    LongTerms() {} // for serialization

    public LongTerms(String name, Order order, int requiredSize, Collection<InternalTerms.Bucket> buckets) {
        this(name, order, null, requiredSize, buckets);
    }

    public LongTerms(String name, Order order, ValueFormatter valueFormatter, int requiredSize, Collection<InternalTerms.Bucket> buckets) {
        super(name, order, requiredSize, buckets);
        this.valueFormatter = valueFormatter;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        this.name = in.readString();
        this.order = InternalOrder.Streams.readOrder(in);
        this.valueFormatter = ValueFormatterStreams.readOptional(in);
        this.requiredSize = in.readVInt();
        int size = in.readVInt();
        List<InternalTerms.Bucket> buckets = new ArrayList<InternalTerms.Bucket>(size);
        for (int i = 0; i < size; i++) {
            buckets.add(new Bucket(in.readVLong(), in.readVLong(), InternalAggregations.readAggregations(in)));
        }
        this.buckets = buckets;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        InternalOrder.Streams.writeOrder(order, out);
        ValueFormatterStreams.writeOptional(valueFormatter, out);
        out.writeVInt(requiredSize);
        out.writeVInt(buckets.size());
        for (InternalTerms.Bucket bucket : buckets) {
            out.writeVLong(((Bucket) bucket).term);
            out.writeVLong(bucket.getDocCount());
            ((InternalAggregations) bucket.getAggregations()).writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.startArray(Fields.TERMS);
        for (InternalTerms.Bucket bucket : buckets) {
            builder.startObject();
            if (valueFormatter != null) {
                builder.field(Fields.TERM, valueFormatter.format(((Bucket) bucket).term));
            } else {
                builder.field(Fields.TERM, ((Bucket) bucket).term);
            }
            builder.field(CommonFields.DOC_COUNT, bucket.getDocCount());
            ((InternalAggregations) bucket.getAggregations()).toXContentInternal(builder, params);
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

}
