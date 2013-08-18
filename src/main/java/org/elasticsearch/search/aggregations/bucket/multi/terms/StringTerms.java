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

package org.elasticsearch.search.aggregations.bucket.multi.terms;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.text.BytesText;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *
 */
public class StringTerms extends InternalTerms {

    public static final InternalAggregation.Type TYPE = new Type("terms", "sterms");

    public static AggregationStreams.Stream STREAM = new AggregationStreams.Stream<StringTerms>() {
        @Override
        public StringTerms readResult(StreamInput in) throws IOException {
            StringTerms buckets = new StringTerms();
            buckets.readFrom(in);
            return buckets;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }


    public static class Bucket extends InternalTerms.Bucket {

        private Text term;

        public Bucket(String term, long docCount, List<InternalAggregation> aggregations) {
            super(docCount, aggregations);
            this.term = new StringText(term);
        }

        public Bucket(BytesRef term, long docCount, InternalAggregations aggregations) {
            super(docCount, aggregations);
            this.term = new BytesText(new BytesArray(term));
        }

        public Bucket(Text term, long docCount, InternalAggregations aggregations) {
            super(docCount, aggregations);
            this.term = term;
        }

        @Override
        public Text getTerm() {
            return term;
        }

        @Override
        public Number getTermAsNumber() {
            return Double.parseDouble(term.string());
        }

        @Override
        protected int compareTerm(Terms.Bucket other) {
            return this.term.compareTo(other.getTerm());
        }
    }

    StringTerms() {} // for serialization

    public StringTerms(String name, InternalOrder order, int requiredSize, Collection<InternalTerms.Bucket> buckets) {
        super(name, order, requiredSize, buckets);
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        this.name = in.readString();
        this.order = InternalOrder.Streams.readOrder(in);
        this.requiredSize = in.readVInt();
        int size = in.readVInt();
        List<InternalTerms.Bucket> buckets = new ArrayList<InternalTerms.Bucket>(size);
        for (int i = 0; i < size; i++) {
            buckets.add(new Bucket(in.readText(), in.readVLong(), InternalAggregations.readAggregations(in)));
        }
        this.buckets = buckets;
        this.bucketMap = null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        InternalOrder.Streams.writeOrder(order, out);
        out.writeVInt(requiredSize);
        out.writeVInt(buckets.size());
        for (InternalTerms.Bucket bucket : buckets) {
            out.writeText(((Bucket) bucket).term);
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
            builder.field(Fields.TERM, ((Bucket) bucket).term);
            builder.field(CommonFields.DOC_COUNT, bucket.getDocCount());
            ((InternalAggregations) bucket.getAggregations()).toXContentInternal(builder, params);
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

}
