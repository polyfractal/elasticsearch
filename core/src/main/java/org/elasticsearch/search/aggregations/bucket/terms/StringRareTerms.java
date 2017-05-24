/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BloomFilter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.BucketOrder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Result of the {@link RareTermsAggregator} when the field is a String.
 */
public class StringRareTerms extends InternalMappedRareTerms<StringRareTerms, StringRareTerms.Bucket> {
    public static final String NAME = "srareterms";
    public static class Bucket extends InternalTerms.Bucket<Bucket> {
        BytesRef termBytes;

        public Bucket(BytesRef term, long docCount, InternalAggregations aggregations, boolean showDocCountError, long docCountError,
                      DocValueFormat format) {
            super(docCount, aggregations, showDocCountError, docCountError, format);
            this.termBytes = term;
        }

        /**
         * Read from a stream.
         */
        public Bucket(StreamInput in, DocValueFormat format, boolean showDocCountError) throws IOException {
            super(in, format, showDocCountError);
            termBytes = in.readBytesRef();
        }

        @Override
        protected void writeTermTo(StreamOutput out) throws IOException {
            out.writeBytesRef(termBytes);
        }

        @Override
        public Object getKey() {
            return getKeyAsString();
        }

        @Override
        public Number getKeyAsNumber() {
            // this method is needed for scripted numeric aggs
            return Double.parseDouble(termBytes.utf8ToString());
        }

        @Override
        public String getKeyAsString() {
            return format.format(termBytes);
        }

        @Override
        public int compareKey(Bucket other) {
            return termBytes.compareTo(other.termBytes);
        }

        @Override
        Bucket newBucket(long docCount, InternalAggregations aggs, long docCountError) {
            return new Bucket(termBytes, docCount, aggs, showDocCountError, docCountError, format);
        }

        @Override
        protected final XContentBuilder keyToXContent(XContentBuilder builder) throws IOException {
            return builder.field(CommonFields.KEY.getPreferredName(), getKeyAsString());
        }

        @Override
        public boolean equals(Object obj) {
            return super.equals(obj) && Objects.equals(termBytes, ((Bucket) obj).termBytes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), termBytes);
        }
    }

    public StringRareTerms(String name, BucketOrder order, int requiredSize, List<PipelineAggregator> pipelineAggregators,
                       Map<String, Object> metaData, DocValueFormat format, int shardSize,
                       List<Bucket> buckets, long maxDocCount, BloomFilter bloom) {
        super(name, order, requiredSize, pipelineAggregators, metaData, format,
            shardSize, buckets, maxDocCount, bloom);
    }

    /**
     * Read from a stream.
     */
    public StringRareTerms(StreamInput in) throws IOException {
        super(in, Bucket::new);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public StringRareTerms create(List<Bucket> buckets) {
        return new StringRareTerms(name, order, requiredSize, pipelineAggregators(), metaData, format, shardSize,
            buckets, maxDocCount, bloom);
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(prototype.termBytes, prototype.getDocCount(), aggregations, false,
            prototype.docCountError, prototype.format);
    }

    @Override
    protected StringRareTerms create(String name, List<Bucket> buckets, long docCountError, long otherDocCount) {
        return new StringRareTerms(name, order, requiredSize, pipelineAggregators(), metaData, format, shardSize,
            buckets, maxDocCount, bloom);
    }

    @Override
    protected Bucket[] createBucketsArray(int size) {
        return new Bucket[size];
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        return super.doReduce(aggregations, reduceContext, (StringRareTerms.Bucket b) -> b.termBytes);
    }
}
