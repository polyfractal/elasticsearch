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


import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BloomFilter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.terms.support.BucketPriorityQueue;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Result of the {@link TermsAggregator} when the field is some kind of whole number like a integer, long, or a date.
 */
public class LongRareTerms extends InternalMappedRareTerms<LongRareTerms, LongRareTerms.Bucket> {
    public static final String NAME = "rarelterms";

    public static class Bucket extends InternalTerms.Bucket<Bucket> {
        long term;

        public Bucket(long term, long docCount, InternalAggregations aggregations, boolean showDocCountError, long docCountError,
                      DocValueFormat format) {
            super(docCount, aggregations, showDocCountError, docCountError, format);
            this.term = term;
        }

        /**
         * Read from a stream.
         */
        public Bucket(StreamInput in, DocValueFormat format, boolean showDocCountError) throws IOException {
            super(in, format, showDocCountError);
            term = in.readLong();
        }

        @Override
        protected void writeTermTo(StreamOutput out) throws IOException {
            out.writeLong(term);
        }

        @Override
        public String getKeyAsString() {
            return format.format(term);
        }

        @Override
        public Object getKey() {
            return term;
        }

        @Override
        public Number getKeyAsNumber() {
            return term;
        }

        @Override
        public int compareKey(LongRareTerms.Bucket other) {
            return Long.compare(term, other.term);
        }

        @Override
        protected Bucket newBucket(long docCount, InternalAggregations aggs, long docCountError) {
            return new Bucket(term, docCount, aggs, showDocCountError, docCountError, format);
        }

        @Override
        protected final XContentBuilder keyToXContent(XContentBuilder builder) throws IOException {
            builder.field(CommonFields.KEY.getPreferredName(), term);
            if (format != DocValueFormat.RAW) {
                builder.field(CommonFields.KEY_AS_STRING.getPreferredName(), format.format(term));
            }
            return builder;
        }
    }

    public LongRareTerms(String name, BucketOrder order, int requiredSize, List<PipelineAggregator> pipelineAggregators,
                         Map<String, Object> metaData, DocValueFormat format, int shardSize,
                         List<Bucket> buckets, long maxDocCount, BloomFilter bloom) {
        super(name, order, requiredSize, pipelineAggregators, metaData, format, shardSize, buckets, maxDocCount, bloom);
    }

    /**
     * Read from a stream.
     */
    public LongRareTerms(StreamInput in) throws IOException {
        super(in, Bucket::new);
    }


    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public LongRareTerms create(List<Bucket> buckets) {
        return new LongRareTerms(name, order, requiredSize, pipelineAggregators(), metaData, format, shardSize,
            buckets, maxDocCount, bloom);
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(prototype.term, prototype.getDocCount(), aggregations, prototype.showDocCountError,
            prototype.docCountError, prototype.format);
    }

    @Override
    protected LongRareTerms create(String name, List<Bucket> buckets, long docCountError, long otherDocCount) {
        return new LongRareTerms(name, order, requiredSize, pipelineAggregators(), getMetaData(), format, shardSize,
            buckets, maxDocCount, bloom);
    }

    @Override
    protected Bucket[] createBucketsArray(int size) {
        return new Bucket[size];
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        for (InternalAggregation agg : aggregations) {
            if (agg instanceof DoubleTerms) {
                return agg.doReduce(aggregations, reduceContext);
            }
        }
        return super.doReduce(aggregations, reduceContext, (LongRareTerms.Bucket b) -> b.term);
    }

}
