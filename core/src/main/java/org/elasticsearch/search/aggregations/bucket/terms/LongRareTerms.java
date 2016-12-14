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
public class LongRareTerms extends InternalMappedTerms<LongRareTerms, LongRareTerms.Bucket> {
    public static final String NAME = "rarelterms";
    private final long maxDocCount;
    private final BloomFilter bloom;

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
        protected int compareTerm(Terms.Bucket other) {
            return Long.compare(term, ((Number) other.getKey()).longValue());
        }

        @Override
        protected Bucket newBucket(long docCount, InternalAggregations aggs, long docCountError) {
            return new Bucket(term, docCount, aggs, showDocCountError, docCountError, format);
        }
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CommonFields.KEY, term);
            if (format != DocValueFormat.RAW) {
                builder.field(CommonFields.KEY_AS_STRING, format.format(term));
            }
            builder.field(CommonFields.DOC_COUNT, getDocCount());
            if (showDocCountError) {
                builder.field(InternalTerms.DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME, getDocCountError());
            }
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }
    }

    public LongRareTerms(String name, Terms.Order order, int requiredSize, List<PipelineAggregator> pipelineAggregators,
                     Map<String, Object> metaData, DocValueFormat format, int shardSize, boolean showTermDocCountError, long otherDocCount,
                     List<Bucket> buckets, long docCountError, long maxDocCount, BloomFilter bloom) {
        super(name, order, requiredSize, 1, pipelineAggregators, metaData, format, shardSize, showTermDocCountError,
            otherDocCount, buckets, docCountError);
        this.maxDocCount = maxDocCount;
        this.bloom = bloom;
    }

    /**
     * Read from a stream.
     */
    public LongRareTerms(StreamInput in) throws IOException {
        super(in, Bucket::new);
        maxDocCount = in.readLong();
        bloom = new BloomFilter(in);
    }

    @Override
    protected void writeTermTypeInfoTo(StreamOutput out) throws IOException {
        super.writeTermTypeInfoTo(out);
        out.writeLong(maxDocCount);
        bloom.writeTo(out);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public LongRareTerms create(List<Bucket> buckets) {
        return new LongRareTerms(name, order, requiredSize, pipelineAggregators(), metaData, format, shardSize,
            showTermDocCountError, otherDocCount, buckets, docCountError, maxDocCount, bloom);
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(prototype.term, prototype.getDocCount(), aggregations, prototype.showDocCountError,
            prototype.docCountError, prototype.format);
    }

    @Override
    protected LongRareTerms create(String name, List<Bucket> buckets, long docCountError, long otherDocCount) {
        return new LongRareTerms(name, order, requiredSize, pipelineAggregators(), getMetaData(), format, shardSize,
            showTermDocCountError, otherDocCount, buckets, docCountError, maxDocCount, bloom);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(InternalTerms.DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME, docCountError);
        builder.field(SUM_OF_OTHER_DOC_COUNTS, otherDocCount);
        builder.startArray(CommonFields.BUCKETS);
        for (Bucket bucket : buckets) {
            bucket.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    @Override
    protected Bucket[] createBucketsArray(int size) {
        return new Bucket[size];
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        Map<Object, List<LongRareTerms.Bucket>> buckets = new HashMap<>();
        long sumDocCountError = 0;
        long otherDocCount = 0;
        InternalTerms<LongRareTerms, LongRareTerms.Bucket> referenceTerms = null;
        BloomFilter bloom = null;

        for (InternalAggregation aggregation : aggregations) {
            @SuppressWarnings("unchecked")
            InternalTerms<LongRareTerms, LongRareTerms.Bucket> terms = (InternalTerms<LongRareTerms, LongRareTerms.Bucket>) aggregation;
            LongRareTerms rareTerms = (LongRareTerms) aggregation;
            if (bloom == null) {
                bloom = rareTerms.bloom;
            } else {
                bloom.merge(rareTerms.bloom);
            }

            if (referenceTerms == null && !aggregation.getClass().equals(UnmappedTerms.class)) {
                referenceTerms = terms;
            }
            if (referenceTerms != null &&
                !referenceTerms.getClass().equals(terms.getClass()) &&
                !terms.getClass().equals(UnmappedTerms.class)) {
                // control gets into this loop when the same field name against which the query is executed
                // is of different types in different indices.
                throw new AggregationExecutionException("Merging/Reducing the aggregations failed when computing the aggregation ["
                    + referenceTerms.getName() + "] because the field you gave in the aggregation query existed as two different "
                    + "types in two different indices");
            }
            otherDocCount += terms.getSumOfOtherDocCounts();
            final long thisAggDocCountError;
            if (terms.getBucketsInternal().size() < getShardSize() || InternalOrder.isTermOrder(order)) {
                thisAggDocCountError = 0;
            } else if (InternalOrder.isCountDesc(this.order)) {
                thisAggDocCountError = terms.getBucketsInternal().get(terms.getBucketsInternal().size() - 1).getDocCount();
            } else {
                thisAggDocCountError = -1;
            }
            if (sumDocCountError != -1) {
                if (thisAggDocCountError == -1) {
                    sumDocCountError = -1;
                } else {
                    sumDocCountError += thisAggDocCountError;
                }
            }
            setDocCountError(thisAggDocCountError);
            for (LongRareTerms.Bucket bucket : terms.getBucketsInternal()) {
                bucket.docCountError = thisAggDocCountError;
                List<LongRareTerms.Bucket> bucketList = buckets.get(bucket.getKey());
                if (bucketList == null) {
                    bucketList = new ArrayList<>();
                    buckets.put(bucket.getKey(), bucketList);
                }
                bucketList.add(bucket);
            }

        }

        final int size = Math.min(requiredSize, buckets.size());
        BucketPriorityQueue<LongRareTerms.Bucket> ordered = new BucketPriorityQueue<>(size, order.comparator(null));
        for (List<LongRareTerms.Bucket> sameTermBuckets : buckets.values()) {
            final LongRareTerms.Bucket b = sameTermBuckets.get(0).reduce(sameTermBuckets, reduceContext);
            if (b.docCountError != -1) {
                if (sumDocCountError == -1) {
                    b.docCountError = -1;
                } else {
                    b.docCountError = sumDocCountError - b.docCountError;
                }
            }
            if (b.getDocCount() <= maxDocCount && !(bloom != null && bloom.mightContain(b.term))) {

                LongRareTerms.Bucket removed = ordered.insertWithOverflow(b);
                if (removed != null) {
                    otherDocCount += removed.getDocCount();
                }
            }
        }
        LongRareTerms.Bucket[] list = createBucketsArray(ordered.size());
        for (int i = ordered.size() - 1; i >= 0; i--) {
            list[i] = ordered.pop();
        }
        long docCountError;
        if (sumDocCountError == -1) {
            docCountError = -1;
        } else {
            docCountError = aggregations.size() == 1 ? 0 : sumDocCountError;
        }
        return create(name, Arrays.asList(list), docCountError, otherDocCount);
    }
}
