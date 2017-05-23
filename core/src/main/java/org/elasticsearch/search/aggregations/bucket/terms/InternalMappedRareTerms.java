package org.elasticsearch.search.aggregations.bucket.terms;


import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BloomFilter;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.bucket.terms.support.BucketPriorityQueue;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public abstract class InternalMappedRareTerms<A extends InternalTerms<A, B>, B extends InternalTerms.Bucket<B>>
    extends InternalMappedTerms<A,B> {

    protected final long maxDocCount;
    protected final BloomFilter bloom;

    protected InternalMappedRareTerms(String name, BucketOrder order, int requiredSize,
                                      List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData,
                                      DocValueFormat format, int shardSize, List<B> buckets,
                                      long maxDocCount, BloomFilter bloom) {
        super(name, order, requiredSize, 1, pipelineAggregators, metaData, format, shardSize, false,
            0, buckets, 0);
        //todo norelease: doc count errors
        this.maxDocCount = maxDocCount;
        this.bloom = bloom;
    }

    /**
     * Read from a stream.
     */
    protected InternalMappedRareTerms(StreamInput in, Bucket.Reader<B> bucketReader) throws IOException {
        super(in, bucketReader);
        maxDocCount = in.readLong();
        bloom = new BloomFilter(in);
    }

    @Override
    protected void writeTermTypeInfoTo(StreamOutput out) throws IOException {
        super.writeTermTypeInfoTo(out);
        out.writeLong(maxDocCount);
        bloom.writeTo(out);
    }

    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext,
                                        Function<B, Long> termConverter) {
        Map<Object, List<B>> buckets = new HashMap<>();
        InternalTerms<A, B> referenceTerms = null;
        BloomFilter bloomFilter = null;

        for (InternalAggregation aggregation : aggregations) {
            @SuppressWarnings("unchecked")
            InternalTerms<A, B> terms = (InternalTerms<A, B>) aggregation;
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
            for (B bucket : terms.getBuckets()) {
                List<B> bucketList = buckets.computeIfAbsent(bucket.getKey(), k -> new ArrayList<>());
                bucketList.add(bucket);
            }

            if (bloomFilter == null) {
                bloomFilter = ((InternalMappedRareTerms)aggregation).bloom;
            } else {
                bloomFilter.merge(((InternalMappedRareTerms)aggregation).bloom);
            }
        }

        final int size = reduceContext.isFinalReduce() == false ? buckets.size() : Math.min(requiredSize, buckets.size());
        final BucketPriorityQueue<B> ordered = new BucketPriorityQueue<>(size, order.comparator(null));
        for (List<B> sameTermBuckets : buckets.values()) {
            final B b = sameTermBuckets.get(0).reduce(sameTermBuckets, reduceContext);
            if (b.getDocCount() <= maxDocCount && !(bloom != null && bloom.mightContain(termConverter.apply(b)))) {
                ordered.insertWithOverflow(b);
            }
        }
        B[] list = createBucketsArray(ordered.size());
        for (int i = ordered.size() - 1; i >= 0; i--) {
            list[i] = ordered.pop();
        }

        //todo norelease: doc count error, don't really need these...
        return create(name, Arrays.asList(list), 0, 0);
    }

    /*
    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        Map<Object, List<B>> buckets = new HashMap<>();
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
    */
}
