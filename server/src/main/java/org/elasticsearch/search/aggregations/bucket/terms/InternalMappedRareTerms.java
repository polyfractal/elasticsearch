package org.elasticsearch.search.aggregations.bucket.terms;


import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BloomFilter;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class InternalMappedRareTerms<A extends InternalTerms<A, B>, B extends InternalTerms.Bucket<B>>
    extends InternalMappedTerms<A,B> {

    final long maxDocCount;
    final BloomFilter bloom;

    InternalMappedRareTerms(String name, BucketOrder order, int requiredSize,
                                      List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData,
                                      DocValueFormat format, int shardSize, List<B> buckets,
                                      long maxDocCount, BloomFilter bloom) {
        super(name, order, requiredSize, 1, pipelineAggregators, metaData, format, shardSize, false,
            0, buckets, 0);
        //todo: doc count errors
        this.maxDocCount = maxDocCount;
        this.bloom = bloom;
    }

    public long getMaxDocCount() {
        return maxDocCount;
    }

    public BloomFilter getBloom() {
        return bloom;
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

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
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

        // Always return all results, so just proactively size the array to num buckets
        final int size = buckets.size();
        final List<B> rare = new ArrayList<>(size);
        for (List<B> sameTermBuckets : buckets.values()) {
            final B b = sameTermBuckets.get(0).reduce(sameTermBuckets, reduceContext);
            if (b.getDocCount() <= maxDocCount && containsTerm(bloom, b) == false) {
                rare.add(b);
            }
        }

        //todo: doc count error, don't really need these...
        return create(name, rare, 0, 0);
    }

    public abstract boolean containsTerm(BloomFilter bloom, B b);
}
