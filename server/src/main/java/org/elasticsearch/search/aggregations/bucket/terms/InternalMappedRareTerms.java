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

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.SetBackedBloomFilter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public abstract class InternalMappedRareTerms<A extends InternalRareTerms<A, B>, B extends InternalRareTerms.Bucket<B>>
    extends InternalRareTerms<A, B> {

    protected DocValueFormat format;
    protected List<B> buckets;
    protected Map<String, B> bucketMap;

    final BloomSet bloomSet;

    public static class BloomSet implements Writeable {
        private Map<Integer, SetBackedBloomFilter> bloomFilters = new TreeMap<>();

        public BloomSet() {
        }

        public BloomSet(StreamInput in) throws IOException {
            this.bloomFilters = in.readMap(StreamInput::readVInt, SetBackedBloomFilter::new);
        }

        public void add(BloomSet filters) {
            filters.getBloomFilters().forEach((key, value) -> {
                // if we have a bloom stored and the incoming bloom is still in set-mode,
                // we can replay that set into the existing bloom and sidestep numBit issues
                if (bloomFilters.isEmpty() == false && value.isSetMode()) {
                    // treemap sorts according to key, so this will give us the largest bloom
                    boolean success = bloomFilters.entrySet().iterator().next().getValue().merge(value);
                    assert success;
                } else {
                    // Otherwise, merge same numBit blooms together
                    bloomFilters.merge(key, value, (b1, b2) -> {
                        b1.merge(b2);
                        return b1;
                    });
                }
            });
        }

        public Map<Integer, SetBackedBloomFilter> getBloomFilters() {
            return bloomFilters;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(bloomFilters, StreamOutput::writeVInt, (out1, value) -> value.writeTo(out1));
        }
    }

    InternalMappedRareTerms(String name, BucketOrder order, List<PipelineAggregator> pipelineAggregators,
                            Map<String, Object> metaData, DocValueFormat format,
                            List<B> buckets, long maxDocCount, BloomSet bloomSet) {
        super(name, order, maxDocCount, pipelineAggregators, metaData);
        this.format = format;
        this.buckets = buckets;
        this.bloomSet = bloomSet;
    }

    public long getMaxDocCount() {
        return maxDocCount;
    }

    private BloomSet getBloomSet() {
        return bloomSet;
    }

    /**
     * Read from a stream.
     */
    InternalMappedRareTerms(StreamInput in, Bucket.Reader<B> bucketReader) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        buckets = in.readList(stream -> bucketReader.read(stream, format));
        bloomSet = new BloomSet(in);
    }

    @Override
    protected void writeTermTypeInfoTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeList(buckets);
        bloomSet.writeTo(out);
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        Map<Object, List<B>> buckets = new HashMap<>();
        InternalRareTerms<A, B> referenceTerms = null;
        BloomSet blooms = new BloomSet();


        for (InternalAggregation aggregation : aggregations) {
            // Unmapped rare terms don't have a bloom filter so we'll skip all this work
            // and save some type casting headaches later.
            if (aggregation.isMapped() == false) {
                continue;
            }

            @SuppressWarnings("unchecked")
            InternalRareTerms<A, B> terms = (InternalRareTerms<A, B>) aggregation;
            if (referenceTerms == null && aggregation.getClass().equals(UnmappedRareTerms.class) == false) {
                referenceTerms = terms;
            }
            if (referenceTerms != null &&
                referenceTerms.getClass().equals(terms.getClass()) == false &&
                terms.getClass().equals(UnmappedRareTerms.class) == false) {
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

            blooms.add(((InternalMappedRareTerms)aggregation).getBloomSet());
        }

        final List<B> rare = new ArrayList<>();
        for (List<B> sameTermBuckets : buckets.values()) {
            final B b = sameTermBuckets.get(0).reduce(sameTermBuckets, reduceContext);
            if (b.getDocCount() <= maxDocCount && containsTerm(blooms, b) == false) {
                rare.add(b);
                reduceContext.consumeBucketsAndMaybeBreak(1);
            } else if (b.getDocCount() > maxDocCount) {
                // this term has gone over threshold while merging, so add it to the bloom.
                // Note this may happen during incremental reductions too
                // Add into our first and largest bloom
                addToBloom(bloomSet, b);
            }
        }
        CollectionUtil.introSort(rare, order.comparator(null));
        return createWithBloom(name, rare, bloomFilters);
    }

    public abstract boolean containsTerm(BloomSet bloom, B bucket);

    public abstract void addToBloom(BloomSet bloom, B bucket);

    @Override
    public List<B> getBuckets() {
        return buckets;
    }

    @Override
    public B getBucketByKey(String term) {
        if (bucketMap == null) {
            bucketMap = buckets.stream().collect(Collectors.toMap(InternalRareTerms.Bucket::getKeyAsString, Function.identity()));
        }
        return bucketMap.get(term);
    }

    @Override
    protected boolean doEquals(Object obj) {
        InternalMappedRareTerms<?,?> that = (InternalMappedRareTerms<?,?>) obj;
        return super.doEquals(obj)
            && Objects.equals(buckets, that.buckets)
            && Objects.equals(format, that.format)
            && Objects.equals(bloomSet, that.bloomSet);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(super.doHashCode(), buckets, format, bloomSet);
    }

    @Override
    public final XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return doXContentCommon(builder, params, buckets);
    }
}
