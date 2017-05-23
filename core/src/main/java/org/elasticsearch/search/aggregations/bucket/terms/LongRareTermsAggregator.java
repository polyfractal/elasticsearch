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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BloomFilter;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.common.util.LongObjectPagedHashMap;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.terms.support.BucketPriorityQueue;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude.LongFilter;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;

public class LongRareTermsAggregator extends TermsAggregator {

    protected final ValuesSource.Numeric valuesSource;

    //norelease: there is likely a smarter, better implementation to use
    protected final LongObjectPagedHashMap<Long> map;
    protected final BloomFilter bloom;

    private final LongFilter longFilter;
    private final long maxDocCount;
    private final BigArrays bigArrays;

    public LongRareTermsAggregator(String name, AggregatorFactories factories, ValuesSource.Numeric valuesSource, DocValueFormat format,
                                   BucketOrder order, BucketCountThresholds bucketCountThresholds, SearchContext aggregationContext, Aggregator parent,
                                   SubAggCollectionMode subAggCollectMode, IncludeExclude.LongFilter longFilter,
                                   long maxDocCount, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        super(name, factories, aggregationContext, parent, bucketCountThresholds, order, format, subAggCollectMode, pipelineAggregators, metaData);
        this.valuesSource = valuesSource;
        this.longFilter = longFilter;
        this.maxDocCount = maxDocCount;
        map = new LongObjectPagedHashMap<>(16, aggregationContext.bigArrays());
        bloom = BloomFilter.Factory.DEFAULT.createFilter(10000000);
        bigArrays = aggregationContext.bigArrays();
    }

    @Override
    public boolean needsScores() {
        return (valuesSource != null && valuesSource.needsScores()) || super.needsScores();
    }

    protected SortedNumericDocValues getValues(ValuesSource.Numeric valuesSource, LeafReaderContext ctx) throws IOException {
        return valuesSource.longValues(ctx);
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
                                                final LeafBucketCollector sub) throws IOException {
        final SortedNumericDocValues values = getValues(valuesSource, ctx);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long owningBucketOrdinal) throws IOException {
                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();

                    long previous = Long.MAX_VALUE;
                    for (int i = 0; i < valuesCount; ++i) {
                        final long val = values.nextValue();
                        if (previous != val || i == 0) {
                            if ((longFilter == null) || (longFilter.accept(val))) {

                                if (!bloom.mightContain(val)) {
                                    Long valueCount = map.get(val);
                                    if (valueCount == null) {
                                        // Brand new term, save into map
                                        map.put(val, 1L);
                                    } else {
                                        // We've seen this term before, but less than the threshold
                                        // so just increment its counter
                                        if (valueCount < maxDocCount) {
                                            map.put(val, valueCount + 1);
                                        } else {
                                            // Otherwise we've breached the threshold, remove from
                                            // the map and add to the bloom filter
                                            map.remove(val);
                                            bloom.put(val);
                                        }
                                    }
                                }
                            }
                            previous = val;
                        }
                    }
                }
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;

        LongHash bucketOrds = new LongHash(0, bigArrays);

        final int size = (int) Math.min(map.size(), bucketCountThresholds.getShardSize());

        long otherDocCount = 0;
        BucketPriorityQueue<LongRareTerms.Bucket> ordered = new BucketPriorityQueue<>(size, order.comparator(this));
        LongRareTerms.Bucket spare = null;
        for (LongObjectPagedHashMap.Cursor<Long> cursor : map) {

            // We have to replay our map into a LongHash map because downstream
            // methods expect the same hashing layout.
            long bucketOrdinal = bucketOrds.add(cursor.key);
            if (bucketOrdinal < 0) { // already seen
                bucketOrdinal = - 1 - bucketOrdinal;
            }

            if (spare == null) {
                spare = new LongRareTerms.Bucket(0, 0, null, false, 0, format);
            }
            spare.term = cursor.key;
            spare.docCount = cursor.value;
            otherDocCount += spare.docCount;
            spare.bucketOrd = bucketOrdinal;
            if (bucketCountThresholds.getShardMinDocCount() <= spare.docCount) {
                spare = ordered.insertWithOverflow(spare);
            }
        }

        // Get the top buckets
        final LongRareTerms.Bucket[] list = new LongRareTerms.Bucket[ordered.size()];
        long survivingBucketOrds[] = new long[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; --i) {
            final LongRareTerms.Bucket bucket = (LongRareTerms.Bucket) ordered.pop();
            survivingBucketOrds[i] = bucket.bucketOrd;
            list[i] = bucket;
            otherDocCount -= bucket.docCount;
        }

        runDeferredCollections(survivingBucketOrds);

        // Now build the aggs
        for (LongRareTerms.Bucket aList : list) {
            aList.aggregations = bucketAggregations(aList.bucketOrd);
            aList.docCountError = 0;
        }

        return new LongRareTerms(name, order, bucketCountThresholds.getRequiredSize(),
            pipelineAggregators(), metaData(), format, bucketCountThresholds.getShardSize(),
            Arrays.asList(list), maxDocCount, bloom);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new LongRareTerms(name, order, bucketCountThresholds.getRequiredSize(),
            pipelineAggregators(), metaData(), format, bucketCountThresholds.getShardSize(), emptyList(),
            maxDocCount, bloom);
    }

    @Override
    public void doClose() {
        Releasables.close(map, bloom);
    }
}
