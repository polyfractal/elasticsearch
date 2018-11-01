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

import com.carrotsearch.hppc.LongLongHashMap;
import com.carrotsearch.hppc.cursors.LongLongCursor;
import org.apache.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BloomFilter;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.common.util.LongObjectPagedHashMap;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.DeferableBucketAggregator;
import org.elasticsearch.search.aggregations.bucket.DeferringBucketCollector;
import org.elasticsearch.search.aggregations.bucket.MergingBucketsDeferringCollector;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;

public class LongRareTermsAggregator extends DeferableBucketAggregator {

    static final BucketOrder ORDER = BucketOrder.compound(BucketOrder.count(true), BucketOrder.key(true)); // sort by count ascending
    private static final Logger logger = Logger.getLogger(LongRareTermsAggregator.class.getName());

    //TODO better way to do this?
    protected LongLongHashMap map;
    protected LongHash bucketOrds;
    private LeafBucketCollector subCollectors;
    private final BloomFilter bloom;
    private final ValuesSource.Numeric valuesSource;
    private final IncludeExclude.LongFilter longFilter;
    private final int maxDocCount;
    private final DocValueFormat format;
    private MergingBucketsDeferringCollector deferringCollector;

    private final long GC_THRESHOLD = 10;

    public LongRareTermsAggregator(String name, AggregatorFactories factories, ValuesSource.Numeric valuesSource, DocValueFormat format,
                                   SearchContext aggregationContext, Aggregator parent, IncludeExclude.LongFilter longFilter,
                                   int maxDocCount, List<PipelineAggregator> pipelineAggregators,
                                   Map<String, Object> metaData) throws IOException {
        super(name, factories, aggregationContext, parent, pipelineAggregators, metaData);
        this.valuesSource = valuesSource;
        this.longFilter = longFilter;
        this.maxDocCount = maxDocCount;
        this.map = new LongLongHashMap();
        this.bloom = BloomFilter.Factory.DEFAULT.createFilter(10000000);
        this.bucketOrds = new LongHash(1, aggregationContext.bigArrays());
        this.format = format;
    }

    @Override
    protected boolean shouldDefer(Aggregator aggregator) {
        return true;
    }

    @Override
    public DeferringBucketCollector getDeferringCollector() {
        deferringCollector = new MergingBucketsDeferringCollector(context);
        return deferringCollector;
    }

    protected SortedNumericDocValues getValues(ValuesSource.Numeric valuesSource, LeafReaderContext ctx) throws IOException {
        return valuesSource.longValues(ctx);
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
                                                final LeafBucketCollector sub) throws IOException {
        final SortedNumericDocValues values = getValues(valuesSource, ctx);
        if (subCollectors == null) {
            subCollectors = sub;
        }
        return new LeafBucketCollectorBase(sub, values) {
            private long numDeleted = 0;

            @Override
            public void collect(int docId, long owningBucketOrdinal) throws IOException {
                if (values.advanceExact(docId)) {
                    final int valuesCount = values.docValueCount();

                    long previous = Long.MAX_VALUE;
                    for (int i = 0; i < valuesCount; ++i) {
                        final long val = values.nextValue();
                        if (previous != val || i == 0) {
                            if ((longFilter == null) || (longFilter.accept(val))) {
                                if (bloom.mightContain(val) == false) {
                                    long termCount = map.get(val);
                                    if (termCount == 0) {
                                        // Brand new term, save into map
                                        map.put(val, 1L);
                                        long bucketOrdinal = bucketOrds.add(val);
                                        if (bucketOrdinal < 0) { // already seen
                                            bucketOrdinal = - 1 - bucketOrdinal;
                                            collectExistingBucket(subCollectors, docId, bucketOrdinal);
                                        } else {
                                            collectBucket(subCollectors, docId, bucketOrdinal);
                                        }
                                    } else {
                                        // We've seen this term before, but less than the threshold
                                        // so just increment its counter
                                        if (termCount < maxDocCount) {
                                            // TODO if we only need maxDocCount==1, we could specialize
                                            // and use a bitset instead of a counter scheme
                                            map.put(val, termCount + 1);
                                        } else {
                                            // Otherwise we've breached the threshold, remove from
                                            // the map and add to the bloom filter
                                            map.remove(val);
                                            bloom.put(val);
                                            numDeleted += 1;

                                            if (numDeleted > GC_THRESHOLD) {
                                                gcDeletedEntries();
                                            }
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

    private void gcDeletedEntries() {
        try (LongHash oldBucketOrds = bucketOrds) {
            LongHash newBucketOrds = new LongHash(1, context.bigArrays());
            long[] mergeMap = new long[(int) oldBucketOrds.size()];

            for (int i = 0; i < oldBucketOrds.size(); i++) {
                long oldKey = oldBucketOrds.get(i);
                long newBucketOrd = -1;

                // if the key still exists in our map, reinsert into the new ords
                if (map.containsKey(Math.toIntExact(oldKey))) {
                    newBucketOrd = newBucketOrds.add(oldKey);
                }
                mergeMap[i] = newBucketOrd;
            }
            mergeBuckets(mergeMap, newBucketOrds.size());
            if (deferringCollector != null) {
                deferringCollector.mergeBuckets(mergeMap);
            }
            bucketOrds = newBucketOrds;
        }
    }

    @Override
    protected void doPostCollection() {
        logger.error("PostCollection()");
        gcDeletedEntries();
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;
        List<LongTerms.Bucket> buckets = new ArrayList<>(map.size());

        logger.error("BuildAggregation");
        for (LongLongCursor cursor : map) {
            // All the term ordinals were already inserted during post-collection,
            // so we can just get them here
            logger.error(cursor.key);
            long bucketOrdinal = bucketOrds.find(cursor.key);
            LongTerms.Bucket bucket = new LongTerms.Bucket(0, 0, null, false, 0, format);
            bucket.term = cursor.key;
            bucket.docCount = cursor.value;
            bucket.bucketOrd = bucketOrdinal;
            buckets.add(bucket);

            consumeBucketsAndMaybeBreak(1);
        }

        // Done with map and bloom, reclaim some memory
        bloom.close();
        map = null;

        runDeferredCollections(buckets.stream().mapToLong(b -> b.bucketOrd).toArray());

        // Now build the aggs
        for (LongTerms.Bucket bucket : buckets) {
            bucket.aggregations = bucketAggregations(bucket.bucketOrd);
            bucket.docCountError = 0;
        }

        CollectionUtil.introSort(buckets, ORDER.comparator(this));
        return new LongRareTerms(name, ORDER, pipelineAggregators(), metaData(), format, buckets, maxDocCount, bloom);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new LongRareTerms(name, ORDER, pipelineAggregators(), metaData(), format, emptyList(), 0, bloom);
    }

    @Override
    public void doClose() {
        Releasables.close(bloom);
    }
}
