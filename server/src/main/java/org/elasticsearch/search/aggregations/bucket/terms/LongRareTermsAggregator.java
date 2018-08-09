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
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;

public class LongRareTermsAggregator extends TermsAggregator {

    private static final Logger logger = Logger.getLogger(LongRareTermsAggregator.class.getName());

    protected final ValuesSource.Numeric valuesSource;

    //TODO better way to do this?
    protected final LongObjectPagedHashMap<IntArray> map;
    protected final LongHash bucketOrds;
    protected LeafBucketCollector subCollectors;
    protected final BloomFilter bloom;

    private final IncludeExclude.LongFilter longFilter;
    private final int maxDocCount;
    private final BigArrays bigArrays;


    public LongRareTermsAggregator(String name, AggregatorFactories factories, ValuesSource.Numeric valuesSource, DocValueFormat format,
                                   BucketOrder order, SearchContext aggregationContext, Aggregator parent,
                                   SubAggCollectionMode subAggCollectMode, IncludeExclude.LongFilter longFilter,
                                   int maxDocCount, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        super(name, factories, aggregationContext, parent, null, order, format, subAggCollectMode, pipelineAggregators, metaData);
        this.valuesSource = valuesSource;
        this.longFilter = longFilter;
        this.maxDocCount = maxDocCount;
        map = new LongObjectPagedHashMap<>(16, aggregationContext.bigArrays());
        bloom = BloomFilter.Factory.DEFAULT.createFilter(10000000);
        bigArrays = aggregationContext.bigArrays();
        bucketOrds = new LongHash(1, aggregationContext.bigArrays());
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
        if (subCollectors == null) {
            subCollectors = sub;
        }
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int docId, long owningBucketOrdinal) throws IOException {
                if (values.advanceExact(docId)) {
                    final int valuesCount = values.docValueCount();

                    long previous = Long.MAX_VALUE;
                    for (int i = 0; i < valuesCount; ++i) {
                        final long val = values.nextValue();
                        if (previous != val || i == 0) {
                            if ((longFilter == null) || (longFilter.accept(val))) {

                                if (!bloom.mightContain(val)) {
                                    IntArray docs = map.get(val);
                                    if (docs == null) {
                                        // Brand new term, save into map
                                        // size BigArray to 1, we'll be optimistic the term is rare
                                        // and most rare terms are freq 1 instead of maxDocCount
                                        docs = bigArrays.newIntArray(1, false);
                                        docs.set(0, docId);
                                        map.put(val, docs);
                                    } else {
                                        // We've seen this term before, but less than the threshold
                                        // so just increment its counter
                                        if (docs.size() < maxDocCount) {
                                            // TODO if we only need maxDocCount==1, we could specialize
                                            // and use a bitset instead of a counter scheme
                                            bigArrays.grow(docs, docs.size() + 1);
                                            docs.set(docs.size(), docId);
                                            //map.put(val, valueCount + 1);
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
    protected void doPostCollection() throws IOException {
        // During collection, we didn't run any of the sub-agg collections.
        // Even though RareTerms only runs in breadth_first, if we collected sub-aggs
        // during the main collection we'd potentially be storing ordinals that would
        // later get removed.
        //
        // By collecting sub-aggs in postCollection(), we guarantee it is only
        // called on the surviving buckets
        logger.error("PostCollection()");
        for (LongObjectPagedHashMap.Cursor<IntArray> cursor : map) {
            for (int i = 0; i < cursor.value.size(); i++) {
                long bucketOrdinal = bucketOrds.add(cursor.key);
                if (bucketOrdinal < 0) { // already seen
                    bucketOrdinal = - 1 - bucketOrdinal;
                    collectExistingBucket(subCollectors, cursor.value.get(i), bucketOrdinal);
                } else {
                    collectBucket(subCollectors, cursor.value.get(i), bucketOrdinal);
                }
                logger.error(cursor.key + ":" + bucketOrdinal);
            }
        }
    }


    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;
        List<LongTerms.Bucket> buckets = new ArrayList<>((int)map.size());

        logger.error("BuildAggregation");
        for (LongObjectPagedHashMap.Cursor<IntArray> cursor : map) {
            // All the term ordinals were already inserted during post-collection,
            // so we can just get them here
            logger.error(cursor.key);
            long bucketOrdinal = bucketOrds.find(cursor.key);
            LongTerms.Bucket bucket = new LongTerms.Bucket(0, 0, null, false, 0, format);
            bucket.term = cursor.key;
            bucket.docCount = cursor.value.size();
            bucket.bucketOrd = bucketOrdinal;
            buckets.add(bucket);

            consumeBucketsAndMaybeBreak(1);
        }

        // Done with map and bloom, reclaim some memory
        bloom.close();
        map.close();

        runDeferredCollections(buckets.stream().mapToLong(b -> b.bucketOrd).toArray());

        // Now build the aggs
        for (LongTerms.Bucket bucket : buckets) {
            bucket.aggregations = bucketAggregations(bucket.bucketOrd);
            bucket.docCountError = 0;
        }

        CollectionUtil.introSort(buckets, order.comparator(this));
        return new LongRareTerms(name, order, pipelineAggregators(), metaData(), format, buckets, 0, bloom);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new LongRareTerms(name, order, pipelineAggregators(), metaData(), format, emptyList(), 0, bloom);
    }

    @Override
    public void doClose() {
        Releasables.close(map, bloom);
    }
}
