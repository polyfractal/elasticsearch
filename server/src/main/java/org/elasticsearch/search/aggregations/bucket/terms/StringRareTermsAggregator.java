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

import com.carrotsearch.hppc.ObjectLongHashMap;
import com.carrotsearch.hppc.cursors.ObjectLongCursor;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BloomFilter;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
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

/**
 * An aggregator that finds "rare" string values (e.g. terms agg that orders ascending)
 */
public class StringRareTermsAggregator extends DeferableBucketAggregator {
    private final ValuesSource valuesSource;
    private final IncludeExclude.StringFilter includeExclude;

    // TODO review question: is there equivalent to LongObjectPagedHashMap like used in LongRareTerms?
    protected ObjectLongHashMap<BytesRef> map;
    protected BytesRefHash bucketOrds;
    private LeafBucketCollector subCollectors;
    private final BloomFilter bloom;
    private final long maxDocCount;
    private MergingBucketsDeferringCollector deferringCollector;
    private final DocValueFormat format;

    // TODO review question: What to set this at?
    /**
     Sets the number of "removed" values to accumulate before we purge ords
     via the MergingBucketCollector's mergeBuckets() method
     */
    private final long GC_THRESHOLD = 10;

    public StringRareTermsAggregator(String name, AggregatorFactories factories, ValuesSource valuesSource,
                                     DocValueFormat format,  IncludeExclude.StringFilter includeExclude,
                                     SearchContext context, Aggregator parent, List<PipelineAggregator> pipelineAggregators,
                                     Map<String, Object> metaData, long maxDocCount) throws IOException {
        super(name, factories, context, parent, pipelineAggregators, metaData);
        this.maxDocCount = maxDocCount;
        this.valuesSource = valuesSource;
        this.includeExclude = includeExclude;
        this.map = new ObjectLongHashMap<>();
        // TODO review: should we expose the BF settings?  What's a good default?
        this.bloom = BloomFilter.Factory.DEFAULT.createFilter(10000000);
        this.format = format;
        this.bucketOrds = new BytesRefHash(1, context.bigArrays());
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

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
                                                final LeafBucketCollector sub) throws IOException {
        final SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
        if (subCollectors == null) {
            subCollectors = sub;
        }
        return new LeafBucketCollectorBase(sub, values) {
            final BytesRefBuilder previous = new BytesRefBuilder();
            private long numDeleted = 0;

            @Override
            public void collect(int docId, long bucket) throws IOException {
                assert bucket == 0;
                if (values.advanceExact(docId)) {
                    final int valuesCount = values.docValueCount();
                    previous.clear();

                    // SortedBinaryDocValues don't guarantee uniqueness so we
                    // need to take care of dups
                    for (int i = 0; i < valuesCount; ++i) {
                        final BytesRef bytes = values.nextValue();
                        if (includeExclude != null && !includeExclude.accept(bytes)) {
                            continue;
                        }
                        if (i > 0 && previous.get().equals(bytes)) {
                            continue;
                        }

                        if (bloom.mightContain(bytes) == false) {
                            long valueCount = map.get(bytes);
                            if (valueCount == 0) {
                                // Brand new term, save into map
                                map.put(BytesRef.deepCopyOf(bytes), 1L);
                                long bucketOrdinal = bucketOrds.add(bytes);
                                if (bucketOrdinal < 0) { // already seen
                                    bucketOrdinal = - 1 - bucketOrdinal;
                                    collectExistingBucket(subCollectors, docId, bucketOrdinal);
                                } else {
                                    collectBucket(subCollectors, docId, bucketOrdinal);
                                }
                            } else {
                                // We've seen this term before, but less than the threshold
                                // so just increment its counter
                                if (valueCount < maxDocCount) {
                                    map.put(bytes, valueCount + 1);
                                } else {
                                    // Otherwise we've breached the threshold, remove from
                                    // the map and add to the bloom filter
                                    map.remove(bytes);
                                    bloom.put(bytes);
                                    numDeleted += 1;

                                    if (numDeleted > GC_THRESHOLD) {
                                        gcDeletedEntries();
                                    }
                                }
                            }
                        }
                        previous.copyBytes(bytes);
                    }
                }
            }
        };
    }

    private void gcDeletedEntries() {
        try (BytesRefHash oldBucketOrds = bucketOrds) {
            BytesRefHash newBucketOrds = new BytesRefHash(1, context.bigArrays());
            long[] mergeMap = new long[(int) oldBucketOrds.size()];

            BytesRef scratch = new BytesRef();
            for (int i = 0; i < oldBucketOrds.size(); i++) {
                BytesRef oldKey = oldBucketOrds.get(i, scratch);
                long newBucketOrd = -1;

                // if the key still exists in our map, reinsert into the new ords
                if (map.containsKey(oldKey)) {
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
        // Make sure we do one final GC to clean up any deleted ords
        // that may be lingering (but still below GC threshold)
        gcDeletedEntries();
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;

        List<StringTerms.Bucket> buckets = new ArrayList<>(map.size());

        for (ObjectLongCursor<BytesRef> cursor : map) {
            StringTerms.Bucket bucket = new StringTerms.Bucket(new BytesRef(), 0, null, false, 0, format);

            // We have to replay our map into a LongHash map because downstream
            // methods expect the same hashing layout.
            /*
            long bucketOrdinal = bucketOrds.add(cursor.key);
            if (bucketOrdinal < 0) { // already seen
                bucketOrdinal = -1 - bucketOrdinal;
            }

            bucket.termBytes = cursor.key;
            bucket.docCount = cursor.value;
            bucket.bucketOrd = bucketOrdinal;
            buckets.add(bucket);
            */

            // The collection managed pruning unwanted terms, so any
            // terms that made it this far are "rare" and we want buckets
            long bucketOrdinal = bucketOrds.find(cursor.key);
            bucket.termBytes = BytesRef.deepCopyOf(cursor.key);
            bucket.docCount = cursor.value;
            bucket.bucketOrd = bucketOrdinal;
            buckets.add(bucket);

            consumeBucketsAndMaybeBreak(1);
        }

        // Done with map and bloom, reclaim some memory
        bloom.close();
        map = null;

        runDeferredCollections(buckets.stream().mapToLong(b -> b.bucketOrd).toArray());

        // Finalize the buckets
        for (StringTerms.Bucket bucket : buckets) {
            bucket.aggregations = bucketAggregations(bucket.bucketOrd);
            bucket.docCountError = -1;  // TODO can we determine an error based on the bloom accuracy?
        }

        CollectionUtil.introSort(buckets, LongRareTermsAggregator.ORDER.comparator(this));
        return new StringRareTerms(name, LongRareTermsAggregator.ORDER, pipelineAggregators(), metaData(),
            format, buckets, maxDocCount, bloom);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new StringRareTerms(name, LongRareTermsAggregator.ORDER, pipelineAggregators(), metaData(), format, emptyList(), 0, bloom);
    }

    @Override
    public void doClose() {
        Releasables.close(bloom, bucketOrds);
    }
}

