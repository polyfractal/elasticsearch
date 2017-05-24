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
import org.elasticsearch.common.util.BloomFilter;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * An aggregator of string values.
 */
public class StringRareTermsAggregator extends StringTermsAggregator {
    private final ValuesSource valuesSource;
    private final IncludeExclude.StringFilter includeExclude;

    // TODO norelease: is there equivalent to LongObjectPagedHashMap like used in LongRareTerms?
    protected final ObjectLongHashMap<BytesRef> map;
    protected final BloomFilter bloom;
    private final long maxDocCount;

    public StringRareTermsAggregator(String name, AggregatorFactories factories, ValuesSource valuesSource,
                                     BucketOrder order, DocValueFormat format, BucketCountThresholds bucketCountThresholds,
                                     IncludeExclude.StringFilter includeExclude, SearchContext context, Aggregator parent,
                                     SubAggCollectionMode collectionMode, List<PipelineAggregator> pipelineAggregators,
                                     Map<String, Object> metaData, long maxDocCount) throws IOException {
        super(name, factories, valuesSource, order, format, bucketCountThresholds, includeExclude, context, parent,
            collectionMode, false, pipelineAggregators, metaData);
        this.maxDocCount = maxDocCount;
        this.valuesSource = valuesSource;
        this.includeExclude = includeExclude;
        this.map = new ObjectLongHashMap<>();
        this.bloom = BloomFilter.Factory.DEFAULT.createFilter(10000000);
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
                                                final LeafBucketCollector sub) throws IOException {
        final SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {
            final BytesRefBuilder previous = new BytesRefBuilder();

            @Override
            public void collect(int doc, long bucket) throws IOException {
                assert bucket == 0;
                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();

                    // SortedBinaryDocValues don't guarantee uniqueness so we
                    // need to take care of dups
                    previous.clear();
                    for (int i = 0; i < valuesCount; ++i) {
                        final BytesRef bytes = values.nextValue();
                        if (includeExclude != null && !includeExclude.accept(bytes)) {
                            continue;
                        }
                        if (previous.get().equals(bytes)) {
                            continue;
                        }

                        if (bloom.mightContain(bytes) == false) {
                            Long valueCount = map.get(bytes);
                            if (valueCount == 0) {
                                // Brand new term, save into map
                                map.put(bytes, 1L);
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
                                }
                            }
                        }
                        previous.copyBytes(bytes);
                    }
                }
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;

        final int size = Math.min(map.size(), bucketCountThresholds.getShardSize());

        //TODO norelease: it feels like a priorityqueue isn't needed here, hence the list.  But what to
        // do about the shard sizes, etc?  Are those needed anymore?
        List<StringRareTerms.Bucket> buckets = new ArrayList<>(size);


        Iterator<ObjectLongCursor<BytesRef>> iter = map.iterator();
        while (iter.hasNext()) {
            ObjectLongCursor<BytesRef> cursor = iter.next();

            StringRareTerms.Bucket bucket = new StringRareTerms.Bucket(new BytesRef(), 0, null, false, 0, format);


            // We have to replay our map into a LongHash map because downstream
            // methods expect the same hashing layout.
            long bucketOrdinal = bucketOrds.add(cursor.key);
            if (bucketOrdinal < 0) { // already seen
                bucketOrdinal = - 1 - bucketOrdinal;
            }

            bucket.termBytes = cursor.key;
            bucket.docCount = cursor.value;
            bucket.bucketOrd = bucketOrdinal;
            buckets.add(bucket);
        }

        // Get the top buckets
        final StringRareTerms.Bucket[] list = new StringRareTerms.Bucket[buckets.size()];
        long survivingBucketOrds[] = new long[buckets.size()];
        for (int i = buckets.size() - 1; i >= 0; --i) {
            final StringRareTerms.Bucket bucket = buckets.get(i);
            survivingBucketOrds[i] = bucket.bucketOrd;
            list[i] = bucket;
        }
        // replay any deferred collections
        runDeferredCollections(survivingBucketOrds);

        // Now build the aggs
        for (int i = 0; i < list.length; i++) {
            final StringRareTerms.Bucket bucket = list[i];
            bucket.termBytes = BytesRef.deepCopyOf(bucket.termBytes);
            bucket.aggregations = bucketAggregations(bucket.bucketOrd);
            bucket.docCountError = 0;
        }

        return new StringRareTerms(name, order, bucketCountThresholds.getRequiredSize(),
            pipelineAggregators(), metaData(), format, bucketCountThresholds.getShardSize(),
            Arrays.asList(list), maxDocCount, bloom);
    }
}

