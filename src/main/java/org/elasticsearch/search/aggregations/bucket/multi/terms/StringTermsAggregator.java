/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.multi.terms;

import com.google.common.collect.ImmutableList;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.common.collect.ReusableGrowableArray;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.common.trove.ExtTHashMap;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.bucket.BytesBucketAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.ValueSpace;
import org.elasticsearch.search.aggregations.context.ValuesSource;
import org.elasticsearch.search.facet.terms.support.EntryPriorityQueue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.search.aggregations.bucket.BucketAggregator.buildAggregations;

/**
 *
 */
public class StringTermsAggregator extends BytesBucketAggregator {

    private final List<Aggregator.Factory> factories;
    private final InternalOrder order;
    private final int requiredSize;

    ExtTHashMap<HashedBytesRef, BucketCollector> buckets;

    public StringTermsAggregator(String name, List<Aggregator.Factory> factories, ValuesSource valuesSource,
                                 InternalOrder order, int requiredSize, AggregationContext aggregationContext, Aggregator parent) {

        super(name, valuesSource, aggregationContext, parent);
        this.factories = factories;
        this.order = order;
        this.requiredSize = requiredSize;
        buckets = aggregationContext.cacheRecycler().popHashMap();
    }

    @Override
    public Collector collector() {
        return new Collector();
    }

    @Override
    public StringTerms buildAggregation() {

        if (buckets.isEmpty()) {
            return new StringTerms(name, order, requiredSize, ImmutableList.<InternalTerms.Bucket>of());
        }

        if (requiredSize < EntryPriorityQueue.LIMIT) {
            BucketPriorityQueue ordered = new BucketPriorityQueue(requiredSize, order.comparator());
            Object[] collectors = buckets.internalValues();
            for (int i = 0; i < collectors.length; i++) {
                if (collectors[i] != null) {
                    ordered.insertWithOverflow(((BucketCollector) collectors[i]).buildBucket());
                }
            }
            aggregationContext.cacheRecycler().pushHashMap(buckets);
            InternalTerms.Bucket[] list = new InternalTerms.Bucket[ordered.size()];
            for (int i = ordered.size() - 1; i >= 0; i--) {
                list[i] = (StringTerms.Bucket) ordered.pop();
            }
            return new StringTerms(name, order, requiredSize, Arrays.asList(list));
        } else {
            BoundedTreeSet<InternalTerms.Bucket> ordered = new BoundedTreeSet<InternalTerms.Bucket>(order.comparator(), requiredSize);
            Object[] collectors = buckets.internalValues();
            for (int i = 0; i < collectors.length; i++) {
                if (collectors[i] != null) {
                    ordered.add(((BucketCollector) collectors[i]).buildBucket());
                }
            }
            aggregationContext.cacheRecycler().pushHashMap(buckets);
            return new StringTerms(name, order, requiredSize, ordered);
        }
    }

    class Collector implements Aggregator.Collector {

        private ReusableGrowableArray<BucketCollector> matchedBuckets;

        @Override
        public void collect(int doc, ValueSpace valueSpace) throws IOException {
            BytesValues values = valuesSource.bytesValues();

            if (!values.hasValue(doc)) {
                return;
            }

            Object valuesSourceKey = valuesSource.key();
            BytesRef scratch = new BytesRef();
            if (!values.isMultiValued()) {
                int hash = values.getValueHashed(doc, scratch);
                if (!valueSpace.accept(valuesSourceKey, scratch)) {
                    return;
                }
                HashedBytesRef term = new HashedBytesRef(scratch, hash);
                BucketCollector bucket = buckets.get(term);
                if (bucket == null) {
                    term.bytes = values.makeSafe(scratch);
                    bucket = new BucketCollector(valuesSource, term.bytes, factories, StringTermsAggregator.this);
                    buckets.put(term, bucket);
                }
                bucket.collect(doc, valueSpace);
                return;
            }

            if (matchedBuckets == null) {
                matchedBuckets = new ReusableGrowableArray<BucketCollector>(BucketCollector.class);
            }

            // we'll first find all the buckets that match the values, and then propagate the document through them
            // we need to do that to avoid counting the same document more than once.
            populateMatchingBuckets(doc, valuesSourceKey, values, valueSpace);
            BucketCollector[] mBuckets = matchedBuckets.innerValues();
            for (int i = 0; i < matchedBuckets.size(); i++) {
                mBuckets[i].collect(doc, valueSpace);
            }
        }

        private void populateMatchingBuckets(int doc, Object valuesSourceKey, BytesValues values, ValueSpace context) throws IOException {
            matchedBuckets.reset();
            for (BytesValues.Iter iter = values.getIter(doc); iter.hasNext();) {
                BytesRef value = iter.next();
                int hash = iter.hash();
                if (!context.accept(valuesSourceKey, value)) {
                    continue;
                }
                HashedBytesRef term = new HashedBytesRef(value, hash);
                BucketCollector bucket = buckets.get(term);
                if (bucket == null) {
                    term.bytes = values.makeSafe(value);
                    bucket = new BucketCollector(valuesSource, term.bytes, factories, StringTermsAggregator.this);
                    buckets.put(term, bucket);
                }
                matchedBuckets.add(bucket);
            }
        }


        @Override
        public void postCollection() {
            for (Object collector : buckets.internalValues()) {
                if (collector != null) {
                    ((BucketCollector) collector).postCollection();
                }
            }
            StringTermsAggregator.this.buckets = buckets;
        }
    }

    static class BucketCollector extends BytesBucketAggregator.BucketCollector {

        final BytesRef term;

        long docCount;

        BucketCollector(ValuesSource valuesSource, BytesRef term, List<Aggregator.Factory> factories, Aggregator aggregator) {
            super(valuesSource, factories, aggregator);
            this.term = term;
        }

        @Override
        protected boolean onDoc(int doc, BytesValues values, ValueSpace context) throws IOException {
            docCount++;
            return true;
        }

        @Override
        public boolean accept(BytesRef value) {
            return value.equals(term);
        }

        @Override
        protected void postCollection(Aggregator[] aggregators) {
        }

        StringTerms.Bucket buildBucket() {
            return new StringTerms.Bucket(term, docCount, buildAggregations(subAggregators));
        }
    }

}
