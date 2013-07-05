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

package org.elasticsearch.search.aggregations.bucket.multi.terms.longs;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.common.collect.ReusableGrowableArray;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.bucket.BucketAggregator;
import org.elasticsearch.search.aggregations.bucket.LongBucketAggregator;
import org.elasticsearch.search.aggregations.bucket.multi.terms.BucketPriorityQueue;
import org.elasticsearch.search.aggregations.bucket.multi.terms.InternalTerms;
import org.elasticsearch.search.aggregations.bucket.multi.terms.Terms;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.ValueSpace;
import org.elasticsearch.search.aggregations.context.numeric.NumericValuesSource;
import org.elasticsearch.search.facet.terms.support.EntryPriorityQueue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.search.aggregations.bucket.BucketAggregator.buildAggregations;

/**
 *
 */
public class LongTermsAggregator extends LongBucketAggregator {

    private final List<Aggregator.Factory> factories;
    private final Terms.Order order;
    private final int requiredSize;

    final ExtTLongObjectHashMap<BucketCollector> bucketCollectors;

    public LongTermsAggregator(String name, List<Aggregator.Factory> factories, NumericValuesSource valuesSource,
                               Terms.Order order, int requiredSize, AggregationContext aggregationContext, Aggregator parent) {
        super(name, valuesSource, aggregationContext, parent);
        this.factories = factories;
        this.order = order;
        this.requiredSize = requiredSize;
        this.bucketCollectors = CacheRecycler.popLongObjectMap();
    }

    @Override
    public Collector collector() {
        return new Collector();
    }

    @Override
    public LongTerms buildAggregation() {
        if (bucketCollectors.isEmpty()) {
            return new LongTerms(name, order, valuesSource.formatter(), requiredSize, ImmutableList.<InternalTerms.Bucket>of());
        }

        if (requiredSize < EntryPriorityQueue.LIMIT) {
            BucketPriorityQueue ordered = new BucketPriorityQueue(requiredSize, order.comparator());
            Object[] collectors = bucketCollectors.internalValues();
            for (int i = 0; i < collectors.length; i++) {
                if (collectors[i] != null) {
                    ordered.insertWithOverflow(((BucketCollector) collectors[i]).buildBucket());
                }
            }
            CacheRecycler.pushLongObjectMap(bucketCollectors);
            InternalTerms.Bucket[] list = new InternalTerms.Bucket[ordered.size()];
            for (int i = ordered.size() - 1; i >= 0; i--) {
                list[i] = (LongTerms.Bucket) ordered.pop();
            }
            return new LongTerms(name, order, valuesSource.formatter(), requiredSize, Arrays.asList(list));
        } else {
            BoundedTreeSet<InternalTerms.Bucket> ordered = new BoundedTreeSet<InternalTerms.Bucket>(order.comparator(), requiredSize);
            Object[] collectors = bucketCollectors.internalValues();
            for (int i = 0; i < collectors.length; i++) {
                if (collectors[i] != null) {
                    ordered.add(((BucketCollector) collectors[i]).buildBucket());
                }
            }
            CacheRecycler.pushLongObjectMap(bucketCollectors);
            return new LongTerms(name, order, valuesSource.formatter(), requiredSize, ordered);
        }
    }

    class Collector implements Aggregator.Collector {

        private ReusableGrowableArray<BucketCollector> matchedBuckets;

        @Override
        public void collect(int doc, ValueSpace valueSpace) throws IOException {

            LongValues values = valuesSource.longValues();

            if (!values.hasValue(doc)) {
                return;
            }

            Object valuesSourceKey = valuesSource.key();
            if (!values.isMultiValued()) {
                long term = values.getValue(doc);
                if (!valueSpace.accept(valuesSourceKey, term)) {
                    return;
                }

                BucketCollector bucket = bucketCollectors.get(term);
                if (bucket == null) {
                    bucket = new BucketCollector(term, LongTermsAggregator.this);
                    bucketCollectors.put(term, bucket);
                }

                bucket.collect(doc, valueSpace);
                return;
            }

            if (matchedBuckets == null) {
                matchedBuckets = new ReusableGrowableArray<BucketCollector>();
            }
            populateMatchingBuckets(doc, valuesSourceKey, values, valueSpace);
            for (int i = 0; i < matchedBuckets.size(); i++) {
                matchedBuckets.get(i).collect(doc, valueSpace);
            }
        }

        private void populateMatchingBuckets(int doc, Object valuesSourceKey, LongValues values, ValueSpace valueSpace) {
            matchedBuckets.reset();
            for (LongValues.Iter iter = values.getIter(doc); iter.hasNext();) {
                long term = iter.next();
                if (!valueSpace.accept(valuesSourceKey, term)) {
                    continue;
                }
                BucketCollector bucket = bucketCollectors.get(term);
                if (bucket == null) {
                    bucket = new BucketCollector(term, LongTermsAggregator.this);
                    bucketCollectors.put(term, bucket);
                }
                matchedBuckets.add(bucket);
            }
        }

        @Override
        public void postCollection() {
            Object[] collectors = bucketCollectors.internalValues();
            for (int i = 0; i < collectors.length; i++) {
                if (collectors[i] != null) {
                    ((BucketCollector) collectors[i]).postCollection();
                }
            }
        }
    }

    static class BucketCollector extends BucketAggregator.BucketCollector {

        final long term;
        long docCount;

        BucketCollector(long term, LongTermsAggregator parent) {
            super(parent.factories, parent);
            this.term = term;
        }

        @Override
        protected ValueSpace onDoc(int doc, ValueSpace valueSpace) throws IOException {
            docCount++;
            return valueSpace;
        }

        @Override
        protected void postCollection(Aggregator[] aggregators) {
        }

        LongTerms.Bucket buildBucket() {
            return new LongTerms.Bucket(term, docCount, buildAggregations(subAggregators));
        }
    }

}
