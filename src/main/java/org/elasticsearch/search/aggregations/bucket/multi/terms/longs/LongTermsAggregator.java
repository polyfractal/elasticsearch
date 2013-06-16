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
import com.google.common.collect.Lists;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.bucket.BucketAggregator;
import org.elasticsearch.search.aggregations.bucket.LongBucketAggregator;
import org.elasticsearch.search.aggregations.bucket.multi.terms.BucketPriorityQueue;
import org.elasticsearch.search.aggregations.bucket.multi.terms.InternalTerms;
import org.elasticsearch.search.aggregations.bucket.multi.terms.Terms;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.ValuesSourceFactory;
import org.elasticsearch.search.aggregations.context.numeric.NumericValuesSource;
import org.elasticsearch.search.facet.terms.support.EntryPriorityQueue;
import org.elasticsearch.search.internal.SearchContext;

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
                               Terms.Order order, int requiredSize, SearchContext searchContext, ValuesSourceFactory valuesSourceFactory, Aggregator parent) {
        super(name, valuesSource, searchContext, valuesSourceFactory, parent);
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
            InternalTerms.Bucket[] list = new InternalTerms.Bucket[ordered.size()];
            for (int i = ordered.size() - 1; i >= 0; i--) {
                list[i] = (LongTerms.Bucket) ordered.pop();
            }
            CacheRecycler.pushLongObjectMap(bucketCollectors);
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

        LongValues values;
        AggregationContext context;

        @Override
        public void setNextContext(AggregationContext context) throws IOException {
            this.context = context;
            values = valuesSource.longValues();
            Object[] collectors = bucketCollectors.internalValues();
            for (int i = 0; i <collectors.length; i++) {
                if (collectors[i] != null) {
                    ((BucketCollector) collectors[i]).setNextContext(context);
                }
            }
        }

        long time = 0;

        @Override
        public void collect(int doc) throws IOException {

            if (!values.hasValue(doc)) {
                return;
            }

            String valuesSourceKey = valuesSource.key();
            if (!values.isMultiValued()) {
                long term = values.getValue(doc);
                if (!context.accept(valuesSourceKey, term)) {
                    return;
                }
                BucketCollector bucket = bucketCollectors.get(term);
                if (bucket == null) {
                    long start = System.currentTimeMillis();
                    bucket = new BucketCollector(term, context, LongTermsAggregator.this);
                    time += System.currentTimeMillis() - start;
                    bucketCollectors.put(term, bucket);
                }
                bucket.collect(doc);

                return;
            }

            List<BucketCollector> matchedBuckets = findMatchingBuckets(doc, valuesSourceKey, values);
            if (matchedBuckets != null) {
                for (int i = 0; i < matchedBuckets.size(); i++) {
                    matchedBuckets.get(i).collect(doc);
                }
            }
        }

        private List<BucketCollector> findMatchingBuckets(int doc, String valuesSourceKey, LongValues values) {
            List<BucketCollector> matchedBuckets = null;
            for (LongValues.Iter iter = values.getIter(doc); iter.hasNext();) {
                long term = iter.next();
                if (!context.accept(valuesSourceKey, term)) {
                    continue;
                }
                BucketCollector bucket = bucketCollectors.get(term);
                if (bucket == null) {
                    bucket = new BucketCollector(term, context, LongTermsAggregator.this);
                    bucketCollectors.put(term, bucket);
                }
                if (matchedBuckets == null) {
                    matchedBuckets = Lists.newArrayListWithCapacity(4);
                }
                matchedBuckets.add(bucket);
            }
            return matchedBuckets;
        }

        @Override
        public void postCollection() {
            System.out.println("agg: total collect - " + time);
            for (Object bucketCollector : bucketCollectors.internalValues()) {
                if (bucketCollector != null) {
                    ((BucketCollector) bucketCollector).postCollection();
                }
            }
        }
    }

    static class BucketCollector extends BucketAggregator.BucketCollector {

        final long term;

        long docCount;

        BucketCollector(long term, AggregationContext context, LongTermsAggregator parent) {
            super(parent.factories, context, parent);
            this.term = term;
        }

        @Override
        protected boolean onDoc(int doc) throws IOException {
            docCount++;
            return true;
        }

        @Override
        protected AggregationContext setAndGetContext(AggregationContext context) throws IOException {
            return context;
        }

        @Override
        protected void postCollection(Aggregator[] aggregators) {
        }

        LongTerms.Bucket buildBucket() {
            return new LongTerms.Bucket(term, docCount, buildAggregations(subAggregators));
        }
    }

}
