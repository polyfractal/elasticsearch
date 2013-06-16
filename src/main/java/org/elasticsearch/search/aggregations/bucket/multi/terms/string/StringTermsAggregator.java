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

package org.elasticsearch.search.aggregations.bucket.multi.terms.string;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.common.trove.ExtTHashMap;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.bucket.BucketAggregator;
import org.elasticsearch.search.aggregations.bucket.BytesBucketAggregator;
import org.elasticsearch.search.aggregations.bucket.multi.terms.BucketPriorityQueue;
import org.elasticsearch.search.aggregations.bucket.multi.terms.InternalTerms;
import org.elasticsearch.search.aggregations.bucket.multi.terms.Terms;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.ValuesSourceFactory;
import org.elasticsearch.search.aggregations.context.bytes.BytesValuesSource;
import org.elasticsearch.search.facet.terms.support.EntryPriorityQueue;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.aggregations.bucket.BucketAggregator.buildAggregations;

/**
 *
 */
public class StringTermsAggregator extends BytesBucketAggregator {

    private final List<Aggregator.Factory> factories;
    private final Terms.Order order;
    private final int requiredSize;

    ExtTHashMap<HashedBytesRef, BucketCollector> buckets;

    public StringTermsAggregator(String name, List<Aggregator.Factory> factories, BytesValuesSource valuesSource,
                                 Terms.Order order, int requiredSize, SearchContext searchContext,
                                 ValuesSourceFactory valuesSourceFactory, Aggregator parent) {

        super(name, valuesSource, searchContext, valuesSourceFactory, parent);
        this.factories = factories;
        this.order = order;
        this.requiredSize = requiredSize;
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
            for (Map.Entry<HashedBytesRef, BucketCollector> entry : buckets.entrySet()) {
                ordered.insertWithOverflow(entry.getValue().buildBucket());
            }
            InternalTerms.Bucket[] list = new InternalTerms.Bucket[ordered.size()];
            for (int i = ordered.size() - 1; i >= 0; i--) {
                list[i] = (StringTerms.Bucket) ordered.pop();
            }
            return new StringTerms(name, order, requiredSize, Arrays.asList(list));
        } else {
            BoundedTreeSet<InternalTerms.Bucket> ordered = new BoundedTreeSet<InternalTerms.Bucket>(order.comparator(), requiredSize);
            for (Map.Entry<HashedBytesRef, BucketCollector> entry : buckets.entrySet()) {
                ordered.add(entry.getValue().buildBucket());
            }
            return new StringTerms(name, order, requiredSize, ordered);
        }
    }

    class Collector implements Aggregator.Collector {

        final ExtTHashMap<HashedBytesRef, BucketCollector> buckets = new ExtTHashMap<HashedBytesRef, BucketCollector>();

        BytesValues values;
        AggregationContext context;

        @Override
        public void setNextContext(AggregationContext context) throws IOException {
            this.context = context;
            values = valuesSource.values();
            for (Map.Entry<HashedBytesRef, BucketCollector> entry : buckets.entrySet()) {
                entry.getValue().setNextContext(context);
            }
        }

        @Override
        public void collect(int doc) throws IOException {

            if (!values.hasValue(doc)) {
                return;
            }

            String valuesSourceKey = valuesSource.key();
            BytesRef scratch = new BytesRef();
            if (!values.isMultiValued()) {
                int hash = values.getValueHashed(doc, scratch);
                if (!context.accept(valuesSourceKey, scratch)) {
                    return;
                }
                HashedBytesRef term = new HashedBytesRef(scratch, hash);
                BucketCollector bucket = buckets.get(term);
                if (bucket == null) {
                    term.bytes = values.makeSafe(scratch);
                    bucket = new BucketCollector(term.bytes, factories, context, StringTermsAggregator.this);
                    buckets.put(term, bucket);
                }
                bucket.collect(doc);
                return;
            }

            // we'll first find all the buckets that match the values, and then propagate the document through them
            // we need to do that to avoid counting the same document more than once.
            List<BucketCollector> matchedBuckets = findMatchedBuckets(doc, valuesSourceKey, values, context);
            if (matchedBuckets != null) {
                for (int i = 0; i < matchedBuckets.size(); i++) {
                    matchedBuckets.get(i).collect(doc);
                }
            }

        }

        private List<BucketCollector> findMatchedBuckets(int doc, String valuesSourceKey, BytesValues values, AggregationContext context) throws IOException {
            List<BucketCollector> matchedBuckets = null;
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
                    bucket = new BucketCollector(term.bytes, factories, context, StringTermsAggregator.this);
                    buckets.put(term, bucket);
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
            for (Object collector : buckets.internalValues()) {
                if (collector != null) {
                    ((BucketCollector) collector).postCollection();
                }
            }
            StringTermsAggregator.this.buckets = buckets;
        }
    }

    static class BucketCollector extends BucketAggregator.BucketCollector {

        final BytesRef term;

        long docCount;

        BucketCollector(BytesRef term, List<Aggregator.Factory> factories, AggregationContext context, Aggregator aggregator) {
            super(factories, context, aggregator);
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

        StringTerms.Bucket buildBucket() {
            return new StringTerms.Bucket(term, docCount, buildAggregations(subAggregators));
        }
    }

}
