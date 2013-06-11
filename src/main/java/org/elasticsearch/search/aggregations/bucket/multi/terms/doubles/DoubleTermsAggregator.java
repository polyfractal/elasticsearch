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

package org.elasticsearch.search.aggregations.bucket.multi.terms.doubles;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.common.trove.ExtTDoubleObjectHashMap;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.bucket.BucketAggregator;
import org.elasticsearch.search.aggregations.bucket.DoubleBucketAggregator;
import org.elasticsearch.search.aggregations.bucket.multi.terms.BucketPriorityQueue;
import org.elasticsearch.search.aggregations.bucket.multi.terms.InternalTerms;
import org.elasticsearch.search.aggregations.bucket.multi.terms.Terms;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.doubles.DoubleValuesSource;
import org.elasticsearch.search.facet.terms.support.EntryPriorityQueue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.search.aggregations.bucket.BucketAggregator.buildAggregations;

/**
 *
 */
public class DoubleTermsAggregator extends DoubleBucketAggregator {

    private final List<Aggregator.Factory> factories;
    private final Terms.Order order;
    private final int requiredSize;

    ExtTDoubleObjectHashMap<BucketCollector> bucketCollectors;

    public DoubleTermsAggregator(String name, List<Aggregator.Factory> factories, DoubleValuesSource valuesSource,
                                 Terms.Order order, int requiredSize, Aggregator parent) {
        super(name, valuesSource, parent);
        this.factories = factories;
        this.order = order;
        this.requiredSize = requiredSize;
    }

    @Override
    public Collector collector() {
        return new Collector();
    }

    @Override
    public DoubleTerms buildAggregation() {

        if (bucketCollectors.isEmpty()) {
            return new DoubleTerms(name, order, requiredSize, ImmutableList.<InternalTerms.Bucket>of());
        }

        if (requiredSize < EntryPriorityQueue.LIMIT) {
            BucketPriorityQueue ordered = new BucketPriorityQueue(requiredSize, order.comparator());
            for (Object bucketCollector : bucketCollectors.internalValues()) {
                if (bucketCollector != null) {
                    ordered.insertWithOverflow(((BucketCollector) bucketCollector).buildBucket());
                }
            }
            InternalTerms.Bucket[] list = new InternalTerms.Bucket[ordered.size()];
            for (int i = ordered.size() - 1; i >= 0; i--) {
                list[i] = (DoubleTerms.Bucket) ordered.pop();
            }
            return new DoubleTerms(name, order, requiredSize, Arrays.asList(list));
        } else {
            BoundedTreeSet<InternalTerms.Bucket> ordered = new BoundedTreeSet<InternalTerms.Bucket>(order.comparator(), requiredSize);
            for (Object bucketCollector : bucketCollectors.internalValues()) {
                if (bucketCollector != null) {
                    ordered.add(((BucketCollector) bucketCollector).buildBucket());
                }
            }
            return new DoubleTerms(name, order, requiredSize, ordered);
        }
    }

    class Collector implements Aggregator.Collector {

        final ExtTDoubleObjectHashMap<BucketCollector> bucketCollectors = new ExtTDoubleObjectHashMap<BucketCollector>();

        DoubleValues values;
        Scorer scorer;
        AtomicReaderContext reader;
        AggregationContext context;

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            this.scorer = scorer;
            valuesSource.setNextScorer(scorer);
            for (Object bucketCollector : bucketCollectors.internalValues()) {
                if (bucketCollector != null) {
                    ((BucketCollector) bucketCollector).setScorer(scorer);
                }
            }
        }

        @Override
        public void setNextReader(AtomicReaderContext reader, AggregationContext context) throws IOException {
            this.reader = reader;
            this.context = context;
            valuesSource.setNextReader(reader);
            values = valuesSource.values();
            for (Object bucketCollector : bucketCollectors.internalValues()) {
                if (bucketCollector != null) {
                    ((BucketCollector) bucketCollector).setNextReader(reader, context);
                }
            }
        }

        @Override
        public void collect(int doc) throws IOException {

            if (!values.hasValue(doc)) {
                return;
            }

            String valuesSourceKey = valuesSource.key();
            if (!values.isMultiValued()) {
                double term = values.getValue(doc);
                if (!context.accept(doc, valuesSourceKey, term)) {
                    return;
                }
                BucketCollector bucket = bucketCollectors.get(term);
                if (bucket == null) {
                    bucket = new BucketCollector(term, reader, scorer, context, DoubleTermsAggregator.this);
                    bucketCollectors.put(term, bucket);
                }
                bucket.collect(doc);
                return;
            }

            List<BucketCollector> matchedBuckets = findMatchedBuckets(doc, valuesSourceKey, values);
            if (matchedBuckets != null) {
                for (int i = 0; i < matchedBuckets.size(); i++) {
                    matchedBuckets.get(i).collect(doc);
                }
            }

        }

        private List<BucketCollector> findMatchedBuckets(int doc, String valuesSourceKey, DoubleValues values) {
            List<BucketCollector> matchedBuckets = null;
            for (DoubleValues.Iter iter = values.getIter(doc); iter.hasNext();) {
                double term = iter.next();
                if (!context.accept(doc, valuesSourceKey, term)) {
                    continue;
                }
                BucketCollector bucket = bucketCollectors.get(term);
                if (bucket == null) {
                    bucket = new BucketCollector(term, reader, scorer, context, DoubleTermsAggregator.this);
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
            for (Object bucketCollector : bucketCollectors.internalValues()) {
                if (bucketCollector != null) {
                    ((BucketCollector) bucketCollector).postCollection();
                }
            }
            DoubleTermsAggregator.this.bucketCollectors = bucketCollectors;
        }
    }

    static class BucketCollector extends BucketAggregator.BucketCollector {

        final double term;

        long docCount;

        BucketCollector(double term, AtomicReaderContext reader, Scorer scorer, AggregationContext context, DoubleTermsAggregator parent) {
            super(parent.name, parent.factories, reader, scorer, context, parent);
            this.term = term;
        }

        @Override
        protected AggregationContext setReaderAngGetContext(AtomicReaderContext reader, AggregationContext context) throws IOException {
            return context;
        }

        @Override
        protected boolean onDoc(int doc) throws IOException {
            docCount++;
            return true;
        }

        @Override
        protected void postCollection(Aggregator[] aggregators) {
        }

        DoubleTerms.Bucket buildBucket() {
            return new DoubleTerms.Bucket(term, docCount, buildAggregations(aggregators));
        }
    }

}
