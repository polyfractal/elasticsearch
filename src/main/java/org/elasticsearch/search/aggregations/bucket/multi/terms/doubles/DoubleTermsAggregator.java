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
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.common.trove.ExtTDoubleObjectHashMap;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.single.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.bucket.FieldDataBucketAggregator;
import org.elasticsearch.search.aggregations.bucket.multi.terms.BucketPriorityQueue;
import org.elasticsearch.search.aggregations.bucket.multi.terms.InternalTerms;
import org.elasticsearch.search.aggregations.bucket.multi.terms.Terms;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.FieldDataContext;
import org.elasticsearch.search.aggregations.context.ValueTransformer;
import org.elasticsearch.search.aggregations.format.ValueFormatter;
import org.elasticsearch.search.facet.terms.support.EntryPriorityQueue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class DoubleTermsAggregator extends FieldDataBucketAggregator {

    private final Terms.Order order;
    private final int requiredSize;
    private final ValueFormatter valueFormatter;

    ExtTDoubleObjectHashMap<BucketCollector> bucketCollectors;

    public DoubleTermsAggregator(String name, List<Aggregator.Factory> factories, FieldDataContext fieldDataContext,
                                 ValueTransformer valueTransformer, ValueFormatter valueFormatter, Terms.Order order, int requiredSize, Aggregator parent) {
        super(name, factories, fieldDataContext, valueTransformer, parent, IndexNumericFieldData.class);
        this.order = order;
        this.requiredSize = requiredSize;
        this.valueFormatter = valueFormatter;
    }

    @Override
    public Collector collector() {
        return new Collector(fieldDataContext);
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

    class Collector extends SingleBucketAggregator.Collector {

        final ExtTDoubleObjectHashMap<BucketCollector> bucketCollectors = new ExtTDoubleObjectHashMap<BucketCollector>();
        final FieldDataContext fieldDataContext;
        final DoubleValues[] values;
        Scorer scorer;
        AtomicReaderContext readerContext;

        Collector(FieldDataContext fieldDataContext) {
            this.fieldDataContext = fieldDataContext;
            this.values = new DoubleValues[fieldDataContext.indexFieldDatas().length];
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            this.scorer = scorer;
            valueTransformer.setScorer(scorer);
            for (Object bucketCollector : bucketCollectors.internalValues()) {
                if (bucketCollector != null) {
                    ((BucketCollector) bucketCollector).setScorer(scorer);
                }
            }
        }

        @Override
        public void collect(int doc, AggregationContext context) throws IOException {
            valueTransformer.setNextDocId(doc);
            if (values.length == 1) {
                onDoc(doc, fieldDataContext.fields()[0], values[0], context);
            } else {
                for (int i = 0; i < values.length; i++) {
                    onDoc(doc, fieldDataContext.fields()[i], values[i], context);
                }
            }
        }

        private void onDoc(int doc, String field, DoubleValues values, AggregationContext context) throws IOException {
            if (!values.hasValue(doc)) {
                return;
            }

            if (!values.isMultiValued()) {
                double term = valueTransformer.transform(values.getValue(doc));
                if (!context.accept(field, term)) {
                    return;
                }
                BucketCollector bucket = bucketCollectors.get(term);
                if (bucket == null) {
                    bucket = new BucketCollector(term, 0, DoubleTermsAggregator.this, scorer, readerContext);
                    bucketCollectors.put(term, bucket);
                }
                bucket.collect(doc, context);
                return;
            }

            for (DoubleValues.Iter iter = values.getIter(doc); iter.hasNext();) {
                double term = valueTransformer.transform(iter.next());
                if (!context.accept(field, term)) {
                    continue;
                }
                BucketCollector bucket = bucketCollectors.get(term);
                if (bucket == null) {
                    bucket = new BucketCollector(term, 0, DoubleTermsAggregator.this, scorer, readerContext);
                    bucketCollectors.put(term, bucket);
                }
                bucket.collect(doc, context);
            }
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            this.readerContext = context;
            valueTransformer.setNextReader(context);
            fieldDataContext.loadDoubleValues(context, values);
            for (Object bucketCollector : bucketCollectors.internalValues()) {
                if (bucketCollector != null) {
                    ((BucketCollector) bucketCollector).setNextReader(context);
                }
            }
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

    static class BucketCollector extends SingleBucketAggregator.BucketCollector {

        double term;
        long count;

        BucketCollector(double term, long count, DoubleTermsAggregator parent, Scorer scorer, AtomicReaderContext context) {
            super(parent, scorer, context);
            this.term = term;
            this.count = count;
        }

        @Override
        protected void postCollection(List<Aggregator> aggregators) {
        }

        protected AggregationContext onDoc(int docId, AggregationContext context) throws IOException {
            count++;
            return context;
        }

        DoubleTerms.Bucket buildBucket() {
            List<InternalAggregation> aggregations = new ArrayList<InternalAggregation>(aggregators.size());
            for (Aggregator aggregator : aggregators) {
                aggregations.add(aggregator.buildAggregation());
            }
            return new DoubleTerms.Bucket(term, count, aggregations);
        }
    }

}
