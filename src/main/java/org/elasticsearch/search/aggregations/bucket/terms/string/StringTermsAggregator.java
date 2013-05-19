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

package org.elasticsearch.search.aggregations.bucket.terms.string;

import com.google.common.collect.ImmutableList;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.common.text.BytesText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.trove.ExtTHashMap;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.BucketAggregator;
import org.elasticsearch.search.aggregations.bucket.FieldDataBucketAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.BucketPriorityQueue;
import org.elasticsearch.search.aggregations.bucket.terms.InternalTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.FieldDataContext;
import org.elasticsearch.search.aggregations.context.ValueTransformer;
import org.elasticsearch.search.facet.terms.support.EntryPriorityQueue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class StringTermsAggregator extends FieldDataBucketAggregator {

    private final Terms.Order order;
    private final int requiredSize;

    ExtTHashMap<HashedBytesRef, BucketCollector> buckets;

    public StringTermsAggregator(String name, List<Aggregator.Factory> factories, FieldDataContext fieldDataContext,
                                 ValueTransformer valueTransformer, Terms.Order order, int requiredSize, Aggregator parent) {
        super(name, factories, fieldDataContext, valueTransformer, parent, false);
        this.order = order;
        this.requiredSize = requiredSize;
    }

    @Override
    public Collector collector() {
        return new Collector(fieldDataContext);
    }

    @Override
    public StringTerms buildAggregation() {

        if (buckets.isEmpty()) {
            return new StringTerms(name, order, requiredSize, ImmutableList.<InternalTerms.Bucket>of());
        }

        if (requiredSize < EntryPriorityQueue.LIMIT) {
            BucketPriorityQueue ordered = new BucketPriorityQueue(requiredSize, order.comparator());
            for (Map.Entry<HashedBytesRef, BucketCollector> entry : buckets.entrySet()) {
                ordered.add(entry.getValue().buildBucket());
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

    class Collector extends Aggregator.Collector {

        final ExtTHashMap<HashedBytesRef, BucketCollector> buckets = new ExtTHashMap<HashedBytesRef, BucketCollector>();
        final FieldDataContext fieldDataContext;
        final BytesValues[] values;
        Scorer scorer;
        AtomicReaderContext readerContext;

        Collector(FieldDataContext fieldDataContext) {
            this.fieldDataContext = fieldDataContext;
            this.values = new BytesValues[fieldDataContext.indexFieldDatas().length];
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            this.scorer = scorer;
            valueTransformer.setScorer(scorer);
            for (Map.Entry<HashedBytesRef, BucketCollector> entry : buckets.entrySet()) {
                entry.getValue().setScorer(scorer);
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

        private void onDoc(int doc, String field, BytesValues values, AggregationContext context) throws IOException {
            if (!values.hasValue(doc)) {
                return;
            }

            if (!values.isMultiValued()) {
                BytesRef value = values.getValue(doc);
                if (!context.accept(field, value)) {
                    return;
                }
                onValue(doc, valueTransformer.transform(value), context);
                return;
            }

            for (BytesValues.Iter iter = values.getIter(doc); iter.hasNext();) {
                BytesRef value = iter.next();
                if (!context.accept(field, value)) {
                    continue;
                }
                onValue(doc, valueTransformer.transform(value), context);
            }
        }

        private void onValue(int doc, BytesRef value, AggregationContext context) throws IOException {
            HashedBytesRef term = new HashedBytesRef(value);
            BucketCollector bucket = buckets.get(term);
            if (bucket == null) {
                bucket = new BucketCollector(term, StringTermsAggregator.this, scorer, readerContext);
                buckets.put(term, bucket);
            }
            bucket.collect(doc, context);
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            this.readerContext = context;
            valueTransformer.setNextReader(context);
            fieldDataContext.loadBytesValues(context, values);
            for (Map.Entry<HashedBytesRef, BucketCollector> entry : buckets.entrySet()) {
                entry.getValue().setNextReader(context);
            }
        }

        @Override
        public void postCollection() {
            for (Map.Entry<HashedBytesRef, BucketCollector> entry : buckets.entrySet()) {
                entry.getValue().postCollection();
            }
            StringTermsAggregator.this.buckets = buckets;
        }
    }

    static class BucketCollector extends BucketAggregator.BucketCollector {

        Text term;
        long docCount;

        BucketCollector(HashedBytesRef term, StringTermsAggregator parent, Scorer scorer, AtomicReaderContext context) {
            super(parent, scorer, context);
            this.term = new BytesText(new BytesArray(term.bytes));
        }

        @Override
        protected void postCollection(List<Aggregator> aggregators) {
        }

        protected AggregationContext onDoc(int docId, AggregationContext context) throws IOException {
            docCount++;
            return context;
        }

        StringTerms.Bucket buildBucket() {
            List<InternalAggregation> aggregations = new ArrayList<InternalAggregation>(aggregators.size());
            for (Aggregator aggregator : aggregators) {
                aggregations.add(aggregator.buildAggregation());
            }
            return new StringTerms.Bucket(term, docCount, aggregations);
        }
    }

}
