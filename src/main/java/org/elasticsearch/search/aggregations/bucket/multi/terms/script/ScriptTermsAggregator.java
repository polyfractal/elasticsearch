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

package org.elasticsearch.search.aggregations.bucket.multi.terms.script;

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
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.single.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.bucket.multi.terms.BucketPriorityQueue;
import org.elasticsearch.search.aggregations.bucket.multi.terms.InternalTerms;
import org.elasticsearch.search.aggregations.bucket.multi.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.multi.terms.string.StringTerms;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.facet.terms.support.EntryPriorityQueue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class ScriptTermsAggregator extends SingleBucketAggregator {

    private final Terms.Order order;
    private final int requiredSize;
    private final SearchScript script;

    ExtTHashMap<HashedBytesRef, BucketCollector> buckets;

    public ScriptTermsAggregator(String name, List<Aggregator.Factory> factories, Terms.Order order,
                                 int requiredSize, SearchScript script, Aggregator parent) {
        super(name, factories, parent);
        this.order = order;
        this.requiredSize = requiredSize;
        this.script = script;
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

    class Collector extends Aggregator.Collector {

        final ExtTHashMap<HashedBytesRef, BucketCollector> buckets = new ExtTHashMap<HashedBytesRef, BucketCollector>();
        Scorer scorer;
        AtomicReaderContext readerContext;

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            this.scorer = scorer;
            script.setScorer(scorer);
            for (Map.Entry<HashedBytesRef, BucketCollector> entry : buckets.entrySet()) {
                entry.getValue().setScorer(scorer);
            }
        }

        @Override
        public void collect(int doc, AggregationContext context) throws IOException {
            script.setNextDocId(doc);
            Object o = script.run();
            if (o == null) {
                return;
            }
            if (o instanceof Iterable) {
                for (Object val : (Iterable) o) {
                    String strVal = val.toString();
                    onValue(doc, new BytesRef(strVal), context);
                }
                return;
            }

            if (o instanceof Object[]) {
                for (Object val : (Object[]) o) {
                    String strVal = val.toString();
                    onValue(doc, new BytesRef(strVal), context);
                }
                return;
            }

            onValue(doc, new BytesRef(o.toString()), context);
        }

        private void onValue(int doc, BytesRef value, AggregationContext context) throws IOException {
            HashedBytesRef term = new HashedBytesRef(value);
            BucketCollector bucket = buckets.get(term);
            if (bucket == null) {
                bucket = new BucketCollector(term, ScriptTermsAggregator.this, scorer, readerContext);
                buckets.put(term, bucket);
            }
            bucket.collect(doc, context);
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            this.readerContext = context;
            script.setNextReader(context);
            for (Map.Entry<HashedBytesRef, BucketCollector> entry : buckets.entrySet()) {
                entry.getValue().setNextReader(context);
            }
        }

        @Override
        public void postCollection() {
            for (Map.Entry<HashedBytesRef, BucketCollector> entry : buckets.entrySet()) {
                entry.getValue().postCollection();
            }
            ScriptTermsAggregator.this.buckets = buckets;
        }
    }

    static class BucketCollector extends SingleBucketAggregator.BucketCollector {

        Text term;
        long docCount;

        BucketCollector(HashedBytesRef term, ScriptTermsAggregator parent, Scorer scorer, AtomicReaderContext context) {
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

    public static class Factory extends SingleBucketAggregator.Factory<ScriptTermsAggregator, Factory> {

        private final Terms.Order order;
        private final int requiredSize;
        private final SearchScript script;

        public Factory(String name, Terms.Order order, int requiredSize, SearchScript script) {
            super(name);
            this.order = order;
            this.requiredSize = requiredSize;
            this.script = script;
        }

        @Override
        public ScriptTermsAggregator create(Aggregator parent) {
            return new ScriptTermsAggregator(name, factories, order, requiredSize, script, parent);
        }
    }

}
