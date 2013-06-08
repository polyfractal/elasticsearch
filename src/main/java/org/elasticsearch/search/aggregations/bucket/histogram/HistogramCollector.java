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

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.bucket.BucketAggregator;
import org.elasticsearch.search.aggregations.bucket.FieldDataBucketAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.FieldDataContext;
import org.elasticsearch.search.aggregations.context.ValueTransformer;

import java.io.IOException;
import java.util.List;

/**
 * A collector that is used by the histogram aggregator which aggregates the histogram buckets
 */
public class HistogramCollector extends Aggregator.Collector {

    /**
     * A listener which is called when the aggregation of this collector finishes
     */
    public static interface Listener {

        /**
         * Called when aggregation is finished.
         *
         * @param collectors The bucket collectors that hold all the aggregated data
         */
        void onFinish(ExtTLongObjectHashMap<BucketCollector> collectors);

    }

    final FieldDataBucketAggregator aggregator;
    final FieldDataContext fieldDataContext;
    final ExtTLongObjectHashMap<BucketCollector> bucketCollectors = CacheRecycler.popLongObjectMap();
    final DoubleValues[] values;
    final ValueTransformer valueTransformer;
    final Rounding rounding;
    final Listener listener;

    Scorer scorer;
    AtomicReaderContext reader;

    /**
     * Constructs a new histogram collector.
     *
     * @param aggregator        The histogram aggregator this collector is associated with (will serve as the parent aggregator
     *                          all bucket-level sub-aggregators
     * @param fieldDataContext  The field data context for the aggregation
     * @param rounding          The rounding strategy by which the aggregation will bucket documents
     * @param valueTransformer  A value transformer which will manipulate/transform the time value before aggregated
     * @param listener          Will be called when aggregation finishes (see {@link Listener}).
     */
    public HistogramCollector(FieldDataBucketAggregator aggregator, FieldDataContext fieldDataContext, Rounding rounding, ValueTransformer valueTransformer, Listener listener) {
        this.aggregator = aggregator;
        this.fieldDataContext = fieldDataContext;
        this.values = new DoubleValues[fieldDataContext.fieldCount()];
        this.rounding = rounding;
        this.valueTransformer = valueTransformer;
        this.listener = listener;
    }

    @Override
    public void collect(int doc, AggregationContext context) throws IOException {

        if (values.length == 1) {

            DoubleValues vals = values[0];

            if (!vals.hasValue(doc)) {
                return;
            }

            if (!vals.isMultiValued()) {

                // optimization for a single valued field when aggregating a single field. In this case,
                // there's no need to mark buckets as a match on a bucket will always be a single match per doc
                // so we can just collect it

                double value = vals.getValue(doc);
                if (!context.accept(fieldDataContext.field(0), value)) {
                    return;
                }
                long key = rounding.round((long) valueTransformer.transform(value));
                BucketCollector bucketCollector = bucketCollectors.get(key);
                if (bucketCollector == null) {
                    bucketCollector = new BucketCollector(key, rounding, aggregator, scorer, reader, fieldDataContext);
                    bucketCollectors.put(key, bucketCollector);
                }
                bucketCollector.collect(doc, context);
                return;

            } else {

                // it's a multi-valued field, meaning, some values of the field may fit the bucket, while other
                // don't. thus, we need to iterate on all values and mark the bucket that they fall in (or
                // create new buckets if needed). Only after this "mark" phase ends, we can iterate over all the buckets
                // and aggregate only those that are marked (and while at it, clear the mark, making it ready for
                // the next aggregation).

                markBuckets(doc, fieldDataContext.field(0), vals, context);

            }

        } else {

            // we're aggregating across multiple fields, so we need to mark the matching buckets first
            // (same approach we take when aggregating on a single field which is multi-valued)

            for (int i = 0; i < values.length; i++) {
                markBuckets(doc, fieldDataContext.field(i), values[i], context);
            }

        }

        for (Object collector : bucketCollectors.internalValues()) {
            if (collector != null) {
                if (((BucketCollector) collector).docMatched) {
                    ((BucketCollector) collector).docMatched = false;
                    ((BucketCollector) collector).collect(doc, context);
                }
            }
        }
    }

    // marking all the buckets (and creating new buckets if needed) that match the given doc
    // based on the values of the given field. This method only *marks* the buckets, after this method
    // is called we'll go over the buckets and only collect the marked ones. We need to do this to avoid
    // situations where multiple values in a single field or multiple values across the aggregated fields
    // match the bucket and then the bucket will collect the same document multiple times.
    private void markBuckets(int doc, String field, DoubleValues values, AggregationContext context) {

        if (!values.isMultiValued()) {
            double value = valueTransformer.transform(values.getValue(doc));
            if (!context.accept(field, value)) {
                return;
            }
            long key = rounding.round((long) value);
            BucketCollector bucket = bucketCollectors.get(key);
            if (bucket == null) {
                bucket = new BucketCollector(key, rounding, aggregator, scorer, reader, fieldDataContext);
                bucketCollectors.put(key, bucket);
            }
            bucket.docMatched = true;
            return;
        }

        for (DoubleValues.Iter iter = values.getIter(doc); iter.hasNext();) {
            double value = valueTransformer.transform(iter.next());
            if (!context.accept(field, value)) {
                continue;
            }
            long key = rounding.round((long) value);
            BucketCollector bucket = bucketCollectors.get(key);
            if (bucket == null) {
                bucket = new BucketCollector(key, rounding, aggregator, scorer, reader, fieldDataContext);
                bucketCollectors.put(key, bucket);
            }
            bucket.docMatched = true;
        }
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        this.scorer = scorer;
        valueTransformer.setScorer(scorer);
        for (Object collector : bucketCollectors.internalValues()) {
            if (collector != null) {
                ((BucketCollector) collector).setScorer(scorer);
            }
        }
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) throws IOException {
        this.reader = reader;
        fieldDataContext.loadDoubleValues(reader, values);
        valueTransformer.setNextReader(reader);
        for (Object collector : bucketCollectors.internalValues()) {
            if (collector != null) {
                ((BucketCollector) collector).setNextReader(reader);
            }
        }
    }

    @Override
    public void postCollection() {
        for (Object collector : bucketCollectors.internalValues()) {
            if (collector != null) {
                ((BucketCollector) collector).postCollection();
            }
        }
        listener.onFinish(bucketCollectors);
    }


    /**
     * A collector for a histogram bucket. This collector counts the number of documents that fall into it,
     * but also serves as the aggregation context for all the sub aggregations it contains.
     */
    public static class BucketCollector extends BucketAggregator.BucketCollector implements AggregationContext {

        public final long key;
        final Rounding rounding;

        private LongValues values;
        public long docCount;
        public List<Aggregator> aggregators;

        // this enables us to mark the bucket as matching for a specific document (see explanation above)
        boolean docMatched = false;

        BucketCollector(long key, Rounding rounding, BucketAggregator parent, Scorer scorer, AtomicReaderContext reader, AggregationContext context) {
            context.
            super(parent, scorer, values.isMultiValued() ? this : );
            this.fieldDataContext = fieldDataContext;
            this.key = key;
            this.rounding = rounding;
        }

        @Override
        protected AggregationContext onDoc(int doc, AggregationContext context) throws IOException {
            currentParentContext = context;
            docCount++;
            return this;
        }

        public long docCount() {
            return docCount;
        }

        public List<Aggregator> aggregators() {
            return aggregators;
        }

        @Override
        protected void postCollection(List<Aggregator> aggregators) {
            this.aggregators = aggregators;
        }

        @Override
        public boolean accept(String field, double value) {
            if (!currentParentContext.accept(field, value)) {
                return false;
            }
            if (!fieldDataContext.hasField(field)) {
                return true;
            }
            return this.key == rounding.round((long) value);
        }

        @Override
        public boolean accept(String field, long value) {
            if (!currentParentContext.accept(field, value)) {
                return false;
            }
            if (!fieldDataContext.hasField(field)) {
                return true;
            }
            return this.key == rounding.round(value);
        }

        @Override
        public boolean accept(String field, BytesRef value) {
            return currentParentContext.accept(field, value);
        }

        @Override
        public boolean accept(String field, GeoPoint value) {
            return currentParentContext.accept(field, value);
        }
    }

}
