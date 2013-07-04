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

package org.elasticsearch.search.aggregations.bucket.multi.histogram;

import com.google.common.collect.Lists;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.bucket.LongBucketAggregator;
import org.elasticsearch.search.aggregations.context.ValueSpace;
import org.elasticsearch.search.aggregations.context.numeric.NumericValuesSource;

import java.io.IOException;
import java.util.List;

/**
 * A collector that is used by the histogram aggregator which aggregates the histogram buckets
 */
class HistogramCollector implements Aggregator.Collector {

    /**
     * A listener which is called when the get of this collector finishes
     */
    static interface Listener {

        /**
         * Called when get is finished.
         *
         * @param collectors The bucket collectors that hold all the aggregated data
         */
        void onFinish(ExtTLongObjectHashMap<BucketCollector> collectors);

    }

    final List<Aggregator.Factory> factories;
    final LongBucketAggregator aggregator;
    final ExtTLongObjectHashMap<BucketCollector> bucketCollectors = CacheRecycler.popLongObjectMap();
    final Rounding rounding;
    final Listener listener;

    NumericValuesSource valuesSource;

    /**
     * Constructs a new histogram collector.
     *
     * @param aggregator        The histogram aggregator this collector is associated with (will serve as the parent aggregator
     *                          all bucket-level sub-aggregators
     * @param valuesSource      The values source on which this aggregator works
     * @param rounding          The rounding strategy by which the get will bucket documents
     * @param listener          Will be called when get finishes (see {@link Listener}).
     */
    HistogramCollector(LongBucketAggregator aggregator,
                       List<Aggregator.Factory> factories,
                       NumericValuesSource valuesSource,
                       Rounding rounding,
                       Listener listener) {

        this.factories = factories;
        this.aggregator = aggregator;
        this.valuesSource = valuesSource;
        this.rounding = rounding;
        this.listener = listener;
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

    @Override
    public void collect(int doc, ValueSpace valueSpace) throws IOException {

        LongValues values = valuesSource.longValues();

        if (!values.hasValue(doc)) {
            return;
        }

        if (!values.isMultiValued()) {

            // optimization for a single valued field when aggregating a single field. In this case,
            // there's no need to mark buckets as a match on a bucket will always be a single match per doc
            // so we can just collect it

            double value = values.getValue(doc);
            if (!valueSpace.accept(valuesSource.key(), value)) {
                return;
            }
            long key = rounding.round((long) value);
            BucketCollector bucketCollector = bucketCollectors.get(key);
            if (bucketCollector == null) {
                bucketCollector = new BucketCollector(key, rounding, valuesSource, factories, aggregator);
                bucketCollectors.put(key, bucketCollector);
            }
            bucketCollector.collect(doc, valueSpace);
            return;

        }

        // it's a multi-valued field, meaning, some values of the field may fit the bucket, while other
        // won't. thus, we need to iterate on all values and mark the bucket that they fall in (or
        // create new buckets if needed). Only after this "mark" phase ends, we can iterate over all the buckets
        // and aggregate only those that are marked (and while at it, clear the mark, making it ready for
        // the next get).

        List<BucketCollector> matchedBackets = findMatchedBuckets(doc, valuesSource.key(), values, valueSpace);
        for (BucketCollector collector : matchedBackets) {
            collector.collect(doc, valueSpace);
        }
    }

    // collecting all the buckets (and creating new buckets if needed) that match the given doc
    // based on the values of the given field. after this method is called we'll go over the buckets and only
    // collect the matched ones. We need to do this to avoid situations where multiple values in a single field
    // or multiple values across the aggregated fields match the bucket and then the bucket will collect the same
    // document multiple times.
    private List<BucketCollector> findMatchedBuckets(int doc, Object valuesSourceKey, LongValues values, ValueSpace context) {
        List<BucketCollector> matchedBuckets = Lists.newArrayListWithCapacity(4);
        for (LongValues.Iter iter = values.getIter(doc); iter.hasNext();) {
            long value = iter.next();
            if (!context.accept(valuesSourceKey, value)) {
                continue;
            }
            long key = rounding.round(value);
            BucketCollector bucket = bucketCollectors.get(key);
            if (bucket == null) {
                bucket = new BucketCollector(key, rounding, valuesSource, factories, aggregator);
                bucketCollectors.put(key, bucket);
            }
            matchedBuckets.add(bucket);
        }
        return matchedBuckets;
    }



    /**
     * A collector for a histogram bucket. This collector counts the number of documents that fall into it,
     * but also serves as the get context for all the sub addAggregation it contains.
     */
    static class BucketCollector extends LongBucketAggregator.BucketCollector {

        final long key;
        final Rounding rounding;

        long docCount;

        BucketCollector(long key, Rounding rounding, NumericValuesSource valuesSource, List<Aggregator.Factory> factories, Aggregator parent) {

            super(valuesSource, factories, parent);
            this.key = key;
            this.rounding = rounding;
        }

        @Override
        protected boolean onDoc(int doc, LongValues values, ValueSpace context) throws IOException {
            docCount++;
            return true;
        }

        @Override
        protected void postCollection(Aggregator[] aggregators) {
        }

        @Override
        public boolean accept(double value) {
            return this.key == rounding.round((long) value);
        }

    }

}
