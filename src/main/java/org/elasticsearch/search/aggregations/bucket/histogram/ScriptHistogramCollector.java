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
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.bucket.BucketAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;

import java.io.IOException;
import java.util.List;

/**
*
*/
public class ScriptHistogramCollector extends Aggregator.Collector {

    public static interface Listener {

        void onFinish(ExtTLongObjectHashMap<BucketCollector> collectors);

    }

    private final BucketAggregator aggregator;
    private final ExtTLongObjectHashMap<BucketCollector> bucketCollectors = CacheRecycler.popLongObjectMap();
    private final SearchScript script;
    private final Rounding rounding;
    private final Listener listener;

    Scorer scorer;
    AtomicReaderContext reader;

    public ScriptHistogramCollector(BucketAggregator aggregator, SearchScript script, Rounding rounding, Listener listener) {
        this.aggregator = aggregator;
        this.script = script;
        this.rounding = rounding;
        this.listener = listener;
    }

    @Override
    public void collect(int doc, AggregationContext context) throws IOException {
        script.setNextDocId(doc);
        long value = script.runAsLong();
        long key = rounding.round(value);
        BucketCollector collector = bucketCollectors.get(key);
        if (collector == null) {
            collector = new BucketCollector(aggregator, scorer, reader, key);
            bucketCollectors.put(key, collector);
        }
        collector.collect(doc, context);
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        this.scorer = scorer;
        script.setScorer(scorer);
        for (Object collector : bucketCollectors.internalValues()) {
            if (collector != null) {
                ((ScriptHistogramAggregator.BucketCollector) collector).setScorer(scorer);
            }
        }
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) throws IOException {
        this.reader = reader;
        script.setNextReader(reader);
        for (Object collector : bucketCollectors.internalValues()) {
            if (collector != null) {
                ((ScriptHistogramAggregator.BucketCollector) collector).setNextReader(reader);
            }
        }
    }

    @Override
    public void postCollection() {
        for (Object collector : bucketCollectors.internalValues()) {
            if (collector != null) {
                ((ScriptHistogramAggregator.BucketCollector) collector).postCollection();
            }
        }
        listener.onFinish(bucketCollectors);
    }

    public static class BucketCollector extends BucketAggregator.BucketCollector {

        public final long key;
        public long docCount;
        public List<Aggregator> aggregators;

        BucketCollector(BucketAggregator parent, Scorer scorer, AtomicReaderContext context, long key) {
            super(parent, scorer, context);
            this.key = key;
        }

        @Override
        protected AggregationContext onDoc(int doc, AggregationContext context) throws IOException {
            docCount++;
            return context;
        }

        @Override
        protected void postCollection(List<Aggregator> aggregators) {
            this.aggregators = aggregators;
        }
    }
}
