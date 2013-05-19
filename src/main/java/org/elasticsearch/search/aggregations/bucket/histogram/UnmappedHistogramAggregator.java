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

import com.google.common.collect.Lists;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.BucketAggregator;

import java.util.List;

/**
 * A histogram aggregator knows how to "aggregate" unmapped fields
 */
public class UnmappedHistogramAggregator extends BucketAggregator {

    private final InternalOrder order;
    private final boolean keyed;

    public UnmappedHistogramAggregator(String name, InternalOrder order, boolean keyed, Aggregator parent) {
        super(name, null, parent);
        this.order = order;
        this.keyed = keyed;
    }

    @Override
    public Collector collector() {
        return null;
    }

    @Override
    public InternalAggregation buildAggregation() {
        List<Histogram.Bucket> buckets = Lists.newArrayListWithCapacity(0);
        return new InternalHistogram(name, buckets, order, keyed);
    }

    public static class Factory extends BucketAggregator.Factory<UnmappedHistogramAggregator, Factory> {

        private final InternalOrder order;
        private final boolean keyed;

        public Factory(String name, InternalOrder order, boolean keyed) {
            super(name);
            this.order = order;
            this.keyed = keyed;
        }

        @Override
        public UnmappedHistogramAggregator create(Aggregator parent) {
            return new UnmappedHistogramAggregator(name, order, keyed, parent);
        }
    }

}
