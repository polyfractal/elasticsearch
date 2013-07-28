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

package org.elasticsearch.search.aggregations.bucket.single;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;

import java.util.List;

/**
 * A bucket aggregator that creates a single bucket
 */
public abstract class SingleBucketAggregator extends BucketsAggregator {

    // since we only have one bucket we can eagerly initialize the sub aggregators

    private final Aggregator[] subAggregators;

    protected SingleBucketAggregator(String name, List<Aggregator.Factory> factories,
                                     AggregationContext aggregationContext, Aggregator parent) {
        super(name, aggregationContext, parent);
        subAggregators = createSubAggregators(factories, this);
    }

    @Override
    public final Collector collector() {
        return collector(subAggregators);
    }

    protected abstract Collector collector(Aggregator[] aggregators);

    @Override
    public final InternalAggregation buildAggregation() {
        return buildAggregation(buildAggregations(subAggregators));
    }

    protected abstract InternalAggregation buildAggregation(InternalAggregations aggregations);

}
