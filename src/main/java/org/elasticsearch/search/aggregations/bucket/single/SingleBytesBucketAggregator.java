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
import org.elasticsearch.search.aggregations.bucket.BucketAggregator;
import org.elasticsearch.search.aggregations.bucket.BytesBucketAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.bytes.BytesValuesSource;

import java.util.List;

/**
 *
 */
public abstract class SingleBytesBucketAggregator extends BytesBucketAggregator {

    private final Aggregator[] subAggregators;

    public SingleBytesBucketAggregator(String name, List<Aggregator.Factory> factories, BytesValuesSource valuesSource,
                                       AggregationContext aggregationContext, Aggregator parent) {
        super(name, valuesSource, aggregationContext, parent);
        subAggregators = BucketAggregator.createSubAggregators(factories, this);
    }

    @Override
    public final Collector collector() {
        return collector(subAggregators);
    }

    protected abstract Collector collector(Aggregator[] aggregators);

    @Override
    public final InternalAggregation buildAggregation() {
        return buildAggregation(BucketAggregator.buildAggregations(subAggregators));
    }

    protected abstract InternalAggregation buildAggregation(InternalAggregations aggregations);

}
