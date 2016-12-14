/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.InternalOrder;
import org.elasticsearch.search.aggregations.bucket.terms.InternalOrder.Aggregation;
import org.elasticsearch.search.aggregations.bucket.terms.InternalOrder.CompoundOrder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationPath;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public abstract class RareTermsAggregator extends BucketsAggregator {

    protected final DocValueFormat format;
    protected final TermsAggregator.BucketCountThresholds bucketCountThresholds;
    protected final Terms.Order order;
    protected final Set<Aggregator> aggsUsedForSorting = new HashSet<>();
    protected final SubAggCollectionMode collectMode;

    public RareTermsAggregator(String name, AggregatorFactories factories, SearchContext context, Aggregator parent,
                               TermsAggregator.BucketCountThresholds bucketCountThresholds, Terms.Order order, DocValueFormat format, SubAggCollectionMode collectMode,
                               List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        super(name, factories, context, parent, pipelineAggregators, metaData);
        this.bucketCountThresholds = bucketCountThresholds;
        this.order = InternalOrder.validate(order, this);
        this.format = format;
        this.collectMode = collectMode;
        // Don't defer any child agg if we are dependent on it for pruning results
        if (order instanceof Aggregation){
            AggregationPath path = ((Aggregation) order).path();
            aggsUsedForSorting.add(path.resolveTopmostAggregator(this));
        } else if (order instanceof CompoundOrder) {
            CompoundOrder compoundOrder = (CompoundOrder) order;
            for (Terms.Order orderElement : compoundOrder.orderElements()) {
                if (orderElement instanceof Aggregation) {
                    AggregationPath path = ((Aggregation) orderElement).path();
                    aggsUsedForSorting.add(path.resolveTopmostAggregator(this));
                }
            }
        }
    }

    @Override
    protected boolean shouldDefer(Aggregator aggregator) {
        return collectMode == SubAggCollectionMode.BREADTH_FIRST
            && !aggsUsedForSorting.contains(aggregator);
    }

}
