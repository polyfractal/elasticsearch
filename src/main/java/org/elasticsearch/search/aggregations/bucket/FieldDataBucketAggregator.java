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

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.context.FieldDataBased;
import org.elasticsearch.search.aggregations.context.FieldDataContext;
import org.elasticsearch.search.aggregations.context.ValueTransformer;

import java.util.List;

/**
 * A base class for all field data based bucket aggregators
 */
@SuppressWarnings("unchecked")
public abstract class FieldDataBucketAggregator extends BucketAggregator implements FieldDataBased {

    protected FieldDataContext fieldDataContext;
    protected ValueTransformer valueTransformer;

    @Override
    public FieldDataContext fieldDataContext() {
        return fieldDataContext;
    }

    public ValueTransformer valueTransformer() {
        return valueTransformer;
    }

    public FieldDataBucketAggregator(String name, List<Aggregator.Factory> factories, FieldDataContext fieldDataContext, Aggregator parent, Class<? extends IndexFieldData> requiredType) {
        this(name, factories, fieldDataContext, ValueTransformer.NONE, parent, requiredType);
    }

    /**
     * Constructs a new field data bucket aggregator.
     *
     * @param name                      The name of the aggregation.
     * @param factories                 The aggregator factories for the buckets
     * @param fieldDataContext          The field data context. Can be {@code null}, indicating that the user didn't specify the field
     *                                  context, in which case, we will attempt to pick it up from the closest ancestor aggregator.
     * @param parent                    The parent aggregator
     * @param requiredType              Indicates the type of the index field data that this aggregator requires (may be {@code null} which
     *                                  indicates that the type doesn't matter). If the given field data context is {@code null}, we'll try
     *                                  to resolve the field data context from the closes field data based ancestor aggregator.
     */
    public FieldDataBucketAggregator(String name, List<Aggregator.Factory> factories, FieldDataContext fieldDataContext, ValueTransformer valueTransformer, Aggregator parent, Class<? extends IndexFieldData> requiredType) {
        super(name, factories, parent);
        this.fieldDataContext = fieldDataContext;
        this.valueTransformer = valueTransformer;
        if (this.fieldDataContext == null) {
            while (parent != null) {
                if (parent instanceof FieldDataBased) {
                    this.fieldDataContext = ((FieldDataBased) parent).fieldDataContext();
                    this.valueTransformer = ((FieldDataBased) parent).valueTransformer();
                    if (requiredType == null || fieldDataContext.isFullyOfType(requiredType)) {
                        return;
                    }
                }
                parent = parent.parent();
            }
            throw new AggregationExecutionException("Aggregator [" + name + "] requires a field context");
        }
    }

}
