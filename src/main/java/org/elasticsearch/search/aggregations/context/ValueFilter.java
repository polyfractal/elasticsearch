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

package org.elasticsearch.search.aggregations.context;

import org.apache.lucene.util.BytesRef;

/**
 * A field value filter. Determines whether a value for a specific field should be aggregated.
 *
 * @see {@link FilteredAggregationContext}
 */
public interface ValueFilter {

    /**
     * @return The field with which this filter is associated.
     */
    String field();

    /**
     * Determines whether the given double value should be aggregated.
     *
     * @param value The value
     * @return {@code true} if the given value should be aggregated, {@code false} otherwise.
     */
    boolean accept(double value);

    /**
     * Determines whether the given long value should be aggregated.
     *
     * @param value The value
     * @return {@code true} if the given value should be aggregated, {@code false} otherwise.
     */
    boolean accept(long value);

    /**
     * Determines whether the given bytesref value should be aggregated.
     *
     * @param value The value
     * @return {@code true} if the given value should be aggregated, {@code false} otherwise.
     */
    boolean accept(BytesRef value);

}
