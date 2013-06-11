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
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.search.aggregations.context.bytes.BytesValuesSource;
import org.elasticsearch.search.aggregations.context.doubles.DoubleValuesSource;
import org.elasticsearch.search.aggregations.context.geopoints.GeoPointValuesSource;
import org.elasticsearch.search.aggregations.context.longs.LongValuesSource;

/**
 * The aggregation context is passed down the collector hierarchy during the aggregation process. The context may
 * determine whether a specific value for a specific field should be counted for aggregation. This only applies for
 * calc aggregators - as bucketing aggregators aggregation (bucket) based on doc ids, while calc aggregators aggregate
 * based on field values.
 */
public interface AggregationContext {

    /**
     * Determines whether the given double value for the given field should be aggregated.
     *
     * @param valueSourceKey    The key of the value source
     * @param value             The value
     * @return {@code true} if the value should be aggregated, {@code false} otherwise.
     */
    boolean accept(String valueSourceKey, double value);

    /**
     * Determines whether the given long value for the given field should be aggregated.
     *
     * @param valueSourceKey    The key of the value source
     * @param value             The value
     * @return {@code true} if the value should be aggregated, {@code false} otherwise.
     */
    boolean accept(String valueSourceKey, long value);

    /**
     * Determines whether the given bytesref value for the given field should be aggregated.
     *
     * @param valueSourceKey    The key of the value source
     * @param value             The value
     * @return {@code true} if the value should be aggregated, {@code false} otherwise.
     */
    boolean accept(String valueSourceKey, BytesRef value);

    /**
     * Determines whether the given geo point value for the given field should be aggregated.
     *
     * @param valueSourceKey    The key of the value source
     * @param value             The value
     * @return {@code true} if the value should be aggregated, {@code false} otherwise.
     */
    boolean accept(String valueSourceKey, GeoPoint value);

}
