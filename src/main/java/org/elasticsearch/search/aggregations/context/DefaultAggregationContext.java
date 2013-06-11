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

/**
 * The default aggregation context (a fast one) which determines that all values for all fields should be aggregated.
 */
public class DefaultAggregationContext implements AggregationContext {

    public static final DefaultAggregationContext INSTANCE = new DefaultAggregationContext();

    @Override
    public boolean accept(String valuesSourceKey, double value) {
        return true;
    }

    @Override
    public boolean accept(String valuesSourceKey, long value) {
        return true;
    }

    @Override
    public boolean accept(String valuesSourceKey, BytesRef value) {
        return true;
    }

    @Override
    public boolean accept(String valuesSourceKey, GeoPoint value) {
        return true;
    }
}
