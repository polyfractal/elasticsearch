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

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregated;

import java.io.IOException;
import java.util.Comparator;

/**
 * An internal {@link HistogramBase.Order} strategy which is identified by a unique id.
 */
class InternalOrder extends HistogramBase.Order {

    final byte id;
    final String key;
    final boolean asc;
    final Comparator<HistogramBase.Bucket> comparator;

    InternalOrder(byte id, String key, boolean asc, Comparator<HistogramBase.Bucket> comparator) {
        this.id = id;
        this.key = key;
        this.asc = asc;
        this.comparator = comparator;
    }

    byte id() {
        return id;
    }

    String key() {
        return key;
    }

    boolean asc() {
        return asc;
    }

    @Override
    Comparator<HistogramBase.Bucket> comparator() {
        return comparator;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field(key, asc ? "asc" : "desc").endObject();
    }

    static class Aggregation extends InternalOrder {

        static final byte ID = 0;

        Aggregation(String key, boolean asc) {
            super(ID, key, asc, new Aggregated.Comparator<HistogramBase.Bucket>(key, asc));
        }

        Aggregation(String aggName, String valueName, boolean asc) {
            super(ID, key(aggName, valueName), asc, new Aggregated.Comparator<HistogramBase.Bucket>(aggName, valueName, asc));
        }

        private static String key(String aggName, String valueName) {
            return (valueName == null) ? aggName : aggName + "." + valueName;
        }

    }


}
