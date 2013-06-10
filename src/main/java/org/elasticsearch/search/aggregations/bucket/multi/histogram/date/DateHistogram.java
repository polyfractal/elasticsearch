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

package org.elasticsearch.search.aggregations.bucket.multi.histogram.date;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.search.aggregations.Aggregated;
import org.elasticsearch.search.aggregations.Aggregation;

import java.util.Comparator;

/**
 *
 */
public interface DateHistogram extends Aggregation, Iterable<DateHistogram.Bucket> {

    static interface Bucket extends Aggregated {

        long getTime();

        String getTimeAsString();

        long getDocCount();

    }

    static interface Order {

        static enum Standard implements InternalDateOrder {

            TIME_ASC((byte) 1, new Comparator<Bucket>() {
                @Override
                public int compare(Bucket b1, Bucket b2) {
                    if (b1.getTime() > b2.getTime()) {
                        return 1;
                    }
                    if (b1.getTime() < b2.getTime()) {
                        return -1;
                    }
                    return 0;
                }
            }),
            TIME_DESC((byte) 2, new Comparator<Bucket>() {
                @Override
                public int compare(Bucket b1, Bucket b2) {
                    return -TIME_ASC.comparator().compare(b1, b2);
                }
            }),
            COUNT_ASC((byte) 3, new Comparator<Bucket>() {
                @Override
                public int compare(Bucket b1, Bucket b2) {
                    if (b1.getDocCount() > b2.getDocCount()) {
                        return 1;
                    }
                    if (b1.getDocCount() < b2.getDocCount()) {
                        return -1;
                    }
                    return 0;
                }
            }),
            COUNT_DESC((byte) 4, new Comparator<Bucket>() {
                @Override
                public int compare(Bucket b1, Bucket b2) {
                    return -COUNT_ASC.comparator().compare(b1, b2);
                }
            });


            private final byte id;
            private final Comparator<Bucket> comparator;

            private Standard(byte id, Comparator<Bucket> comparator) {
                this.id = id;
                this.comparator = comparator;
            }

            @Override
            public byte id() {
                return id;
            }

            @Override
            public Comparator<Bucket> comparator() {
                return comparator;
            }

            public static Standard resolveById(byte id) {
                switch (id) {
                    case 1: return TIME_ASC;
                    case 2: return TIME_DESC;
                    case 3: return COUNT_ASC;
                    case 4: return COUNT_DESC;
                    default: throw new ElasticSearchIllegalArgumentException("Unknown date histogram order");
                }
            }
        }

        static class Aggregation implements InternalDateOrder {

            static final byte ID = 0;

            public static Aggregation create(String aggregationName, boolean asc) {
                return new Aggregation(aggregationName, null, asc);
            }

            public static Aggregation create(String aggregationName, String valueName, boolean asc) {
                return new Aggregation(aggregationName, valueName, asc);
            }

            public static Order asc(String aggregationName) {
                return new Aggregation(aggregationName, null, true);
            }

            public static Order asc(String aggregationName, String valueName) {
                return new Aggregation(aggregationName, valueName, true);
            }

            public static Order desc(String aggregationName) {
                return new Aggregation(aggregationName, null, false);
            }

            public static Order desc(String aggregationName, String valueName) {
                return new Aggregation(aggregationName, valueName, false);
            }

            private final Aggregated.Comparator<Bucket> comparator;

            Aggregation(String aggName, String valueName, boolean asc) {
                this.comparator = new Aggregated.Comparator<Bucket>(aggName, valueName, asc);
            }

            @Override
            public byte id() {
                return ID;
            }

            @Override
            public Aggregated.Comparator<Bucket> comparator() {
                return comparator;
            }
        }

        Comparator<Bucket> comparator();
    }

    Bucket getByTime(long time);

}

