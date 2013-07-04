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

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregated;
import org.elasticsearch.search.aggregations.Aggregation;

import java.io.IOException;
import java.util.Comparator;

/**
 * A histogram get result
 */
public interface Histogram<B extends Histogram.Bucket> extends Aggregation, Iterable<B> {

    /**
     * A bucket in the histogram where documents fall in
     */
    static interface Bucket extends Aggregated {

        /**
         * @return The key associated with the bucket (all documents that fall in this bucket were rounded to this key)
         */
        long getKey();

        /**
         * @return The number of documents that fall within this bucket (number of documents that were rounded to the key of this bucket)
         */
        long getDocCount();

    }

    /**
     * Returns a bucket by the key associated with it.
     *
     * @param key The key of the bucket.
     * @return The bucket that is associated with the given key.
     */
    B getByKey(long key);


    /**
     * A strategy defining the order in which the buckets in this histogram are ordered.
     */
    static interface Order extends ToXContent {

        /**
         * Defines standard ordering strategies for histogram buckets.
         */
        static enum Standard implements InternalOrder {

            /**
             * An {@link Order} strategy which sorts the buckets by key in an ascending manner
             */
            KEY_ASC((byte) 1, new Comparator<Bucket>() {
                @Override
                public int compare(Bucket b1, Bucket b2) {
                    if (b1.getKey() > b2.getKey()) {
                        return 1;
                    }
                    if (b1.getKey() < b2.getKey()) {
                        return -1;
                    }
                    return 0;
                }
            }, new ToXContent() {
                @Override
                public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                    return builder.startObject().field("_key", "asc").endObject();
                }
            }),

            /**
             * An {@link Order} strategy which sorts the buckets by key in an descending manner
             */
            KEY_DESC((byte) 2, new Comparator<Bucket>() {
                @Override
                public int compare(Bucket b1, Bucket b2) {
                    return -KEY_ASC.comparator().compare(b1, b2);
                }
            }, new ToXContent() {
                @Override
                public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                    return builder.startObject().field("_key", "desc").endObject();
                }
            }),

            /**
             * An {@link Order} strategy which sorts the buckets by their document count in an ascending manner
             */
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
            }, new ToXContent() {
                @Override
                public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                    return builder.startObject().field("_count", "asc").endObject();
                }
            }),

            /**
             * An {@link Order} strategy which sorts the buckets by their document count in an descending manner
             */
            COUNT_DESC((byte) 4, new Comparator<Bucket>() {
                @Override
                public int compare(Bucket b1, Bucket b2) {
                    return -COUNT_ASC.comparator().compare(b1, b2);
                }
            }, new ToXContent() {
                @Override
                public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                    return builder.startObject().field("_count", "desc").endObject();
                }
            });


            private final byte id;
            private final Comparator<Bucket> comparator;
            private final ToXContent toXContent;

            private Standard(byte id, Comparator<Bucket> comparator, ToXContent toXContent) {
                this.id = id;
                this.comparator = comparator;
                this.toXContent = toXContent;
            }

            @Override
            public byte id() {
                return id;
            }

            public static Standard resolveById(byte id) {
                switch (id) {
                    case 1: return KEY_ASC;
                    case 2: return KEY_DESC;
                    case 3: return COUNT_ASC;
                    case 4: return COUNT_DESC;
                    default: throw new ElasticSearchIllegalArgumentException("Unknown value histogram order type");
                }
            }


            @Override
            public Comparator<Bucket> comparator() {
                return comparator;
            }


            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return toXContent.toXContent(builder, params);
            }
        }

        /**
         * An order strategy which sorts the buckets based on their sub Calc aggregators
         */
        static class Aggregation implements InternalOrder {

            static final byte ID = 0;

            /**
             * Creates a bucket ordering strategy which sorts buckets based on a single-valued calc get
             *
             * @param   aggregationName the name of the get
             * @param   asc             The direction of the order (ascending or descending)
             */
            public static Aggregation create(String aggregationName, boolean asc) {
                return new Aggregation(aggregationName, null, asc);
            }

            /**
             * Creates a bucket ordering strategy which sorts buckets based on a multi-valued calc get
             *
             * @param   aggregationName the name of the get
             * @param   valueName       The name of the value of the multi-value get by which the sorting will be applied
             * @param   asc             The direction of the order (ascending or descending)
             */
            public static Aggregation create(String aggregationName, String valueName, boolean asc) {
                return new Aggregation(aggregationName, valueName, asc);
            }

            /**
             * Creates a bucket ordering strategy which sorts buckets based on single-valued calc get in an ascending manner.
             *
             * @param   aggregationName the name of the get
             */
            public static Aggregation asc(String aggregationName) {
                return new Aggregation(aggregationName, null, true);
            }

            /**
             * Creates a bucket ordering strategy which sorts buckets based on multi-valued calc get in an ascending manner.
             *
             * @param   aggregationName the name of the get
             * @param   valueName       The name of the value in the multi-valued get by which the sort will be applied.
             */
            public static Aggregation asc(String aggregationName, String valueName) {
                return new Aggregation(aggregationName, valueName, true);
            }

            /**
             * Creates a bucket ordering strategy which sorts buckets based on single-valued calc get in an descending manner.
             *
             * @param   aggregationName the name of the get
             */
            public static Aggregation desc(String aggregationName) {
                return new Aggregation(aggregationName, null, false);
            }

            /**
             * Creates a bucket ordering strategy which sorts buckets based on multi-valued calc get in an descending manner.
             *
             * @param   aggregationName the name of the get
             * @param   valueName       The name of the value in the multi-valued get by which the sort will be applied.
             */
            public static Aggregation desc(String aggregationName, String valueName) {
                return new Aggregation(aggregationName, valueName, false);
            }

            private final Aggregated.Comparator<Bucket> comparator;

            Aggregation(String aggName, String valueName, boolean asc) {
                this.comparator = new Aggregated.Comparator<Bucket>(aggName, valueName, asc);
            }

            @Override
            public byte id() {
                return 0;
            }

            @Override
            public Aggregated.Comparator<Bucket> comparator() {
                return comparator;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                String name = comparator.valueName() != null ? comparator.aggName() + "." + comparator.valueName() : comparator.aggName();
                return builder.startObject().field(name, comparator.asc() ? "asc" : "desc").endObject();
            }
        }

        /**
         * @return The bucket comparator by which the order will be applied.
         */
        Comparator<Bucket> comparator();


    }
}
