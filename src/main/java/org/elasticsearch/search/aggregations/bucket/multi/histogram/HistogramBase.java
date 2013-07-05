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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.search.aggregations.Aggregated;
import org.elasticsearch.search.aggregations.Aggregation;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

/**
 * A histogram get result
 */
interface HistogramBase<B extends HistogramBase.Bucket> extends Aggregation, Iterable<B> {

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

    List<B> buckets();

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
    static abstract class Order implements ToXContent {

        public static final InternalOrder KEY_ASC = new InternalOrder((byte) 1, "_key", true, new Comparator<Bucket>() {
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
        });

        public static final InternalOrder KEY_DESC = new InternalOrder((byte) 2, "_key", false, new Comparator<Bucket>() {
            @Override
            public int compare(Bucket b1, Bucket b2) {
                return -KEY_ASC.comparator().compare(b1, b2);
            }
        });

        public static final InternalOrder COUNT_ASC = new InternalOrder((byte) 3, "_count", true, new Comparator<Bucket>() {
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
        });

        public static final InternalOrder COUNT_DESC = new InternalOrder((byte) 4, "_count", false, new Comparator<Bucket>() {
            @Override
            public int compare(Bucket b1, Bucket b2) {
                return -COUNT_ASC.comparator().compare(b1, b2);
            }
        });

        /**
         * Creates a bucket ordering strategy which sorts buckets based on a single-valued calc get
         *
         * @param   aggregationName the name of the get
         * @param   asc             The direction of the order (ascending or descending)
         */
        public static InternalOrder aggregation(String aggregationName, boolean asc) {
            return new InternalOrder.Aggregation(aggregationName, null, asc);
        }

        /**
         * Creates a bucket ordering strategy which sorts buckets based on a multi-valued calc get
         *
         * @param   aggregationName the name of the get
         * @param   valueName       The name of the value of the multi-value get by which the sorting will be applied
         * @param   asc             The direction of the order (ascending or descending)
         */
        public static InternalOrder aggregation(String aggregationName, String valueName, boolean asc) {
            return new InternalOrder.Aggregation(aggregationName, valueName, asc);
        }

        /**
         * @return The bucket comparator by which the order will be applied.
         */
        abstract Comparator<Bucket> comparator();


        static class Streams {

            /**
             * Writes the given order to the given output (based on the id of the order).
             */
            public static void writeOrder(InternalOrder order, StreamOutput out) throws IOException {
                out.writeByte(order.id());
                if (order instanceof Aggregation) {
                    out.writeBoolean(order.asc());
                    out.writeString(order.key());
                }
            }

            /**
             * Reads an order from the given input (based on the id of the order).
             *
             * @see Streams#writeOrder(InternalOrder, org.elasticsearch.common.io.stream.StreamOutput)
             */
            public static InternalOrder readOrder(StreamInput in) throws IOException {
                byte id = in.readByte();
                switch (id) {
                    case 1: return KEY_ASC;
                    case 2: return KEY_DESC;
                    case 3: return COUNT_ASC;
                    case 4: return COUNT_DESC;
                    default:
                        boolean asc = in.readBoolean();
                        String key = in.readString();
                        return new InternalOrder.Aggregation(key, asc);
                }
            }

        }
    }
}
