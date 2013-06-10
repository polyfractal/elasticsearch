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

import java.io.IOException;

/**
 * An internal {@link Histogram.Order} strategy which is identified by a unique id.
 */
public interface InternalOrder extends Histogram.Order {

    byte id();

    static class Streams {

        /**
         * Writes the given order to the given output (based on the id of the order).
         */
        public static void writeOrder(InternalOrder order, StreamOutput out) throws IOException {
            out.writeByte(order.id());
            if (order instanceof Histogram.Order.Aggregation) {
                out.writeBoolean(((Histogram.Order.Aggregation) order).comparator().asc());
                out.writeString(((Histogram.Order.Aggregation) order).comparator().aggName());
                String valueName = ((Histogram.Order.Aggregation) order).comparator().valueName();
                if (valueName == null) {
                    out.writeBoolean(false);
                } else {
                    out.writeBoolean(true);
                    out.writeString(valueName);
                }
            }
        }

        /**
         * Reads an order from the given input (based on the id of the order).
         *
         * @see Streams#writeOrder(InternalOrder, org.elasticsearch.common.io.stream.StreamOutput)
         */
        public static InternalOrder readOrder(StreamInput in) throws IOException {
            byte id = in.readByte();
            if (id != Aggregation.ID) {
                return Histogram.Order.Standard.resolveById(id);
            }
            boolean asc = in.readBoolean();
            String aggName = in.readString();
            String valueName = in.readBoolean() ? in.readString() : null;
            return Histogram.Order.Aggregation.create(aggName, valueName, asc);
        }

    }

}
