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

package org.elasticsearch.search.aggregations.bucket.histogram.date;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 *
 */
public interface InternalDateOrder extends DateHistogram.Order {

    byte id();

    static class Streams {

        public static void writeOrder(DateHistogram.Order order, StreamOutput out) throws IOException {
            byte id = ((InternalDateOrder) order).id();
            out.writeByte(id);
            if (order instanceof Aggregation) {
                out.writeBoolean(((Aggregation) order).comparator().asc());
                out.writeString(((Aggregation) order).comparator().aggName());
                String valueName = ((Aggregation) order).comparator().valueName();
                if (valueName == null) {
                    out.writeBoolean(false);
                } else {
                    out.writeBoolean(true);
                    out.writeString(valueName);
                }
            }
        }


        public static DateHistogram.Order readOrder(StreamInput in) throws IOException {
            byte id = in.readByte();
            if (id != Aggregation.ID) {
                return Standard.resolveById(id);
            }
            boolean asc = in.readBoolean();
            String aggName = in.readString();
            String valueName = in.readBoolean() ? in.readString() : null;
            return Aggregation.create(aggName, valueName, asc);
        }

    }


}
