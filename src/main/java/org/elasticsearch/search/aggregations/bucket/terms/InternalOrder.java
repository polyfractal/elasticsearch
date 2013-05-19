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

package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 *
 */
public interface InternalOrder extends Terms.Order {

    byte id();

    public static class Streams {

        public static void writeOrder(Terms.Order order, StreamOutput out) throws IOException {
            out.writeByte(((InternalOrder) order).id());
            if (order instanceof Aggregation) {
                out.writeBoolean(((Aggregation) order).comparator.asc());
                out.writeString(((Aggregation) order).comparator.aggName());
                boolean hasValueName = ((Aggregation) order).comparator.aggName() != null;
                out.writeBoolean(hasValueName);
                if (hasValueName) {
                    out.writeString(((Aggregation) order).comparator.valueName());
                }
            }
        }

        public static Terms.Order readOrder(StreamInput in) throws IOException {
            byte id = in.readByte();
            if (id == 0) {
                boolean asc = in.readBoolean();
                String aggName = in.readString();
                String valueName = null;
                if (in.readBoolean()) {
                    valueName = in.readString();
                }
                return new Aggregation(aggName, valueName, asc);
            }
            return Standard.resolveById(id);
        }
    }
}
