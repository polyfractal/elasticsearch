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

package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.util.List;

/**
 * An internal implementation of {@link Aggregation}. Serves as a base class for all aggregation implementations.
 */
public abstract class InternalAggregation implements Aggregation, ToXContent, Streamable {

    /**
     * The aggregation type that holds all the string types that are associated with an aggregation:
     * <ul>
     *     <li>name - used as the parser type</li>
     *     <li>stream - used as the stream type</li>
     * </ul>
     */
    public static class Type {

        private String name;
        private BytesReference stream;

        public Type(String name) {
            this(name, new HashedBytesArray(name));
        }

        public Type(String name, String stream) {
            this(name, new HashedBytesArray(stream));
        }

        public Type(String name, BytesReference stream) {
            this.name = name;
            this.stream = stream;
        }

        /**
         * @return The name of the type (mainly used for registering the parser for the aggregator (see {@link AggregatorParser#type()}).
         */
        public String name() {
            return name;
        }

        /**
         * @return  The name of the stream type (used for registering the aggregation stream
         *          (see {@link AggregationStreams#registerStream(AggregationStreams.Stream, org.elasticsearch.common.bytes.BytesReference...)}).
         */
        public BytesReference stream() {
            return stream;
        }
    }


    protected String name;

    /** Constructs an un initialized aggregations (used for serialization) **/
    protected InternalAggregation() {}

    /**
     * Constructs an aggregation with a given name.
     *
     * @param name The name of the aggregation.
     */
    protected InternalAggregation(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    /**
     * @return The {@link Type} of this aggregation
     */
    public abstract Type type();

    /**
     * Reduces the given aggregations to a single one and returns it. In <b>most</b> cases, the assumption will be the all given
     * aggregations are of the same type (the same type as this aggregation). For best efficiency, when implementing,
     * try reusing an existing aggregation instance (typically the first in the given list) to save on redundant object
     * construction.
     *
     * @param aggregations  The aggregations to reduce
     * @return              The reduced aggregation (may be one of the given instances)
     */
    public abstract InternalAggregation reduce(List<InternalAggregation> aggregations);


    /**
     * Common xcontent fields that are shared among aggregations
     */
    public static final class CommonFields {
        public static final XContentBuilderString VALUE = new XContentBuilderString("value");
        public static final XContentBuilderString VALUE_AS_STRING = new XContentBuilderString("value_as_string");
        public static final XContentBuilderString DOC_COUNT = new XContentBuilderString("doc_count");
        public static final XContentBuilderString KEY = new XContentBuilderString("key");
        public static final XContentBuilderString KEY_AS_STRING = new XContentBuilderString("key_as_string");
        public static final XContentBuilderString FROM = new XContentBuilderString("from");
        public static final XContentBuilderString FROM_AS_STRING = new XContentBuilderString("from");
        public static final XContentBuilderString TO = new XContentBuilderString("to");
        public static final XContentBuilderString TO_AS_STRING = new XContentBuilderString("to");
    }

}
