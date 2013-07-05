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

package org.elasticsearch.search.aggregations.bucket.multi.terms;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregated;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;

/**
 *
 */
public interface Terms extends Aggregation, Iterable<Terms.Bucket> {

    static enum ValueType {

        STRING, LONG, DOUBLE;

        static ValueType resolveType(String type) {
            if ("string".equals(type)) {
                return STRING;
            }
            if ("double".equals(type) || "float".equals(type)) {
                return DOUBLE;
            }
            if ("long".equals(type) || "integer".equals(type) || "short".equals(type) || "byte".equals(type)) {
                return LONG;
            }
            return null;
        }
    }

    static interface Bucket extends Comparable<Bucket>, Aggregated {

        Text getTerm();

        Number getTermAsNumber();

        long getDocCount();

        Aggregations getAggregations();
    }

    Collection<Bucket> buckets();


    /**
     *
     */
    static interface Order extends ToXContent {

        public static enum Standard implements InternalOrder {

            /**
             * Order by the (higher) count of each term.
             */
            COUNT_DESC((byte) 1, new Comparator<Bucket>() {

                @Override
                public int compare(Bucket o1, Bucket o2) {
                    long i = o2.getDocCount() - o1.getDocCount();
                    if (i == 0) {
                        i = o2.compareTo(o1);
                        if (i == 0) {
                            i = System.identityHashCode(o2) - System.identityHashCode(o1);
                        }
                    }
                    return i > 0 ? 1 : -1;
                }
            }, new ToXContent() {
                @Override
                public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                    return builder.startObject().field("_count", "desc").endObject();
                }
            }),
            /**
             * Order by the (lower) count of each term.
             */
            COUNT_ASC((byte) 2, new Comparator<Bucket>() {

                @Override
                public int compare(Bucket o1, Bucket o2) {
                    return -COUNT_DESC.comparator().compare(o1, o2);
                }
            }, new ToXContent() {
                @Override
                public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                    return builder.startObject().field("_count", "asc").endObject();
                }
            }),
            /**
             * Order by the terms.
             */
            TERM_DESC((byte) 3, new Comparator<Bucket>() {

                @Override
                public int compare(Bucket o1, Bucket o2) {
                    return o2.compareTo(o1);
                }
            }, new ToXContent() {
                @Override
                public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                    return builder.startObject().field("_term", "desc").endObject();
                }
            }),
            /**
             * Order by the terms.
             */
            TERM_ASC((byte) 4, new Comparator<Bucket>() {

                @Override
                public int compare(Bucket o1, Bucket o2) {
                    return -TERM_DESC.comparator().compare(o1, o2);
                }
            }, new ToXContent() {
                @Override
                public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                    return builder.startObject().field("_term", "asc").endObject();
                }
            });


            private final byte id;
            private final Comparator<Bucket> comparator;
            private final ToXContent toXContent;

            Standard(byte id, Comparator<Bucket> comparator, ToXContent toXContent) {
                this.id = id;
                this.comparator = comparator;
                this.toXContent = toXContent;
            }

            public byte id() {
                return this.id;
            }

            public Comparator<Bucket> comparator() {
                return comparator;
            }

            static Standard resolveById(byte id) {
                switch (id) {
                    case 1: return COUNT_DESC;
                    case 2: return COUNT_ASC;
                    case 3: return TERM_DESC;
                    case 4: return TERM_ASC;
                    default: throw new ElasticSearchIllegalArgumentException("Unknown order type");
                }
            }


            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return toXContent.toXContent(builder, params);
            }
        }

        public static class Aggregation implements InternalOrder {

            public static Aggregation create(String aggregationName, boolean asc) {
                return new Aggregation(aggregationName, null, asc);
            }

            public static Aggregation create(String aggregationName, String valueName, boolean asc) {
                return new Aggregation(aggregationName, valueName, asc);
            }

            public static Aggregation asc(String aggregationName) {
                return new Aggregation(aggregationName, null, true);
            }

            public static Aggregation desc(String aggregationName) {
                return new Aggregation(aggregationName, null, false);
            }

            public static Aggregation asc(String aggregationName, String valueName) {
                return new Aggregation(aggregationName, valueName, true);
            }

            public static Aggregation desc(String aggregationName, String valueName) {
                return new Aggregation(aggregationName, valueName, false);
            }

            final Aggregated.Comparator<Bucket> comparator;

            Aggregation(String aggName, String valueName, boolean asc) {
                this.comparator = new Aggregated.Comparator<Bucket>(aggName, valueName, asc);
            }

            @Override
            public byte id() {
                return (byte) 0;
            }

            @Override
            public Comparator<Bucket> comparator() {
                return comparator;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                String name = comparator.valueName() != null ? comparator.aggName() + "." + comparator.valueName() : comparator.aggName();
                return builder.startObject().field(name, comparator.asc() ? "asc" : "desc").endObject();
            }
        }


        Comparator<Bucket> comparator();

    }
}
