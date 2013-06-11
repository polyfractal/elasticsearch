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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.search.aggregations.context.AggregationContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Instantiated per named aggregation in the request (every aggregation type has a dedicated aggregator). The aggregator
 * handles the aggregation by providing the appropriate collector (see {@link #collector()}), and when the aggregation finishes, it is also used
 * for generating the result aggregation (see {@link #buildAggregation()}).
 */
public interface Aggregator<A extends InternalAggregation> {

    /**
     * @return  The name of the aggregation.
     */
    String name();

    /**
     * @return  The collector what is responsible for the aggregation.
     */
    Collector collector();

    /**
     * @return  The aggregated & built aggregation.
     */
    A buildAggregation();

    /**
     * @return  The parent aggregator of this aggregator. The aggregations are hierarchical in the sense that some can
     *          be composed out of others (more specifically, bucket aggregations can define other aggregations that will
     *          be aggregated per bucket). This method returns the direct parent aggregator that contains this aggregator, or
     *          {@code null} if there is none (meaning, this aggregator is a top level one)
     */
    Aggregator parent();

    /**
     * The lucene collector that will be responsible for the aggregation
     */
    static interface Collector {

        void setScorer(Scorer scorer) throws IOException;

        void collect(int doc) throws IOException;

        void setNextReader(AtomicReaderContext reader, AggregationContext context) throws IOException;

        void postCollection();

    }

    /**
     * A factory that knows how to create an {@link Aggregator} of a specific type.
     *
     * @param <A> The type of the aggregator.
     */
    static abstract class Factory<A extends Aggregator> {

        protected String name;

        protected Factory(String name) {
            this.name = name;
        }

        public abstract A create(Aggregator parent);
    }


    static abstract class CompoundFactory<A extends Aggregator> extends Factory<A> {

        protected List<Factory> factories = new ArrayList<Factory>();

        protected CompoundFactory(String name) {
            super(name);
        }

        @SuppressWarnings("unchecked")
        public Factory<A> set(List<Aggregator.Factory> factories) {
            this.factories = factories;
            return this;
        }

    }




}
