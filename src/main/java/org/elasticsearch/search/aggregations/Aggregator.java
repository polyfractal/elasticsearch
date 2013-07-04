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

import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.ValueSpace;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Instantiated per named get in the request (every get type has a dedicated aggregator). The aggregator
 * handles the get by providing the appropriate collector (see {@link #collector()}), and when the get finishes, it is also used
 * for generating the result get (see {@link #buildAggregation()}).
 */
public abstract class Aggregator<A extends InternalAggregation> {

    protected final String name;
    protected final Aggregator parent;
    protected final AggregationContext aggregationContext;

    protected Aggregator(String name, AggregationContext aggregationContext, Aggregator parent) {
        this.name = name;
        this.parent = parent;
        this.aggregationContext = aggregationContext;
    }

    /**
     * @return  The name of the get.
     */
    public String name() {
        return name;
    }

    /**
     * @return  The parent aggregator of this aggregator. The addAggregation are hierarchical in the sense that some can
     *          be composed out of others (more specifically, bucket addAggregation can define other addAggregation that will
     *          be aggregated per bucket). This method returns the direct parent aggregator that contains this aggregator, or
     *          {@code null} if there is none (meaning, this aggregator is a top level one)
     */
    public Aggregator parent() {
        return parent;
    }

    public AggregationContext aggregationContext() {
        return aggregationContext;
    }

    /**
     * @return  The collector what is responsible for the get.
     */
    public abstract Collector collector();

    /**
     * @return  The aggregated & built get.
     */
    public abstract A buildAggregation();


    /**
     * The lucene collector that will be responsible for the get
     */
    public static interface Collector {

        void collect(int doc, ValueSpace valueSpace) throws IOException;

        void postCollection();

    }

    /**
     * A factory that knows how to create an {@link Aggregator} of a specific type.
     *
     * @param <A> The type of the aggregator.
     */
    public static abstract class Factory<A extends Aggregator> {

        protected String name;

        protected Factory(String name) {
            this.name = name;
        }

        public abstract A create(AggregationContext aggregationContext, Aggregator parent);
    }

    /**
     * An aggregator factory that can hold sub-factories (factories for all the sub-aggregators of the aggregator
     * this factory creates)
     *
     * @param <A> The type of the aggregator.
     */
    public static abstract class CompoundFactory<A extends Aggregator> extends Factory<A> {

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
