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

import org.elasticsearch.common.lucene.search.XCollector;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.DefaultAggregationContext;

import java.io.IOException;

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
    static abstract class Collector extends XCollector {

        /**
         * Called directly after the document collection is finished. This is an opportunity for the collector to update
         * its aggregator with the collected data.
         */
        @Override
        public abstract void postCollection();

        @Override
        public final void collect(int doc) throws IOException {
            collect(doc, new DefaultAggregationContext());
        }

        public abstract void collect(int doc, AggregationContext context) throws IOException;

        /**
         * By default all aggregation collectors support out of order doc collection.
         */
        @Override
        public boolean acceptsDocsOutOfOrder() {
            return true;
        }

    }

    /**
     * A factory that knows how to create an {@link Aggregator} of a specific type.
     *
     * @param <A> The type of the aggregator.
     */
    static interface Factory<A extends Aggregator> {

        A create(Aggregator parent);

    }


}
