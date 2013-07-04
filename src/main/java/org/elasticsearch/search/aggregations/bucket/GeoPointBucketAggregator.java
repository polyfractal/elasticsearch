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

package org.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.fielddata.GeoPointValues;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.FieldContext;
import org.elasticsearch.search.aggregations.context.ValueSpace;
import org.elasticsearch.search.aggregations.context.geopoints.GeoPointValuesSource;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public abstract class GeoPointBucketAggregator extends ValuesSourceBucketAggregator<GeoPointValuesSource> {

    protected GeoPointBucketAggregator(String name,
                                       GeoPointValuesSource valuesSource,
                                       AggregationContext aggregationContext,
                                       Aggregator parent) {

        super(name, valuesSource, GeoPointValuesSource.class, aggregationContext, parent);
    }

    public static abstract class BucketCollector extends ValuesSourceBucketAggregator.BucketCollector<GeoPointValuesSource> implements ValueSpace {

        private ValueSpace parentContext;

        protected BucketCollector(GeoPointValuesSource valuesSource, Aggregator[] subAggregators, Aggregator aggregator) {
            super(valuesSource, subAggregators, aggregator);
        }

        protected BucketCollector(GeoPointValuesSource valuesSource, List<Aggregator.Factory> factories, Aggregator parent) {
            super(valuesSource, factories, parent);
        }


        @Override
        protected final ValueSpace onDoc(int doc, ValueSpace context) throws IOException {
            GeoPointValues values = valuesSource.values();
            if (!onDoc(doc, values, context)) {
                return null;
            }
            if (values.isMultiValued()) {
                parentContext = context;
                return this;
            }
            return context;
        }

        /**
         * Called for every doc in the current docset context.
         *
         * @param doc           The doc id
         * @param values        The geo_point values
         * @param valueSpace    The current get value space
         * @return              {@code true} if the given doc falls within this bucket, {@code false} otherwise.
         * @throws IOException
         */
        protected abstract boolean onDoc(int doc, GeoPointValues values, ValueSpace valueSpace) throws IOException;

        @Override
        public boolean accept(Object valueSourceKey, double value) {
            return parentContext.accept(valueSourceKey, value);
        }

        @Override
        public boolean accept(Object valueSourceKey, long value) {
            return parentContext.accept(valueSourceKey, value);
        }

        @Override
        public boolean accept(Object valueSourceKey, GeoPoint value) {
            if (!parentContext.accept(valueSourceKey, value)) {
                return false;
            }
            if (valuesSource.key().equals(valueSourceKey)) {
                return accept(value);
            }
            return true;
        }

        @Override
        public boolean accept(Object valueSourceKey, BytesRef value) {
            return parentContext.accept(valueSourceKey, value);
        }

        public abstract boolean accept(GeoPoint value);
    }

    protected abstract static class FieldDataFactory<A extends GeoPointBucketAggregator> extends CompoundFactory<A> {

        private final FieldContext fieldContext;
        private final SearchScript valueScript;

        public FieldDataFactory(String name, FieldContext fieldContext, @Nullable SearchScript valueScript) {
            super(name);
            this.fieldContext = fieldContext;
            this.valueScript = valueScript;
        }

        @Override
        public A create(AggregationContext aggregationContext, Aggregator parent) {
            GeoPointValuesSource source = aggregationContext.geoPointField(fieldContext);
            return create(source, aggregationContext, parent);
        }

        protected abstract A create(GeoPointValuesSource source, AggregationContext aggregationContext, Aggregator parent);
    }

    protected abstract static class ContextBasedFactory<A extends GeoPointBucketAggregator> extends CompoundFactory<A> {

        protected ContextBasedFactory(String name) {
            super(name);
        }

    }
}
