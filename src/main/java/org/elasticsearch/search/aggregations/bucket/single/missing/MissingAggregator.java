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

package org.elasticsearch.search.aggregations.bucket.single.missing;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.BytesBucketAggregator;
import org.elasticsearch.search.aggregations.bucket.single.SingleBytesBucketAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.FieldContext;
import org.elasticsearch.search.aggregations.context.ValueSpace;
import org.elasticsearch.search.aggregations.context.bytes.BytesValuesSource;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class MissingAggregator extends SingleBytesBucketAggregator {

    long docCount;

    public MissingAggregator(String name, List<Aggregator.Factory> factories, BytesValuesSource valuesSource,
                             AggregationContext aggregationContext, Aggregator parent) {
        super(name, factories, valuesSource, aggregationContext, parent);
    }

    @Override
    public Collector collector(Aggregator[] aggregators) {
        return new Collector(valuesSource, aggregators);
    }

    @Override
    public InternalMissing buildAggregation(InternalAggregations aggregations) {
        return new InternalMissing(name, docCount, aggregations);
    }

    class Collector extends BytesBucketAggregator.BucketCollector {

        private long docCount;

        Collector(BytesValuesSource valuesSource, Aggregator[] subAggregators) {
            super(valuesSource, subAggregators, MissingAggregator.this);
        }

        @Override
        protected boolean onDoc(int doc, BytesValues values, ValueSpace context) throws IOException {
            if (!values.hasValue(doc)) {
                docCount++;
                return true;
            }
            return false;
        }

        @Override
        protected void postCollection(Aggregator[] aggregators) {
            MissingAggregator.this.docCount = docCount;
        }

        @Override
        public boolean accept(BytesRef value) {
            // doesn't matter what we return here... this method will never be called anyway
            // if a doc made it down the hierarchy, by definition the doc has no values for the field
            // so there's no way this method will be called with a value for this field
            return false;
        }
    }

    public static class Factory extends Aggregator.CompoundFactory<MissingAggregator> {

        private final FieldContext fieldContext;

        public Factory(String name, FieldContext fieldContext) {
            super(name);
            this.fieldContext = fieldContext;
        }

        @Override
        public MissingAggregator create(AggregationContext aggregationContext, Aggregator parent) {
            if (fieldContext == null) {
                return new MissingAggregator(name, factories, null, aggregationContext, parent);
            }
            BytesValuesSource valuesSource = aggregationContext.bytesField(fieldContext, null);
            return new MissingAggregator(name, factories, valuesSource, aggregationContext, parent);
        }
    }

}


