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

import com.google.common.collect.Lists;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.single.BytesSingleBucketAggregator;
import org.elasticsearch.search.aggregations.bucket.single.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.context.FieldDataContext;
import org.elasticsearch.search.aggregations.context.bytes.BytesValuesSource;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class MissingAggregator extends BytesSingleBucketAggregator {

    long docCount;

    public MissingAggregator(String name, List<Aggregator.Factory> factories, Aggregator parent) {
        super(name, factories, parent);
    }

    public MissingAggregator(String name, List<Aggregator.Factory> factories, BytesValuesSource valuesSource, Aggregator parent) {
        super(name, factories, valuesSource, parent);
    }

    @Override
    public Collector collector(Aggregator[] aggregators) {
        return new Collector(name, aggregators, valuesSource);
    }

    @Override
    public InternalMissing buildAggregation(Aggregator[] aggregators) {
        List<InternalAggregation> aggregationResults = Lists.newArrayListWithCapacity(aggregators.length);
        for (Aggregator aggregator : aggregators) {
            aggregationResults.add(aggregator.buildAggregation());
        }
        return new InternalMissing(name, docCount, aggregationResults);
    }

    class Collector extends BytesSingleBucketAggregator.BucketCollector {

        private long docCount;

        Collector(String aggregatorName, Aggregator[] aggregators, BytesValuesSource valuesSource) {
            super(aggregatorName, aggregators, valuesSource);
        }

        @Override
        protected boolean onDoc(int doc, BytesValues values) throws IOException {
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
        public boolean accept(int doc, BytesRef value, BytesValues values) {
            // doesn't matter what we return here... this method will never be called anyway
            // if a doc made it down the hierarchy, by definition the doc has no values for the field
            // so there's no way this method will be called with a value for this field
            return false;
        }
    }

    public static class Factory extends SingleBucketAggregator.Factory<MissingAggregator, Factory> {

        private final FieldDataContext fieldDataContext;

        public Factory(String name) {
            this(name, null);
        }

        public Factory(String name, FieldDataContext fieldDataContext) {
            super(name);
            this.fieldDataContext = fieldDataContext;
        }

        @Override
        public MissingAggregator create(Aggregator parent) {
            BytesValuesSource valuesSource = fieldDataContext != null ?
                    new BytesValuesSource.FieldData(fieldDataContext.field(), fieldDataContext.indexFieldData()) : null;
            return new MissingAggregator(name, factories, valuesSource, parent);
        }

    }
}


