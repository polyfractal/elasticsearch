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

package org.elasticsearch.search.aggregations.bucket.missing;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.BucketAggregator;
import org.elasticsearch.search.aggregations.bucket.FieldDataBucketAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.FieldDataContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class MissingAggregator extends FieldDataBucketAggregator {

    long docCount;
    List<Aggregator> aggregators;

    public MissingAggregator(String name, List<Aggregator.Factory> factories, FieldDataContext fieldDataContext, Aggregator parent) {
        super(name, factories, fieldDataContext, parent, IndexFieldData.class);
    }

    @Override
    public Collector collector() {
        return new Collector(fieldDataContext);
    }

    @Override
    public InternalMissing buildAggregation() {
        List<InternalAggregation> aggregationResults = new ArrayList<InternalAggregation>(aggregators.size());
        for (Aggregator aggregator : aggregators) {
            aggregationResults.add(aggregator.buildAggregation());
        }
        return new InternalMissing(name, docCount, aggregationResults);
    }

    class Collector extends BucketAggregator.BucketCollector {

        private long docCount;
        protected final FieldDataContext fieldDataContext;
        protected final BytesValues[] values;

        protected Collector(FieldDataContext fieldDataContext) {
            super(MissingAggregator.this);
            this.fieldDataContext = fieldDataContext;
            this.values = new BytesValues[fieldDataContext.indexFieldDatas().length];
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            fieldDataContext.loadBytesValues(context, values);
            super.setNextReader(context);
        }

        @Override
        protected AggregationContext onDoc(int doc, AggregationContext context) throws IOException {
            if (values.length == 1) {
                if (!values[0].hasValue(doc)) {
                    docCount++;
                    return context;
                }
                return null;
            }
            boolean missing = true;
            for (int i = 0; i < values.length; i++) {
                if (values[i].hasValue(doc)) {
                    missing = false;
                    break;
                }
            }
            if (missing) {
                docCount++;
                return context;
            }
            return null;
        }

        @Override
        protected void postCollection(List<Aggregator> aggregators) {
            MissingAggregator.this.aggregators = aggregators;
            MissingAggregator.this.docCount = docCount;
        }

    }

    public static class Factory extends BucketAggregator.Factory<MissingAggregator, Factory> {

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
            return new MissingAggregator(name, factories, fieldDataContext, parent);
        }

    }
}


