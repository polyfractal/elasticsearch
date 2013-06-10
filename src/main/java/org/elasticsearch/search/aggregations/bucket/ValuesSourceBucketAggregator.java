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

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.ValuesSource;

import java.io.IOException;

/**
 *
 */
public abstract class ValuesSourceBucketAggregator<VS extends ValuesSource> extends BucketAggregator {

    protected final VS valuesSource;

    public ValuesSourceBucketAggregator(String name, VS valuesSource, Aggregator parent) {
        super(name, parent);
        this.valuesSource = valuesSource;
    }

    protected static abstract class BucketCollector<VS extends ValuesSource> extends BucketAggregator.BucketCollector {

        protected VS valuesSource;

        public BucketCollector(Aggregator[] aggregators, VS valuesSource) {
            super(aggregators);
            this.valuesSource = valuesSource;
        }

        @Override
        protected AggregationContext setReaderAngGetContext(AtomicReaderContext reader, AggregationContext context) throws IOException {
            VS valuesSource = this.valuesSource;
            if (valuesSource != null) {
                valuesSource.setNextReader(reader);
            } else {
                valuesSource = extractValuesSourceFromContext(context);
            }
            return setNextValues(valuesSource, context);
        }

        protected abstract AggregationContext setNextValues(VS valuesSource, AggregationContext context) throws IOException;

        /**
         * Extracts the appropriate values source from the given aggregation context. The returned values source cannot
         * be {@code null}. If the underlying implementation cannot find the appropriate values source in the context
         * it should throw an exception.
         *
         * @param context   The aggregation context
         * @return          The value source
         */
        protected abstract VS extractValuesSourceFromContext(AggregationContext context);
    }
}
