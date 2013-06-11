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

package org.elasticsearch.search.aggregations.calc;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.search.aggregations.AbstractValuesSourceAggregator;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.ValuesSource;
import org.elasticsearch.search.aggregations.context.ValuesSourceBased;

import java.io.IOException;

/**
 *
 */
public abstract class ValuesSourceCalcAggregator<VS extends ValuesSource> extends AbstractValuesSourceAggregator<VS> implements ValuesSourceBased {

    public ValuesSourceCalcAggregator(String name, VS valuesSource, Class<VS> valueSourceType, Aggregator parent) {
        super(name, valuesSource, valueSourceType, parent);
    }


    protected static abstract class Collector<VS extends ValuesSource> implements Aggregator.Collector {

        protected VS valuesSource;

        protected Collector(VS valuesSource) {
            this.valuesSource = valuesSource;
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            if (valuesSource != null) {
                valuesSource.setNextScorer(scorer);
            }
        }

        @Override
        public final void setNextReader(AtomicReaderContext reader, AggregationContext context) throws IOException {
            if (valuesSource != null) {
                valuesSource.setNextReader(reader);
            } else {
                valuesSource = extractValuesSourceFromContext(context);
            }
            setNextValues(valuesSource, context);
        }

        /**
         * Extracts the appropriate values source from the given aggregation context. The returned values source cannot
         * be {@code null}. If the underlying implementation cannot find the appropriate values source in the context
         * it should throw an exception.
         *
         * @param context   The aggregation context
         * @return          The value source
         */
        protected abstract VS extractValuesSourceFromContext(AggregationContext context);

        /**
         * Calls on the underlying implementation to set the next values on which this aggregator will act.
         *
         * @param valuesSource  The value source this aggregator is working with
         * @param context       The aggregation context
         * @throws IOException
         */
        protected abstract void setNextValues(VS valuesSource, AggregationContext context) throws IOException;

    }

}
