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
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.aggregations.context.FieldDataBased;
import org.elasticsearch.search.aggregations.context.FieldDataContext;
import org.elasticsearch.search.aggregations.context.ValueTransformer;

import java.io.IOException;

/**
 * A base class for all field data based calc aggregators.
 */
@SuppressWarnings("unchecked")
public abstract class FieldDataCalcAggregator extends CalcAggregator implements FieldDataBased {

    protected FieldDataContext fieldDataContext;

    protected ValueTransformer valueTransformer;

    /**
     * Constructs a new field data based calc aggregator. If the given {@code fieldDataContext} is {@code null}, both the field
     * data context and the value transformer will be inherited from the first field data based aggregator up the ancestry line. If
     * {@code requiresNumericFieldData} is set to {@code true}, non-numeric field data based ancestor aggregator will be ignored.
     *
     * @param name                      The name of the aggregation.
     * @param fieldDataContext          The field data context. Can be {@code null}, indicating that the user didn't specify the field
     *                                  context, in which case, we will attempt to pick it up from the closest ancestor aggregator.
     * @param valueTransformer          The value transformer that will be used to transform the field data values just before they're
     *                                  aggregated.
     * @param parent                    The parent bucket aggregator
     * @param requiresNumericFieldData  Indicates whether the field data that this aggregator requires needs to be of a numeric type.
     *                                  If the given field data context is {@code null}, we'll try to resolve the field data context from
     *                                  the closes field data based ancestor aggregator. This flag indicates whether we need to search
     *                                  for a field data aggregator (can only be a bucket aggregator) that has a numeric type field data
     *                                  (for example, stats aggregator will require a numeric field data)
     */
    protected FieldDataCalcAggregator(String name, FieldDataContext fieldDataContext, ValueTransformer valueTransformer, Aggregator parent, boolean requiresNumericFieldData) {
        super(name, parent);
        this.fieldDataContext = fieldDataContext;
        this.valueTransformer = valueTransformer;
        if (this.fieldDataContext == null) {
            while (parent != null) {
                if (parent instanceof FieldDataBased) {
                    this.fieldDataContext = ((FieldDataBased) parent).fieldDataContext();
                    this.valueTransformer = ((FieldDataBased) parent).valueTransformer();
                    if (!requiresNumericFieldData || this.fieldDataContext.isFullyNumeric()) {
                        return;
                    }
                }
                parent = parent.parent();
            }
            throw new AggregationExecutionException("Aggregator [" + name + "] requires a field context");
        }
    }

    @Override
    public FieldDataContext fieldDataContext() {
        return fieldDataContext;
    }

    @Override
    public ValueTransformer valueTransformer() {
        return valueTransformer;
    }

    protected static abstract class Collector extends Aggregator.Collector {

        protected FieldDataContext fieldDataContext;
        protected ValueTransformer valueTransformer;
        protected AtomicFieldData[] fieldDatas;

        protected Collector(FieldDataContext fieldDataContext, ValueTransformer valueTransformer) {
            this.fieldDataContext = fieldDataContext;
            this.valueTransformer = valueTransformer;
            this.fieldDatas = new AtomicFieldData[fieldDataContext.indexFieldDatas().length];
        }

        @Override
        public void collect(int doc, AggregationContext context) throws IOException {
            valueTransformer.setNextDocId(doc);
            if (fieldDatas.length == 1) {
                collect(doc, fieldDataContext.fields()[0], fieldDatas[0], context);
            } else {
                for (int i = 0; i < fieldDatas.length; i++) {
                    collect(doc, fieldDataContext.fields()[i], fieldDatas[i], context);
                }
            }
        }

        public abstract void collect(int doc, String field, AtomicFieldData fieldData, AggregationContext context) throws IOException;

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            valueTransformer.setScorer(scorer);
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            valueTransformer.setNextReader(context);
            fieldDataContext.loadAtomic(context, fieldDatas);
        }
    }

    protected static abstract class Factory<A extends FieldDataCalcAggregator> implements Aggregator.Factory<A> {

        protected final String name;
        protected final FieldDataContext fieldDataContext;
        protected final ValueTransformer valueTransformer;

        protected Factory(String name, FieldDataContext fieldDataContext) {
            this(name, fieldDataContext, ValueTransformer.NONE);
        }

        protected Factory(String name, FieldDataContext fieldDataContext, ValueTransformer valueTransformer) {
            this.name = name;
            this.fieldDataContext = fieldDataContext;
            this.valueTransformer = valueTransformer;
        }
    }

}
