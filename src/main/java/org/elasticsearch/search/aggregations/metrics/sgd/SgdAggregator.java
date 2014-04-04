/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.aggregations.metrics.sgd;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.support.*;

import java.io.IOException;
import java.util.ArrayList;

import static java.lang.Math.min;

/**
 *
 */
public class SgdAggregator extends MetricsAggregator.SingleValue {

    private final ArrayList<ValuesSource.Numeric> valuesSources;
    private DoubleValues[] values;

    private final SgdRegressor regressor;
    private final boolean displayThetas;
    private final double[] predictXs;


    public SgdAggregator(String name, long estimatedBucketsCount, ArrayList<ValuesSource.Numeric> valuesSources, AggregationContext context,
                         Aggregator parent, SgdRegressor regressor, boolean displayThetas, double[] predictXs) {
        super(name, estimatedBucketsCount, context, parent);
        this.valuesSources = valuesSources;
        this.values = new DoubleValues[valuesSources.size()];
        this.displayThetas = displayThetas;
        this.regressor = regressor;
        this.predictXs = predictXs;
    }

    @Override
    public boolean shouldCollect() {

        boolean shouldCollect = true;
        for (ValuesSource valueSource : valuesSources) {
            if (valueSource == null) {
                shouldCollect = false;
                break;
            }
        }
        return shouldCollect;
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        for (int i = 0; i < valuesSources.size(); ++i) {
            values[i] = valuesSources.get(i).doubleValues();
        }
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {

        double y;

        // -1 for first value being y, +1 for dummy intercept
        double[] xs = new double[valuesSources.size()];

        for (int i = 0; i < valuesSources.size(); ++i) {
            values[i].setDocument(doc);
        }

        y = values[0].nextValue();

        xs[0] = 1;  //dummy intercept
        for (int j = 1; j < valuesSources.size(); ++j) {
            xs[j] = values[j].nextValue();
        }

        regressor.step(xs, y, owningBucketOrdinal);

    }

    @Override
    public double metric(long owningBucketOrd) {
        return 0;
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        /*
        if (valuesSource == null) {
            return new InternalSum(name, 0);
        }
        */
        return new InternalSgd(name, regressor.thetas(owningBucketOrdinal), predictXs, displayThetas);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalSgd(name, regressor.emptyResult(), predictXs, displayThetas);      //TODO don't really need to pass predictXs
    }

    @Override
    protected void doRelease() {
        regressor.release();
    }

    public static class Factory extends MultiValuesSourceAggregatorFactory.LeafOnly<ValuesSource.Numeric> {

        private final SgdRegressor.Factory estimatorFactory;

        private final boolean keyed;
        private final double[] predictXs;

        public Factory(String name, ArrayList<ValuesSourceConfig<ValuesSource.Numeric>> valuesSourceConfigs, SgdRegressor.Factory estimatorFactory, boolean keyed, double[] predictXs) {
            super(name, InternalSgd.TYPE.name(), valuesSourceConfigs);
            this.estimatorFactory = estimatorFactory;
            this.keyed = keyed;
            this.predictXs = predictXs;
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            return new SgdAggregator(name, 0, null, aggregationContext, parent, estimatorFactory.create(0, aggregationContext), keyed, predictXs);
        }

        @Override
        protected Aggregator create(ArrayList<ValuesSource.Numeric> valuesSources, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
            SgdRegressor estimator = estimatorFactory.create( expectedBucketsCount, aggregationContext);
            return new SgdAggregator(name, expectedBucketsCount, valuesSources, aggregationContext, parent, estimator, keyed, predictXs);
        }
    }

}
