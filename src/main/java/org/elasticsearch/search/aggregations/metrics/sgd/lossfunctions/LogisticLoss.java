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
package org.elasticsearch.search.aggregations.metrics.sgd.lossfunctions;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.search.aggregations.metrics.sgd.SgdRegressor;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.util.Map;


public class LogisticLoss extends SgdRegressor {

    public LogisticLoss(long estimatedBucketsCount, AggregationContext context, double alpha) {
        super(context, estimatedBucketsCount, alpha);
    }

    protected double loss(double[] thetas, double[] xs, double y) {
        double y_hat = 0;
        for (int i = 0; i < thetas.length; i++) {
            y_hat += thetas[i] * xs[i];
        }
        return y - 1 / (1 + Math.exp(-y_hat));
    }


    public static class Factory implements SgdRegressor.Factory {

        private double alpha = 0.5;

        public Factory(Map<String, Object> settings) {
            if (settings != null) {
                Object alphaObject = settings.get("alpha");
                if (alphaObject != null) {
                    if (!(alphaObject instanceof Number)) {
                        throw new ElasticsearchIllegalArgumentException("alpha must be number, got a " + alphaObject.getClass());
                    }
                    alpha = ((Number) alphaObject).doubleValue();
                }

            }
        }

        public LogisticLoss create(long estimatedBucketCount, AggregationContext context) {
            return new LogisticLoss(estimatedBucketCount, context, alpha);
        }
    }

}