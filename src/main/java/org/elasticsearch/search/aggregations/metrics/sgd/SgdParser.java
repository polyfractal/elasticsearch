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

import com.google.common.primitives.Doubles;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.metrics.sgd.lossfunctions.LogisticLoss;
import org.elasticsearch.search.aggregations.metrics.sgd.lossfunctions.SquaredLoss;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class SgdParser implements Aggregator.Parser {

    @Override
    public String type() {
        return InternalSgd.TYPE.name();
    }

    /**
     * We must override the parse method because we need to allow custom parameters
     * (execution_hint, etc) which is not possible otherwise
     */
    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        ArrayList<ValuesSourceConfig<ValuesSource.Numeric>> configs; // new ValuesSourceConfig<>(ValuesSource.Numeric.class);

        String y = null;
        String script = null;
        String scriptLang = null;

        SgdRegressor.Factory regressorFactory = null;
        RegressorType regressorType = null;

        String[] xs = null;
        double[] predictXs = null;
        Map<String, Object> scriptParams = null;
        boolean displayThetas = false;
        Map<String, Object> settings = null;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("y".equals(currentFieldName)) {
                    y = parser.text();
                } else if ("regressor".equals(currentFieldName)) {
                    regressorType = RegressorType.resolve(parser.text(), context);
                } else if ("script".equals(currentFieldName)) {
                    script = parser.text();
                } else if ("lang".equals(currentFieldName)) {
                    scriptLang = parser.text();
                } else {
                    if (settings == null) {
                        settings = new HashMap<>();
                    }
                    settings.put(currentFieldName, parser.text());
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("xs".equals(currentFieldName)) {
                    ArrayList<String> values = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        values.add(parser.text());
                    }
                    xs   = new String[values.size()];
                    xs = values.toArray(xs);
                } else if ("predict".equals(currentFieldName)) {
                    ArrayList<Double> values = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        values.add(parser.doubleValue());
                    }
                    predictXs = Doubles.toArray(values);
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("params".equals(currentFieldName)) {
                    scriptParams = parser.map();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if ("display_thetas".equals(currentFieldName)) {
                    displayThetas = parser.booleanValue();
                } else {
                    if (settings == null) {
                        settings = new HashMap<>();
                    }
                    settings.put(currentFieldName, parser.booleanValue());
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if (settings == null) {
                    settings = new HashMap<>();
                }
                settings.put(currentFieldName, parser.numberValue());
            } else {
                throw new SearchParseException(context, "Unexpected token " + token + " in [" + aggregationName + "].");
            }
        }

        if (regressorType == null) {
           regressorFactory = RegressorType.SQUARED.regressorFactory(settings);
        } else {
            regressorFactory = regressorType.regressorFactory(settings);
        }


        if (script != null) {
            //config.script(context.scriptService().search(context.lookup(), scriptLang, script, scriptParams));
        }

        if (y == null) {
            throw new SearchParseException(context, "y field must be specified for " + aggregationName + ".");
        }

        if (xs == null || xs.length == 0) {
            throw new SearchParseException(context, "xs must contain one or more fields" + aggregationName + ".");
        }

        if (predictXs == null || predictXs.length == 0) {
            throw new SearchParseException(context, "predict values must be supplied for regression for " + aggregationName + ".");
        }

        if (predictXs.length != xs.length) {
            throw new SearchParseException(context, "Must have same number of inputs as prediction values for " + aggregationName + ".");
        }


        configs = new ArrayList<>(xs.length + 1);

        FieldMapper<?>[] mappers = new FieldMapper<?>[xs.length + 1];
        mappers[0] = context.smartNameFieldMapper(y);
        for (int i = 0; i < xs.length; i++) {
            mappers[i+1] = context.smartNameFieldMapper(xs[i]);
        }

        //if (mapper == null) {
        //    config.unmapped(true);
            //return new SgdAggregator.Factory(aggregationName, config, regressorFactory, keyed);
        //}

        IndexFieldData<?>[] indexFieldData = new IndexFieldData<?>[xs.length + 1];

        indexFieldData[0] = context.fieldData().getForField(mappers[0]);

        ValuesSourceConfig<ValuesSource.Numeric> config = new ValuesSourceConfig<>(ValuesSource.Numeric.class);
        config.fieldContext(new FieldContext(y, indexFieldData[0]));
        configs.add(config);

        for (int i = 0; i < xs.length; i++) {
            indexFieldData[i+1] = context.fieldData().getForField(mappers[i+1]);
            config = new ValuesSourceConfig<>(ValuesSource.Numeric.class);
            config.fieldContext(new FieldContext(xs[i], indexFieldData[i + 1]));
            configs.add(config);
        }

        return new SgdAggregator.Factory(aggregationName, configs, regressorFactory, displayThetas, predictXs);
    }

    /**
     *
     */
    public static enum RegressorType {
        SQUARED() {
            @Override
            public SgdRegressor.Factory regressorFactory(Map<String, Object> settings) {
                return new SquaredLoss.Factory(settings);
            }
        },
        LOGISTIC() {
            @Override
            public SgdRegressor.Factory regressorFactory(Map<String, Object> settings) {
                return new LogisticLoss.Factory(settings);
            }
        };

        public abstract SgdRegressor.Factory regressorFactory(Map<String, Object> settings);

        public static RegressorType resolve(String name, SearchContext context) {
            if (name.equals("squared")) {
                return SQUARED;
            } else if (name.equals("logistic")) {
                return LOGISTIC;
            }
            throw new SearchParseException(context, "Unknown regressor type [" + name + "]");
        }

    }
}
