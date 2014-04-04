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
package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.aggregations.support.format.ValueParser;

import java.util.ArrayList;

/**
 *
 */
public abstract class MultiValuesSourceAggregatorFactory<VS extends ValuesSource> extends AggregatorFactory {

    public static abstract class LeafOnly<VS extends ValuesSource> extends MultiValuesSourceAggregatorFactory<VS> {

        protected LeafOnly(String name, String type, ArrayList<ValuesSourceConfig<VS>> valuesSourceConfigs) {
            super(name, type, valuesSourceConfigs);
        }

        protected LeafOnly(String name, String type, ArrayList<ValuesSourceConfig<VS>> valuesSourceConfigs, ValueFormatter formatter, ValueParser parser) {
            super(name, type, valuesSourceConfigs, formatter, parser);
        }

        @Override
        public AggregatorFactory subFactories(AggregatorFactories subFactories) {
            throw new AggregationInitializationException("Aggregator [" + name + "] of type [" + type + "] cannot accept sub-aggregations");
        }
    }

    protected ArrayList<ValuesSourceConfig<VS>> configs;
    protected ValueFormatter formatter;
    protected ValueParser parser;

    protected MultiValuesSourceAggregatorFactory(String name, String type, ArrayList<ValuesSourceConfig<VS>> configs) {
        this(name, type, configs, null, null);
    }

    protected MultiValuesSourceAggregatorFactory(String name, String type, ArrayList<ValuesSourceConfig<VS>> configs, ValueFormatter formatter, ValueParser parser) {
        super(name, type);
        this.configs = configs;
        this.formatter = formatter;
        this.parser = parser;
    }

    @Override
    public Aggregator create(AggregationContext context, Aggregator parent, long expectedBucketsCount) {
        /*
        if (config.unmapped()) {
            return createUnmapped(context, parent);
        }
        */
        ArrayList<VS> vss = new ArrayList<>(configs.size());
        for (ValuesSourceConfig<VS> config : configs) {
            VS vs = context.valuesSource(config, parent == null ? 0 : 1 + parent.depth());
            vss.add(vs);
        }

        return create(vss, expectedBucketsCount, context, parent);
    }

    @Override
    public void doValidate() {
        /*
        if (config == null || !config.valid()) {
            resolveValuesSourceConfigFromAncestors(name, parent, config.valueSourceType());
        }
        */
    }

    protected abstract Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent);

    protected abstract Aggregator create(ArrayList<VS> valuesSources, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent);

    private void resolveValuesSourceConfigFromAncestors(String aggName, AggregatorFactory parent, Class<VS> requiredValuesSourceType) {
        /*
        ValuesSourceConfig config;
        while (parent != null) {
            if (parent instanceof MultiValuesSourceAggregatorFactory) {
                config = ((MultiValuesSourceAggregatorFactory) parent).config;
                if (config != null && config.valid()) {
                    if (requiredValuesSourceType == null || requiredValuesSourceType.isAssignableFrom(config.valueSourceType)) {
                        this.config = config;
                        this.formatter = ((MultiValuesSourceAggregatorFactory) parent).formatter;
                        this.parser = ((MultiValuesSourceAggregatorFactory) parent).parser;
                        return;
                    }
                }
            }
            parent = parent.parent();
        }
        throw new AggregationExecutionException("could not find the appropriate value context to perform aggregation [" + aggName + "]");
        */
    }
}
