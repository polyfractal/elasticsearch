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

package org.elasticsearch.search.aggregations;

import com.google.common.collect.Lists;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.search.aggregations.bucket.filter.FilterParser;
import org.elasticsearch.search.aggregations.bucket.geo.distance.GeoDistanceParser;
import org.elasticsearch.search.aggregations.bucket.global.GlobalParser;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramParser;
import org.elasticsearch.search.aggregations.bucket.histogram.date.DateHistogramParser;
import org.elasticsearch.search.aggregations.bucket.missing.MissingParser;
import org.elasticsearch.search.aggregations.bucket.range.RangeParser;
import org.elasticsearch.search.aggregations.bucket.terms.TermsParser;
import org.elasticsearch.search.aggregations.calc.count.CountParser;
import org.elasticsearch.search.aggregations.calc.numeric.avg.AvgParser;
import org.elasticsearch.search.aggregations.calc.numeric.max.MaxParser;
import org.elasticsearch.search.aggregations.calc.numeric.min.MinParser;
import org.elasticsearch.search.aggregations.calc.numeric.stats.ExtendedStatsParser;
import org.elasticsearch.search.aggregations.calc.numeric.stats.StatsParser;
import org.elasticsearch.search.aggregations.calc.numeric.sum.SumParser;

import java.util.List;

/**
 * The main module for the aggregation (binding all aggregation components together)
 */
public class AggregationModule extends AbstractModule {

    private List<Class<? extends AggregatorParser>> parsers = Lists.newArrayList();

    public AggregationModule() {
        parsers.add(AvgParser.class);
        parsers.add(SumParser.class);
        parsers.add(MinParser.class);
        parsers.add(MaxParser.class);
        parsers.add(StatsParser.class);
        parsers.add(ExtendedStatsParser.class);
        parsers.add(CountParser.class);

        parsers.add(GlobalParser.class);
        parsers.add(MissingParser.class);
        parsers.add(FilterParser.class);
        parsers.add(TermsParser.class);
        parsers.add(RangeParser.class);
        parsers.add(HistogramParser.class);
        parsers.add(DateHistogramParser.class);
        parsers.add(GeoDistanceParser.class);
    }

    /**
     * Enabling extending the aggregation module by adding a custom aggregation parser.
     *
     * @param parser The parser for the custom aggregator.
     */
    public void addAggregatorParser(Class<? extends AggregatorParser> parser) {
        parsers.add(parser);
    }

    @Override
    protected void configure() {
        Multibinder<AggregatorParser> multibinder = Multibinder.newSetBinder(binder(), AggregatorParser.class);
        for (Class<? extends AggregatorParser> parser : parsers) {
            multibinder.addBinding().to(parser);
        }
        bind(AggregatorParsers.class).asEagerSingleton();
        bind(AggregationParseElement.class).asEagerSingleton();
        bind(AggregationPhase.class).asEagerSingleton();
    }

}
