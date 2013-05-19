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

package org.elasticsearch.search.aggregations.calc.stats;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.calc.ScriptCalcAggregator;
import org.elasticsearch.search.aggregations.context.AggregationContext;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class ScriptStatsAggregator<S extends Stats> extends ScriptCalcAggregator {

    private final Stats.Factory<S> statsFactory;

    S stats;

    public ScriptStatsAggregator(String name, SearchScript script, Stats.Factory<S> statsFactory, Aggregator parent) {
        super(name, script, parent);
        this.statsFactory = statsFactory;
    }

    @Override
    public Collector collector() {
        return new Collector(statsFactory.create(name));
    }

    @Override
    public InternalAggregation buildAggregation() {
        return stats;
    }

    class Collector extends Aggregator.Collector {

        S stats;

        Collector(S stats) {
            this.stats = stats;
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            script.setScorer(scorer);
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            script.setNextReader(context);
        }

        @Override
        public void collect(int doc, AggregationContext context) throws IOException {
            script.setNextDocId(doc);
            stats.collect(doc, script.runAsDouble());
            //TODO introduce _values var which will be a lookup for field values withing the current field data
            // context of the aggration
        }

        @Override
        public void postCollection() {
            ScriptStatsAggregator.this.stats = stats;
        }
    }

    public static class Factory<S extends Stats> extends ScriptCalcAggregator.Factory {

        private final Stats.Factory<S> statsFactory;

        public Factory(String name, Stats.Factory<S> statsFactory, String script, String lang, Map<String, Object> params, SearchContext context) {
            super(name, script, lang, params, context);
            this.statsFactory = statsFactory;
        }

        @Override
        public Aggregator create(Aggregator parent) {
            return new ScriptStatsAggregator<S>(name, script, statsFactory, parent);
        }
    }
}
