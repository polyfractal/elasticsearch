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

import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.internal.SearchContext;

import java.util.Map;

/**
 *
 */
public abstract class ScriptCalcAggregator extends CalcAggregator {

    protected SearchScript script;

    public ScriptCalcAggregator(String name, SearchScript script, Aggregator parent) {
        super(name, parent);
        this.script = script;
    }

    public static abstract class Factory implements Aggregator.Factory {

        protected final String name;
        protected final SearchScript script;

        public Factory(String name, String script, String lang, Map<String, Object> params, SearchContext context) {
            this.name = name;
            this.script = context.scriptService().search(context.lookup(), lang, script, params);
        }
    }

}
