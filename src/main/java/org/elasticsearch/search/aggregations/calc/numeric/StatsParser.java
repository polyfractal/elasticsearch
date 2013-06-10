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

package org.elasticsearch.search.aggregations.calc.numeric;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorParser;
import org.elasticsearch.search.aggregations.context.FieldDataContext;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class StatsParser<S extends Stats> implements AggregatorParser {

    private final String type;
    private final Stats.Factory<S> statsFactory;

    public StatsParser(String type, Stats.Factory<S> statsFactory) {
        this.type = type;
        this.statsFactory = statsFactory;
    }

    @Override
    public String type() {
        return type;
    }

    @Override
    public Aggregator.Factory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        String field = null;
        String script = null;
        String scriptLang = null;
        Map<String, Object> scriptParams = null;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("field".equals(currentFieldName)) {
                    field = parser.text();
                } else if ("script".equals(currentFieldName)) {
                    script = parser.text();
                } else if ("script_lang".equals(currentFieldName) || "scriptLang".equals(currentFieldName)) {
                    scriptLang = parser.text();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("params".equals(currentFieldName)) {
                    scriptParams = parser.map();
                }
            }
        }

        SearchScript searchScript = null;
        if (script != null) {
            searchScript = context.scriptService().search(context.lookup(), scriptLang, script, scriptParams);
        }

        if (field == null) {
            if (searchScript != null) {
                return new NumberAggregator.ScriptFactory<S>(aggregationName, searchScript, statsFactory);
            }
            // both "field" and "script" don't exist, so we fall back to the field context of the ancestors
            return new NumberAggregator.ContextBasedFactory<S>(aggregationName, statsFactory);
        }

        FieldMapper mapper = context.smartNameFieldMapper(field);
        if (mapper != null) {
            IndexFieldData indexFieldData = context.fieldData().getForField(mapper);
            FieldDataContext fieldDataContext = new FieldDataContext(field, indexFieldData, context);
            if (searchScript == null) {
                return new NumberAggregator.FieldDataFactory<S>(aggregationName, statsFactory, fieldDataContext);
            } else {
                return new NumberAggregator.FieldDataFactory<S>(aggregationName, statsFactory, fieldDataContext, searchScript);
            }
        }

        return new UnmappedStatsAggregator.Factory<S>(aggregationName, statsFactory);
    }
}
