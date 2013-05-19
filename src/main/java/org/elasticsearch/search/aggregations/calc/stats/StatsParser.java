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

import com.google.common.collect.Lists;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorParser;
import org.elasticsearch.search.aggregations.context.FieldDataContext;
import org.elasticsearch.search.aggregations.context.ValueTransformer;
import org.elasticsearch.search.aggregations.format.ValueFormatter;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
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
        String[] fields = null;
        String script = null;
        String scriptLang = null;
        String format = null;
        Map<String, Object> scriptParams = null;


        boolean fieldExists = false;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("field".equals(currentFieldName)) {
                    fieldExists = true;
                    field = parser.text();
                } else if ("script".equals(currentFieldName)) {
                    script = parser.text();
                } else if ("script_lang".equals(currentFieldName) || "scriptLang".equals(currentFieldName)) {
                    scriptLang = parser.text();
                } else if ("format".equals(currentFieldName)) {
                    format = parser.text();
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("field".equals(currentFieldName)) {
                    fieldExists = true;
                    List<String> fieldList = Lists.newArrayListWithCapacity(4);
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        fieldList.add(parser.text());
                    }
                    fields = fieldList.toArray(new String[fieldList.size()]);
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("params".equals(currentFieldName)) {
                    scriptParams = parser.map();
                }
            }
        }

        ValueFormatter formatter = format == null ? null : new ValueFormatter.Number.Pattern(format);

        if (!fieldExists) {
            if (script != null) {
                return new ScriptStatsAggregator.Factory<S>(aggregationName, statsFactory, script, scriptLang, scriptParams, context);
            }
            // both "field" and "script" don't exist, so we fall back to the field context of the ancestors
            return new StatsAggregator.Factory<S>(aggregationName, formatter, statsFactory);
        }

        if (field != null) {
            FieldMapper mapper = context.smartNameFieldMapper(field);
            if (mapper != null) {
                IndexFieldData indexFieldData = context.fieldData().getForField(mapper);
                FieldDataContext fieldDataContext = new FieldDataContext(field, indexFieldData);
                if (script == null) {
                    return new StatsAggregator.Factory<S>(aggregationName, formatter, statsFactory, fieldDataContext);
                } else {
                    ValueTransformer.Script transformer = new ValueTransformer.Script(context, script, scriptLang, scriptParams);
                    return new StatsAggregator.Factory<S>(aggregationName, formatter, statsFactory, fieldDataContext, transformer);
                }
            } else {
                return new UnmappedStatsAggregator.Factory<S>(aggregationName, statsFactory);
            }
        }


        if (fields.length == 0) {
            // if there is an empty "field" array, we fall back to the context of the ancestors
            return new StatsAggregator.Factory<S>(aggregationName, formatter, statsFactory);
        }

        List<IndexFieldData> indexFieldDatas = Lists.newArrayListWithCapacity(fields.length);
        List<String> mappedFields = Lists.newArrayListWithCapacity(fields.length);
        for (int i = 0; i < fields.length; i++) {
            FieldMapper mapper = context.smartNameFieldMapper(fields[i]);
            if (mapper != null) {
                indexFieldDatas.add(context.fieldData().getForField(mapper));
                mappedFields.add(fields[i]);
            }
        }

        if (indexFieldDatas.isEmpty()) {
            return new UnmappedStatsAggregator.Factory<S>(aggregationName, statsFactory);
        }

        FieldDataContext fieldDataContext = new FieldDataContext(mappedFields, indexFieldDatas);

        if (script == null) {
            return new StatsAggregator.Factory<S>(aggregationName, formatter, statsFactory, fieldDataContext);
        }

        return new StatsAggregator.Factory<S>(aggregationName, formatter, statsFactory, fieldDataContext);
    }
}
