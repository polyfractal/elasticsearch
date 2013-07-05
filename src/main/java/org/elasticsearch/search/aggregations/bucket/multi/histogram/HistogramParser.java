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

package org.elasticsearch.search.aggregations.bucket.multi.histogram;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorParser;
import org.elasticsearch.search.aggregations.context.FieldContext;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 * Parses the histogram request
 */
public class HistogramParser implements AggregatorParser {

    @Override
    public String type() {
        return InternalHistogram.TYPE.name();
    }

    @Override
    public Aggregator.Factory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        String field = null;
        String script = null;
        String scriptLang = null;
        Map<String, Object> scriptParams = null;
        boolean keyed = false;
        InternalOrder order = HistogramBase.Order.KEY_ASC;
        long interval = -1;
        boolean multiValued = true;

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
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("interval".equals(currentFieldName)) {
                    interval = parser.longValue();
                }
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if ("keyed".equals(currentFieldName)) {
                    keyed = parser.booleanValue();
                } else if ("multi_valued".equals(currentFieldName) || "multiValued".equals(currentFieldName)) {
                    multiValued = parser.booleanValue();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("params".equals(currentFieldName)) {
                    scriptParams = parser.map();
                } else if ("order".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_STRING) {
                            String dir = parser.text();
                            boolean asc = "asc".equals(dir);
                            order = resolveOrder(currentFieldName, asc);
                            //TODO should we throw an error if the value is not "asc" or "desc"???
                        }
                    }
                }
            }
        }

        InternalHistogram.Factory histogramFactory = new InternalHistogram.Factory();

        if (interval < 0) {
            throw new SearchParseException(context, "Missing required field [interval] for histogram aggregation [" + aggregationName + "]");
        }
        Rounding rounding = new Rounding.Interval(interval);

        SearchScript searchScript = null;
        if (script != null) {
            searchScript = context.scriptService().search(context.lookup(), scriptLang, script, scriptParams);
        }

        if (field == null) {

            if (searchScript != null) {
                return new HistogramAggregator.ScriptFactory(aggregationName, searchScript, multiValued, rounding, order, keyed, null, histogramFactory);
            }

            // falling back on the get field data context
            return new HistogramAggregator.ContextBasedFactory(aggregationName, rounding, order, keyed, histogramFactory);
        }

        FieldMapper mapper = context.smartNameFieldMapper(field);
        if (mapper == null) {
            return new UnmappedHistogramAggregator.Factory(aggregationName, order, keyed);
        }

        IndexFieldData indexFieldData = context.fieldData().getForField(mapper);
        FieldContext fieldContext = new FieldContext(field, indexFieldData, mapper);
        return new HistogramAggregator.FieldDataFactory(aggregationName, fieldContext, searchScript, rounding, order, keyed, null, histogramFactory);

    }

    static InternalOrder resolveOrder(String key, boolean asc) {
        if ("_key".equals(key)) {
            return asc ? HistogramBase.Order.KEY_ASC : HistogramBase.Order.KEY_DESC;
        }
        if ("_count".equals(key)) {
            return asc ? HistogramBase.Order.COUNT_ASC : HistogramBase.Order.COUNT_DESC;
        }
        int i = key.indexOf('.');
        if (i < 0) {
            return HistogramBase.Order.aggregation(key, asc);
        }
        return HistogramBase.Order.aggregation(key.substring(0, i), key.substring(i+1), asc);
    }
}
