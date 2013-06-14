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

package org.elasticsearch.search.aggregations.bucket.multi.terms;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorParser;
import org.elasticsearch.search.aggregations.bucket.multi.terms.string.StringTerms;
import org.elasticsearch.search.aggregations.context.FieldContext;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class TermsParser implements AggregatorParser {

    @Override
    public String type() {
        return StringTerms.TYPE.name();
    }

    @Override
    public Aggregator.Factory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        String field = null;
        String script = null;
        String scriptLang = null;
        Map<String, Object> scriptParams = null;
        Terms.ValueType valueType = Terms.ValueType.STRING;
        int requiredSize = 10;
        String orderKey = "_count";
        boolean orderAsc = false;
        String format = null;
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
                } else if ("value_type".equals(currentFieldName) || "valueType".equals(currentFieldName)) {
                    valueType = Terms.ValueType.resolveType(parser.text());
                } else if ("format".equals(currentFieldName)) {
                    format = parser.text();
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("size".equals(currentFieldName)) {
                    requiredSize = parser.intValue();
                }
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if ("mult_valued".equals(currentFieldName) || "multiValued".equals(currentFieldName)) {
                    multiValued = parser.booleanValue();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("params".equals(currentFieldName)) {
                    scriptParams = parser.map();
                } else if ("order".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            orderKey = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_STRING) {
                            String dir = parser.text();
                            orderAsc = "asc".equalsIgnoreCase(dir);
                            //TODO: do we want to throw a parse error if the alternative is not "desc"???
                        }
                    }
                }
            }
        }

        Terms.Order order = resolveOrder(orderKey, orderAsc);
        SearchScript searchScript = null;
        if (script != null) {
            searchScript = context.scriptService().search(context.lookup(), scriptLang, script, scriptParams);
        }

        if (field == null) {
            return new TermsAggregatorFactory(aggregationName, null, searchScript, multiValued, order, requiredSize, valueType, format);
        }

        FieldMapper mapper = context.smartNameFieldMapper(field);
        if (mapper == null) {
            return null; // skipping aggregation on unmapped fields
        }
        IndexFieldData indexFieldData = context.fieldData().getForField(mapper);
        FieldContext fieldContext = new FieldContext(field, indexFieldData, mapper);
        return new TermsAggregatorFactory(aggregationName, fieldContext, searchScript, multiValued, order, requiredSize, valueType, format);
    }

    static Terms.Order resolveOrder(String key, boolean asc) {
        if ("_term".equals(key)) {
            return asc ? Terms.Order.Standard.TERM_ASC : Terms.Order.Standard.TERM_DESC;
        }
        if ("_count".equals(key)) {
            return asc ? Terms.Order.Standard.COUNT_ASC : Terms.Order.Standard.COUNT_DESC;
        }
        int i = key.indexOf('.');
        if (i < 0) {
            return Terms.Order.Aggregation.create(key, asc);
        }
        return Terms.Order.Aggregation.create(key.substring(0, i), key.substring(i+1), asc);
    }

}
