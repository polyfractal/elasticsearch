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

package org.elasticsearch.search.aggregations.bucket.multi.range.date;

import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorParser;
import org.elasticsearch.search.aggregations.bucket.multi.range.RangeAggregator;
import org.elasticsearch.search.aggregations.context.FieldContext;
import org.elasticsearch.search.aggregations.context.numeric.ValueFormatter;
import org.elasticsearch.search.aggregations.context.numeric.ValueParser;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class DateRangeParser implements AggregatorParser {

    private static final ValueParser DEFAULT_VALUE_PARSER = new ValueParser.DateMath(new DateMathParser(DateFieldMapper.Defaults.DATE_TIME_FORMATTER, DateFieldMapper.Defaults.TIME_UNIT));
    private static final ValueFormatter DEFAULT_VALUE_FORMATTER = new ValueFormatter.DateTime(DateFieldMapper.Defaults.DATE_TIME_FORMATTER);


    @Override
    public String type() {
        return InternalDateRange.TYPE.name();
    }

    @Override
    public Aggregator.Factory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        String field = null;
        List<RangeAggregator.Range> ranges = null;
        String script = null;
        String scriptLang = null;
        Map<String, Object> scriptParams = null;
        boolean keyed = false;
        boolean multiValued = true;
        String format = null;

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
                } else if ("format".equals(currentFieldName)) {
                    format = parser.text();
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("ranges".equals(currentFieldName)) {
                    ranges = new ArrayList<RangeAggregator.Range>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        double from = Double.NEGATIVE_INFINITY;
                        String fromAsStr = null;
                        double to = Double.POSITIVE_INFINITY;
                        String toAsStr = null;
                        String key = null;
                        String toOrFromOrKey = null;
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                toOrFromOrKey = parser.currentName();
                            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                                if ("from".equals(toOrFromOrKey)) {
                                    from = parser.doubleValue();
                                } else if ("to".equals(toOrFromOrKey)) {
                                    to = parser.doubleValue();
                                }
                            } else if (token == XContentParser.Token.VALUE_STRING) {
                                if ("from".equals(toOrFromOrKey)) {
                                    fromAsStr = parser.text();
                                } else if ("to".equals(toOrFromOrKey)) {
                                    toAsStr = parser.text();
                                } else if ("key".equals(toOrFromOrKey)) {
                                    key = parser.text();
                                }
                            }
                        }
                        ranges.add(new RangeAggregator.Range(key, from, fromAsStr, to, toAsStr));
                    }
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("params".equals(currentFieldName)) {
                    scriptParams = parser.map();
                }
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if ("keyed".equals(currentFieldName)) {
                    keyed = parser.booleanValue();
                } else if ("multi_valued".equals(currentFieldName) || "multiValued".equals(currentFieldName)) {
                    multiValued = parser.booleanValue();
                }
            }
        }

        if (ranges == null) {
            throw new SearchParseException(context, "Missing [ranges] in ranges aggregator [" + aggregationName + "]");
        }

        SearchScript searchScript = null;
        if (script != null) {
            searchScript = context.scriptService().search(context.lookup(), scriptLang, script, scriptParams);
        }

        ValueFormatter formatter = null;
        if (format != null) {
            formatter = new ValueFormatter.DateTime(format);
        }

        if (field == null) {

            if (searchScript != null) {
                if (formatter == null) {
                    formatter = DEFAULT_VALUE_FORMATTER;
                }
                return new RangeAggregator.ScriptFactory(aggregationName, searchScript, multiValued, formatter, DEFAULT_VALUE_PARSER, InternalDateRange.FACTORY, ranges, keyed);
            }

            // "field" doesn't exist, so we fall back to the context of the ancestors
            return new RangeAggregator.ContextBasedFactory(aggregationName, InternalDateRange.FACTORY, ranges, keyed);
        }


        FieldMapper mapper = context.smartNameFieldMapper(field);
        if (mapper == null) {
            return new UnmappedDateRangeAggregator.Factory(aggregationName, ranges, keyed, DEFAULT_VALUE_FORMATTER, DEFAULT_VALUE_PARSER);
        }

        if (!(mapper instanceof DateFieldMapper)) {
            throw new AggregationExecutionException("date_range aggregation can only be applied to date fields which is not the case with field [" + field + "]");
        }

        IndexFieldData indexFieldData = context.fieldData().getForField(mapper);
        FieldContext fieldContext = new FieldContext(field, indexFieldData, mapper);
        if (formatter == null) {
            formatter = new ValueFormatter.DateTime(((DateFieldMapper) mapper).dateTimeFormatter());
        }
        ValueParser valueParser = new ValueParser.DateMath(((DateFieldMapper) mapper).dateMathParser());
        return new RangeAggregator.FieldDataFactory(aggregationName, fieldContext, searchScript, formatter, valueParser, InternalDateRange.FACTORY, ranges, keyed);
    }
}
