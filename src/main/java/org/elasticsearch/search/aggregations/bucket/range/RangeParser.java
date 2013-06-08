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

package org.elasticsearch.search.aggregations.bucket.range;

import com.google.common.collect.Lists;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorParser;
import org.elasticsearch.search.aggregations.context.FieldDataContext;
import org.elasticsearch.search.aggregations.context.ValueTransformer;
import org.elasticsearch.search.aggregations.format.ValueFormatter;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class RangeParser implements AggregatorParser {

    @Override
    public String type() {
        return InternalRange.TYPE.name();
    }

    @Override
    public Aggregator.Factory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        String field = null;
        List<String> fields = null;
        List<RangeAggregator.Range> ranges = null;
        String script = null;
        String scriptLang = null;
        Map<String, Object> scriptParams = null;
        boolean keyed = false;
        String format = null;

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
                    fields = new ArrayList<String>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        fields.add(parser.text());
                    }
                } else if ("ranges".equals(currentFieldName)) {
                    ranges = new ArrayList<RangeAggregator.Range>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        double from = Double.NEGATIVE_INFINITY;
                        double to = Double.POSITIVE_INFINITY;
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
                                if ("key".equals(toOrFromOrKey)) {
                                    key = parser.text();
                                }
                            }
                        }
                        ranges.add(new RangeAggregator.Range(key, from, to));
                    }
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("params".equals(currentFieldName)) {
                    scriptParams = parser.map();
                }
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if ("keyed".equals(currentFieldName)) {
                    keyed = parser.booleanValue();
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

        if (!fieldExists) {

            if (searchScript != null) {
                return new ScriptRangeAggregator.Factory(aggregationName, searchScript, ranges, keyed);
            }

            // "field" doesn't exist, so we fall back to the context of the ancestors
            return new RangeAggregator.Factory(aggregationName, null, ranges, keyed);
        }

        if (field != null) {
            FieldMapper mapper = context.smartNameFieldMapper(field);
            if (mapper == null) {
                return new UnmappedRangeAggregator.Factory(aggregationName, ranges, keyed);
            }
            ValueFormatter valueFormatter = null;
            if (format != null) {
                valueFormatter = mapper instanceof DateFieldMapper ? new ValueFormatter.DateTime(format) : new ValueFormatter.Number.Pattern(format);
            }
            IndexFieldData indexFieldData = context.fieldData().getForField(mapper);
            FieldDataContext fieldDataContext = new FieldDataContext(field, indexFieldData, context);
            if (searchScript != null) {
                return new RangeAggregator.Factory(aggregationName, fieldDataContext, new ValueTransformer.Script(searchScript), valueFormatter, ranges, keyed);
            }
            return new RangeAggregator.Factory(aggregationName, fieldDataContext, valueFormatter, ranges, keyed);
        }

        // fields is specified by the user

        if (fields.isEmpty()) {
            // the user specified an empty array... so we're falling back to the field context of the ancestors
            //TODO what do we do if the script is defined and the user defined an empty array of fields?
            return new RangeAggregator.Factory(aggregationName, null, ranges, keyed);
        }

        List<String> mappedFields = Lists.newArrayListWithCapacity(4);
        List<IndexFieldData> indexFieldDatas = Lists.newArrayListWithCapacity(4);
        ValueFormatter valueFormatter = null;
        for (String fieldName : fields) {
            FieldMapper mapper = context.smartNameFieldMapper(fieldName);
            if (mapper != null) {
                if (format != null && valueFormatter == null) {
                    valueFormatter = mapper instanceof DateFieldMapper ? new ValueFormatter.DateTime(format) : new ValueFormatter.Number.Pattern(format);
                }
                mappedFields.add(fieldName);
                indexFieldDatas.add(context.fieldData().getForField(mapper));
            }
        }

        if (mappedFields.isEmpty()) {
            return new UnmappedRangeAggregator.Factory(aggregationName, ranges, keyed);
        }

        FieldDataContext fieldDataContext = new FieldDataContext(mappedFields, indexFieldDatas, context);
        if (searchScript != null) {
            return new RangeAggregator.Factory(aggregationName, fieldDataContext, new ValueTransformer.Script(searchScript), valueFormatter, ranges, keyed);
        }
        return new RangeAggregator.Factory(aggregationName, fieldDataContext, valueFormatter, ranges, keyed);
    }
}
