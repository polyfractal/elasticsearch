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

package org.elasticsearch.search.aggregations.bucket.histogram;

import com.google.common.collect.Lists;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorParser;
import org.elasticsearch.search.aggregations.context.FieldDataContext;
import org.elasticsearch.search.aggregations.context.ValueTransformer;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
        List<String> fields = null;
        String script = null;
        String scriptLang = null;
        Map<String, Object> scriptParams = null;
        boolean keyed = false;
        InternalOrder order = Histogram.Order.Standard.KEY_ASC;
        long interval = -1;

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
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("interval".equals(currentFieldName)) {
                    interval = parser.longValue();
                }
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if ("keyed".equals(currentFieldName)) {
                    keyed = parser.booleanValue();
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("field".equals(currentFieldName)) {
                    fieldExists = true;
                    fields = new ArrayList<String>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        fields.add(parser.text());
                    }
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
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if ("keyed".equals(currentFieldName)) {
                    keyed = parser.booleanValue();
                }
            }
        }

        if (interval < 0) {
            throw new SearchParseException(context, "Missing required field [interval] for histogram aggregation [" + aggregationName + "]");
        }

        SearchScript searchScript = null;
        if (script != null) {
            searchScript = context.scriptService().search(context.lookup(), scriptLang, script, scriptParams);
        }

        if (!fieldExists) {

            if (searchScript != null) {
                return new ScriptHistogramAggregator.Factory(aggregationName, searchScript, interval, order, keyed);
            }

            // falling back on the aggregation field data context
            return new HistogramAggregator.Factory(aggregationName, interval, order, keyed);
        }

        if (field != null) {
            FieldMapper mapper = context.smartNameFieldMapper(field);
            if (mapper == null) {
                return new UnmappedHistogramAggregator.Factory(aggregationName, order, keyed);
            }
            IndexFieldData indexFieldData = context.fieldData().getForField(mapper);
            FieldDataContext fieldDataContext = new FieldDataContext(field, indexFieldData);
            if (searchScript != null) {
                return new HistogramAggregator.Factory(aggregationName, fieldDataContext, new ValueTransformer.Script(searchScript), interval, order, keyed);
            }
            return new HistogramAggregator.Factory(aggregationName, fieldDataContext, ValueTransformer.NONE, interval, order, keyed);
        }

        // multiple fields are specified by the user

        if (fields.isEmpty()) {
            // the user specified an empty array... so we're falling back to the field context of the ancestors
            //TODO what do we do if the script is defined and the user defined an empty array of fields?
            return new HistogramAggregator.Factory(aggregationName, interval, order, keyed);
        }

        List<String> mappedFields = Lists.newArrayListWithCapacity(4);
        List<IndexFieldData> indexFieldDatas = Lists.newArrayListWithCapacity(4);
        for (String fieldName : fields) {
            FieldMapper mapper = context.smartNameFieldMapper(fieldName);
            if (mapper != null) {
                mappedFields.add(fieldName);
                indexFieldDatas.add(context.fieldData().getForField(mapper));
            }
        }

        if (mappedFields.isEmpty()) {
            return new UnmappedHistogramAggregator.Factory(aggregationName, order, keyed);
        }

        FieldDataContext fieldDataContext = new FieldDataContext(mappedFields, indexFieldDatas);
        if (searchScript != null) {
            return new HistogramAggregator.Factory(aggregationName, fieldDataContext, new ValueTransformer.Script(searchScript), interval, order, keyed);
        }

        return new HistogramAggregator.Factory(aggregationName, fieldDataContext, ValueTransformer.NONE, interval, order, keyed);

    }

    static InternalOrder resolveOrder(String key, boolean asc) {
        if ("_key".equals(key)) {
            return asc ? Histogram.Order.Standard.KEY_ASC : Histogram.Order.Standard.KEY_DESC;
        }
        if ("_count".equals(key)) {
            return asc ? Histogram.Order.Standard.COUNT_ASC : Histogram.Order.Standard.COUNT_DESC;
        }
        int i = key.indexOf('.');
        if (i < 0) {
            return Histogram.Order.Aggregation.create(key, asc);
        }
        return Histogram.Order.Aggregation.create(key.substring(0, i), key.substring(i+1), asc);
    }
}
