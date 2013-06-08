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

package org.elasticsearch.search.aggregations.bucket.missing;

import com.google.common.collect.Lists;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorParser;
import org.elasticsearch.search.aggregations.context.FieldDataContext;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class MissingParser implements AggregatorParser {

    @Override
    public String type() {
        return InternalMissing.TYPE.name();
    }

    @Override
    public Aggregator.Factory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        String field = null;
        List<String> fields = null;

        boolean fieldExists = false;
        boolean mapperExists = false;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("field".equals(currentFieldName)) {
                    fieldExists = true;
                    field = parser.text();
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("field".equals(currentFieldName)) {
                    fieldExists = true;
                    fields = new ArrayList<String>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        fields.add(parser.text());
                    }
                }
            }
        }

        if (!fieldExists) {
            // "field" doesn't exist, so we fall back to the context of the ancestors
            return new MissingAggregator.Factory(aggregationName);
        }

        if (field != null) {
            FieldMapper mapper = context.smartNameFieldMapper(field);
            if (mapper == null) {
                return new UnmappedMissingAggregator.Factory(aggregationName);
            }
            FieldDataContext fieldDataContext = new FieldDataContext(field, context.fieldData().getForField(mapper), context);
            return new MissingAggregator.Factory(aggregationName, fieldDataContext);
        }

        // we have multiple fields

        if (fields.isEmpty()) {
            // the "field" array is empty, so we fall back to the context of the ancestors
            return new MissingAggregator.Factory(aggregationName);
        }

        List<IndexFieldData> indexFieldDatas = Lists.newArrayListWithCapacity(4);
        List<String> mappedFields = Lists.newArrayListWithCapacity(4);
        FieldMapper mapper = null;
        for (String fieldName : fields) {
            mapper = context.smartNameFieldMapper(fieldName);
            if (mapper != null) {
                mappedFields.add(fieldName);
                indexFieldDatas.add(context.fieldData().getForField(mapper));
            }
        }

        if (mappedFields.isEmpty()) {
            return new UnmappedMissingAggregator.Factory(aggregationName);
        }

        FieldDataContext fieldDataContext = new FieldDataContext(mappedFields, indexFieldDatas, context);
        return new MissingAggregator.Factory(aggregationName, fieldDataContext);

    }
}
