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

package org.elasticsearch.search.aggregations.context.values;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.script.SearchScript;

import java.io.IOException;

/**
 *
 */
public class FieldDataValuesSource implements ValuesSource {

    private final String field;
    private final IndexFieldData indexFieldData;
    private final SearchScript valueScript;

    private AtomicFieldData fieldData;

    public FieldDataValuesSource(String field, IndexFieldData indexFieldData) {
        this(field, indexFieldData, null);
    }

    public FieldDataValuesSource(String field, IndexFieldData indexFieldData, SearchScript valueScript) {
        this.field = field;
        this.indexFieldData = indexFieldData;
        this.valueScript = valueScript;
    }

    @Override
    public String id() {
        return field;
    }

    @Override
    public void setNextScorer(Scorer scorer) {
        if (valueScript != null) {
            valueScript.setScorer(scorer);
        }
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        fieldData = indexFieldData != null ? indexFieldData.load(reader) : null;
        if (valueScript != null) {
            valueScript.setNextReader(reader);
        }
    }

    @Override
    public DoubleValues doubleValues() throws IOException {
        DoubleValues values = fieldData != null ? ((AtomicNumericFieldData) fieldData).getDoubleValues() : null;
        if (values == null) {
            return null;
        }
        return valueScript != null ? new ValueScriptDoubleValues(values, valueScript) : values;
    }

    @Override
    public LongValues longValues() throws IOException {
        LongValues values = fieldData != null ? ((AtomicNumericFieldData) fieldData).getLongValues() : null;
        if (values == null) {
            return null;
        }
        return valueScript != null ? new ValueScriptLongValues(values, valueScript) : values;
    }

    @Override
    public BytesValues bytesValues() throws IOException {
        BytesValues values = fieldData != null ? fieldData.getBytesValues() : null;
        if (values == null) {
            return null;
        }
        return valueScript != null ? new ValueScriptBytesValues(values, valueScript) : values;
    }

    @Override
    public GeoPointValues geoPointValues() throws IOException {
        //TODO implement
        return null;
    }

}
