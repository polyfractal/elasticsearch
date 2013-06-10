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

package org.elasticsearch.search.aggregations.context.doubles;

import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.context.ValuesSource;

import java.io.IOException;

/**
 *
 */
public interface DoubleValuesSource extends ValuesSource {

    static final DoubleValuesSource EMPTY = new Empty();

    DoubleValues values() throws IOException;


    public static class Empty extends ValuesSource.Empty implements DoubleValuesSource {

        @Override
        public DoubleValues values() throws IOException {
            return null;
        }
    }

    public static class FieldData extends ValuesSource.FieldData<DoubleValues> implements DoubleValuesSource {

        public FieldData(String field, IndexFieldData indexFieldData) {
            super(field, indexFieldData);
        }

        public FieldData(String field, IndexFieldData indexFieldData, SearchScript valueScript) {
            super(field, indexFieldData, valueScript);
        }

        @Override
        public DoubleValues loadValues() throws IOException {
            DoubleValues values = fieldData != null ? ((AtomicNumericFieldData) fieldData).getDoubleValues() : null;
            if (values == null) {
                return null;
            }
            return valueScript != null ? new ValueScriptDoubleValues(values, valueScript) : values;
        }
    }

    public static class Script extends ValuesSource.Script<DoubleValues> implements DoubleValuesSource {

        public Script(SearchScript script) {
            super(script);
        }

        @Override
        public DoubleValues createValues(SearchScript script) throws IOException {
            return new ScriptDoubleValues(script);
        }
    }
}
