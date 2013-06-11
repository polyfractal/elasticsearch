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

package org.elasticsearch.search.aggregations.context.longs;

import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.context.ValuesSource;

import java.io.IOException;

/**
 *
 */
public interface LongValuesSource extends ValuesSource {

    static final LongValuesSource EMPTY = new Empty();

    LongValues values() throws IOException;


    public static class Empty extends ValuesSource.Empty implements LongValuesSource {

        @Override
        public LongValues values() throws IOException {
            return null;
        }

    }

    public static class FieldData extends ValuesSource.FieldData<LongValues> implements LongValuesSource {

        public FieldData(String field, IndexFieldData indexFieldData) {
            super(field, indexFieldData);
        }

        public FieldData(String field, IndexFieldData indexFieldData, SearchScript valueScript) {
            super(field, indexFieldData, valueScript);
        }

        @Override
        public LongValues loadValues() throws IOException {
            LongValues values = fieldData != null ? ((AtomicNumericFieldData) fieldData).getLongValues() : null;
            if (values == null) {
                return null;
            }
            return valueScript != null ? new ValueScriptLongValues(values, valueScript) : values;
        }
    }

    public static class Script extends ValuesSource.Script<ScriptLongValues> implements LongValuesSource {

        public Script(SearchScript script) {
            super(script);
        }

        public Script(SearchScript script, boolean multiValue) {
            super(script, multiValue);
        }

        @Override
        public ScriptLongValues createValues(SearchScript script, boolean multiValue) throws IOException {
            return new ScriptLongValues(script, multiValue);
        }
    }

}
