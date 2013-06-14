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

package org.elasticsearch.search.aggregations.context.bytes;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.context.FieldContext;
import org.elasticsearch.search.aggregations.context.ValuesSource;

import java.io.IOException;

/**
 *
 */
public interface BytesValuesSource extends ValuesSource {

    BytesValues values() throws IOException;

    public static class FieldData extends ValuesSource.FieldData<BytesValues> implements BytesValuesSource {

        public FieldData(FieldContext fieldContext, @Nullable SearchScript valueScript) {
            this(fieldContext.field(), fieldContext.indexFieldData(), valueScript);
        }

        public FieldData(String field, IndexFieldData indexFieldData, @Nullable SearchScript valueScript) {
            super(field, indexFieldData, valueScript);
        }

        @Override
        public BytesValues loadValues() throws IOException {
            BytesValues values = fieldData != null ? fieldData.getBytesValues() : null;
            if (values == null) {
                return null;
            }
            return valueScript != null ? new ValueScriptBytesValues(values, valueScript) : values;
        }
    }

    public class Script extends ValuesSource.Script<ScriptBytesValues> implements BytesValuesSource {

        public Script(SearchScript script, boolean multiValue) {
            super(script, multiValue);
        }

        @Override
        public ScriptBytesValues createValues(SearchScript script, boolean multiValue) throws IOException {
            return new ScriptBytesValues(script);
        }
    }

}
