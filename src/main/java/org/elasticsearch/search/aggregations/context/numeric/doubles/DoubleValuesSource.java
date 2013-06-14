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

package org.elasticsearch.search.aggregations.context.numeric.doubles;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.context.FieldContext;
import org.elasticsearch.search.aggregations.context.numeric.NumericValuesSource;
import org.elasticsearch.search.aggregations.context.numeric.ValueFormatter;
import org.elasticsearch.search.aggregations.context.numeric.ValueParser;

import java.io.IOException;

/**
 *
 */
public interface DoubleValuesSource extends NumericValuesSource {

    public static class FieldData extends NumericValuesSource.FieldData<DoubleValues> implements DoubleValuesSource {

        private DoubleLongValues longValues;

        public FieldData(FieldContext fieldContext,
                         @Nullable SearchScript valueScript,
                         @Nullable ValueFormatter formatter,
                         @Nullable ValueParser parser) {
            this(fieldContext.field(), fieldContext.indexFieldData(), valueScript, formatter, parser);
        }

        public FieldData(String field,
                         IndexFieldData indexFieldData,
                         @Nullable SearchScript valueScript,
                         @Nullable ValueFormatter formatter,
                         @Nullable ValueParser parser) {

            super(field, indexFieldData, valueScript, formatter, parser);
        }

        @Override
        public boolean isFloatingPoint() {
            return true;
        }

        @Override
        public LongValues longValues() throws IOException {
            if (longValues == null) {
                longValues = new DoubleLongValues(values());
            } else {
                longValues.reset(values());
            }
            return longValues;
        }

        @Override
        public DoubleValues doubleValues() throws IOException {
            return values();
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

    public static class Script extends NumericValuesSource.Script<ScriptDoubleValues> implements DoubleValuesSource {

        private DoubleLongValues longValues;

        public Script(SearchScript script, boolean multiValue, @Nullable ValueFormatter formatter) {
            super(script, multiValue, formatter);
        }

        @Override
        public boolean isFloatingPoint() {
            return true;
        }

        @Override
        public LongValues longValues() throws IOException {
            if (longValues == null) {
                longValues = new DoubleLongValues(values());
            } else {
                longValues.reset(values());
            }
            return longValues;
        }

        @Override
        public DoubleValues doubleValues() throws IOException {
            return values();
        }

        @Override
        public ScriptDoubleValues createValues(SearchScript script, boolean multiValue) throws IOException {
            return new ScriptDoubleValues(script, multiValue);
        }
    }
}
