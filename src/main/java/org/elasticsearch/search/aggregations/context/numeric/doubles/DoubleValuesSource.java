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
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.context.FieldDataSource;
import org.elasticsearch.search.aggregations.context.ValueScriptValues;
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

        public FieldData(FieldDataSource<DoubleValues> source,
                         @Nullable SearchScript script,
                         @Nullable ValueFormatter formatter,
                         @Nullable ValueParser parser) {
            super(source, script, formatter, parser);
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
        protected ValueScriptValues<DoubleValues> createScriptValues(SearchScript script) {
            return new ValueScriptDoubleValues(script);
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
        public ScriptDoubleValues createValues(SearchScript script, boolean multiValue) {
            return new ScriptDoubleValues(script, multiValue);
        }
    }

    /**
     *
     */
    public static class ValueScriptDoubleValues extends DoubleValues implements ValueScriptValues<DoubleValues>  {

        private DoubleValues values;
        private final SearchScript script;
        private final InternalIter iter;

        public ValueScriptDoubleValues(SearchScript script) {
            super(true);
            this.script = script;
            this.iter = new InternalIter(script);
        }

        public void reset(DoubleValues values) {
            this.multiValued = values.isMultiValued();
            this.values = values;
        };

        @Override
        public boolean hasValue(int docId) {
            return values.hasValue(docId);
        }

        @Override
        public double getValue(int docId) {
            script.setNextVar("_value", values.getValue(docId));
            return script.runAsDouble();
        }

        @Override
        public Iter getIter(int docId) {
            this.iter.iter = values.getIter(docId);
            return this.iter;
        }

        static class InternalIter implements Iter {

            private final SearchScript script;
            private Iter iter;

            InternalIter(SearchScript script) {
                this.script = script;
            }

            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public double next() {
                script.setNextVar("_value", iter.next());
                return script.runAsDouble();
            }
        }
    }
}
