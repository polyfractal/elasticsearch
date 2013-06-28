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

package org.elasticsearch.search.aggregations.context.numeric;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.context.FieldDataSource;
import org.elasticsearch.search.aggregations.context.ValueScriptValues;
import org.elasticsearch.search.aggregations.context.ValuesSource;

import java.io.IOException;

/**
 *
 */
public interface NumericValuesSource extends ValuesSource {

    boolean isFloatingPoint();

    LongValues longValues() throws IOException;

    DoubleValues doubleValues() throws IOException;

    ValueFormatter formatter();

    ValueParser parser();

    public static class FieldData extends ValuesSource.FieldData<FieldDataSource.Numeric> implements NumericValuesSource {

        private final ValueScriptLongValues longScriptValues;
        private final ValueScriptDoubleValues doubleScriptValues;

        private final ValueFormatter formatter;
        private final ValueParser parser;

        public FieldData(FieldDataSource.Numeric source, SearchScript script, @Nullable ValueFormatter formatter, @Nullable ValueParser parser) {
            super(source);
            longScriptValues = script == null ? null : new ValueScriptLongValues(script);
            doubleScriptValues = script == null ? null : new ValueScriptDoubleValues(script);
            this.formatter = formatter;
            this.parser = parser;
        }

        @Override
        public LongValues longValues() throws IOException {
            if (longScriptValues != null) {
                longScriptValues.reset(source.longValues());
                return longScriptValues;
            }
            return source.longValues();
        }

        @Override
        public DoubleValues doubleValues() throws IOException {
            if (doubleScriptValues != null) {
                doubleScriptValues.reset(source.doubleValues());
                return doubleScriptValues;
            }
            return source.doubleValues();
        }

        @Override
        public boolean isFloatingPoint() {
            return source.isFloatingPoint();
        }

        @Override
        public ValueFormatter formatter() {
            return formatter;
        }

        @Override
        public ValueParser parser() {
            return parser;
        }
    }

    public static class LongScript extends ValuesSource.Script<ScriptLongValues> implements NumericValuesSource {

        private final ValueFormatter formatter;
        private final LongDoubleValues doubleValues = new LongDoubleValues();

        public LongScript(SearchScript script, boolean multiValue, @Nullable ValueFormatter formatter) {
            super(new ScriptLongValues(script, multiValue));
            this.formatter = formatter;
        }

        @Override
        public boolean isFloatingPoint() {
            return false;
        }

        @Override
        public LongValues longValues() throws IOException {
            return values;
        }

        @Override
        public DoubleValues doubleValues() throws IOException {
            doubleValues.reset(values);
            return doubleValues;
        }

        @Override
        public ValueFormatter formatter() {
            return formatter;
        }

        @Override
        public ValueParser parser() {
            return null;
        }

    }

    public static class DoubleScript extends ValuesSource.Script<ScriptDoubleValues> implements NumericValuesSource {

        private final ValueFormatter formatter;
        private final DoubleLongValues longValues = new DoubleLongValues();

        public DoubleScript(SearchScript script, boolean multiValue, @Nullable ValueFormatter formatter) {
            super(new ScriptDoubleValues(script, multiValue));
            this.formatter = formatter;
        }

        @Override
        public boolean isFloatingPoint() {
            return false;
        }

        @Override
        public LongValues longValues() throws IOException {
            longValues.reset(values);
            return longValues;
        }

        @Override
        public DoubleValues doubleValues() throws IOException {
            return values;
        }

        @Override
        public ValueFormatter formatter() {
            return formatter;
        }

        @Override
        public ValueParser parser() {
            return null;
        }

    }

    /**
     * Wraps another numeric values source, and associates with it a different formatter and/or parser
     */
    static class Delegate implements  NumericValuesSource {

        private final NumericValuesSource valuesSource;
        private final ValueFormatter formatter;
        private final ValueParser parser;

        public Delegate(NumericValuesSource valuesSource, ValueFormatter formatter) {
            this(valuesSource, formatter, valuesSource.parser());
        }

        public Delegate(NumericValuesSource valuesSource, ValueParser parser) {
            this(valuesSource, valuesSource.formatter(), parser);
        }

        public Delegate(NumericValuesSource valuesSource, ValueFormatter formatter, ValueParser parser) {
            this.valuesSource = valuesSource;
            this.formatter = formatter;
            this.parser = parser;
        }

        @Override
        public boolean isFloatingPoint() {
            return valuesSource.isFloatingPoint();
        }

        @Override
        public LongValues longValues() throws IOException {
            return valuesSource.longValues();
        }

        @Override
        public DoubleValues doubleValues() throws IOException {
            return valuesSource.doubleValues();
        }

        @Override
        public ValueFormatter formatter() {
            return formatter;
        }

        @Override
        public ValueParser parser() {
            return parser;
        }

        @Override
        public Object key() {
            return valuesSource.key();
        }
    }

    static class ValueScriptDoubleValues extends DoubleValues implements ValueScriptValues<DoubleValues> {

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

    static class ValueScriptLongValues extends LongValues implements ValueScriptValues<LongValues> {

        private LongValues values;
        private final SearchScript script;
        private final InternalIter iter;

        public ValueScriptLongValues(SearchScript script) {
            super(true);
            this.script = script;
            this.iter = new InternalIter(script);
        }

        @Override
        public void reset(LongValues values) {
            this.multiValued = values.isMultiValued();
            this.values = values;
        }

        @Override
        public boolean hasValue(int docId) {
            return values.hasValue(docId);
        }

        @Override
        public long getValue(int docId) {
            script.setNextVar("_value", values.getValue(docId));
            return script.runAsLong();
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
            public long next() {
                script.setNextVar("_value", iter.next());
                return script.runAsLong();
            }
        }
    }
}
