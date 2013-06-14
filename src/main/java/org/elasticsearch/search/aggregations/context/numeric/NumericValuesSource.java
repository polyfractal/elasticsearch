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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.context.ScriptValues;
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

    abstract static class FieldData<Values> extends ValuesSource.FieldData<Values> implements NumericValuesSource {

        private final ValueFormatter formatter;
        private final ValueParser parser;

        protected FieldData(String field,
                            IndexFieldData indexFieldData,
                            @Nullable SearchScript valueScript,
                            @Nullable ValueFormatter formatter,
                            @Nullable ValueParser parser) {

            super(field, indexFieldData, valueScript);
            this.formatter = formatter;
            this.parser = parser;
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

    abstract static class Script<Values extends ScriptValues> extends ValuesSource.Script<Values> implements NumericValuesSource {

        private final ValueFormatter formatter;

        protected Script(SearchScript script, boolean multiValue, @Nullable ValueFormatter formatter) {
            super(script, multiValue);
            this.formatter = formatter;
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
        public String key() {
            return valuesSource.key();
        }

        @Override
        public void setNextScorer(Scorer scorer) {
            valuesSource.setNextScorer(scorer);
        }

        @Override
        public void setNextReader(AtomicReaderContext reader) {
            valuesSource.setNextReader(reader);
        }
    }
}
