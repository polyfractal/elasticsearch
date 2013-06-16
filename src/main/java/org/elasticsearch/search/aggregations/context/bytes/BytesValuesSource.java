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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.context.FieldDataSource;
import org.elasticsearch.search.aggregations.context.ValueScriptValues;
import org.elasticsearch.search.aggregations.context.ValuesSource;

import java.io.IOException;

/**
 *
 */
public interface BytesValuesSource extends ValuesSource {

    BytesValues values() throws IOException;

    public static class FieldData extends ValuesSource.FieldData<FieldDataSource> implements BytesValuesSource {

        private final ValueScriptBytesValues scriptValues;

        public FieldData(FieldDataSource source, SearchScript script) {
            super(source);
            scriptValues = script == null ? null : new ValueScriptBytesValues(script);
        }

        public BytesValues values() throws IOException {
            if (scriptValues != null) {
                scriptValues.reset(source.bytesValues());
                return scriptValues;
            }
            return source.bytesValues();
        }
    }

    public class Script extends ValuesSource.Script<ScriptBytesValues> implements BytesValuesSource {

        public Script(SearchScript script, boolean multiValue) {
            super(new ScriptBytesValues(script, multiValue));
        }

        @Override
        public BytesValues values() throws IOException {
            return values;
        }
    }

    /**
     *
     */
    static class ValueScriptBytesValues extends BytesValues implements ValueScriptValues<BytesValues> {

        private final SearchScript script;
        private final InternalIter iter;
        private BytesValues values;

        private int docId;
        private Object value;

        private BytesRef scratch = new BytesRef();

        public ValueScriptBytesValues(SearchScript script) {
            super(true);
            this.script = script;
            this.iter = new InternalIter(script);
        }

        @Override
        public void reset(BytesValues values) {
            this.multiValued = values.isMultiValued();
            this.values = values;
        }

        @Override
        public boolean hasValue(int docId) {
            return getValueScratch(docId, scratch) != null;
        }

        @Override
        public BytesRef getValue(int docId) {
            return getValueScratch(docId, scratch);
        }

        @Override
        public BytesRef getValueScratch(int docId, BytesRef ret) {
            this.docId = docId;
            script.setNextVar("_value", values.getValue(docId).utf8ToString());
            value = script.run();
            if (value == null) {
                return null;
            }
            ret.copyChars(value.toString());
            return ret;
        }

        @Override
        public Iter getIter(int docId) {
            this.iter.iter = values.getIter(docId);
            return this.iter;
        }

        static class InternalIter implements Iter {

            private final SearchScript script;
            private Iter iter;

            BytesRef next = new BytesRef();

            InternalIter(SearchScript script) {
                this.script = script;
                advance();
            }

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public BytesRef next() {
                return next;
            }

            @Override
            public int hash() {
                return next.hashCode();
            }

            private void advance() {
                while (iter.hasNext()) {
                    script.setNextVar("_value", iter.next());
                    Object value = script.run();
                    if (value != null) {
                        next.copyChars(value.toString());
                        return;
                    }
                }
                next = null;
            }
        }
    }
}
