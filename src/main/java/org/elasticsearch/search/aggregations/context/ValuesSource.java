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

package org.elasticsearch.search.aggregations.context;

import org.elasticsearch.script.SearchScript;

import java.io.IOException;

/**
 *
 */
public interface ValuesSource {

    String key();

    public abstract static class FieldData<Values> implements ValuesSource {

        private final FieldDataSource<Values> source;
        protected final ValueScriptValues<Values> scriptValues;

        public FieldData(FieldDataSource<Values> source, SearchScript script) {
            this.source = source;
            scriptValues = script == null ? null : createScriptValues(script);
        }

        @Override
        public String key() {
            return source.field();
        }

        public Values values() throws IOException {
            if (scriptValues != null) {
                scriptValues.reset(source.values());
                return (Values) scriptValues;
            }
            return source.values();
        }

        protected abstract ValueScriptValues<Values> createScriptValues(SearchScript script);

    }

    public abstract static class Script<Values extends ScriptValues> implements ValuesSource {

        protected final SearchScript script;
        protected final Values values;

        public Script(SearchScript script, boolean multiValue) {
            this.script = script;
            this.values = createValues(script, multiValue);
        }

        @Override
        public String key() {
            return script.toString();
        }

        public Values values() throws IOException {
            return values;
        }

        protected abstract Values createValues(SearchScript script, boolean multiValue);

    }
}
