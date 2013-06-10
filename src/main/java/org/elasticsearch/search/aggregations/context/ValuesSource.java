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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.script.SearchScript;

import java.io.IOException;

/**
 *
 */
public interface ValuesSource {

    String key();

    void setNextScorer(Scorer scorer);

    void setNextReader(AtomicReaderContext reader);

    public abstract static class Empty implements ValuesSource {
        @Override
        public String key() {
            return null;
        }

        @Override
        public void setNextScorer(Scorer scorer) {
        }

        @Override
        public void setNextReader(AtomicReaderContext reader) {
        }
    }

    public abstract static class FieldData<Values> implements ValuesSource {

        protected final String field;
        protected final IndexFieldData indexFieldData;
        protected final SearchScript valueScript;

        protected AtomicFieldData fieldData;
        protected Values values;
        protected boolean loaded;

        public FieldData(String field, IndexFieldData indexFieldData) {
            this(field, indexFieldData, null);
        }

        public FieldData(String field, IndexFieldData indexFieldData, SearchScript valueScript) {
            this.field = field;
            this.indexFieldData = indexFieldData;
            this.valueScript = valueScript;
        }

        @Override
        public String key() {
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
            values = null;
            loaded = false;
        }

        public Values values() throws IOException {
            if (!loaded) {
                values = loadValues();
                loaded = true;
            }
            return values;
        }

        protected abstract Values loadValues() throws IOException;
    }

    public abstract static class Script<Values> implements ValuesSource {

        protected final SearchScript script;
        protected Values values;

        public Script(SearchScript script) {
            this.script = script;
        }

        @Override
        public String key() {
            return script.toString();
        }

        @Override
        public void setNextScorer(Scorer scorer) {
            script.setScorer(scorer);
        }

        @Override
        public void setNextReader(AtomicReaderContext reader) {
            script.setNextReader(reader);
        }

        public Values values() throws IOException {
            if (values == null) {
                values = createValues(script);
            }
            return values;
        }

        protected abstract Values createValues(SearchScript script) throws IOException;

    }
}
