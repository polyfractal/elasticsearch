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

package org.elasticsearch.search.aggregations.context.values;

import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.script.SearchScript;

/**
 *
 */
public class ValueScriptLongValues extends LongValues {

    private final LongValues values;
    private final SearchScript script;
    private final InternalIter iter;

    public ValueScriptLongValues(LongValues values, SearchScript script) {
        super(values.isMultiValued());
        this.values = values;
        this.script = script;
        this.iter = new InternalIter(script);
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
