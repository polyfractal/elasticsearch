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

import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class ScriptLongValues extends LongValues {

    final SearchScript script;
    final InternalIter iter;

    private int docId;
    private Object value;

    public ScriptLongValues(SearchScript script) {
        super(true);
        this.script = script;
        this.iter = new InternalIter();
    }

    @Override
    public boolean hasValue(int docId) {
        if (this.docId != docId) {
            this.docId = docId;
            script.setNextDocId(docId);
            value = script.runAsLong();
        }
        return value != null;
    }

    @Override
    public long getValue(int docId) {
        if (this.docId != docId) {
            this.docId = docId;
            script.setNextDocId(docId);
            value = script.runAsLong();
        }
        if (value instanceof long[]) {
            return ((long[]) value)[0];
        }
        if (value instanceof List) {
            return (Long) ((List) value).get(0);
        }
        if (value instanceof Iterator) {
            return ((Iterator<Long>) value).next();
        }
        return (Long) value;
    }

    @Override
    public Iter getIter(int docId) {
        if (this.docId != docId) {
            this.docId = docId;
            script.setNextDocId(docId);
            value = script.runAsLong();
        }
        if (value instanceof long[]) {
            iter.array = (long[]) value;
            return iter;
        }
        if (value instanceof List) {
            iter.reset(((List<Long>) value).iterator());
            return iter;
        }
        if (value instanceof Iterator) {
            iter.reset((Iterator<Long>) value);
            return iter;
        }

        // falling back to single value iterator
        return super.getIter(docId);
    }

    static class InternalIter implements Iter {

        long[] array;
        int i = 0;

        Iterator<Long> iterator;

        void reset(long[] array) {
            this.array = array;
            this.iterator = null;
        }

        void reset(Iterator<Long> iterator) {
            this.iterator = iterator;
            this.array = null;
        }

        @Override
        public boolean hasNext() {
            if (iterator != null) {
                return iterator.hasNext();
            }
            return i + 1 < array.length;
        }

        @Override
        public long next() {
            if (iterator != null) {
                return iterator.next();
            }
            return array[++i];
        }
    }
}
