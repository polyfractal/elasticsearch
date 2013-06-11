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

package org.elasticsearch.search.aggregations.context.longs;

import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.context.ScriptValues;

import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class ScriptLongValues extends LongValues implements ScriptValues {

    final SearchScript script;
    final InternalIter iter;

    private int docId = -1;
    private Object value;

    public ScriptLongValues(SearchScript script) {
        this(script, true);
    }

    public ScriptLongValues(SearchScript script, boolean multiValue) {
        super(multiValue);
        this.script = script;
        this.iter = new InternalIter();
    }

    @Override
    public void clearCache() {
        docId = -1;
        value = null;
    }

    @Override
    public boolean hasValue(int docId) {
        if (this.docId != docId) {
            this.docId = docId;
            script.setNextDocId(docId);
            value = script.runAsLong();
        }
        if (value == null) {
            return false;
        }

        // shortcutting on single valued
        if (!isMultiValued()) {
            return true;
        }

        if (value instanceof long[]) {
            return ((long[]) value).length != 0;
        }
        if (value instanceof List) {
            return !((List) value).isEmpty();
        }
        if (value instanceof Iterator) {
            return ((Iterator<Number>) value).hasNext();
        }
        return true;
//        throw new AggregationExecutionException("value of type [" + value.getClass().getName() + "] is not supported as long script output");
    }

    @Override
    public long getValue(int docId) {
        if (this.docId != docId) {
            this.docId = docId;
            script.setNextDocId(docId);
            value = script.runAsLong();
        }

        // shortcutting on single valued
        if (!isMultiValued()) {
            return (Long) value;
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

        // shortcutting on single valued
        if (!isMultiValued()) {
            return super.getIter(docId);
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
