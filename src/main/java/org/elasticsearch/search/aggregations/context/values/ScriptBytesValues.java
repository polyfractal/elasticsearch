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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.script.SearchScript;

import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class ScriptBytesValues extends BytesValues {

    final SearchScript script;
    final InternalIter iter;
    final Iter.Single singleIter = new Iter.Single();

    private int docId;
    private Object value;
    private BytesRef scratch = new BytesRef();

    public ScriptBytesValues(SearchScript script) {
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
    public BytesRef getValueScratch(int docId, BytesRef ret) {
        if (this.docId != docId) {
            this.docId = docId;
            script.setNextDocId(docId);
            value = script.run();
        }
        if (value instanceof Object[]) {
            ret.copyChars(((Object[]) value)[0].toString());
            return ret;
        }
        if (value instanceof List) {
            ret.copyChars(((List) value).get(0).toString());
            return ret;
        }
        if (value instanceof Iterator) {
            ret.copyChars(((Iterator) value).next().toString());
            return ret;
        }
        ret.copyChars(value.toString());
        return ret;
    }

    @Override
    public BytesRef getValue(int docId) {
        return getValueScratch(docId, scratch);
    }

    @Override
    public Iter getIter(int docId) {
        if (this.docId != docId) {
            this.docId = docId;
            script.setNextDocId(docId);
            value = script.runAsLong();
        }
        if (value instanceof Object[]) {
            iter.reset((Object[]) value);
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

        scratch.copyChars(value.toString());
        singleIter.reset(scratch, 0);
        return singleIter;
    }

    static class InternalIter implements Iter {

        Object[] array;
        int i = 0;

        Iterator iterator;

        final BytesRef scratch = new BytesRef();

        void reset(Object[] array) {
            this.array = array;
            this.iterator = null;
        }

        void reset(Iterator iterator) {
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
        public BytesRef next() {
            if (iterator != null) {
                scratch.copyChars(iterator.next().toString());
                return scratch;
            }
            scratch.copyChars(array[++i].toString());
            return scratch;
        }

        @Override
        public int hash() {
            return scratch.hashCode();
        }
    }
}
