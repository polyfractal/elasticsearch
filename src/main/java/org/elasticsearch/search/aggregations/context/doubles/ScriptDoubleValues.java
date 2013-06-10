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

package org.elasticsearch.search.aggregations.context.doubles;

import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.AggregationExecutionException;

import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class ScriptDoubleValues extends DoubleValues  {

    final SearchScript script;
    final InternalIter iter;

    private int docId;
    private Object value;

    public ScriptDoubleValues(SearchScript script) {
        super(true);
        this.script = script;
        this.iter = new InternalIter();
    }

    @Override
    public boolean hasValue(int docId) {
        if (this.docId != docId) {
            this.docId = docId;
            script.setNextDocId(docId);
            value = script.runAsDouble();
        }
        if (value == null) {
            return false;
        }
        if (value instanceof double[]) {
            return ((double[]) value).length != 0;
        }
        if (value instanceof List) {
            return !((List) value).isEmpty();
        }
        if (value instanceof Iterator) {
            return ((Iterator<Number>) value).hasNext();
        }
        throw new AggregationExecutionException("Unsupported double script value [" + value.toString() + "]");
    }

    @Override
    public double getValue(int docId) {
        if (this.docId != docId) {
            this.docId = docId;
            script.setNextDocId(docId);
            value = script.runAsDouble();
        }
        if (value instanceof double[]) {
            return ((double[]) value)[0];
        }
        if (value instanceof List) {
            return (Double) ((List) value).get(0);
        }
        if (value instanceof Iterator) {
            return ((Iterator<Number>) value).next().doubleValue();
        }
        return (Double) value;
    }

    @Override
    public Iter getIter(int docId) {
        if (this.docId != docId) {
            this.docId = docId;
            script.setNextDocId(docId);
            value = script.runAsDouble();
        }
        if (value instanceof double[]) {
            iter.reset((double[]) value);
            return iter;
        }
        if (value instanceof List) {
            iter.reset(((List<Number>) value).iterator());
            return iter;
        }
        if (value instanceof Iterator) {
            iter.reset((Iterator<Number>) value);
            return iter;
        }

        // falling back to single value iterator
        return super.getIter(docId);
    }

    static class InternalIter implements Iter {

        double[] array;
        int i = 0;

        Iterator<Number> iterator;

        void reset(double[] array) {
            this.array = array;
            this.iterator = null;
        }

        void reset(Iterator<Number> iterator) {
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
        public double next() {
            if (iterator != null) {
                return iterator.next().doubleValue();
            }
            return array[++i];
        }
    }
}
