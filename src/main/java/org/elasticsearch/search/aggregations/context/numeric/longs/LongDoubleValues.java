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

package org.elasticsearch.search.aggregations.context.numeric.longs;

import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.LongValues;

/**
 *
 */
public class LongDoubleValues extends DoubleValues {

    private final DoubleIter scratchIter = new DoubleIter();

    private LongValues values;

    LongDoubleValues(LongValues values) {
        super(values.isMultiValued());
        this.values = values;
    }

    void reset(LongValues values) {
        this.values = values;
        this.multiValued = values.isMultiValued();
    }

    @Override
    public boolean hasValue(int docId) {
        return values.hasValue(docId);
    }

    @Override
    public double getValue(int docId) {
        return (double) values.getValue(docId);
    }

    @Override
    public Iter getIter(int docId) {
        scratchIter.iter = values.getIter(docId);
        return scratchIter;
    }

    private static class DoubleIter implements Iter {

        private LongValues.Iter iter;

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public double next() {
            return (double) iter.next();
        }
    }
}