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

import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.LongValues;

/**
 * {@link LongValues} implementation which wraps a {@link DoubleValues} instance.
 */
public class DoubleLongValues extends LongValues {

    private final LongIter scratchIter = new LongIter();

    private DoubleValues values;

    public DoubleLongValues() {
        super(true);
    }

    public DoubleLongValues(DoubleValues values) {
        super(values.isMultiValued());
        this.values = values;
    }

    public void reset(DoubleValues values) {
        this.values = values;
        this.multiValued = values.isMultiValued();
    }

    @Override
    public boolean hasValue(int docId) {
        return values.hasValue(docId);
    }

    @Override
    public long getValue(int docId) {
        return (long) values.getValue(docId);
    }

    @Override
    public Iter getIter(int docId) {
        scratchIter.iter = values.getIter(docId);
        return scratchIter;
    }

    private static class LongIter implements Iter {

        private DoubleValues.Iter iter;

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public long next() {
            return (long) iter.next();
        }
    }

}