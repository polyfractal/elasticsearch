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
import org.elasticsearch.common.lucene.ReaderContextAware;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.search.aggregations.context.numeric.DoubleLongValues;
import org.elasticsearch.search.aggregations.context.numeric.LongDoubleValues;

/**
 *
 */
public abstract class FieldDataSource<Values> implements ReaderContextAware {

    protected final String field;
    protected final IndexFieldData indexFieldData;
    protected AtomicFieldData fieldData;
    protected Values values;
    protected BytesValues bytesValues;

    public FieldDataSource(String field, IndexFieldData indexFieldData) {
        this.field = field;
        this.indexFieldData = indexFieldData;
    }

    public void setNextReader(AtomicReaderContext reader) {
        fieldData = indexFieldData.load(reader);
        values = loadValues();
    }

    public String field() {
        return field;
    }

    public BytesValues bytesValues() {
        if (bytesValues == null) {
            bytesValues = fieldData.getBytesValues();
        }
        return bytesValues;
    }

    protected abstract Values loadValues();

    public static abstract class Numeric<Values> extends FieldDataSource<Values> {

        public Numeric(String field, IndexFieldData indexFieldData) {
            super(field, indexFieldData);
        }

        public abstract DoubleValues doubleValues();

        public abstract LongValues longValues();

        public abstract boolean isFloatingPoint();

    }

    public static class Double extends Numeric<DoubleValues> {

        private final DoubleLongValues longValues = new DoubleLongValues();

        public Double(String field, IndexFieldData indexFieldData) {
            super(field, indexFieldData);
        }

        @Override
        public DoubleValues doubleValues() {
            return values;
        }

        @Override
        public LongValues longValues() {
            longValues.reset(values);
            return longValues();
        }

        @Override
        public boolean isFloatingPoint() {
            return true;
        }

        @Override
        protected DoubleValues loadValues() {
            return ((AtomicNumericFieldData) fieldData).getDoubleValues();
        }
    }

    public static class Long extends Numeric<LongValues> {

        private final LongDoubleValues doubleValues = new LongDoubleValues();

        public Long(String field, IndexFieldData indexFieldData) {
            super(field, indexFieldData);
        }

        @Override
        public DoubleValues doubleValues() {
            doubleValues.reset(values);
            return doubleValues;
        }

        @Override
        public LongValues longValues() {
            return values;
        }

        @Override
        public boolean isFloatingPoint() {
            return false;
        }

        @Override
        protected LongValues loadValues() {
            return ((AtomicNumericFieldData) fieldData).getLongValues();
        }
    }

    public static class Bytes extends FieldDataSource<BytesValues> {

        public Bytes(String field, IndexFieldData indexFieldData) {
            super(field, indexFieldData);
        }

        public BytesValues values() {
            return values;
        }

        @Override
        protected BytesValues loadValues() {
            return fieldData.getBytesValues();
        }

        @Override
        public BytesValues bytesValues() {
            return values;
        }
    }

    public static class GeoPoint extends FieldDataSource<GeoPointValues> {

        public GeoPoint(String field, IndexFieldData indexFieldData) {
            super(field, indexFieldData);
        }

        public GeoPointValues values() {
            return values;
        }

        @Override
        protected GeoPointValues loadValues() {
            return ((AtomicGeoPointFieldData) fieldData).getGeoPointValues();
        }
    }
}
