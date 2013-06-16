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
import org.elasticsearch.index.fielddata.*;

import java.io.IOException;

/**
 *
 */
public abstract class FieldDataSource<Values> {

    protected final String field;
    protected final IndexFieldData indexFieldData;
    protected AtomicFieldData fieldData;
    private Values values;


    public FieldDataSource(String field, IndexFieldData indexFieldData) {
        this.field = field;
        this.indexFieldData = indexFieldData;
    }

    public void setNextReader(AtomicReaderContext reader) throws IOException {
        fieldData = indexFieldData.load(reader);
        values = loadValues();
    }

    public String field() {
        return field;
    }

    public Values values() {
        return values;
    }

    protected abstract Values loadValues();

    public static class Double extends FieldDataSource<DoubleValues> {

        public Double(String field, IndexFieldData indexFieldData) {
            super(field, indexFieldData);
        }

        @Override
        protected DoubleValues loadValues() {
            return ((AtomicNumericFieldData) fieldData).getDoubleValues();
        }
    }

    public static class Long extends FieldDataSource<LongValues> {

        public Long(String field, IndexFieldData indexFieldData) {
            super(field, indexFieldData);
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

        @Override
        protected BytesValues loadValues() {
            return fieldData.getBytesValues();
        }
    }

    public static class GeoPoint extends FieldDataSource<GeoPointValues> {

        public GeoPoint(String field, IndexFieldData indexFieldData) {
            super(field, indexFieldData);
        }

        @Override
        protected GeoPointValues loadValues() {
            return ((AtomicGeoPointFieldData) fieldData).getGeoPointValues();
        }
    }
}
