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

/**
 *
 */
public abstract class FieldDataSource implements ReaderContextAware {

    protected final String field;
    protected final IndexFieldData indexFieldData;
    protected AtomicFieldData fieldData;
    protected BytesValues bytesValues;

    public FieldDataSource(String field, IndexFieldData indexFieldData) {
        this.field = field;
        this.indexFieldData = indexFieldData;
    }

    public void setNextReader(AtomicReaderContext reader) {
        fieldData = indexFieldData.load(reader);
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

    public static class Numeric extends FieldDataSource {

        private DoubleValues doubleValues;
        private LongValues longValues;

        public Numeric(String field, IndexFieldData indexFieldData) {
            super(field, indexFieldData);
        }

        public DoubleValues doubleValues() {
            if (doubleValues == null) {
                doubleValues = ((AtomicNumericFieldData) fieldData).getDoubleValues();
            }
            return doubleValues;
        }

        public LongValues longValues() {
            if (longValues == null) {
                longValues = ((AtomicNumericFieldData) fieldData).getLongValues();
            }
            return longValues;
        }

        public boolean isFloatingPoint() {
            return ((IndexNumericFieldData) indexFieldData).getNumericType().isFloatingPoint();
        }

    }

    public static class Bytes extends FieldDataSource {

        public Bytes(String field, IndexFieldData indexFieldData) {
            super(field, indexFieldData);
        }

    }

    public static class GeoPoint extends FieldDataSource {

        private GeoPointValues geoPointValues;

        public GeoPoint(String field, IndexFieldData indexFieldData) {
            super(field, indexFieldData);
        }

        public GeoPointValues geoPointValues() {
            if (geoPointValues == null) {
                geoPointValues = ((AtomicGeoPointFieldData) fieldData).getGeoPointValues();
            }
            return geoPointValues;
        }
    }
}
