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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.GeoPointValues;
import org.elasticsearch.index.fielddata.LongValues;

import java.io.IOException;

/**
 *
 */
public interface ValuesSource {

    static final ValuesSource EMPTY = new Empty();

    String id();

    void setNextScorer(Scorer scorer);

    void setNextReader(AtomicReaderContext reader);

    DoubleValues doubleValues() throws IOException;

    LongValues longValues() throws IOException;

    BytesValues bytesValues() throws IOException;

    GeoPointValues geoPointValues() throws IOException;

    public static class Empty implements ValuesSource {

        @Override
        public String id() {
            return null;
        }

        @Override
        public void setNextScorer(Scorer scorer) {
        }

        @Override
        public void setNextReader(AtomicReaderContext reader) {
        }

        @Override
        public DoubleValues doubleValues() throws IOException {
            return null;
        }

        @Override
        public LongValues longValues() throws IOException {
            return null;
        }

        @Override
        public BytesValues bytesValues() throws IOException {
            return null;
        }

        @Override
        public GeoPointValues geoPointValues() throws IOException {
            return null;
        }
    }

}
