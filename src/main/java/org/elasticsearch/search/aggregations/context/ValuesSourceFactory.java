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
import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.trove.ExtTHashMap;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.context.bytes.BytesValuesSource;
import org.elasticsearch.search.aggregations.context.geopoints.GeoPointValuesSource;
import org.elasticsearch.search.aggregations.context.numeric.ValueFormatter;
import org.elasticsearch.search.aggregations.context.numeric.ValueParser;
import org.elasticsearch.search.aggregations.context.numeric.doubles.DoubleValuesSource;
import org.elasticsearch.search.aggregations.context.numeric.longs.LongValuesSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public interface ValuesSourceFactory {

    DoubleValuesSource doubleField(FieldContext fieldContext, SearchScript script, ValueFormatter formatter, ValueParser parser);

    DoubleValuesSource doubleScript(SearchScript script, boolean multiValued, ValueFormatter formatter);

    LongValuesSource longField(FieldContext fieldContext, SearchScript script, ValueFormatter formatter, ValueParser parser);

    LongValuesSource longScript(SearchScript script, boolean multiValued, ValueFormatter formatter);

    BytesValuesSource bytesField(FieldContext fieldContext, SearchScript script);

    BytesValuesSource bytesScript(SearchScript script, boolean multiValued);

    GeoPointValuesSource geoPointField(FieldContext fieldContext);

    void registerDataSource(ReaderBasedDataSource dataSource);

}
