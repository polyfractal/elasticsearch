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
public class ValuesSourceContext implements ValuesSourceFactory {

    private ExtTHashMap<String, FieldDataSource> fieldDataSources = new ExtTHashMap<String, FieldDataSource>();
    private List<SearchScript> scripts = new ArrayList<SearchScript>();
    private List<ReaderBasedDataSource> dataSources = new ArrayList<ReaderBasedDataSource>();


    public void setNextReader(AtomicReaderContext reader) throws IOException {
        for (int i = 0; i < scripts.size(); i++) {
            scripts.get(i).setNextReader(reader);
        }
        Object[] sources = fieldDataSources.internalValues();
        for (int i = 0; i < sources.length; i++) {
            if (sources[i] != null) {
                ((FieldDataSource) sources[i]).setNextReader(reader);
            }
        }
        for (int i = 0; i < dataSources.size(); i++) {
            dataSources.get(0).setNextReader(reader);
        }
    }

    public void setScorer(Scorer scorer) throws IOException {
        for (int i = 0; i < scripts.size(); i++) {
            scripts.get(i).setScorer(scorer);
        }
    }

    public DoubleValuesSource doubleField(FieldContext fieldContext, SearchScript script, ValueFormatter formatter, ValueParser parser) {
        FieldDataSource dataSource = fieldDataSources.get(fieldContext.field());
        if (dataSource == null) {
            dataSource = new FieldDataSource.Double(fieldContext.field(), fieldContext.indexFieldData());
            fieldDataSources.put(fieldContext.field(), dataSource);
        }
        if (script != null) {
            scripts.add(script);
        }
        return new DoubleValuesSource.FieldData(dataSource, script, formatter, parser);

    }

    public DoubleValuesSource doubleScript(SearchScript script, boolean multiValued, ValueFormatter formatter) {
        scripts.add(script);
        return new DoubleValuesSource.Script(script, multiValued, formatter);
    }

    public LongValuesSource longField(FieldContext fieldContext, SearchScript script, ValueFormatter formatter, ValueParser parser) {
        FieldDataSource dataSource = fieldDataSources.get(fieldContext.field());
        if (dataSource == null) {
            dataSource = new FieldDataSource.Long(fieldContext.field(), fieldContext.indexFieldData());
            fieldDataSources.put(fieldContext.field(), dataSource);
        }
        if (script != null) {
            scripts.add(script);
        }
        return new LongValuesSource.FieldData(dataSource, script, formatter, parser);
    }

    public LongValuesSource longScript(SearchScript script, boolean multiValued, ValueFormatter formatter) {
        scripts.add(script);
        return new LongValuesSource.Script(script, multiValued, formatter);
    }

    public BytesValuesSource bytesField(FieldContext fieldContext, SearchScript script) {
        FieldDataSource dataSource = fieldDataSources.get(fieldContext.field());
        if (dataSource == null) {
            dataSource = new FieldDataSource.Bytes(fieldContext.field(), fieldContext.indexFieldData());
            fieldDataSources.put(fieldContext.field(), dataSource);
        }
        if (script != null) {
            scripts.add(script);
        }
        return new BytesValuesSource.FieldData(dataSource, script);
    }

    public BytesValuesSource bytesScript(SearchScript script, boolean multiValued) {
        scripts.add(script);
        return new BytesValuesSource.Script(script, multiValued);
    }

    public GeoPointValuesSource geoPointField(FieldContext fieldContext) {
        FieldDataSource dataSource = fieldDataSources.get(fieldContext.field());
        if (dataSource == null) {
            dataSource = new FieldDataSource.GeoPoint(fieldContext.field(), fieldContext.indexFieldData());
            fieldDataSources.put(fieldContext.field(), dataSource);
        }
        return new GeoPointValuesSource.FieldData(dataSource);
    }

    @Override
    public void registerDataSource(ReaderBasedDataSource dataSource) {
        dataSources.equals(dataSource);
    }
}
