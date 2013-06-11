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

package org.elasticsearch.search.aggregations.context.geopoints;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.fielddata.GeoPointValues;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.context.ScriptValues;

import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class ScriptGeoPointValues implements GeoPointValues, ScriptValues {

    final boolean multiValue;
    final SearchScript script;
    final InternalIter iter;
    final InternalSafeIter safeIter;
    final InternalIter.Single singleIter = new Iter.Single();

    private int docId = -1;
    private Object value;

    public ScriptGeoPointValues(SearchScript script) {
        this(script, true);
    }

    public ScriptGeoPointValues(SearchScript script, boolean multiValue) {
        this.multiValue = multiValue;
        this.script = script;
        this.iter = new InternalIter();
        this.safeIter = new InternalSafeIter();
    }

    @Override
    public void clearCache() {
        docId = -1;
        value = null;
    }

    @Override
    public boolean isMultiValued() {
        return multiValue;
    }

    @Override
    public boolean hasValue(int docId) {
        if (this.docId != docId) {
            this.docId = docId;
            script.setNextDocId(docId);
            value = script.run();
        }
        if (value == null) {
            return false;
        }

        // shortcutting on single valued
        if (!isMultiValued()) {
            return true;
        }

        if (value instanceof GeoPoint[]) {
            return ((GeoPoint[]) value).length != 0;
        }
        if (value instanceof List) {
            return !((List) value).isEmpty();
        }
        if (value instanceof Iterator) {
            return ((Iterator<GeoPoint>) value).hasNext();
        }
        return true;
    }

    @Override
    public GeoPoint getValue(int docId) {
        if (this.docId != docId) {
            this.docId = docId;
            script.setNextDocId(docId);
            value = script.run();
        }

        // shortcutting on single valued
        if (!isMultiValued()) {
            return (GeoPoint) value;
        }

        if (value instanceof GeoPoint[]) {
            return ((GeoPoint[]) value)[0];
        }
        if (value instanceof List) {
            return (GeoPoint) ((List) value).get(0);
        }
        if (value instanceof Iterator) {
            return ((Iterator<GeoPoint>) value).next();
        }
        return (GeoPoint) value;
    }

    @Override
    public GeoPoint getValueSafe(int docId) {
        GeoPoint value = getValue(docId);
        return new GeoPoint(value.lat(), value.lon());
    }

    @Override
    public Iter getIter(int docId) {
        if (this.docId != docId) {
            this.docId = docId;
            script.setNextDocId(docId);
            value = script.run();
        }

        // shortcutting on single valued
        if (!isMultiValued()) {
            singleIter.reset((GeoPoint) value);
            return singleIter;
        }

        if (value instanceof GeoPoint[]) {
            iter.reset((GeoPoint[]) value);
            return iter;
        }
        if (value instanceof List) {
            iter.reset(((List<GeoPoint>) value).iterator());
            return iter;
        }
        if (value instanceof Iterator) {
            iter.reset((Iterator<GeoPoint>) value);
            return iter;
        }

        // falling back to single value iterator
        singleIter.reset((GeoPoint) value);
        return singleIter;
    }

    @Override
    public Iter getIterSafe(int docId) {
        safeIter.reset(getIter(docId));
        return safeIter;
    }

    static class InternalIter implements Iter {

        GeoPoint[] array;
        int i = 0;

        Iterator<GeoPoint> iterator;

        void reset(GeoPoint[] array) {
            this.array = array;
            this.iterator = null;
        }

        void reset(Iterator<GeoPoint> iterator) {
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
        public GeoPoint next() {
            if (iterator != null) {
                return iterator.next();
            }
            return array[++i];
        }
    }

    static class InternalSafeIter implements Iter {

        Iter iter;

        void reset(Iter iter) {
            this.iter = iter;
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public GeoPoint next() {
            GeoPoint point = iter.next();
            return new GeoPoint(point.lat(), point.lon());
        }
    }
}
