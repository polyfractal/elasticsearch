/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.aggregations.support;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.ScorerAware;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.fielddata.AbstractSortingNumericDocValues;
import org.elasticsearch.index.fielddata.AtomicOrdinalsFieldData;
import org.elasticsearch.index.fielddata.DocValueBits;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.SortingBinaryDocValues;
import org.elasticsearch.index.fielddata.SortingNumericDoubleValues;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.search.aggregations.support.ValuesSource.WithScript.BytesValues;
import org.elasticsearch.search.aggregations.support.values.ScriptBytesValues;
import org.elasticsearch.search.aggregations.support.values.ScriptDoubleValues;
import org.elasticsearch.search.aggregations.support.values.ScriptLongValues;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.util.SortedSet;
import java.util.function.Function;
import java.util.function.LongUnaryOperator;

public class DataProvider {


    public static DataProvider getOrdinalFieldDataBytes(IndexOrdinalsFieldData fd) {
        Function<LeafReaderContext, SortedSetDocValues> ordinalsProducer = context -> {
            final AtomicOrdinalsFieldData atomicFieldData = fd.load(context);
            return atomicFieldData.getOrdinalsValues();
        };

        return new DataProvider(context -> {
            final AtomicOrdinalsFieldData atomicFieldData = fd.load(context);
            return atomicFieldData.getBytesValues();
        }, context -> {
            final SortedSetDocValues ordinals = ordinalsProducer.apply(context);
            return org.elasticsearch.index.fielddata.FieldData.docsWithValue(ordinals);
        }, ordinalsProducer, context -> {
            final IndexOrdinalsFieldData global = fd.loadGlobal((DirectoryReader) context.parent.reader());
            final AtomicOrdinalsFieldData atomicFieldData = global.load(context);
            return atomicFieldData.getOrdinalsValues();
        }, context -> {
            final IndexOrdinalsFieldData global = fd.loadGlobal((DirectoryReader) context.parent.reader());
            final OrdinalMap map = global.getOrdinalMap();
            if (map == null) {
                // segments and global ordinals are the same
                return LongUnaryOperator.identity();
            }
            final org.apache.lucene.util.LongValues segmentToGlobalOrd = map.getGlobalOrds(context.ord);
            return segmentToGlobalOrd::get;
        }, null, null, null, true);
    }

    public static DataProvider getFieldDataBytes(IndexFieldData fd) {
        Function<LeafReaderContext, SortedBinaryDocValues> bytesProducer = context -> fd.load(context).getBytesValues();

        return new DataProvider(bytesProducer, context -> {
            final SortedBinaryDocValues bytes = bytesProducer.apply(context);
            return org.elasticsearch.index.fielddata.FieldData.docsWithValue(bytes);
        }, null, null, null, null, null, null,  true);
    }

    public static DataProvider getBytesWithScript(AggregationScript.LeafFactory script) {
        Function<LeafReaderContext, SortedBinaryDocValues> bytesProducer = context -> new ScriptBytesValues(script.newInstance(context));

        return new DataProvider(bytesProducer, context -> {
            final SortedBinaryDocValues bytes = bytesProducer.apply(context);
            return org.elasticsearch.index.fielddata.FieldData.docsWithValue(bytes);
        }, null, null, null, null, null, null, script.needs_score());
    }


    private Function<LeafReaderContext, SortedBinaryDocValues> bytesValuesProducer;
    private Function<LeafReaderContext, DocValueBits> docsWithValueProducer;
    private Function<LeafReaderContext, SortedSetDocValues> ordinalValuesProducer;
    private Function<LeafReaderContext, SortedSetDocValues> globalOrdinalsValuesProducer;
    private Function<LeafReaderContext, LongUnaryOperator> globalOrdinalsMappingProducer;
    private Function<LeafReaderContext, SortedNumericDocValues> longValuesProducer;
    private Function<LeafReaderContext, SortedNumericDoubleValues> doubleValuesProducer;
    private AggregationScript.LeafFactory script;
    private boolean needsScores;

    private DataProvider(Function<LeafReaderContext, SortedBinaryDocValues> bytesValuesProducer,
                 Function<LeafReaderContext, DocValueBits> docsWithValueProducer,
                 Function<LeafReaderContext, SortedSetDocValues> ordinalValuesProducer,
                 Function<LeafReaderContext, SortedSetDocValues> globalOrdinalsValuesProducer,
                 Function<LeafReaderContext, LongUnaryOperator> globalOrdinalsMappingProducer,
                 Function<LeafReaderContext, SortedNumericDocValues> longValuesProducer,
                 Function<LeafReaderContext, SortedNumericDoubleValues> doubleValuesProducer,
                 AggregationScript.LeafFactory script,
                 boolean needsScores) {
        this.bytesValuesProducer = bytesValuesProducer;
        this.docsWithValueProducer = docsWithValueProducer;
        this.ordinalValuesProducer = ordinalValuesProducer;
        this.globalOrdinalsValuesProducer = globalOrdinalsValuesProducer;
        this.globalOrdinalsMappingProducer = globalOrdinalsMappingProducer;
        this.longValuesProducer = longValuesProducer;
        this.doubleValuesProducer = doubleValuesProducer;
        this.script = script;
        this.needsScores = needsScores;
    }

    public SortedBinaryDocValues bytesValues(LeafReaderContext context) {
        if (bytesValuesProducer == null) {
            throw new NotImplementedException();
        }
        return bytesValuesProducer.apply(context);
    }

    public DocValueBits docsWithValue(LeafReaderContext context) {
        if (docsWithValueProducer == null) {
            throw new NotImplementedException();
        }
        return docsWithValueProducer.apply(context);
    }

    public SortedSetDocValues ordinalValues(LeafReaderContext context) {
        if (ordinalValuesProducer == null) {
            throw new NotImplementedException();
        }
        return ordinalValuesProducer.apply(context);
    }

    public SortedSetDocValues globalOrdinalsValues(LeafReaderContext context) {
        if (globalOrdinalsValuesProducer == null) {
            throw new NotImplementedException();
        }
        return globalOrdinalsValuesProducer.apply(context);
    }

    public LongUnaryOperator globalOrdinalsMapping(LeafReaderContext context) {
        if (globalOrdinalsMappingProducer == null) {
            throw new NotImplementedException();
        }
        return globalOrdinalsMappingProducer.apply(context);
    }

    public boolean needsScores() {
        return needsScores;
    }

    public long getGlobalMaxOrd(IndexSearcher indexSearcher) {
        if (globalOrdinalsValuesProducer == null) {
            throw new NotImplementedException();
        }
        IndexReader indexReader = indexSearcher.getIndexReader();
        if (indexReader.leaves().isEmpty()) {
            return 0;
        } else {
            LeafReaderContext atomicReaderContext = indexReader.leaves().get(0);
            SortedSetDocValues values = globalOrdinalsValues(atomicReaderContext);
            return values.getValueCount();
        }
    }




    public abstract static class Numeric extends ValuesSource {

        /** Whether the underlying data is floating-point or not. */
        public abstract boolean isFloatingPoint();

        /** Get the current {@link SortedNumericDocValues}. */
        public abstract SortedNumericDocValues longValues(LeafReaderContext context) throws IOException;

        /** Get the current {@link SortedNumericDoubleValues}. */
        public abstract SortedNumericDoubleValues doubleValues(LeafReaderContext context) throws IOException;

        @Override
        public DocValueBits docsWithValue(LeafReaderContext context) throws IOException {
            if (isFloatingPoint()) {
                final SortedNumericDoubleValues values = doubleValues(context);
                return org.elasticsearch.index.fielddata.FieldData.docsWithValue(values);
            } else {
                final SortedNumericDocValues values = longValues(context);
                return org.elasticsearch.index.fielddata.FieldData.docsWithValue(values);
            }
        }

        public static class WithScript extends Numeric {

            private final Numeric delegate;
            private final AggregationScript.LeafFactory script;

            public WithScript(Numeric delegate, AggregationScript.LeafFactory script) {
                this.delegate = delegate;
                this.script = script;
            }

            @Override
            public boolean isFloatingPoint() {
                return true; // even if the underlying source produces longs, scripts can change them to doubles
            }

            @Override
            public boolean needsScores() {
                return script.needs_score();
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                return new ValuesSource.WithScript.BytesValues(delegate.bytesValues(context), script.newInstance(context));
            }

            @Override
            public SortedNumericDocValues longValues(LeafReaderContext context) throws IOException {
                return new LongValues(delegate.longValues(context), script.newInstance(context));
            }

            @Override
            public SortedNumericDoubleValues doubleValues(LeafReaderContext context) throws IOException {
                return new DoubleValues(delegate.doubleValues(context), script.newInstance(context));
            }

            static class LongValues extends AbstractSortingNumericDocValues implements ScorerAware {

                private final SortedNumericDocValues longValues;
                private final AggregationScript script;

                LongValues(SortedNumericDocValues values, AggregationScript script) {
                    this.longValues = values;
                    this.script = script;
                }

                @Override
                public void setScorer(Scorable scorer) {
                    script.setScorer(scorer);
                }

                @Override
                public boolean advanceExact(int target) throws IOException {
                    if (longValues.advanceExact(target)) {
                        resize(longValues.docValueCount());
                        script.setDocument(target);
                        for (int i = 0; i < docValueCount(); ++i) {
                            script.setNextAggregationValue(longValues.nextValue());
                            values[i] = script.runAsLong();
                        }
                        sort();
                        return true;
                    }
                    return false;
                }
            }

            static class DoubleValues extends SortingNumericDoubleValues implements ScorerAware {

                private final SortedNumericDoubleValues doubleValues;
                private final AggregationScript script;

                DoubleValues(SortedNumericDoubleValues values, AggregationScript script) {
                    this.doubleValues = values;
                    this.script = script;
                }

                @Override
                public void setScorer(Scorable scorer) {
                    script.setScorer(scorer);
                }

                @Override
                public boolean advanceExact(int target) throws IOException {
                    if (doubleValues.advanceExact(target)) {
                        resize(doubleValues.docValueCount());
                        script.setDocument(target);
                        for (int i = 0; i < docValueCount(); ++i) {
                            script.setNextAggregationValue(doubleValues.nextValue());
                            values[i] = script.runAsDouble();
                        }
                        sort();
                        return true;
                    }
                    return false;
                }
            }
        }

        public static class FieldData extends Numeric {

            protected final IndexNumericFieldData indexFieldData;

            public FieldData(IndexNumericFieldData indexFieldData) {
                this.indexFieldData = indexFieldData;
            }

            @Override
            public boolean isFloatingPoint() {
                return indexFieldData.getNumericType().isFloatingPoint();
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) {
                return indexFieldData.load(context).getBytesValues();
            }

            @Override
            public SortedNumericDocValues longValues(LeafReaderContext context) {
                return indexFieldData.load(context).getLongValues();
            }

            @Override
            public SortedNumericDoubleValues doubleValues(LeafReaderContext context) {
                return indexFieldData.load(context).getDoubleValues();
            }
        }

        public static class Script extends Numeric {
            private final AggregationScript.LeafFactory script;
            private final ValueType scriptValueType;

            public Script(AggregationScript.LeafFactory script, ValueType scriptValueType) {
                this.script = script;
                this.scriptValueType = scriptValueType;
            }

            @Override
            public boolean isFloatingPoint() {
                return scriptValueType != null ? scriptValueType == ValueType.DOUBLE : true;
            }

            @Override
            public SortedNumericDocValues longValues(LeafReaderContext context) throws IOException {
                return new ScriptLongValues(script.newInstance(context));
            }

            @Override
            public SortedNumericDoubleValues doubleValues(LeafReaderContext context) throws IOException {
                return new ScriptDoubleValues(script.newInstance(context));
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                return new ScriptBytesValues(script.newInstance(context));
            }

            @Override
            public boolean needsScores() {
                return script.needs_score();
            }
        }

    }

    // No need to implement ReaderContextAware here, the delegate already takes care of updating data structures
    public static class WithScript extends Bytes {

        private final ValuesSource delegate;
        private final AggregationScript.LeafFactory script;

        public WithScript(ValuesSource delegate, AggregationScript.LeafFactory script) {
            this.delegate = delegate;
            this.script = script;
        }

        @Override
        public boolean needsScores() {
            return script.needs_score();
        }

        @Override
        public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
            return new BytesValues(delegate.bytesValues(context), script.newInstance(context));
        }

        static class BytesValues extends SortingBinaryDocValues implements ScorerAware {

            private final SortedBinaryDocValues bytesValues;
            private final AggregationScript script;

            BytesValues(SortedBinaryDocValues bytesValues, AggregationScript script) {
                this.bytesValues = bytesValues;
                this.script = script;
            }

            @Override
            public void setScorer(Scorable scorer) {
                script.setScorer(scorer);
            }

            @Override
            public boolean advanceExact(int doc) throws IOException {
                if (bytesValues.advanceExact(doc)) {
                    count = bytesValues.docValueCount();
                    grow();
                    for (int i = 0; i < count; ++i) {
                        final BytesRef value = bytesValues.nextValue();
                        script.setNextAggregationValue(value.utf8ToString());
                        Object run = script.execute();
                        CollectionUtils.ensureNoSelfReferences(run, "ValuesSource.BytesValues script");
                        values[i].copyChars(run.toString());
                    }
                    sort();
                    return true;
                } else {
                    count = 0;
                    grow();
                    return false;
                }
            }
        }
    }

    public abstract static class GeoPoint extends ValuesSource {

        public static final GeoPoint EMPTY = new GeoPoint() {

            @Override
            public MultiGeoPointValues geoPointValues(LeafReaderContext context) {
                return org.elasticsearch.index.fielddata.FieldData.emptyMultiGeoPoints();
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                return org.elasticsearch.index.fielddata.FieldData.emptySortedBinary();
            }

        };

        @Override
        public DocValueBits docsWithValue(LeafReaderContext context) throws IOException {
            final MultiGeoPointValues geoPoints = geoPointValues(context);
            return org.elasticsearch.index.fielddata.FieldData.docsWithValue(geoPoints);
        }

        public abstract MultiGeoPointValues geoPointValues(LeafReaderContext context);

        public static class Fielddata extends GeoPoint {

            protected final IndexGeoPointFieldData indexFieldData;

            public Fielddata(IndexGeoPointFieldData indexFieldData) {
                this.indexFieldData = indexFieldData;
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) {
                return indexFieldData.load(context).getBytesValues();
            }

            public org.elasticsearch.index.fielddata.MultiGeoPointValues geoPointValues(LeafReaderContext context) {
                return indexFieldData.load(context).getGeoPointValues();
            }
        }
    }

}
