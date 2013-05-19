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
import org.elasticsearch.common.trove.ExtTObjectIntHasMap;
import org.elasticsearch.index.fielddata.*;

import java.util.List;

/**
 * Used by all field data based aggregators. This determine the context of the field data the aggregators are operating
 * in. I holds both the field names and the index field datas that are associated with them.
 */
public class FieldDataContext {

    private final String[] fields;
    private final IndexFieldData[] indexFieldDatas;
    private final ExtTObjectIntHasMap<String> fieldIndex;

    /**
     * Constructs a field data context for the given field and its index field data
     *
     * @param field             The name of the field
     * @param indexFieldData    The index field data of the field
     */
    public FieldDataContext(String field, IndexFieldData indexFieldData) {
        this(new String[] { field }, new IndexFieldData[] { indexFieldData });
    }

    /**
     * Constructs a field data context for the given fields and their associated index field data (arrays are parallel,
     * meaning, index field data at location i, belongs to field at location i)
     *
     * @param fields            The name of the fields
     * @param indexFieldDatas   The index field datas of the fields
     */
    public FieldDataContext(String[] fields, IndexFieldData[] indexFieldDatas) {
        this.fields = fields;
        this.indexFieldDatas = indexFieldDatas;
        this.fieldIndex = new ExtTObjectIntHasMap<String>(fields.length);
        for (int i = 0; i < fields.length; i++) {
            fieldIndex.put(fields[i], i);
        }
    }

    /**
     * Constructs a field data context for the given fields and their associated index field data (arrays are parallel,
     * meaning, index field data at location i, belongs to field at location i)
     *
     * @param fields            The name of the fields
     * @param indexFieldDatas   The index field datas of the fields
     */
    public FieldDataContext(List<String> fields, List<IndexFieldData> indexFieldDatas) {
        this(fields.toArray(new String[fields.size()]), indexFieldDatas.toArray(new IndexFieldData[indexFieldDatas.size()]));
    }

    /**
     * @return Whether the index field datas are all numeric.
     */
    public boolean isFullyNumeric() {
        for (int i = 0; i < indexFieldDatas.length; i++) {
            if (!(indexFieldDatas[i] instanceof IndexNumericFieldData)) {
                return false;
            }
        }
        return true;
    }

    /**
     * @return The field names in this context
     */
    public String[] fields() {
        return fields;
    }

    public int fieldCount() {
        return fields.length;
    }

    public String field(int i) {
        return fields[i];
    }

    public int fieldIndex(String field) {
        return fieldIndex.get(field);
    }

    public boolean hasField(String field) {
        return fieldIndex.contains(field);
    }


    /**
     * @return The index field datas in this context
     */
    public IndexFieldData[] indexFieldDatas() {
        return indexFieldDatas;
    }

    /**
     * Loads and returns the field datas of this context for the given reader
     *
     * @param context The reader context
     * @return The field data of this context for the given reader
     */
    public AtomicFieldData[] loadAtomic(AtomicReaderContext context) {
        return loadAtomic(context, new AtomicFieldData[indexFieldDatas.length]);
    }

    /**
     * Loads and returns the field datas of this context for the given reader. The field datas will be loaded into
     * the given field data array and this same array will be the one returned.
     *
     * @param context       The reader context
     * @param fieldDatas    The field data arrays that will be loaded and returned
     * @return              The loaded field data array (same as the one passed in)
     */
    public AtomicFieldData[] loadAtomic(AtomicReaderContext context, AtomicFieldData[] fieldDatas) {
        assert fieldDatas.length == indexFieldDatas.length : "Given field data array length must match the number of fields in this context";
        for (int i = 0; i < indexFieldDatas.length; i++) {
            fieldDatas[i] = indexFieldDatas[i].load(context);
        }
        return fieldDatas;
    }

    /**
     * Loads and returns the field values of this context for the given reader.
     *
     * @param context       The reader context
     * @return              The loaded field values array
     */
    public BytesValues[] loadBytesValues(AtomicReaderContext context) {
        return loadBytesValues(context, new BytesValues[indexFieldDatas.length]);
    }

    /**
     * Loads and returns the field values of this context for the given reader. The field values will be loaded into
     * the given field values array and this same array will be the one returned.
     *
     * @param context       The reader context
     * @param values        The field values arrays that will be loaded and returned
     * @return              The loaded field values array (same as the one passed in)
     */
    public BytesValues[] loadBytesValues(AtomicReaderContext context, BytesValues[] values) {
        assert values.length == indexFieldDatas.length : "Given field values array length must match the number of fields in this context";
        for (int i = 0; i < indexFieldDatas.length; i++) {
            values[i] = indexFieldDatas[i].load(context).getBytesValues();
        }
        return values;
    }

    /**
     * Loads and returns the field values of this context for the given reader.
     *
     * @param context       The reader context
     * @return              The loaded field values array
     */
    public DoubleValues[] loadDoubleValues(AtomicReaderContext context) {
        return loadDoubleValues(context, new DoubleValues[indexFieldDatas.length]);
    }

    /**
     * Loads and returns the field values of this context for the given reader. The field values will be loaded into
     * the given field values array and this same array will be the one returned.
     *
     * @param context       The reader context
     * @param values        The field values arrays that will be loaded and returned
     * @return              The loaded field values array (same as the one passed in)
     */
    public DoubleValues[] loadDoubleValues(AtomicReaderContext context, DoubleValues[] values) {
        assert values.length == indexFieldDatas.length : "Given field values array length must match the number of fields in this context";
        for (int i = 0; i < indexFieldDatas.length; i++) {
            values[i] = ((AtomicNumericFieldData) indexFieldDatas[i].load(context)).getDoubleValues();
        }
        return values;
    }

    /**
     * Loads and returns the field values of this context for the given reader.
     *
     * @param context       The reader context
     * @return              The loaded field values array
     */
    public LongValues[] loadLongValues(AtomicReaderContext context) {
        return loadLongValues(context, new LongValues[indexFieldDatas.length]);
    }

    /**
     * Loads and returns the field values of this context for the given reader. The field values will be loaded into
     * the given field values array and this same array will be the one returned.
     *
     * @param context       The reader context
     * @param values        The field values arrays that will be loaded and returned
     * @return              The loaded field values array (same as the one passed in)
     */
    public LongValues[] loadLongValues(AtomicReaderContext context, LongValues[] values) {
        assert values.length == indexFieldDatas.length : "Given field values array length must match the number of fields in this context";
        for (int i = 0; i < indexFieldDatas.length; i++) {
            values[i] = ((AtomicNumericFieldData) indexFieldDatas[i].load(context)).getLongValues();
        }
        return values;
    }

}
