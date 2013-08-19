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

package org.elasticsearch.search.aggregations.bucket.multi.histogram;

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.context.numeric.ValueFormatter;
import org.elasticsearch.search.aggregations.context.numeric.ValueFormatterStreams;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * An internal implementation of {@link HistogramBase}
 */
abstract class AbstractHistogramBase<B extends HistogramBase.Bucket> extends InternalAggregation implements HistogramBase<B>, ToXContent, Streamable {

    public static class Bucket implements HistogramBase.Bucket {

        private long key;
        private long docCount;
        private InternalAggregations aggregations;

        public Bucket(long key, long docCount, InternalAggregations aggregations) {
            this.key = key;
            this.docCount = docCount;
            this.aggregations = aggregations;
        }

        @Override
        public long getKey() {
            return key;
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        Bucket reduce(List<Bucket> buckets, CacheRecycler cacheRecycler) {
            if (buckets.size() == 1) {
                return buckets.get(0);
            }
            List<InternalAggregations> aggregations = new ArrayList<InternalAggregations>(buckets.size());
            Bucket reduced = null;
            for (Bucket bucket : buckets) {
                if (reduced == null) {
                    reduced = bucket;
                } else {
                    reduced.docCount += bucket.docCount;
                }
                aggregations.add((InternalAggregations) bucket.getAggregations());
            }
            reduced.aggregations = InternalAggregations.reduce(aggregations, cacheRecycler);
            return reduced;
        }
    }


    public static interface Factory<B extends HistogramBase.Bucket> {

        public AbstractHistogramBase create(String name, List<B> buckets, InternalOrder order, ValueFormatter formatter, boolean keyed);

        public Bucket createBucket(long key, long docCount, List<InternalAggregation> aggregations);

    }

    private List<B> buckets;
    private ExtTLongObjectHashMap<HistogramBase.Bucket> bucketsMap;
    private InternalOrder order;
    private ValueFormatter formatter;
    private boolean keyed;

    protected AbstractHistogramBase() {} // for serialization

    protected AbstractHistogramBase(String name, List<B> buckets, InternalOrder order, ValueFormatter formatter, boolean keyed) {
        super(name);
        this.buckets = buckets;
        this.order = order;
        this.formatter = formatter;
        this.keyed = keyed;
    }

    @Override
    public Iterator<B> iterator() {
        return buckets.iterator();
    }

    @Override
    public List<B> buckets() {
        return buckets;
    }

    @Override
    public B getByKey(long key) {
        if (bucketsMap == null) {
            bucketsMap = new ExtTLongObjectHashMap<HistogramBase.Bucket>(buckets.size());
            for (HistogramBase.Bucket bucket : buckets) {
                bucketsMap.put(bucket.getKey(), bucket);
            }
        }
        return (B) bucketsMap.get(key);
    }

    @Override
    public InternalAggregation reduce(ReduceContext reduceContext) {
        List<InternalAggregation> aggregations = reduceContext.aggregations();
        if (aggregations.size() == 1) {
            return aggregations.get(0);
        }
        AbstractHistogramBase reduced = (AbstractHistogramBase) aggregations.get(0);

        Recycler.V<ExtTLongObjectHashMap<List<Bucket>>> bucketsByKey = reduceContext.cacheRecycler().longObjectMap(-1);
        for (InternalAggregation aggregation : aggregations) {
            AbstractHistogramBase<B> histogram = (AbstractHistogramBase) aggregation;
            for (B bucket : histogram.buckets) {
                List<Bucket> bucketList = bucketsByKey.v().get(((Bucket) bucket).key);
                if (bucketList == null) {
                    bucketList = new ArrayList<Bucket>(aggregations.size());
                    bucketsByKey.v().put(((Bucket) bucket).key, bucketList);
                }
                bucketList.add((Bucket) bucket);
            }
        }

        List<HistogramBase.Bucket> buckets = new ArrayList<HistogramBase.Bucket>(bucketsByKey.v().size());
        for (Object value : bucketsByKey.v().internalValues()) {
            if (value == null) {
                continue;
            }
            Bucket bucket = ((List<Bucket>) value).get(0).reduce((List<Bucket>) value, reduceContext.cacheRecycler());
            buckets.add(bucket);
        }
        bucketsByKey.release();
        CollectionUtil.introSort(buckets, order.comparator());
        reduced.buckets = buckets;
        return reduced;
    }

    protected B createBucket(long key, long docCount, InternalAggregations aggregations) {
        return (B) new Bucket(key, docCount, aggregations);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        order = InternalOrder.Streams.readOrder(in);
        formatter = ValueFormatterStreams.readOptional(in);
        keyed = in.readBoolean();
        int size = in.readVInt();
        List<B> buckets = new ArrayList<B>(size);
        for (int i = 0; i < size; i++) {
            buckets.add(createBucket(in.readVLong(), in.readVLong(), InternalAggregations.readAggregations(in)));
        }
        this.buckets = buckets;
        this.bucketsMap = null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        InternalOrder.Streams.writeOrder(order, out);
        ValueFormatterStreams.writeOptional(formatter, out);
        out.writeBoolean(keyed);
        out.writeVInt(buckets.size());
        for (HistogramBase.Bucket bucket : buckets) {
            out.writeVLong(((Bucket) bucket).key);
            out.writeVLong(((Bucket) bucket).docCount);
            ((Bucket) bucket).aggregations.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (keyed) {
            builder.startObject(name);
        } else {
            builder.startArray(name);
        }

        for (HistogramBase.Bucket bucket : buckets) {
            if (formatter != null) {
                Text keyTxt = new StringText(formatter.format(bucket.getKey()));
                if (keyed) {
                    builder.startObject(keyTxt.string());
                } else {
                    builder.startObject();
                }
                builder.field(CommonFields.KEY_AS_STRING, keyTxt);
            } else {
                if (keyed) {
                    builder.startObject(String.valueOf(bucket.getKey()));
                } else {
                    builder.startObject();
                }
            }
            builder.field(CommonFields.KEY, ((Bucket) bucket).key);
            builder.field(CommonFields.DOC_COUNT, ((Bucket) bucket).docCount);
            ((Bucket) bucket).aggregations.toXContentInternal(builder, params);
            builder.endObject();
        }

        if (keyed) {
            builder.endObject();
        } else {
            builder.endArray();
        }
        return builder;
    }
}
