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

import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * An internal implementation of {@link Histogram}
 */
public class InternalHistogram extends InternalAggregation implements Histogram, ToXContent, Streamable {

    public final static Type TYPE = new Type("histogram", "histo");

    private final static AggregationStreams.Stream<InternalHistogram> STREAM = new AggregationStreams.Stream<InternalHistogram>() {
        @Override
        public InternalHistogram readResult(StreamInput in) throws IOException {
            InternalHistogram histogram = new InternalHistogram();
            histogram.readFrom(in);
            return histogram;
        }
    };

    public static void registerStream() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    public static class Bucket implements Histogram.Bucket {

        private long key;
        private long docCount;
        private InternalAggregations aggregations;

        public Bucket(long key, long docCount, List<InternalAggregation> aggregations) {
            this(key, docCount, new InternalAggregations(aggregations));
        }

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

        Bucket redcue(List<Bucket> buckets) {
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
            reduced.aggregations = InternalAggregations.reduce(aggregations);
            return reduced;
        }
    }

    private List<Histogram.Bucket> buckets;
    private ExtTLongObjectHashMap<Histogram.Bucket> bucketsMap;
    private InternalOrder order;
    private boolean keyed;

    public InternalHistogram() {} // for serialization

    public InternalHistogram(String name, List<Histogram.Bucket> buckets, InternalOrder order, boolean keyed) {
        super(name);
        this.buckets = buckets;
        this.order = order;
        this.keyed = keyed;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public Histogram.Bucket getByKey(long key) {
        if (bucketsMap == null) {
            bucketsMap = new ExtTLongObjectHashMap<Histogram.Bucket>(buckets.size());
            for (Histogram.Bucket bucket : buckets) {
                bucketsMap.put(bucket.getKey(), bucket);
            }
        }
        return bucketsMap.get(key);
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations) {
        if (aggregations.size() == 1) {
            return aggregations.get(0);
        }
        InternalHistogram reduced = (InternalHistogram) aggregations.get(0);

        ExtTLongObjectHashMap<List<Bucket>> bucketsByKey = CacheRecycler.popLongObjectMap();
        for (InternalAggregation aggregation : aggregations) {
            InternalHistogram histogram = (InternalHistogram) aggregation;
            for (Histogram.Bucket bucket : histogram.buckets) {
                List<Bucket> bucketList = bucketsByKey.get(((Bucket) bucket).key);
                if (bucketList == null) {
                    bucketList = new ArrayList<Bucket>(aggregations.size());
                    bucketsByKey.put(((Bucket) bucket).key, bucketList);
                }
                bucketList.add((Bucket) bucket);
            }
        }

        List<Histogram.Bucket> buckets = new ArrayList<Histogram.Bucket>(bucketsByKey.size());
        for (Object value : bucketsByKey.internalValues()) {
            if (value == null) {
                continue;
            }
            Bucket bucket = ((List<Bucket>) value).get(0).redcue((List<Bucket>) value);
            buckets.add(bucket);
        }
        CacheRecycler.pushLongObjectMap(bucketsByKey);
        Collections.sort(buckets, order.comparator());
        reduced.buckets = buckets;
        return reduced;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        order = InternalOrder.Streams.readOrder(in);
        keyed = in.readBoolean();
        int size = in.readVInt();
        List<Histogram.Bucket> buckets = new ArrayList<Histogram.Bucket>(size);
        for (int i = 0; i < size; i++) {
            buckets.add(new Bucket(in.readVLong(), in.readVLong(), InternalAggregations.readAggregations(in)));
        }
        this.buckets = buckets;
        this.bucketsMap = null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        InternalOrder.Streams.writeOrder(order, out);
        out.writeBoolean(keyed);
        out.writeVInt(buckets.size());
        for (Histogram.Bucket bucket : buckets) {
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

        for (Histogram.Bucket bucket : buckets) {
            if (keyed) {
                builder.startObject(String.valueOf(((Bucket) bucket).key));
            } else {
                builder.startObject();
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
