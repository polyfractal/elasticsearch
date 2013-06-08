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

package org.elasticsearch.search.aggregations.bucket.histogram.date;

import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.aggregations.*;

import java.io.IOException;
import java.util.*;

/**
 *
 */
public class InternalDateHistogram extends InternalAggregation implements DateHistogram, ToXContent, Streamable {

    public static final Type TYPE = new Type("date_histogram", "dhisto");

    private static final AggregationStreams.Stream<InternalDateHistogram> STREAM = new AggregationStreams.Stream<InternalDateHistogram>() {
        @Override
        public InternalDateHistogram readResult(StreamInput in) throws IOException {
            InternalDateHistogram histogram = new InternalDateHistogram();
            histogram.readFrom(in);
            return histogram;
        }
    };

    public static void registerStream() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    public static class Bucket implements DateHistogram.Bucket {

        private long time;
        private String timeAsString;
        private long docCount;
        private InternalAggregations aggregations;

        public Bucket(long time, long docCount, List<InternalAggregation> aggregations) {
            this(time, null, docCount, aggregations);
        }

        public Bucket(long time, @Nullable String timeAsString, long docCount, List<InternalAggregation> aggregations) {
            this(time, timeAsString, docCount, new InternalAggregations(aggregations));
        }

        public Bucket(long time, long docCount, InternalAggregations aggregations) {
            this(time, String.valueOf(time), docCount, aggregations);
        }

        public Bucket(long time, @Nullable String timeAsString, long docCount, InternalAggregations aggregations) {
            this.time = time;
            this.docCount = docCount;
            this.aggregations = aggregations;
            this.timeAsString = timeAsString;
        }

        @Override
        public long getTime() {
            return time;
        }

        @Override
        public String getTimeAsString() {
            return timeAsString == null ? String.valueOf(time) : timeAsString;
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        Bucket reduce(List<Bucket> buckets) {
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

    private Collection<DateHistogram.Bucket> buckets;
    private ExtTLongObjectHashMap<DateHistogram.Bucket> bucketsMap;
    private Order order;
    private boolean keyed;

    public InternalDateHistogram() {} // for serialization

    public InternalDateHistogram(String name, Collection<? extends DateHistogram.Bucket> buckets, Order order, boolean keyed) {
        super(name);
        this.buckets = (Collection<DateHistogram.Bucket>) buckets;
        this.order = order;
        this.keyed = keyed;
    }

    @Override
    public DateHistogram.Bucket getByTime(long time) {
        if (bucketsMap == null) {
            bucketsMap = new ExtTLongObjectHashMap<DateHistogram.Bucket>(buckets.size());
            for (DateHistogram.Bucket bucket : buckets) {
                bucketsMap.put(bucket.getTime(), bucket);
            }
        }
        return bucketsMap.get(time);
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations) {
        if (aggregations.size() == 1) {
            return aggregations.get(0);
        }
        InternalDateHistogram reduced = (InternalDateHistogram) aggregations.get(0);

        ExtTLongObjectHashMap<List<Bucket>> bucketsByKey = CacheRecycler.popLongObjectMap();
        for (InternalAggregation aggregation : aggregations) {
            InternalDateHistogram histogram = (InternalDateHistogram) aggregation;
            for (DateHistogram.Bucket bucket : histogram.buckets) {
                List<Bucket> bucketList = bucketsByKey.get(bucket.getTime());
                if (bucketList == null) {
                    bucketList = new ArrayList<Bucket>(aggregations.size());
                    bucketsByKey.put(bucket.getTime(), bucketList);
                }
                bucketList.add((Bucket) bucket);
            }
        }

        List<DateHistogram.Bucket> buckets = new ArrayList<DateHistogram.Bucket>(bucketsByKey.size());
        for (Object value : bucketsByKey.internalValues()) {
            if (value == null) {
                continue;
            }
            Bucket bucket = ((List<Bucket>) value).get(0).reduce((List<Bucket>) value);
            buckets.add(bucket);
        }
        CacheRecycler.pushLongObjectMap(bucketsByKey);
        Collections.sort(buckets, order.comparator());
        reduced.buckets = buckets;
        return reduced;
    }

    @Override
    public Iterator<DateHistogram.Bucket> iterator() {
        return buckets.iterator();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        order = InternalDateOrder.Streams.readOrder(in);
        keyed = in.readBoolean();
        int size = in.readVInt();
        List<DateHistogram.Bucket> buckets = new ArrayList<DateHistogram.Bucket>(size);
        for (int i = 0; i < size; i++) {
            buckets.add(new Bucket(in.readVLong(), (in.readBoolean() ? in.readString() : null), in.readVLong(), InternalAggregations.readAggregations(in)));
        }
        this.buckets = buckets;
        this.bucketsMap = null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        InternalDateOrder.Streams.writeOrder(order, out);
        out.writeBoolean(keyed);
        out.writeVInt(buckets.size());
        for (DateHistogram.Bucket bucket : buckets) {
            out.writeVLong(bucket.getTime());
            boolean timeAsStringExists = ((Bucket) bucket).timeAsString != null;
            out.writeBoolean(timeAsStringExists);
            if (timeAsStringExists) {
                out.writeString(((Bucket) bucket).timeAsString);
            }
            out.writeVLong(bucket.getDocCount());
            ((Bucket) bucket).aggregations.writeTo(out);
        }
    }

    static class Field {
        public static final XContentBuilderString TIME = new XContentBuilderString("time");
        public static final XContentBuilderString TIME_AS_STRING = new XContentBuilderString("time_as_string");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (keyed) {
            builder.startObject(name);
        } else {
            builder.startArray(name);
        }

        for (DateHistogram.Bucket bucket : buckets) {
            if (keyed) {
                builder.startObject(bucket.getTimeAsString());
            } else {
                builder.startObject();
            }
            builder.field(Field.TIME, bucket.getTime());
            if (((Bucket) bucket).timeAsString != null) {
                builder.field(Field.TIME_AS_STRING, ((Bucket) bucket).timeAsString);
            }
            builder.field(CommonFields.DOC_COUNT, bucket.getDocCount());
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
