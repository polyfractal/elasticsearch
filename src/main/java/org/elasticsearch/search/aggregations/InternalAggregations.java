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

package org.elasticsearch.search.aggregations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.util.*;

import static com.google.common.collect.Maps.newHashMap;

/**
 * An internal implementation of {@link Aggregations}.
 */
public class InternalAggregations implements Aggregations, ToXContent, Streamable {

    public final static InternalAggregations EMPTY = new InternalAggregations();

    private List<InternalAggregation> aggregations = ImmutableList.of();

    private Map<String, InternalAggregation> aggregationsAsMap;

    private InternalAggregations() {
    }

    /**
     * Constructs a new addAggregation.
     */
    public InternalAggregations(List<InternalAggregation> aggregations) {
        this.aggregations = aggregations;
    }

    /** Resets the internal addAggregation */
    void reset(List<InternalAggregation> aggregations) {
        this.aggregations = aggregations;
        this.aggregationsAsMap = null;
    }

    /**
     * Iterates over the {@link Aggregation}s.
     */
    @Override
    public Iterator<Aggregation> iterator() {
        Object iter = aggregations.iterator();
        return (Iterator<Aggregation>) iter;
    }

    /**
     * The list of {@link Aggregation}s.
     */
    public List<Aggregation> asList() {
        Object agg = aggregations;
        return (List<Aggregation>)agg;
    }

    /**
     * Returns the {@link Aggregation}s keyed by map.
     */
    public Map<String, Aggregation> asMap() {
        return getAsMap();
    }

    /**
     * Returns the {@link Aggregation}s keyed by map.
     */
    public Map<String, Aggregation> getAsMap() {
        if (aggregationsAsMap == null) {
            Map<String, InternalAggregation> aggregationsAsMap = newHashMap();
            for (InternalAggregation aggregation : aggregations) {
                aggregationsAsMap.put(aggregation.getName(), aggregation);
            }
            this.aggregationsAsMap = aggregationsAsMap;
        }
        Object map = aggregationsAsMap;
        return (Map<String, Aggregation>) map;
    }

    /**
     * A get of the specified name.
     */
    @SuppressWarnings({"unchecked"})
    @Override
    public <A extends Aggregation> A get(String name) {
        return (A) asMap().get(name);
    }

    /**
     * Reduces the given lists of addAggregation.
     *
     * @param aggregationsList  A list of addAggregation to reduce
     * @return                  The reduced addAggregation
     */
    public static InternalAggregations reduce(List<InternalAggregations> aggregationsList) {
        if (aggregationsList.isEmpty()) {
            return null;
        }

        // first we collect all addAggregation of the same type and list them together

        Map<String, List<InternalAggregation>> aggByName = new HashMap<String, List<InternalAggregation>>();
        for (InternalAggregations aggregations : aggregationsList) {
            for (InternalAggregation aggregation : aggregations.aggregations) {
                List<InternalAggregation> aggs = aggByName.get(aggregation.getName());
                if (aggs == null) {
                    aggs = new ArrayList<InternalAggregation>(aggregationsList.size());
                    aggByName.put(aggregation.getName(), aggs);
                }
                aggs.add(aggregation);
            }
        }

        // now we can use the first get of each list to handle the reduce of its list

        List<InternalAggregation> reducedAggregations = new ArrayList<InternalAggregation>();
        for (Map.Entry<String, List<InternalAggregation>> entry : aggByName.entrySet()) {
            List<InternalAggregation> aggregations = entry.getValue();
            InternalAggregation first = aggregations.get(0); // the list can't be empty as it's created on demand
            reducedAggregations.add(first.reduce(aggregations));
        }
        InternalAggregations result = aggregationsList.get(0);
        result.reset(reducedAggregations);
        return result;
    }

    /** The fields required to write this addAggregation to xcontent */
    static class Fields {
        public static final XContentBuilderString AGGREGATIONS = new XContentBuilderString("aggregations");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (aggregations.isEmpty()) {
            return builder;
        }
        builder.startObject(Fields.AGGREGATIONS);
        toXContentInternal(builder, params);
        return builder.endObject();
    }

    /**
     * Directly write all the addAggregation without their bounding object. Used by sub-addAggregation (non top level addAggregation)
     */
    public XContentBuilder toXContentInternal(XContentBuilder builder, Params params) throws IOException {
        for (Aggregation aggregation : aggregations) {
            ((InternalAggregation) aggregation).toXContent(builder, params);
        }
        return builder;
    }

    public static InternalAggregations readAggregations(StreamInput in) throws IOException {
        InternalAggregations result = new InternalAggregations();
        result.readFrom(in);
        return result;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        if (size == 0) {
            aggregations = ImmutableList.of();
            aggregationsAsMap = ImmutableMap.of();
        } else {
            aggregations = Lists.newArrayListWithCapacity(size);
            for (int i = 0; i < size; i++) {
                BytesReference type = in.readBytesReference();
                InternalAggregation aggregation = AggregationStreams.stream(type).readResult(in);
                aggregations.add(aggregation);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(aggregations.size());
        for (Aggregation aggregation : aggregations) {
            InternalAggregation internal = (InternalAggregation) aggregation;
            out.writeBytesReference(internal.type().stream());
            internal.writeTo(out);
        }
    }

}
