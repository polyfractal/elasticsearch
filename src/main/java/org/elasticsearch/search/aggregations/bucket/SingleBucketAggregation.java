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

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
@SuppressWarnings("unchecked")
public abstract class SingleBucketAggregation<B extends SingleBucketAggregation<B>> extends InternalAggregation {

    protected long docCount;
    protected InternalAggregations aggregations;

    protected SingleBucketAggregation() {} // for serialization

    protected SingleBucketAggregation(String name, long docCount, List<InternalAggregation> subAggregations) {
        super(name);
        this.docCount = docCount;
        this.aggregations = new InternalAggregations(subAggregations);
    }

    public long getDocCount() {
        return docCount;
    }

    public InternalAggregations getAggregations() {
        return aggregations;
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations) {
        if (aggregations.size() == 1) {
            return aggregations.get(0);
        }
        B reduced = null;
        List<InternalAggregations> subAggregationsList = new ArrayList<InternalAggregations>(aggregations.size());
        for (InternalAggregation aggregation : aggregations) {
            if (reduced == null) {
                reduced = (B) aggregation;
            } else {
                this.docCount += ((B) aggregation).docCount;
            }
            subAggregationsList.add(((B) aggregation).aggregations);
        }
        reduced.aggregations = InternalAggregations.reduce(subAggregationsList);
        return reduced;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        docCount = in.readVLong();
        aggregations = InternalAggregations.readAggregations(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeVLong(docCount);
        aggregations.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field(CommonFields.DOC_COUNT, docCount);
        aggregations.toXContentInternal(builder, params);
        return builder.endObject();
    }
}
