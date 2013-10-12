/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.cache.filter;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.metrics.FrugalQuantile;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;

/**
 */
public class FilterCacheStats implements Streamable, ToXContent {

    long memorySize;
    long evictions;
    FrugalQuantile[] quantiles = new FrugalQuantile[4];

    public FilterCacheStats() {
    }

    public FilterCacheStats(long memorySize, long evictions, FrugalQuantile[] quantiles) {
        this.memorySize = memorySize;
        this.evictions = evictions;
        this.quantiles = quantiles;
    }

    public void add(FilterCacheStats stats) {
        this.memorySize += stats.memorySize;
        this.evictions += stats.evictions;

        if (this.quantiles[0] == null) {
            this.quantiles = stats.quantiles;
        } else {
            for (int i = 0; i < 4; ++i) {
                this.quantiles[i].merge(stats.quantiles[i]);
            }
        }

    }

    public long getMemorySizeInBytes() {
        return this.memorySize;
    }

    public ByteSizeValue getMemorySize() {
        return new ByteSizeValue(memorySize);
    }

    public long getEvictions() {
        return this.evictions;
    }

    public static FilterCacheStats readFilterCacheStats(StreamInput in) throws IOException {
        FilterCacheStats stats = new FilterCacheStats();
        stats.readFrom(in);
        return stats;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        memorySize = in.readVLong();
        evictions = in.readVLong();

        for (int i = 0; i < 4; ++i) {
            this.quantiles[i].setValue(in.readVLong());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(memorySize);
        out.writeVLong(evictions);

        for (int i = 0; i < 4; ++i) {
            out.writeVLong(this.quantiles[i].getValue());
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.FILTER_CACHE);
        builder.byteSizeField(Fields.MEMORY_SIZE_IN_BYTES, Fields.MEMORY_SIZE, memorySize);
        builder.field(Fields.EVICTIONS, getEvictions());
        builder.startObject(Fields.EVICTIONS_QUANTILES);
        for (int i = 0; i < 4; ++i) {
            XContentBuilderString field = new XContentBuilderString(quantiles[i].getQuantile() + "%");
            builder.byteSizeField(field, field, quantiles[i].getValue());
        }
        builder.endObject().endObject();
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString FILTER_CACHE = new XContentBuilderString("filter_cache");
        static final XContentBuilderString MEMORY_SIZE = new XContentBuilderString("memory_size");
        static final XContentBuilderString MEMORY_SIZE_IN_BYTES = new XContentBuilderString("memory_size_in_bytes");
        static final XContentBuilderString EVICTIONS = new XContentBuilderString("evictions");
        static final XContentBuilderString EVICTIONS_QUANTILES = new XContentBuilderString("approx_eviction_quantiles_in_bytes");
    }
}
