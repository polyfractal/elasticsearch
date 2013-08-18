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

package org.elasticsearch.test.integration.search.aggregations.bucket.multi;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.ip.IpFieldMapper;
import org.elasticsearch.search.aggregations.bucket.multi.range.ip4v.IPv4Range;
import org.elasticsearch.search.aggregations.calc.numeric.max.Max;
import org.elasticsearch.search.aggregations.calc.numeric.sum.Sum;
import org.elasticsearch.test.integration.AbstractSharedClusterTest;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;

/**
 *
 */
public class IPv4RangeTests extends AbstractSharedClusterTest {

    @Override
    public Settings getSettings() {
        return randomSettingsBuilder()
                .put("index.number_of_shards", numberOfShards())
                .put("index.number_of_replicas", 0)
                .build();
    }

    protected int numberOfShards() {
        return 5;
    }

    @Before
    public void init() throws Exception {
        createIndexWithMappedType("idx", "type",
                "ip", "type:ip", "ips", "type:ip");

        for (int i = 0; i < 255; i++) {
            client().prepareIndex("idx", "type").setSource(jsonBuilder()
                    .startObject()
                    .field("ip", "10.0.0." + (i))
                    .startArray("ips").value("10.0.0." + i).value("10.0.0." + (i + 1)).endArray()
                    .field("value", (i < 100 ? 1 : i < 200 ? 2 : 3))        // 100 1's, 100 2's, and 55 3's
                    .endObject())
                    .execute().actionGet();
        }

        createIndex("idx_unmapped");

        client().admin().indices().prepareFlush().setRefresh(true).execute().actionGet();
    }

    @Test
    public void singleValueField() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(ipRange("range")
                        .field("ip")
                        .addUnboundedTo("10.0.0.100")
                        .addRange("10.0.0.100", "10.0.0.200")
                        .addUnboundedFrom("10.0.0.200"))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        IPv4Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        IPv4Range.Bucket bucket = range.getByKey("*-10.0.0.100");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-10.0.0.100"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("10.0.0.100"));
        assertThat(bucket.getTo(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getDocCount(), equalTo(100l));

        bucket = range.getByKey("10.0.0.100-10.0.0.200");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("10.0.0.100-10.0.0.200"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.100"));
        assertThat(bucket.getFrom(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getToAsString(), equalTo("10.0.0.200"));
        assertThat(bucket.getTo(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getDocCount(), equalTo(100l));

        bucket = range.getByKey("10.0.0.200-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("10.0.0.200-*"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.200"));
        assertThat(bucket.getFrom(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(55l));
    }

    @Test
    public void singleValueField_WithMaskRange() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(ipRange("range")
                        .field("ip")
                        .addMaskRange("10.0.0.0/25")
                        .addMaskRange("10.0.0.128/25"))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        IPv4Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(2));

        IPv4Range.Bucket bucket = range.getByKey("10.0.0.0/25");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("10.0.0.0/25"));
        assertThat(bucket.getFrom(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.0")));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.0"));
        assertThat(bucket.getTo(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.128")));
        assertThat(bucket.getToAsString(), equalTo("10.0.0.128"));
        assertThat(bucket.getDocCount(), equalTo(128l));

        bucket = range.getByKey("10.0.0.128/25");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("10.0.0.128/25"));
        assertThat((long) bucket.getFrom(), equalTo(IpFieldMapper.ipToLong("10.0.0.128")));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.128"));
        assertThat((long) bucket.getTo(), equalTo(IpFieldMapper.ipToLong("10.0.1.0"))); // range is exclusive on the to side
        assertThat(bucket.getToAsString(), equalTo("10.0.1.0"));
        assertThat(bucket.getDocCount(), equalTo(127l)); // include 10.0.0.128
    }

    @Test
    public void singleValueField_WithCustomKey() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(ipRange("range")
                        .field("ip")
                        .addUnboundedTo("r1", "10.0.0.100")
                        .addRange("r2", "10.0.0.100", "10.0.0.200")
                        .addUnboundedFrom("r3", "10.0.0.200"))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        IPv4Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        IPv4Range.Bucket bucket = range.getByKey("r1");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("r1"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("10.0.0.100"));
        assertThat(bucket.getTo(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getDocCount(), equalTo(100l));

        bucket = range.getByKey("r2");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("r2"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.100"));
        assertThat(bucket.getFrom(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getToAsString(), equalTo("10.0.0.200"));
        assertThat(bucket.getTo(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getDocCount(), equalTo(100l));

        bucket = range.getByKey("r3");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("r3"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.200"));
        assertThat(bucket.getFrom(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(55l));
    }

    @Test
    public void singleValuedField_WithSubAggregation() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(ipRange("range")
                        .field("ip")
                        .addUnboundedTo("10.0.0.100")
                        .addRange("10.0.0.100", "10.0.0.200")
                        .addUnboundedFrom("10.0.0.200")
                        .subAggregation(sum("sum").field("value")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        IPv4Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        IPv4Range.Bucket bucket = range.getByKey("*-10.0.0.100");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-10.0.0.100"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("10.0.0.100"));
        assertThat(bucket.getTo(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getDocCount(), equalTo(100l));
        Sum sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo((double) 100));

        bucket = range.getByKey("10.0.0.100-10.0.0.200");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("10.0.0.100-10.0.0.200"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.100"));
        assertThat(bucket.getFrom(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getToAsString(), equalTo("10.0.0.200"));
        assertThat(bucket.getTo(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getDocCount(), equalTo(100l));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo((double) 200));

        bucket = range.getByKey("10.0.0.200-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("10.0.0.200-*"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.200"));
        assertThat(bucket.getFrom(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(55l));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo((double) 55*3));
    }

    @Test
    public void singleValuedField_WithSubAggregation_Inherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(ipRange("range")
                        .field("ip")
                        .addUnboundedTo("10.0.0.100")
                        .addRange("10.0.0.100", "10.0.0.200")
                        .addUnboundedFrom("10.0.0.200")
                        .subAggregation(max("max")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        IPv4Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        IPv4Range.Bucket bucket = range.getByKey("*-10.0.0.100");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-10.0.0.100"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("10.0.0.100"));
        assertThat(bucket.getTo(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getDocCount(), equalTo(100l));
        Max max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.99")));

        bucket = range.getByKey("10.0.0.100-10.0.0.200");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("10.0.0.100-10.0.0.200"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.100"));
        assertThat(bucket.getFrom(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getToAsString(), equalTo("10.0.0.200"));
        assertThat(bucket.getTo(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getDocCount(), equalTo(100l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.199")));

        bucket = range.getByKey("10.0.0.200-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("10.0.0.200-*"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.200"));
        assertThat(bucket.getFrom(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(55l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.254")));
    }

    @Test
    public void singleValuedField_WithValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(ipRange("range")
                        .field("ip")
                        .script("_value")
                        .addUnboundedTo("10.0.0.100")
                        .addRange("10.0.0.100", "10.0.0.200")
                        .addUnboundedFrom("10.0.0.200"))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        IPv4Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        IPv4Range.Bucket bucket = range.getByKey("*-10.0.0.100");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-10.0.0.100"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("10.0.0.100"));
        assertThat(bucket.getTo(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getDocCount(), equalTo(100l));

        bucket = range.getByKey("10.0.0.100-10.0.0.200");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("10.0.0.100-10.0.0.200"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.100"));
        assertThat(bucket.getFrom(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getToAsString(), equalTo("10.0.0.200"));
        assertThat(bucket.getTo(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getDocCount(), equalTo(100l));

        bucket = range.getByKey("10.0.0.200-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("10.0.0.200-*"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.200"));
        assertThat(bucket.getFrom(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(55l));
    }

    /*
    [0, 1]
    [1, 2]
    [2, 3]
    ...
    [99, 100]
    [100, 101]
    [101, 102]
    ...
    [199, 200]
    [200, 201]
    [201, 202]
    ...
    [254, 255]
    [255, 256]
     */

    @Test
    public void multiValuedField() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(ipRange("range")
                        .field("ips")
                        .addUnboundedTo("10.0.0.100")
                        .addRange("10.0.0.100", "10.0.0.200")
                        .addUnboundedFrom("10.0.0.200"))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        IPv4Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        IPv4Range.Bucket bucket = range.getByKey("*-10.0.0.100");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-10.0.0.100"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("10.0.0.100"));
        assertThat(bucket.getTo(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getDocCount(), equalTo(100l));

        bucket = range.getByKey("10.0.0.100-10.0.0.200");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("10.0.0.100-10.0.0.200"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.100"));
        assertThat(bucket.getFrom(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getToAsString(), equalTo("10.0.0.200"));
        assertThat(bucket.getTo(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getDocCount(), equalTo(101l));

        bucket = range.getByKey("10.0.0.200-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("10.0.0.200-*"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.200"));
        assertThat(bucket.getFrom(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(56l));
    }

    @Test
    public void multiValuedField_WithValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(ipRange("range")
                        .field("ips")
                        .script("_value")
                        .addUnboundedTo("10.0.0.100")
                        .addRange("10.0.0.100", "10.0.0.200")
                        .addUnboundedFrom("10.0.0.200"))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        IPv4Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        IPv4Range.Bucket bucket = range.getByKey("*-10.0.0.100");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-10.0.0.100"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("10.0.0.100"));
        assertThat(bucket.getTo(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getDocCount(), equalTo(100l));

        bucket = range.getByKey("10.0.0.100-10.0.0.200");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("10.0.0.100-10.0.0.200"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.100"));
        assertThat(bucket.getFrom(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getToAsString(), equalTo("10.0.0.200"));
        assertThat(bucket.getTo(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getDocCount(), equalTo(101l));

        bucket = range.getByKey("10.0.0.200-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("10.0.0.200-*"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.200"));
        assertThat(bucket.getFrom(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(56l));
    }

    @Test
    public void multiValuedField_WithValueScript_WithInheritedSubAggregator() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(ipRange("range")
                        .field("ips")
                        .script("_value")
                        .addUnboundedTo("10.0.0.100")
                        .addRange("10.0.0.100", "10.0.0.200")
                        .addUnboundedFrom("10.0.0.200")
                        .subAggregation(max("max")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        IPv4Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        IPv4Range.Bucket bucket = range.getByKey("*-10.0.0.100");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-10.0.0.100"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("10.0.0.100"));
        assertThat(bucket.getTo(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getDocCount(), equalTo(100l));
        Max max = bucket.getAggregations().get("max");
        assertThat(max, Matchers.notNullValue());
        assertThat((long) max.getValue(), equalTo(IpFieldMapper.ipToLong("10.0.0.99")));

        bucket = range.getByKey("10.0.0.100-10.0.0.200");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("10.0.0.100-10.0.0.200"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.100"));
        assertThat(bucket.getFrom(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getToAsString(), equalTo("10.0.0.200"));
        assertThat(bucket.getTo(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getDocCount(), equalTo(101l));
        max = bucket.getAggregations().get("max");
        assertThat(max, Matchers.notNullValue());
        assertThat((long) max.getValue(), equalTo(IpFieldMapper.ipToLong("10.0.0.199")));

        bucket = range.getByKey("10.0.0.200-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("10.0.0.200-*"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.200"));
        assertThat(bucket.getFrom(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(56l));
        max = bucket.getAggregations().get("max");
        assertThat(max, Matchers.notNullValue());
        assertThat((long) max.getValue(), equalTo(IpFieldMapper.ipToLong("10.0.0.255")));
    }

    @Test
    public void script_SingleValue() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(ipRange("range")
                        .script("doc['ip'].value")
                        .addUnboundedTo("10.0.0.100")
                        .addRange("10.0.0.100", "10.0.0.200")
                        .addUnboundedFrom("10.0.0.200"))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        IPv4Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        IPv4Range.Bucket bucket = range.getByKey("*-10.0.0.100");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-10.0.0.100"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("10.0.0.100"));
        assertThat(bucket.getTo(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getDocCount(), equalTo(100l));

        bucket = range.getByKey("10.0.0.100-10.0.0.200");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("10.0.0.100-10.0.0.200"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.100"));
        assertThat(bucket.getFrom(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getToAsString(), equalTo("10.0.0.200"));
        assertThat(bucket.getTo(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getDocCount(), equalTo(100l));

        bucket = range.getByKey("10.0.0.200-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("10.0.0.200-*"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.200"));
        assertThat(bucket.getFrom(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(55l));
    }

    @Test
    public void script_SingleValue_WithSubAggregator_Inherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(ipRange("range")
                        .script("doc['ip'].value")
                        .addUnboundedTo("10.0.0.100")
                        .addRange("10.0.0.100", "10.0.0.200")
                        .addUnboundedFrom("10.0.0.200")
                        .subAggregation(max("max")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        IPv4Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        IPv4Range.Bucket bucket = range.getByKey("*-10.0.0.100");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-10.0.0.100"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("10.0.0.100"));
        assertThat(bucket.getTo(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getDocCount(), equalTo(100l));
        Max max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.99")));

        bucket = range.getByKey("10.0.0.100-10.0.0.200");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("10.0.0.100-10.0.0.200"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.100"));
        assertThat(bucket.getFrom(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getToAsString(), equalTo("10.0.0.200"));
        assertThat(bucket.getTo(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getDocCount(), equalTo(100l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.199")));

        bucket = range.getByKey("10.0.0.200-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("10.0.0.200-*"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.200"));
        assertThat(bucket.getFrom(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(55l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.254")));
    }

    @Test
    public void script_MultiValued() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(ipRange("range")
                        .script("doc['ips'].values")
                        .addUnboundedTo("10.0.0.100")
                        .addRange("10.0.0.100", "10.0.0.200")
                        .addUnboundedFrom("10.0.0.200"))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        IPv4Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        IPv4Range.Bucket bucket = range.getByKey("*-10.0.0.100");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-10.0.0.100"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("10.0.0.100"));
        assertThat(bucket.getTo(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getDocCount(), equalTo(100l));

        bucket = range.getByKey("10.0.0.100-10.0.0.200");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("10.0.0.100-10.0.0.200"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.100"));
        assertThat(bucket.getFrom(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getToAsString(), equalTo("10.0.0.200"));
        assertThat(bucket.getTo(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getDocCount(), equalTo(101l));

        bucket = range.getByKey("10.0.0.200-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("10.0.0.200-*"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.200"));
        assertThat(bucket.getFrom(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(56l));
    }

    @Test
    public void script_MultiValued_WithAggregatorInherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(ipRange("range")
                        .script("doc['ips'].values")
                        .addUnboundedTo("10.0.0.100")
                        .addRange("10.0.0.100", "10.0.0.200")
                        .addUnboundedFrom("10.0.0.200")
                        .subAggregation(max("max")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        IPv4Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        IPv4Range.Bucket bucket = range.getByKey("*-10.0.0.100");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-10.0.0.100"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("10.0.0.100"));
        assertThat(bucket.getTo(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getDocCount(), equalTo(100l));
        Max max = bucket.getAggregations().get("max");
        assertThat(max, Matchers.notNullValue());
        assertThat((long) max.getValue(), equalTo(IpFieldMapper.ipToLong("10.0.0.99")));

        bucket = range.getByKey("10.0.0.100-10.0.0.200");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("10.0.0.100-10.0.0.200"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.100"));
        assertThat(bucket.getFrom(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getToAsString(), equalTo("10.0.0.200"));
        assertThat(bucket.getTo(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getDocCount(), equalTo(101l));
        max = bucket.getAggregations().get("max");
        assertThat(max, Matchers.notNullValue());
        assertThat((long) max.getValue(), equalTo(IpFieldMapper.ipToLong("10.0.0.199")));

        bucket = range.getByKey("10.0.0.200-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("10.0.0.200-*"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.200"));
        assertThat(bucket.getFrom(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(56l));
        max = bucket.getAggregations().get("max");
        assertThat(max, Matchers.notNullValue());
        assertThat((long) max.getValue(), equalTo(IpFieldMapper.ipToLong("10.0.0.255")));
    }

    @Test
    public void unmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx_unmapped")
                .addAggregation(ipRange("range")
                        .field("ip")
                        .addUnboundedTo("10.0.0.100")
                        .addRange("10.0.0.100", "10.0.0.200")
                        .addUnboundedFrom("10.0.0.200"))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        IPv4Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        IPv4Range.Bucket bucket = range.getByKey("*-10.0.0.100");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-10.0.0.100"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("10.0.0.100"));
        assertThat(bucket.getTo(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getDocCount(), equalTo(0l));

        bucket = range.getByKey("10.0.0.100-10.0.0.200");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("10.0.0.100-10.0.0.200"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.100"));
        assertThat(bucket.getFrom(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getToAsString(), equalTo("10.0.0.200"));
        assertThat(bucket.getTo(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getDocCount(), equalTo(0l));

        bucket = range.getByKey("10.0.0.200-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("10.0.0.200-*"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.200"));
        assertThat(bucket.getFrom(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(0l));
    }

    @Test
    public void partiallyUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx", "idx_unmapped")
                .addAggregation(ipRange("range")
                        .field("ip")
                        .addUnboundedTo("10.0.0.100")
                        .addRange("10.0.0.100", "10.0.0.200")
                        .addUnboundedFrom("10.0.0.200"))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        IPv4Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        assertThat(range.buckets().size(), equalTo(3));

        IPv4Range.Bucket bucket = range.getByKey("*-10.0.0.100");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("*-10.0.0.100"));
        assertThat(bucket.getFrom(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(bucket.getFromAsString(), nullValue());
        assertThat(bucket.getToAsString(), equalTo("10.0.0.100"));
        assertThat(bucket.getTo(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getDocCount(), equalTo(100l));

        bucket = range.getByKey("10.0.0.100-10.0.0.200");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("10.0.0.100-10.0.0.200"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.100"));
        assertThat(bucket.getFrom(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.100")));
        assertThat(bucket.getToAsString(), equalTo("10.0.0.200"));
        assertThat(bucket.getTo(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getDocCount(), equalTo(100l));

        bucket = range.getByKey("10.0.0.200-*");
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("10.0.0.200-*"));
        assertThat(bucket.getFromAsString(), equalTo("10.0.0.200"));
        assertThat(bucket.getFrom(), equalTo((double) IpFieldMapper.ipToLong("10.0.0.200")));
        assertThat(bucket.getTo(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(55l));
    }


}
