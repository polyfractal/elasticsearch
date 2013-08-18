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
import org.elasticsearch.search.aggregations.bucket.multi.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.calc.numeric.max.Max;
import org.elasticsearch.search.aggregations.calc.numeric.sum.Sum;
import org.elasticsearch.test.integration.AbstractSharedClusterTest;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 *
 */
public class DateHistogramTests extends AbstractSharedClusterTest {

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

    private DateTime date(int month, int day) {
        return new DateTime(2012, month, day, 0, 0, DateTimeZone.UTC);
    }

    private void indexDoc(int month, int day, int value) throws Exception {
        client().prepareIndex("idx", "type").setSource(jsonBuilder()
                .startObject()
                .field("value", value)
                .field("date", date(month, day))
                .startArray("dates").value(date(month, day)).value(date(month + 1, day + 1)).endArray()
                .endObject())
                .execute().actionGet();
    }

    @Before
    public void init() throws Exception {
        createIndex("idx");

        indexDoc(1, 2, 1);  // date: Jan 2, dates: Jan 2, Feb 3
        indexDoc(2, 2, 2);  // date: Feb 2, dates: Feb 2, Mar 3
        indexDoc(2, 15, 3); // date: Feb 15, dates: Feb 15, Mar 16
        indexDoc(3, 2, 4);  // date: Mar 2, dates: Mar 2, Apr 3
        indexDoc(3, 15, 5); // date: Mar 15, dates: Mar 15, Apr 16
        indexDoc(3, 23, 6); // date: Mar 23, dates: Mar 23, Apr 24

        createIndex("idx_unmapped");

        client().admin().indices().prepareFlush().setRefresh(true).execute().actionGet();
    }

    @Test
    public void singleValuedField() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo").field("date").interval(DateHistogram.Interval.MONTH))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(3));

        long key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC).getMillis();
        DateHistogram.Bucket bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(2l));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
    }

    @Test
    public void singleValuedField_OrderedByKeyAsc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("date")
                        .interval(DateHistogram.Interval.MONTH)
                        .order(DateHistogram.Order.KEY_ASC))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(3));

        int i = 0;
        for (DateHistogram.Bucket bucket : histo.buckets()) {
            assertThat(bucket.getKey(), equalTo(new DateTime(2012, i+1, 1, 0, 0, DateTimeZone.UTC).getMillis()));
            i++;
        }
    }

    @Test
    public void singleValuedField_OrderedByKeyDesc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("date")
                        .interval(DateHistogram.Interval.MONTH)
                        .order(DateHistogram.Order.KEY_DESC))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(3));

        int i = 2;
        for (DateHistogram.Bucket bucket : histo.buckets()) {
            assertThat(bucket.getKey(), equalTo(new DateTime(2012, i+1, 1, 0, 0, DateTimeZone.UTC).getMillis()));
            i--;
        }
    }

    @Test
    public void singleValuedField_OrderedByCountAsc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("date")
                        .interval(DateHistogram.Interval.MONTH)
                        .order(DateHistogram.Order.COUNT_ASC))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(3));

        int i = 0;
        for (DateHistogram.Bucket bucket : histo.buckets()) {
            assertThat(bucket.getKey(), equalTo(new DateTime(2012, i+1, 1, 0, 0, DateTimeZone.UTC).getMillis()));
            i++;
        }
    }

    @Test
    public void singleValuedField_OrderedByCountDesc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("date")
                        .interval(DateHistogram.Interval.MONTH)
                        .order(DateHistogram.Order.COUNT_DESC))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(3));

        int i = 2;
        for (DateHistogram.Bucket bucket : histo.buckets()) {
            assertThat(bucket.getKey(), equalTo(new DateTime(2012, i+1, 1, 0, 0, DateTimeZone.UTC).getMillis()));
            i--;
        }
    }

    @Test
    public void singleValuedField_WithSubAggregation() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo").field("date").interval(DateHistogram.Interval.MONTH)
                    .subAggregation(sum("sum").field("value")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(3));

        long key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC).getMillis();
        DateHistogram.Bucket bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));
        Sum sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(1.0));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(2l));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(5.0));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(15.0));
    }

    @Test
    public void singleValuedField_WithSubAggregation_Inherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo").field("date").interval(DateHistogram.Interval.MONTH)
                        .subAggregation(max("max")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(3));

        long key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC).getMillis();
        DateHistogram.Bucket bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));
        Max max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) new DateTime(2012, 1, 2, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(2l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) new DateTime(2012, 2, 15, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) new DateTime(2012, 3, 23, 0, 0, DateTimeZone.UTC).getMillis()));
    }

    @Test
    public void singleValuedField_OrderedBySubAggregationAsc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("date")
                        .interval(DateHistogram.Interval.MONTH)
                        .order(DateHistogram.Order.aggregation("sum", true))
                        .subAggregation(max("sum").field("value")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(3));

        int i = 0;
        for (DateHistogram.Bucket bucket : histo.buckets()) {
            assertThat(bucket.getKey(), equalTo(new DateTime(2012, i+1, 1, 0, 0, DateTimeZone.UTC).getMillis()));
            i++;
        }
    }

    @Test
    public void singleValuedField_OrderedBySubAggregationDesc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("date")
                        .interval(DateHistogram.Interval.MONTH)
                        .order(DateHistogram.Order.aggregation("sum", false))
                        .subAggregation(max("sum").field("value")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(3));

        int i = 2;
        for (DateHistogram.Bucket bucket : histo.buckets()) {
            assertThat(bucket.getKey(), equalTo(new DateTime(2012, i+1, 1, 0, 0, DateTimeZone.UTC).getMillis()));
            i--;
        }
    }

    @Test
    public void singleValuedField_OrderedByMultiValuedSubAggregationAsc_Inherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("date")
                        .interval(DateHistogram.Interval.MONTH)
                        .order(DateHistogram.Order.aggregation("stats", "sum", true))
                        .subAggregation(stats("stats").field("value")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(3));

        int i = 0;
        for (DateHistogram.Bucket bucket : histo.buckets()) {
            assertThat(bucket.getKey(), equalTo(new DateTime(2012, i+1, 1, 0, 0, DateTimeZone.UTC).getMillis()));
            i++;
        }
    }

    @Test
    public void singleValuedField_OrderedByMultiValuedSubAggregationDesc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("date")
                        .interval(DateHistogram.Interval.MONTH)
                        .order(DateHistogram.Order.aggregation("stats", "sum", false))
                        .subAggregation(stats("stats").field("value")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(3));

        int i = 2;
        for (DateHistogram.Bucket bucket : histo.buckets()) {
            assertThat(bucket.getKey(), equalTo(new DateTime(2012, i+1, 1, 0, 0, DateTimeZone.UTC).getMillis()));
            i--;
        }
    }

    @Test
    public void singleValuedField_WithValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("date")
                        .script("new DateTime(_value).plusMonths(1).getMillis()")
                        .interval(DateHistogram.Interval.MONTH))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));

        assertThat(histo.buckets().size(), equalTo(3));

        long key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC).getMillis();
        DateHistogram.Bucket bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(2l));

        key = new DateTime(2012, 4, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
    }

    /*
    [ Jan 2, Feb 3]
    [ Feb 2, Mar 3]
    [ Feb 15, Mar 16]
    [ Mar 2, Apr 3]
    [ Mar 15, Apr 16]
    [ Mar 23, Apr 24]
     */

    @Test
    public void multiValuedField() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo").field("dates").interval(DateHistogram.Interval.MONTH))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(4));

        long key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC).getMillis();
        DateHistogram.Bucket bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(5l));

        key = new DateTime(2012, 4, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
    }

    @Test
    public void multiValuedField_OrderedByKeyDesc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("dates")
                        .interval(DateHistogram.Interval.MONTH)
                        .order(DateHistogram.Order.COUNT_DESC))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(4));

        DateHistogram.Bucket bucket = histo.buckets().get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getDocCount(), equalTo(5l));

        bucket = histo.buckets().get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getDocCount(), equalTo(3l));

        bucket = histo.buckets().get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getDocCount(), equalTo(3l));

        bucket = histo.buckets().get(3);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getDocCount(), equalTo(1l));
    }


    /**
     * The script will change to document date values to the following:
     *
     * doc 1: [ Feb 2, Mar 3]
     * doc 2: [ Mar 2, Apr 3]
     * doc 3: [ Mar 15, Apr 16]
     * doc 4: [ Apr 2, May 3]
     * doc 5: [ Apr 15, May 16]
     * doc 6: [ Apr 23, May 24]
     */
    @Test
    public void multiValuedField_WithValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("dates")
                        .script("new DateTime(_value, DateTimeZone.UTC).plusMonths(1).getMillis()")
                        .interval(DateHistogram.Interval.MONTH))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(4));

        long key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC).getMillis();
        DateHistogram.Bucket bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));

        key = new DateTime(2012, 4, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(5l));

        key = new DateTime(2012, 5, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
    }

    @Test
    public void multiValuedField_WithValueScript_WithInheritedSubAggregator() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .field("dates")
                        .script("new DateTime(_value, DateTimeZone.UTC).plusMonths(1).getMillis()")
                        .interval(DateHistogram.Interval.MONTH)
                        .subAggregation(max("max")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(4));

        long key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC).getMillis();
        DateHistogram.Bucket bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));
        Max max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat((long) max.getValue(), equalTo(new DateTime(2012, 2, 2, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat((long) max.getValue(), equalTo(new DateTime(2012, 3, 15, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 4, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(5l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat((long) max.getValue(), equalTo(new DateTime(2012, 4, 23, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 5, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat((long) max.getValue(), equalTo(new DateTime(2012, 5, 24, 0, 0, DateTimeZone.UTC).getMillis()));
    }

    @Test
    public void script_SingleValue() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo").script("doc['date'].value").interval(DateHistogram.Interval.MONTH))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(3));

        long key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC).getMillis();
        DateHistogram.Bucket bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(2l));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
    }

    @Test
    public void script_SingleValue_WithSubAggregator_Inherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .script("doc['date'].value")
                        .interval(DateHistogram.Interval.MONTH)
                        .subAggregation(max("max")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(3));

        long key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC).getMillis();
        DateHistogram.Bucket bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));
        Max max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) new DateTime(2012, 1, 2, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(2l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) new DateTime(2012, 2, 15, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo((double) new DateTime(2012, 3, 23, 0, 0, DateTimeZone.UTC).getMillis()));
    }

    @Test
    public void script_MultiValued() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo").script("doc['dates'].values").interval(DateHistogram.Interval.MONTH))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(4));

        long key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC).getMillis();
        DateHistogram.Bucket bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(5l));

        key = new DateTime(2012, 4, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
    }

      /*
    [ Jan 2, Feb 3]
    [ Feb 2, Mar 3]
    [ Feb 15, Mar 16]
    [ Mar 2, Apr 3]
    [ Mar 15, Apr 16]
    [ Mar 23, Apr 24]
     */

    @Test
    public void script_MultiValued_WithAggregatorInherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(dateHistogram("histo")
                        .script("doc['dates'].values")
                        .interval(DateHistogram.Interval.MONTH)
                        .subAggregation(max("max")))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(4));

        long key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC).getMillis();
        DateHistogram.Bucket bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));
        Max max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat((long) max.getValue(), equalTo(new DateTime(2012, 1, 2, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat((long) max.getValue(), equalTo(new DateTime(2012, 2, 15, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(5l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat((long) max.getValue(), equalTo(new DateTime(2012, 3, 23, 0, 0, DateTimeZone.UTC).getMillis()));

        key = new DateTime(2012, 4, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
        max = bucket.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat((long) max.getValue(), equalTo(new DateTime(2012, 4, 24, 0, 0, DateTimeZone.UTC).getMillis()));
    }

    @Test
    public void unmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx_unmapped")
                .addAggregation(dateHistogram("histo").field("date").interval(DateHistogram.Interval.MONTH))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(0));
    }

    @Test
    public void partiallyUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx", "idx_unmapped")
                .addAggregation(dateHistogram("histo").field("date").interval(DateHistogram.Interval.MONTH))
                .execute().actionGet();

        assertThat(response.getFailedShards(), equalTo(0));

        DateHistogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.buckets().size(), equalTo(3));

        long key = new DateTime(2012, 1, 1, 0, 0, DateTimeZone.UTC).getMillis();
        DateHistogram.Bucket bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(1l));

        key = new DateTime(2012, 2, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(2l));

        key = new DateTime(2012, 3, 1, 0, 0, DateTimeZone.UTC).getMillis();
        bucket = histo.getByKey(key);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(3l));
    }

}
