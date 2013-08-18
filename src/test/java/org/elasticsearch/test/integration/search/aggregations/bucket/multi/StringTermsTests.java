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

import com.google.common.base.Strings;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.aggregations.bucket.multi.terms.Terms;
import org.elasticsearch.search.aggregations.calc.bytes.count.Count;
import org.elasticsearch.search.aggregations.calc.numeric.sum.Sum;
import org.elasticsearch.test.integration.AbstractSharedClusterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.count;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 *
 */
public class StringTermsTests extends AbstractSharedClusterTest {

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

    @Override
    protected int numberOfNodes() {
        return 2;
    }

    @BeforeMethod
    public void init() throws Exception {
        createIndex("idx");

        for (int i = 0; i < 5; i++) {
            client().prepareIndex("idx", "type").setSource(jsonBuilder()
                    .startObject()
                    .field("value", "val" + i)
                    .startArray("values").value("val" + i).value("val" + (i + 1)).endArray()
                    .endObject())
                    .execute().actionGet();
        }

        for (int i = 0; i < 100; i++) {
            client().prepareIndex("idx", "high_card_type").setSource(jsonBuilder()
                    .startObject()
                    .field("value", "val" + Strings.padStart(i+"", 3, '0'))
                    .startArray("values").value("val" + Strings.padStart(i+"", 3, '0')).value("val" + Strings.padStart((i+1)+"", 3, '0')).endArray()
                    .endObject())
                    .execute().actionGet();
        }

        createIndex("idx_unmapped");

        client().admin().indices().prepareFlush().setRefresh(true).execute().actionGet();
    }

    @Test
    public void singleValueField() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field("value"))
                .execute().actionGet();

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getByTerm("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getTerm().string(), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1l));
        }
    }

    @Test
    public void singleValueField_WithMaxSize() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("high_card_type")
                .addAggregation(terms("terms")
                        .field("value")
                        .size(20)
                        .order(Terms.Order.TERM_ASC)) // we need to sort by terms cause we're checking the first 20 values
                .execute().actionGet();

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(20));

        for (int i = 0; i < 20; i++) {
            Terms.Bucket bucket = terms.getByTerm("val" + Strings.padStart(i+"", 3, '0'));
            assertThat(bucket, notNullValue());
            assertThat(bucket.getTerm().string(), equalTo("val" + Strings.padStart(i+"", 3, '0')));
            assertThat(bucket.getDocCount(), equalTo(1l));
        }
    }

    @Test
    public void singleValueField_OrderedByTermAsc() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field("value")
                        .order(Terms.Order.TERM_ASC))
                .execute().actionGet();

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        int i = 0;
        for (Terms.Bucket bucket : terms.buckets()) {
            assertThat(bucket, notNullValue());
            assertThat(bucket.getTerm().string(), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1l));
            i++;
        }
    }

    @Test
    public void singleValueField_OrderedByTermDesc() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field("value")
                        .order(Terms.Order.TERM_DESC))
                .execute().actionGet();

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        int i = 4;
        for (Terms.Bucket bucket : terms.buckets()) {
            assertThat(bucket, notNullValue());
            assertThat(bucket.getTerm().string(), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1l));
            i--;
        }
    }

    @Test
    public void singleValuedField_WithSubAggregation() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field("value")
                        .subAggregation(count("count").field("values")))
                .execute().actionGet();

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getByTerm("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getTerm().string(), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1l));
            Count count = bucket.getAggregations().get("count");
            assertThat(count, notNullValue());
            assertThat(count.getValue(), equalTo(2l));
        }
    }

    @Test
    public void singleValuedField_WithSubAggregation_Inherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field("value")
                        .subAggregation(count("count")))
                .execute().actionGet();

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getByTerm("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getTerm().string(), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1l));
            Count count = bucket.getAggregations().get("count");
            assertThat(count, notNullValue());
            assertThat(count.getValue(), equalTo(1l));
        }
    }

    @Test
    public void singleValuedField_WithValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field("value")
                        .script("'foo_' + _value"))
                .execute().actionGet();

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getByTerm("foo_val" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getTerm().string(), equalTo("foo_val" + i));
            assertThat(bucket.getDocCount(), equalTo(1l));
        }
    }

    @Test
    public void multiValuedField() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field("values"))
                .execute().actionGet();

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(6));

        for (int i = 0; i < 6; i++) {
            Terms.Bucket bucket = terms.getByTerm("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getTerm().string(), equalTo("val" + i));
            if (i == 0 || i == 5) {
                assertThat(bucket.getDocCount(), equalTo(1l));
            } else {
                assertThat(bucket.getDocCount(), equalTo(2l));
            }
        }
    }

    @Test
    public void multiValuedField_WithValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field("values")
                        .script("'foo_' + _value"))
                .execute().actionGet();

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(6));

        for (int i = 0; i < 6; i++) {
            Terms.Bucket bucket = terms.getByTerm("foo_val" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getTerm().string(), equalTo("foo_val" + i));
            if (i == 0 || i == 5) {
                assertThat(bucket.getDocCount(), equalTo(1l));
            } else {
                assertThat(bucket.getDocCount(), equalTo(2l));
            }
        }
    }

    /*

    [foo_val0, foo_val1]
    [foo_val1, foo_val2]
    [foo_val2, foo_val3]
    [foo_val3, foo_val4]
    [foo_val4, foo_val5]


    foo_val0 - doc_count: 1 - val_count: 1
    foo_val1 - doc_count: 2 - val_count: 2
    foo_val2 - doc_count: 2 - val_count: 2
    foo_val3 - doc_count: 2 - val_count: 2
    foo_val4 - doc_count: 2 - val_count: 2
    foo_val5 - doc_count: 1 - val_count: 1

    */

    @Test
    public void multiValuedField_WithValueScript_WithInheritedSubAggregator() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field("values")
                        .script("'foo_' + _value")
                        .subAggregation(count("count")))
                .execute().actionGet();

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(6));

        for (int i = 0; i < 6; i++) {
            Terms.Bucket bucket = terms.getByTerm("foo_val" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getTerm().string(), equalTo("foo_val" + i));
            if (i == 0 | i == 5) {
                assertThat(bucket.getDocCount(), equalTo(1l));
                Count count = bucket.getAggregations().get("count");
                assertThat(count, notNullValue());
                assertThat(count.getValue(), equalTo(1l));
            } else {
                assertThat(bucket.getDocCount(), equalTo(2l));
                Count count = bucket.getAggregations().get("count");
                assertThat(count, notNullValue());
                assertThat(count.getValue(), equalTo(2l));
            }
        }
    }

    @Test
    public void script_SingleValue() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .script("doc['value'].value"))
                .execute().actionGet();

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getByTerm("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getTerm().string(), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1l));
        }
    }

    @Test
    public void script_SingleValue_ExplicitSingleValue() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .script("doc['value'].value")
                        .multiValued(false))
                .execute().actionGet();

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getByTerm("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getTerm().string(), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1l));
        }
    }

    @Test
    public void script_SingleValue_WithSubAggregator_Inherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .script("doc['value'].value")
                        .subAggregation(count("count")))
                .execute().actionGet();

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getByTerm("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getTerm().string(), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1l));
            Count count = bucket.getAggregations().get("count");
            assertThat(count, notNullValue());
            assertThat(count.getValue(), equalTo(1l));
        }
    }

    @Test
    public void script_MultiValued() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .script("doc['values'].values"))
                .execute().actionGet();

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(6));

        for (int i = 0; i < 6; i++) {
            Terms.Bucket bucket = terms.getByTerm("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getTerm().string(), equalTo("val" + i));
            if (i == 0 || i == 5) {
                assertThat(bucket.getDocCount(), equalTo(1l));
            } else {
                assertThat(bucket.getDocCount(), equalTo(2l));
            }
        }
    }

    @Test
    public void script_MultiValued_WithAggregatorInherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .script("doc['values'].values")
                        .subAggregation(count("count")))
                .execute().actionGet();

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(6));

        for (int i = 0; i < 6; i++) {
            Terms.Bucket bucket = terms.getByTerm("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getTerm().string(), equalTo("val" + i));
            if (i == 0 | i == 5) {
                assertThat(bucket.getDocCount(), equalTo(1l));
                Count count = bucket.getAggregations().get("count");
                assertThat(count, notNullValue());
                assertThat(count.getValue(), equalTo(1l));
            } else {
                assertThat(bucket.getDocCount(), equalTo(2l));
                Count count = bucket.getAggregations().get("count");
                assertThat(count, notNullValue());
                assertThat(count.getValue(), equalTo(2l));
            }
        }
    }

    @Test
    public void unmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx_unmapped").setTypes("type")
                .addAggregation(terms("terms")
                        .field("value"))
                .execute().actionGet();

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(0));
    }

    @Test
    public void partiallyUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx", "idx_unmapped").setTypes("type")
                .addAggregation(terms("terms")
                        .field("value"))
                .execute().actionGet();

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.buckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getByTerm("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getTerm().string(), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1l));
        }
    }

}
