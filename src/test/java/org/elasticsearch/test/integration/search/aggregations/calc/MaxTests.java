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

package org.elasticsearch.test.integration.search.aggregations.calc;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.calc.numeric.max.Max;
import org.testng.annotations.Test;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.max;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
public class MaxTests extends AbstractNumericTests {

    @Test
    public void testUnmapped() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx2")
                .setQuery(matchAllQuery())
                .addAggregation(max("max").field("value"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(0l));

        Max max = searchResponse.getAggregations().aggregation("max");
        assertThat(max, notNullValue());
        assertThat(max.getName(), equalTo("max"));
        assertThat(max.getValue(), equalTo(Double.NEGATIVE_INFINITY));
    }

    @Test
    public void testSingleValuedField() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(max("max").field("value"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Max max = searchResponse.getAggregations().aggregation("max");
        assertThat(max, notNullValue());
        assertThat(max.getName(), equalTo("max"));
        assertThat(max.getValue(), equalTo(10.0));
    }


    @Test
    public void testSingleValuedField_PartiallyUnmapped() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx", "idx2")
                .setQuery(matchAllQuery())
                .addAggregation(max("max").field("value"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Max max = searchResponse.getAggregations().aggregation("max");
        assertThat(max, notNullValue());
        assertThat(max.getName(), equalTo("max"));
        assertThat(max.getValue(), equalTo(10.0));
    }

    @Test
    public void testSingleValuedField_WithValueScript() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(max("max").field("value").script("_value + 1"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Max max = searchResponse.getAggregations().aggregation("max");
        assertThat(max, notNullValue());
        assertThat(max.getName(), equalTo("max"));
        assertThat(max.getValue(), equalTo(11.0));
    }

    @Test
    public void testSingleValuedField_WithValueScript_WithParams() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(max("max").field("value").script("_value + inc").param("inc", 1))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Max max = searchResponse.getAggregations().aggregation("max");
        assertThat(max, notNullValue());
        assertThat(max.getName(), equalTo("max"));
        assertThat(max.getValue(), equalTo(11.0));
    }

    @Test
    public void testMultiValuedField() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(max("max").field("values"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Max max = searchResponse.getAggregations().aggregation("max");
        assertThat(max, notNullValue());
        assertThat(max.getName(), equalTo("max"));
        assertThat(max.getValue(), equalTo(12.0));
    }

    @Test
    public void testMultiValuedField_WithValueScript() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(max("max").field("values").script("_value + 1"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Max max = searchResponse.getAggregations().aggregation("max");
        assertThat(max, notNullValue());
        assertThat(max.getName(), equalTo("max"));
        assertThat(max.getValue(), equalTo(13.0));
    }

    @Test
    public void testMultiValuedField_WithValueScript_WithParams() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(max("max").field("values").script("_value + inc").param("inc", 1))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Max max = searchResponse.getAggregations().aggregation("max");
        assertThat(max, notNullValue());
        assertThat(max.getName(), equalTo("max"));
        assertThat(max.getValue(), equalTo(13.0));
    }

    @Test
    public void testScript_SingleValued() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(max("max").script("doc['value'].value"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Max max = searchResponse.getAggregations().aggregation("max");
        assertThat(max, notNullValue());
        assertThat(max.getName(), equalTo("max"));
        assertThat(max.getValue(), equalTo(10.0));
    }

    @Test
    public void testScript_SingleValued_WithParams() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(max("max").script("doc['value'].value + inc").param("inc", 1))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Max max = searchResponse.getAggregations().aggregation("max");
        assertThat(max, notNullValue());
        assertThat(max.getName(), equalTo("max"));
        assertThat(max.getValue(), equalTo(11.0));
    }

    @Test
    public void testScript_ExplicitSingleValued_WithParams() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(max("max").script("doc['value'].value + inc").param("inc", 1).multiValued(false))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Max max = searchResponse.getAggregations().aggregation("max");
        assertThat(max, notNullValue());
        assertThat(max.getName(), equalTo("max"));
        assertThat(max.getValue(), equalTo(11.0));
    }

    @Test
    public void testScript_MultiValued() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(max("max").script("doc['values'].values"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Max max = searchResponse.getAggregations().aggregation("max");
        assertThat(max, notNullValue());
        assertThat(max.getName(), equalTo("max"));
        assertThat(max.getValue(), equalTo(12.0));
    }

    @Test
    public void testScript_ExplicitMultiValued() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(max("max").script("doc['values'].values").multiValued(true))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Max max = searchResponse.getAggregations().aggregation("max");
        assertThat(max, notNullValue());
        assertThat(max.getName(), equalTo("max"));
        assertThat(max.getValue(), equalTo(12.0));

        try {
            client().prepareSearch("idx")
                    .setQuery(matchAllQuery())
                    .addAggregation(max("max").script("doc['values']").multiValued(false))
                    .execute().actionGet();

            assertThat("expected an error to be thrown as we tell es to treat the value generated by the script as a single value thus it tries to " +
                    "cast it to a Number, but it should fail as the script returns a double array", false);

        } catch (ElasticSearchException ese) {
        }
    }

    @Test
    public void testScript_MultiValued_WithParams() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(max("max").script("new double[] { doc['value'].value, doc['value'].value + inc }").param("inc", 1))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Max max = searchResponse.getAggregations().aggregation("max");
        assertThat(max, notNullValue());
        assertThat(max.getName(), equalTo("max"));
        assertThat(max.getValue(), equalTo(11.0));
    }


}