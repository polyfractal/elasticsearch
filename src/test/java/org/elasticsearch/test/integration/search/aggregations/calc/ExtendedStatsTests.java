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
import org.elasticsearch.search.aggregations.calc.numeric.stats.ExtendedStats;
import org.testng.annotations.Test;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.extendedStats;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 *
 */
public class ExtendedStatsTests extends AbstractNumericTests {

    private static double stdDev(int... vals) {
        return Math.sqrt(variance(vals));
    }

    private static double variance(int... vals) {
        double sum = 0;
        double sumOfSqrs = 0;
        for (int val : vals) {
            sum += val;
            sumOfSqrs += val * val;
        }
        return (sumOfSqrs - ((sum * sum) / vals.length)) / vals.length;
    }

    @Test
    public void testUnmapped() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx2")
                .setQuery(matchAllQuery())
                .addAggregation(extendedStats("stats").field("value"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(0l));

        ExtendedStats stats = searchResponse.getAggregations().aggregation("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo(Double.NaN));
        assertThat(stats.getMin(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(stats.getMax(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(stats.getSum(), equalTo(0.0));
        assertThat(stats.getCount(), equalTo(0l));
        assertThat(stats.getSumOfSquares(), equalTo(0.0));
        assertThat(stats.getVariance(), equalTo(Double.NaN));
        assertThat(stats.getStdDeviation(), equalTo(Double.NaN));
    }

    @Test
    public void testSingleValuedField() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(extendedStats("stats").field("value"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        ExtendedStats stats = searchResponse.getAggregations().aggregation("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (1+2+3+4+5+6+7+8+9+10) / 10));
        assertThat(stats.getMin(), equalTo(1.0));
        assertThat(stats.getMax(), equalTo(10.0));
        assertThat(stats.getSum(), equalTo((double) 1+2+3+4+5+6+7+8+9+10));
        assertThat(stats.getCount(), equalTo(10l));
        assertThat(stats.getSumOfSquares(), equalTo((double) 1+4+9+16+25+36+49+64+81+100));
        assertThat(stats.getVariance(), equalTo(variance(1, 2, 3, 4, 5, 6, 7, 8 ,9, 10)));
        assertThat(stats.getStdDeviation(), equalTo(stdDev(1, 2, 3, 4, 5, 6, 7, 8 ,9, 10)));
    }

    @Test
    public void testSingleValuedField_PartiallyUnmapped() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx", "idx2")
                .setQuery(matchAllQuery())
                .addAggregation(extendedStats("stats").field("value"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        ExtendedStats stats = searchResponse.getAggregations().aggregation("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (1+2+3+4+5+6+7+8+9+10) / 10));
        assertThat(stats.getMin(), equalTo(1.0));
        assertThat(stats.getMax(), equalTo(10.0));
        assertThat(stats.getSum(), equalTo((double) 1+2+3+4+5+6+7+8+9+10));
        assertThat(stats.getCount(), equalTo(10l));
        assertThat(stats.getSumOfSquares(), equalTo((double) 1+4+9+16+25+36+49+64+81+100));
        assertThat(stats.getVariance(), equalTo(variance(1, 2, 3, 4, 5, 6, 7, 8 ,9, 10)));
        assertThat(stats.getStdDeviation(), equalTo(stdDev(1, 2, 3, 4, 5, 6, 7, 8 ,9, 10)));
    }

    @Test
    public void testSingleValuedField_WithValueScript() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(extendedStats("stats").field("value").script("_value + 1"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        ExtendedStats stats = searchResponse.getAggregations().aggregation("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (2+3+4+5+6+7+8+9+10+11) / 10));
        assertThat(stats.getMin(), equalTo(2.0));
        assertThat(stats.getMax(), equalTo(11.0));
        assertThat(stats.getSum(), equalTo((double) 2+3+4+5+6+7+8+9+10+11));
        assertThat(stats.getCount(), equalTo(10l));
        assertThat(stats.getSumOfSquares(), equalTo((double) 4+9+16+25+36+49+64+81+100+121));
        assertThat(stats.getVariance(), equalTo(variance(2, 3, 4, 5, 6, 7, 8 ,9, 10, 11)));
        assertThat(stats.getStdDeviation(), equalTo(stdDev(2, 3, 4, 5, 6, 7, 8 ,9, 10, 11)));
    }

    @Test
    public void testSingleValuedField_WithValueScript_WithParams() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(extendedStats("stats").field("value").script("_value + inc").param("inc", 1))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        ExtendedStats stats = searchResponse.getAggregations().aggregation("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (2+3+4+5+6+7+8+9+10+11) / 10));
        assertThat(stats.getMin(), equalTo(2.0));
        assertThat(stats.getMax(), equalTo(11.0));
        assertThat(stats.getSum(), equalTo((double) 2+3+4+5+6+7+8+9+10+11));
        assertThat(stats.getCount(), equalTo(10l));
        assertThat(stats.getSumOfSquares(), equalTo((double) 4+9+16+25+36+49+64+81+100+121));
        assertThat(stats.getVariance(), equalTo(variance(2, 3, 4, 5, 6, 7, 8 ,9, 10, 11)));
        assertThat(stats.getStdDeviation(), equalTo(stdDev(2, 3, 4, 5, 6, 7, 8 ,9, 10, 11)));
    }

    @Test
    public void testMultiValuedField() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(extendedStats("stats").field("values"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        ExtendedStats stats = searchResponse.getAggregations().aggregation("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (2+3+4+5+6+7+8+9+10+11+3+4+5+6+7+8+9+10+11+12) / 20));
        assertThat(stats.getMin(), equalTo(2.0));
        assertThat(stats.getMax(), equalTo(12.0));
        assertThat(stats.getSum(), equalTo((double) 2+3+4+5+6+7+8+9+10+11+3+4+5+6+7+8+9+10+11+12));
        assertThat(stats.getCount(), equalTo(20l));
        assertThat(stats.getSumOfSquares(), equalTo((double) 4+9+16+25+36+49+64+81+100+121+9+16+25+36+49+64+81+100+121+144));
        assertThat(stats.getVariance(), equalTo(variance(2, 3, 4, 5, 6, 7, 8 ,9, 10, 11, 3, 4, 5, 6, 7, 8 ,9, 10, 11, 12)));
        assertThat(stats.getStdDeviation(), equalTo(stdDev(2, 3, 4, 5, 6, 7, 8 ,9, 10, 11, 3, 4, 5, 6, 7, 8 ,9, 10, 11, 12)));
    }

    @Test
    public void testMultiValuedField_WithValueScript() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(extendedStats("stats").field("values").script("_value - 1"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        ExtendedStats stats = searchResponse.getAggregations().aggregation("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (1+2+3+4+5+6+7+8+9+10+2+3+4+5+6+7+8+9+10+11) / 20));
        assertThat(stats.getMin(), equalTo(1.0));
        assertThat(stats.getMax(), equalTo(11.0));
        assertThat(stats.getSum(), equalTo((double) 1+2+3+4+5+6+7+8+9+10+2+3+4+5+6+7+8+9+10+11));
        assertThat(stats.getCount(), equalTo(20l));
        assertThat(stats.getSumOfSquares(), equalTo((double) 1+4+9+16+25+36+49+64+81+100+4+9+16+25+36+49+64+81+100+121));
        assertThat(stats.getVariance(), equalTo(variance(1, 2, 3, 4, 5, 6, 7, 8 ,9, 10, 2, 3, 4, 5, 6, 7, 8 ,9, 10, 11)));
        assertThat(stats.getStdDeviation(), equalTo(stdDev(1, 2, 3, 4, 5, 6, 7, 8 ,9, 10, 2, 3, 4, 5, 6, 7, 8 ,9, 10, 11)));
    }

    @Test
    public void testMultiValuedField_WithValueScript_WithParams() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(extendedStats("stats").field("values").script("_value - dec").param("dec", 1))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        ExtendedStats stats = searchResponse.getAggregations().aggregation("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (1+2+3+4+5+6+7+8+9+10+2+3+4+5+6+7+8+9+10+11) / 20));
        assertThat(stats.getMin(), equalTo(1.0));
        assertThat(stats.getMax(), equalTo(11.0));
        assertThat(stats.getSum(), equalTo((double) 1+2+3+4+5+6+7+8+9+10+2+3+4+5+6+7+8+9+10+11));
        assertThat(stats.getCount(), equalTo(20l));
        assertThat(stats.getSumOfSquares(), equalTo((double) 1+4+9+16+25+36+49+64+81+100+4+9+16+25+36+49+64+81+100+121));
        assertThat(stats.getVariance(), equalTo(variance(1, 2, 3, 4, 5, 6, 7, 8 ,9, 10, 2, 3, 4, 5, 6, 7, 8 ,9, 10, 11)));
        assertThat(stats.getStdDeviation(), equalTo(stdDev(1, 2, 3, 4, 5, 6, 7, 8 ,9, 10, 2, 3, 4, 5, 6, 7, 8 ,9, 10, 11)));
    }

    @Test
    public void testScript_SingleValued() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(extendedStats("stats").script("doc['value'].value"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        ExtendedStats stats = searchResponse.getAggregations().aggregation("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (1+2+3+4+5+6+7+8+9+10) / 10));
        assertThat(stats.getMin(), equalTo(1.0));
        assertThat(stats.getMax(), equalTo(10.0));
        assertThat(stats.getSum(), equalTo((double) 1+2+3+4+5+6+7+8+9+10));
        assertThat(stats.getCount(), equalTo(10l));
        assertThat(stats.getSumOfSquares(), equalTo((double) 1+4+9+16+25+36+49+64+81+100));
        assertThat(stats.getVariance(), equalTo(variance(1, 2, 3, 4, 5, 6, 7, 8 ,9, 10)));
        assertThat(stats.getStdDeviation(), equalTo(stdDev(1, 2, 3, 4, 5, 6, 7, 8 ,9, 10)));
    }

    @Test
    public void testScript_SingleValued_WithParams() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(extendedStats("stats").script("doc['value'].value + inc").param("inc", 1))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        ExtendedStats stats = searchResponse.getAggregations().aggregation("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (2+3+4+5+6+7+8+9+10+11) / 10));
        assertThat(stats.getMin(), equalTo(2.0));
        assertThat(stats.getMax(), equalTo(11.0));
        assertThat(stats.getSum(), equalTo((double) 2+3+4+5+6+7+8+9+10+11));
        assertThat(stats.getCount(), equalTo(10l));
        assertThat(stats.getSumOfSquares(), equalTo((double) 4+9+16+25+36+49+64+81+100+121));
        assertThat(stats.getVariance(), equalTo(variance(2, 3, 4, 5, 6, 7, 8 ,9, 10, 11)));
        assertThat(stats.getStdDeviation(), equalTo(stdDev(2, 3, 4, 5, 6, 7, 8 ,9, 10, 11)));
    }

    @Test
    public void testScript_ExplicitSingleValued_WithParams() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(extendedStats("stats").script("doc['value'].value + inc").param("inc", 1).multiValued(false))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        ExtendedStats stats = searchResponse.getAggregations().aggregation("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (2+3+4+5+6+7+8+9+10+11) / 10));
        assertThat(stats.getMin(), equalTo(2.0));
        assertThat(stats.getMax(), equalTo(11.0));
        assertThat(stats.getSum(), equalTo((double) 2+3+4+5+6+7+8+9+10+11));
        assertThat(stats.getCount(), equalTo(10l));
        assertThat(stats.getSumOfSquares(), equalTo((double) 4+9+16+25+36+49+64+81+100+121));
        assertThat(stats.getVariance(), equalTo(variance(2, 3, 4, 5, 6, 7, 8 ,9, 10, 11)));
        assertThat(stats.getStdDeviation(), equalTo(stdDev(2, 3, 4, 5, 6, 7, 8 ,9, 10, 11)));
    }

    @Test
    public void testScript_MultiValued() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(extendedStats("stats").script("doc['values'].values"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        ExtendedStats stats = searchResponse.getAggregations().aggregation("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (2+3+4+5+6+7+8+9+10+11+3+4+5+6+7+8+9+10+11+12) / 20));
        assertThat(stats.getMin(), equalTo(2.0));
        assertThat(stats.getMax(), equalTo(12.0));
        assertThat(stats.getSum(), equalTo((double) 2+3+4+5+6+7+8+9+10+11+3+4+5+6+7+8+9+10+11+12));
        assertThat(stats.getCount(), equalTo(20l));
        assertThat(stats.getSumOfSquares(), equalTo((double) 4+9+16+25+36+49+64+81+100+121+9+16+25+36+49+64+81+100+121+144));
        assertThat(stats.getVariance(), equalTo(variance(2, 3, 4, 5, 6, 7, 8 ,9, 10, 11, 3, 4, 5, 6, 7, 8 ,9, 10, 11, 12)));
        assertThat(stats.getStdDeviation(), equalTo(stdDev(2, 3, 4, 5, 6, 7, 8 ,9, 10, 11, 3, 4, 5, 6, 7, 8 ,9, 10, 11, 12)));
    }

    @Test
    public void testScript_ExplicitMultiValued() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(extendedStats("stats").script("doc['values'].values").multiValued(true))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        ExtendedStats stats = searchResponse.getAggregations().aggregation("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (2+3+4+5+6+7+8+9+10+11+3+4+5+6+7+8+9+10+11+12) / 20));
        assertThat(stats.getMin(), equalTo(2.0));
        assertThat(stats.getMax(), equalTo(12.0));
        assertThat(stats.getSum(), equalTo((double) 2+3+4+5+6+7+8+9+10+11+3+4+5+6+7+8+9+10+11+12));
        assertThat(stats.getCount(), equalTo(20l));
        assertThat(stats.getSumOfSquares(), equalTo((double) 4+9+16+25+36+49+64+81+100+121+9+16+25+36+49+64+81+100+121+144));
        assertThat(stats.getVariance(), equalTo(variance(2, 3, 4, 5, 6, 7, 8 ,9, 10, 11, 3, 4, 5, 6, 7, 8 ,9, 10, 11, 12)));
        assertThat(stats.getStdDeviation(), equalTo(stdDev(2, 3, 4, 5, 6, 7, 8 ,9, 10, 11, 3, 4, 5, 6, 7, 8 ,9, 10, 11, 12)));

        try {
            client().prepareSearch("idx")
                    .setQuery(matchAllQuery())
                    .addAggregation(extendedStats("stats").script("doc['values'].values").multiValued(false))
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
                .addAggregation(extendedStats("stats").script("new double[] { doc['value'].value, doc['value'].value - dec }").param("dec", 1))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        ExtendedStats stats = searchResponse.getAggregations().aggregation("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (1+2+3+4+5+6+7+8+9+10+0+1+2+3+4+5+6+7+8+9) / 20));
        assertThat(stats.getMin(), equalTo(0.0));
        assertThat(stats.getMax(), equalTo(10.0));
        assertThat(stats.getSum(), equalTo((double) 1+2+3+4+5+6+7+8+9+10+0+1+2+3+4+5+6+7+8+9));
        assertThat(stats.getCount(), equalTo(20l));
        assertThat(stats.getSumOfSquares(), equalTo((double) 1+4+9+16+25+36+49+64+81+100+0+1+4+9+16+25+36+49+64+81));
        assertThat(stats.getVariance(), equalTo(variance(1, 2, 3, 4, 5, 6, 7, 8 ,9, 10, 0, 1, 2, 3, 4, 5, 6, 7, 8 ,9)));
        assertThat(stats.getStdDeviation(), equalTo(stdDev(1, 2, 3, 4, 5, 6, 7, 8 ,9, 10, 0, 1, 2, 3, 4, 5, 6, 7, 8 ,9)));
    }

}