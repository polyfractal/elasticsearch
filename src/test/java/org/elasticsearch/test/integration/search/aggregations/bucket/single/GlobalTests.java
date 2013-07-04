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

package org.elasticsearch.test.integration.search.aggregations.bucket.single;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.bucket.single.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.single.global.Global;
import org.elasticsearch.search.aggregations.calc.numeric.avg.Avg;
import org.elasticsearch.search.aggregations.calc.numeric.stats.Stats;
import org.elasticsearch.test.integration.AbstractSharedClusterTest;
import org.hamcrest.Matchers;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 *
 */
public class GlobalTests extends AbstractSharedClusterTest {

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
        return 1;
    }

    @BeforeMethod
    public void init() throws Exception {
        createIndex("idx");
        createIndex("idx2");
        for (int i = 0; i < 5; i++) {
            client().prepareIndex("idx", "type", ""+i+1).setSource(jsonBuilder()
                    .startObject()
                    .field("value", i + 1)
                    .field("tag", "tag1")
                    .endObject())
                    .execute().actionGet();
        }
        for (int i = 0; i < 5; i++) {
            client().prepareIndex("idx", "type", ""+i+6).setSource(jsonBuilder()
                    .startObject()
                    .field("value", i + 6)
                    .field("tag", "tag2")
                    .field("name", "name" + i+6)
                    .endObject())
                    .execute().actionGet();
        }
        client().admin().indices().prepareFlush().setRefresh(true).execute().actionGet();
    }

    @Test
    public void testGlobal_WithStatsSubAggregator() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .setQuery(QueryBuilders.termQuery("tag", "tag1"))
                .addAggregation(global("global")
                        .aggregation(stats("value_stats").field("value")))
                .execute().actionGet();

        Global global = response.getAggregations().aggregation("global");
        assertThat(global, notNullValue());
        assertThat(global.getName(), equalTo("global"));
        assertThat(global.getDocCount(), equalTo(10l));
        assertThat(global.getAggregations().asList().isEmpty(), is(false));

        Stats stats = global.getAggregations().aggregation("value_stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("value_stats"));
        assertThat(stats.getAvg(), equalTo((double) (1+2+3+4+5+6+7+8+9+10) / 10));
        assertThat(stats.getMin(), equalTo(1.0));
        assertThat(stats.getMax(), equalTo(10.0));
        assertThat(stats.getCount(), equalTo(10l));
        assertThat(stats.getSum(), equalTo((double) 1+2+3+4+5+6+7+8+9+10));
    }

    @Test
    public void testGlobal_NonTopLevel() throws Exception {

        try {

            client().prepareSearch("idx")
                    .setQuery(QueryBuilders.termQuery("tag", "tag1"))
                    .addAggregation(global("global")
                            .aggregation(global("inner_global")))
                    .execute().actionGet();

            assertThat("expected to fail executing non-top-level global aggregator. global aggregations are only allowed as top level" +
                    "aggregations", false);

        } catch (ElasticSearchException ese) {
            ese.printStackTrace();
        }
    }
}
