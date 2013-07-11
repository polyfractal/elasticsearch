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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.integration.AbstractSharedClusterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 *
 */
public class DateRangeTests extends AbstractSharedClusterTest {

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

        for (int i = 0; i < 10; i++) {
            client().prepareIndex("idx", "type").setSource(jsonBuilder()
                    .startObject()
                    .field("value", i+1)
                    .startArray("values").value(i+1).value(i+2).endArray()
                    .endObject())
                    .execute().actionGet();
        }

        createIndex("idx_unmapped");

        client().admin().indices().prepareFlush().setRefresh(true).execute().actionGet();
    }

    @Test
    public void singleValueField() throws Exception {
    }

    @Test
    public void singleValueField_WithCustomKey() throws Exception {
    }

    @Test
    public void singleValuedField_WithSubAggregation() throws Exception {
    }

    @Test
    public void singleValuedField_WithSubAggregation_Inherited() throws Exception {
    }

    @Test
    public void multiValuedField() throws Exception {
    }

    @Test
    public void multiValuedField_WithValueScript() throws Exception {
    }

    @Test
    public void multiValuedField_WithValueScript_WithInheritedSubAggregator() throws Exception {
    }

    @Test
    public void script_SingleValue() throws Exception {
    }

    @Test
    public void script_SingleValue_WithSubAggregator_Inherited() throws Exception {
    }

    @Test
    public void script_MultiValued() throws Exception {
    }

    @Test
    public void script_MultiValued_WithAggregatorInherited() throws Exception {
    }

    @Test
    public void unmapped() throws Exception {
    }

    @Test
    public void partiallyUnmapped() throws Exception {
    }


}
