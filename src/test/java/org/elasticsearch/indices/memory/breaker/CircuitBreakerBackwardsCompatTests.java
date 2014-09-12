/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.indices.memory.breaker;

import com.google.common.base.Predicate;
import org.apache.lucene.util.English;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ElasticsearchBackwardsCompatIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;




@ElasticsearchIntegrationTest.ClusterScope(scope= ElasticsearchIntegrationTest.Scope.SUITE,  numClientNodes = 0)
public class CircuitBreakerBackwardsCompatTests extends ElasticsearchBackwardsCompatIntegrationTest {

    @Test
    public void testCBStats() throws Exception {
        createIndex("test");

        NodesInfoResponse nodesInfo = client().admin().cluster().prepareNodesInfo().execute().actionGet();
        Map<String, NodeInfo> versions = nodesInfo.getNodesMap();

        Settings settings = ImmutableSettings.settingsBuilder()
                .put("client.transport.ignore_cluster_name", true).build();

        // We explicitly connect to each node with a custom TransportClient
        for (NodeInfo n : nodesInfo.getNodes()) {
            TransportClient tc = new TransportClient(settings).addTransportAddress(n.getNode().address());
            NodesStatsResponse ns = tc.admin().cluster().prepareNodesStats().setBreaker(true).execute().actionGet();

            // This is the version of the node we are talking to via Transport Client
            Version tcNodeVersion = versions.get(n.getNode().getId()).getVersion();

            for (NodeStats stats : ns.getNodes()) {

                // If the node we are talking to is 1.4.0_beta+, it will have full responses
                if (tcNodeVersion.onOrAfter(Version.V_1_4_0_Beta)) {

                    CircuitBreakerStats[] cb = stats.getBreaker().getAllStats();
                    Version nodeVersion = versions.get(stats.getNode().getId()).getVersion();

                    // If the node in the stats (not the node we are talking to) is 1.4.0_beta+, it will have multiple CBs
                    if (nodeVersion.onOrAfter(Version.V_1_4_0_Beta)) {

                        for (CircuitBreakerStats stat : cb) {
                            assertThat(stat.getLimit(), greaterThan(0l));
                            assertThat(stat.getTrippedCount(), equalTo(0l));
                        }
                    } else if (nodeVersion.onOrAfter(Version.V_1_2_0)) {
                        // if it is 1.2.0+ it will have a single CB
                        for (CircuitBreakerStats stat : cb) {
                            if (stat.getName() == CircuitBreaker.Name.FIELDDATA) {
                                assertThat(stat.getLimit(), greaterThan(0l));
                                assertThat(stat.getTrippedCount(), equalTo(0l));
                            } else {
                                assertThat(stat.getLimit(), equalTo(0l));
                                assertThat(stat.getTrippedCount(), equalTo(-1l));
                            }
                        }

                    } else {
                        //Otherwise it will have -1
                        for (CircuitBreakerStats stat : cb) {
                            assertThat(stat.getLimit(), equalTo(0l));
                            assertThat(stat.getTrippedCount(), equalTo(-1l));
                        }
                    }
                } else if (tcNodeVersion.onOrAfter(Version.V_1_2_0)) {
                    // If the node we are talking to is 1.2.0+, it will only have FIELDDATA
                    // But because this test code is executing in 1.4.0_beta+ it will still be an array, rest of values will be -1

                    CircuitBreakerStats[] cb = stats.getBreaker().getAllStats();
                    for (CircuitBreakerStats stat : cb) {
                        if (stat.getName() == CircuitBreaker.Name.FIELDDATA) {
                            assertThat(stat.getLimit(), greaterThan(0l));
                            assertThat(stat.getTrippedCount(), equalTo(0l));
                        } else {
                            assertThat(stat.getLimit(), equalTo(0l));
                            assertThat(stat.getTrippedCount(), equalTo(-1l));
                        }
                    }

                } else {
                    // If node is < 1.2.0, it won't have any CB at all.

                    CircuitBreakerStats[] cb = stats.getBreaker().getAllStats();
                    for (CircuitBreakerStats stat : cb) {
                        assertThat(stat.getLimit(), equalTo(0l));
                        assertThat(stat.getTrippedCount(), equalTo(-1l));
                    }


                }
            }
        }

    }

}
