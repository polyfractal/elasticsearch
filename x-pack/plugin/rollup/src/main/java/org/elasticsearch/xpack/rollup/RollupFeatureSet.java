/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.rollup.RollupFeatureSetUsage;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.action.GetRollupJobsAction;
import org.elasticsearch.xpack.core.rollup.job.RollupJobConfig;
import org.elasticsearch.xpack.rollup.action.RollupIndexCaps;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

public class RollupFeatureSet implements XPackFeatureSet {

    private final boolean enabled;
    private final XPackLicenseState licenseState;
    private final Client client;
    private final ClusterService clusterService;

    @Inject
    public RollupFeatureSet(Settings settings, @Nullable XPackLicenseState licenseState,
                            Client client, ClusterService clusterService) {
        this.enabled = XPackSettings.ROLLUP_ENABLED.get(settings);
        this.licenseState = licenseState;
        this.client = client;
        this.clusterService = clusterService;
    }

    @Override
    public String name() {
        return XPackField.ROLLUP;
    }

    @Override
    public String description() {
        return "Time series pre-aggregation and rollup";
    }

    @Override
    public boolean available() {
        return licenseState != null && licenseState.isRollupAllowed();
    }

    @Override
    public boolean enabled() {
        return enabled;
    }

    @Override
    public Map<String, Object> nativeCodeInfo() {
        return null;
    }

    @Override
    public void usage(ActionListener<XPackFeatureSet.Usage> listener) {

        if (enabled == false) {
            listener.onResponse(new RollupFeatureSetUsage(available(), enabled(), null, null));
            return;
        }
        new UsageAggregator(client, clusterService, listener, available(), enabled);
    }

    public static class UsageAggregator {
        GetRollupJobsAction.Response jobsResponse;

        UsageAggregator(Client client, ClusterService clusterService, ActionListener<XPackFeatureSet.Usage> listener,
                        boolean available, boolean enabled) {

            CountDownLatch latch = new CountDownLatch(2);

            client.execute(GetRollupJobsAction.INSTANCE, new GetRollupJobsAction.Request(MetaData.ALL),
                ActionListener.wrap(response -> {
                    jobsResponse = response;
                    latch.countDown();
                }, listener::onFailure));


            List<RollupJobConfig> configs = new ArrayList<>();
            StreamSupport.stream(clusterService.state().getMetaData().indices().spliterator(), false)
                .forEach(entry -> {
                    // Does this index have rollup metadata?
                    configs.addAll(findRollupJobs(entry.key, entry.value));
                });


            try {
                latch.await(30, TimeUnit.SECONDS);
                listener.onResponse(new RollupFeatureSetUsage(available, enabled,
                    new RollupFeatureSetUsage.JobUsage(jobsResponse),
                    new RollupFeatureSetUsage.JobUsage(configs)));
            } catch (InterruptedException e) {
                listener.onFailure(e);
            }
        }

        static List<RollupJobConfig> findRollupJobs(String indexName, IndexMetaData indexMetaData) {
            if (indexMetaData == null) {
                return Collections.emptyList();
            }

            MappingMetaData rollupMapping = indexMetaData.getMappings().get(RollupField.TYPE_NAME);
            if (rollupMapping == null) {
                return Collections.emptyList();
            }

            Object objMeta = rollupMapping.getSourceAsMap().get("_meta");
            if (objMeta == null) {
                return Collections.emptyList();
            }

            return RollupIndexCaps.parseMetadataXContentToRawJobs(
                new BytesArray(rollupMapping.source().uncompressed()), indexName);
        }
    }
}
