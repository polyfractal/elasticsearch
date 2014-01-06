package org.elasticsearch.search.aggregations.metrics.percentile;

import org.elasticsearch.search.aggregations.metrics.ValuesSourceMetricsAggregationBuilder;

/**
 *
 */
public class PercentileBuilder extends ValuesSourceMetricsAggregationBuilder<PercentileBuilder> {

    public PercentileBuilder(String name) {
        super(name, InternalPercentile.TYPE.name());
    }
}
