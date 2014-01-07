package org.elasticsearch.search.aggregations.metrics.percentile.providers.qdigest;

public interface IQuantileEstimator
{
    void offer(long value);

    long getQuantile(double q);
}
