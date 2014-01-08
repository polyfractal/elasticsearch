package org.elasticsearch.search.aggregations.metrics.percentile.providers.qdigest;

/**
 * Upstream: Stream-lib, master @ 704002a2d8fa01fa7e9868dae9d0c8bedd8e9427
 * https://github.com/addthis/stream-lib/blob/master/src/main/java/com/clearspring/analytics/stream/quantile/IQuantileEstimator.java
 */

public interface IQuantileEstimator
{
    void offer(long value);

    long getQuantile(double q);
}
