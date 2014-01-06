package org.elasticsearch.search.aggregations.metrics.percentile.providers;

import org.elasticsearch.common.io.stream.Streamable;


public abstract class PercentileProvider implements Streamable {


    public abstract void offer(double value);
    public abstract double getEstimate(int index);
    public abstract PercentileProvider merge(PercentileProvider frugal, int expectedMerges);
    public abstract long ramBytesUsed();

    protected double[] percentiles;

    public PercentileProvider(double[] percentiles) {
        this.percentiles = percentiles;
    }

    /**
     * @return list of percentile intervals
     */
    public double[] getPercentiles() {
        return percentiles;
    }

    public abstract static class Factory {
        protected final double[] percentiles;

        public Factory(double[] percentiles) {
            this.percentiles = percentiles;
        }

        public abstract PercentileProvider create();
    }
}
