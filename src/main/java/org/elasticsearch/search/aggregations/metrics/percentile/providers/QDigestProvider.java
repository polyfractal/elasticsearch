package org.elasticsearch.search.aggregations.metrics.percentile.providers;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.search.aggregations.metrics.percentile.providers.qdigest.QDigest;

import java.io.IOException;

public class QDigestProvider extends PercentileProvider {


    private static final ESLogger log= Loggers.getLogger(FrugalProvider.class);

    public QDigest digest;

    /**
     * Instantiate a new FrugalProvider
     * <p>
     * This class implements the Frugal-2U algorithm for streaming quantiles.  See
     * http://dx.doi.org/10.1007/978-3-642-40273-9_7 for original paper, and
     * http://blog.aggregateknowledge.com/2013/09/16/sketch-of-the-day-frugal-streaming/
     * for "layman" explanation.
     * <p>
     * Frugal-2U maintains a probabilistic estimate of the requested percentile, using
     * minimal memory to do so.  Error can grow as high as 50% under certain circumstances,
     * particularly during cold-start and when the stream drifts suddenly
     *
     * @param percentiles how many intervals to calculate quantiles for
     */
    public QDigestProvider(double[] percentiles) {
        super(percentiles);
        digest = new QDigest(2);
    }

    /**
     * Offer a new value to the streaming percentile algo.  May modify the current
     * estimate
     *
     * @param value Value to stream
     */
    public void offer(double value) {
        digest.offer((long)value);
    }


    public double getEstimate(int index) {
        return digest.getQuantile(percentiles[index] / 100);
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {

    }


    @Override
    public void writeTo(StreamOutput out) throws IOException {


    }



    /**
     * This function merges another FrugalQuantile estimator with the
     * current estimator.  Uses a naive average of the two quantiles to
     * determine the merged histogram.
     *
     * @param qdigest Another FrugalQuantile to merge with current estimator
     * @return The current estimator post-merge
     */
    public PercentileProvider merge(PercentileProvider qdigest, int expectedMerges) {
        log.info("QDigest: " + String.valueOf(ramBytesUsed()));

        digest = QDigest.unionOf(digest, ((QDigestProvider) qdigest).digest);

        return this;
    }

    public static class Factory extends PercentileProvider.Factory {

        public Factory(double[] percentiles) {
            super(percentiles);
        }

        public QDigestProvider create() {
            return new QDigestProvider(this.percentiles);
        }
    }

    public long ramBytesUsed() {
        return digest.ramBytesUsed() + percentiles.length * 8;

    }
}