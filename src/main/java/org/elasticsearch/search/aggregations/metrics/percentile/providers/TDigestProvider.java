package org.elasticsearch.search.aggregations.metrics.percentile.providers;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.search.aggregations.metrics.percentile.providers.tdigest.TDigest;

import java.io.IOException;


public class TDigestProvider extends PercentileProvider {

    private static final ESLogger log= Loggers.getLogger(FrugalProvider.class);
    public TDigest digest;


    public TDigestProvider(double[] percentiles) {
        super(percentiles);
        digest = new TDigest(20);
    }

    public void offer(double value) {
        digest.add(value);
    }


    public double getEstimate(int index) {
        return digest.quantile(percentiles[index] / 100);
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {

    }


    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }


    public PercentileProvider merge(PercentileProvider tdigest, int expectedMerges) {
        log.info("TDigest: " + String.valueOf(ramBytesUsed()) + " (" + String.valueOf(digest.byteSize()) + ")");

        digest.add(((TDigestProvider)tdigest).digest);

        return this;
    }

    public static class Factory extends PercentileProvider.Factory {

        public Factory(double[] percentiles) {
            super(percentiles);
        }

        public TDigestProvider create() {
            return new TDigestProvider(this.percentiles);
        }
    }

    public long ramBytesUsed() {
        return digest.byteSize() + percentiles.length * 8;

    }
}