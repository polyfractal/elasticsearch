package org.elasticsearch.search.aggregations.metrics.percentile.providers;

import com.carrotsearch.hppc.BitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.search.aggregations.metrics.percentile.providers.frugal.QuickSelect;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

public class FrugalProvider extends PercentileProvider {

    private final Random rand;

    public double[] estimates;      // Current estimate of percentile
    private int[] steps;      // Current step value for frugal-2u
    private BitSet signs;      // Direction of last movement

    //private int numCounters = 10;

    private boolean first = true;

    double[] merging = null;
    int currentMerge = 0;

    private static final ESLogger log= Loggers.getLogger(FrugalProvider.class);


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
    public FrugalProvider(double[] percentiles) {
        super(percentiles);

        this.estimates = new double[percentiles.length];
        this.steps = new int[percentiles.length];
        this.signs = new BitSet(percentiles.length);

        this.signs.set(0,percentiles.length);
        Arrays.fill(this.steps, 1);


        this.rand = new Random();
    }

    /**
     * Offer a new value to the streaming percentile algo.  May modify the current
     * estimate
     *
     * @param value Value to stream
     */
    public void offer(double value) {

        // Set estimate to first value in stream...helps to avoid fully cold starts
        if (first) {
            Arrays.fill(this.estimates, value);
            first = false;
            return;
        }

        for (int i = 0 ; i < percentiles.length; ++i) {
                offerTo(i, value);
        }
    }

    private void offerTo(int index, double value) {
        
        double percentile = this.percentiles[index];

        /**
         * Movements in the same direction are rewarded with a boost to step, and
         * a big change to estimate. Movement in opposite direction gets negative
         * step boost but still a small boost to estimate
         *
         * 100% percentile doesn't need fancy algo, just save largest
         */
        if (percentile == 100 && value > estimates[index]) {
            estimates[index] = value;
        } else {
            final double randomValue = this.rand.nextDouble() * 100.0d;

            if (value > estimates[index] && randomValue > (100.0d - percentile)) {
                steps[index] += signs.get(index) ? 1 : -1;

                if (steps[index] > 0) {
                    estimates[index] += steps[index];
                } else {
                    ++estimates[index];
                }

                signs.set(index);

                //If we overshot, reduce step and reset estimate
                if (estimates[index] > value) {
                    steps[index] += (value - estimates[index]);
                    estimates[index] = value;
                }
            } else if (value < estimates[index] && randomValue < (100.0d - percentile)) {
                steps[index] += signs.get(index) ? -1 : 1;

                if (steps[index] > 0) {
                    estimates[index] -= steps[index];
                } else {
                    --estimates[index];
                }

                signs.clear(index);

                //If we overshot, reduce step and reset estimate
                if (estimates[index] < value) {
                    steps[index] += (estimates[index] - value);
                    estimates[index] = value;
                }
            }

            // Smooth out oscillations
            if ((estimates[index] - value) * (signs.get(index) ? 1 : -1)  < 0 && steps[index] > 1) {
                steps[index] = 1;
            }

            // Prevent step from growing more negative than necessary
            if (steps[index] <= -Integer.MAX_VALUE + 1000) {
                steps[index] = -Integer.MAX_VALUE + 1000;
            }
        }
    }

    public double getEstimate(int index) {
        return estimates[index];
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {

        first = in.readBoolean();
        int intervals = in.readInt();

        this.percentiles = new double[intervals];
        this.estimates = new double[intervals];
        this.steps = new int[intervals];
        //this.signs = new BitSet(intervals * numCounters);

        for (int i = 0 ; i < percentiles.length; ++i) {
            percentiles[i] = in.readDouble();
            estimates[i] = in.readDouble();
            steps[i] = in.readInt();
        }

        int bitLength = in.readInt();
        long[] bits = new long[bitLength];
        for (int i = 0; i < bitLength; ++i) {
            bits[i] = in.readLong();
        }
        signs = new BitSet(percentiles.length);
        signs.bits = bits;


    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

        out.writeBoolean(first);
        out.writeInt(percentiles.length);
        for (int i = 0 ; i < percentiles.length; ++i) {
            out.writeDouble(percentiles[i]);
            out.writeDouble(estimates[i]);
            out.writeInt(steps[i]);
            //out.writeInt(signs[i]);
        }
        out.writeInt(signs.bits.length);
        for (int i = 0; i < signs.bits.length; ++i) {
            out.writeLong(signs.bits[i]);
        }


    }



    /**
     * This function merges another FrugalQuantile estimator with the
     * current estimator.  Merging is accomplished by taking the median for
     * each percentile.  More accurate than simply averaging, also probably
     * slower
     *
     * @param frugal Another FrugalQuantile to merge with current estimator
     * @return The current estimator post-merge
     */
    public PercentileProvider merge(PercentileProvider frugal, int expectedMerges) {
        log.info("Frugal: " + String.valueOf(ramBytesUsed()));

        if (merging == null) {
            merging = new double[expectedMerges * percentiles.length];
        }

        for (int i = 0; i < percentiles.length; ++i) {
            merging[(expectedMerges* i) + currentMerge] = frugal.getEstimate(i);
        }
        ++currentMerge;

        if (currentMerge == expectedMerges - 1) {
            for (int i = 0; i < percentiles.length; ++i) {
                merging[(expectedMerges* i) + currentMerge] = estimates[i];
                int median = QuickSelect.quickSelect(merging, i * expectedMerges, (i * expectedMerges) + expectedMerges - 1, (int)(expectedMerges / 2));
                estimates[i] = merging[median];
            }

        }
        return this;
    }


    public static class Factory extends PercentileProvider.Factory {

        public Factory(double[] percentiles) {
            super(percentiles);
        }

        public FrugalProvider create() {
            return new FrugalProvider(this.percentiles);
        }
    }

    public long ramBytesUsed() {

        return percentiles.length * 8
                + estimates.length * 8
                + steps.length * 4
                + signs.bits.length * 8
                + 1  //(bool) first
                + RamUsageEstimator.sizeOf(rand);
    }
}