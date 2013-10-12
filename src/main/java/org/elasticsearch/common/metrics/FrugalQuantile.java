package org.elasticsearch.common.metrics;


import java.util.Random;


public class FrugalQuantile implements Metric {


    private final int quantile;     // Quantile to estimate
    private final Random rand;

    private long estimate = 0;      // Current estimate of quantile
    private int  step     = 1;      // Current step value for frugal-2u
    private int  sign     = 1;      // Direction of last movement
    private boolean first = true;


    /**
     * Instantiate a new FrugralQuantile
     * <p>
     * This class implements the Frugal-2U algorithm for streaming quantiles.  See
     * http://dx.doi.org/10.1007/978-3-642-40273-9_7 for original paper, and
     * http://blog.aggregateknowledge.com/2013/09/16/sketch-of-the-day-frugal-streaming/
     * for "layman" explanation.
     * <p>
     * Frugal-2U maintains a probabilistic estimate of the requested quantile, using
     * minimal memory to do so.  Error can grow as high as 50% under certain circumstances,
     * particularly during cold-start and when the stream drifts suddenly
     *
     * @param quantile Quantile to estimate. 0-100
     */
    public FrugalQuantile(int quantile) {
        this.quantile = quantile;
        this.rand = new Random();
    }

    /**
     * Offer a new value to the streaming quantile algo.  May modify the current
     * estimate
     *
     * @param value Value to stream
     */
    public void offer(long value) {

        // Set estimate to first value in stream...helps to avoid fully cold starts
        if (first) {
            estimate = value;
            first = false;
            return;
        }

        /**
         * Movements in the same direction are rewarded with a boost to step, and
         * a big change to estimate. Movement in opposite direction gets negative
         * step boost but still a small boost to estimate
         *
         * 100% quantile doesn't need fancy algo, just save largest
         */
        if (quantile == 100 && value > estimate) {
            estimate = value;
        } else {
            final int randomValue = this.rand.nextInt(100);

            if (value > estimate && randomValue > (100 - quantile)) {
                step += sign * 10;

                if (step > 0) {
                    estimate += step;
                } else {
                    ++estimate;
                }

                sign = 1;

                //If we overshot, reduce step and reset estimate
                if (estimate > value) {
                    step += (value - estimate);
                    estimate = value;
                }
            } else if (value < estimate && randomValue < (100 - quantile)) {
                step += -sign * 10;

                if (step > 0) {
                    estimate -= step;
                } else {
                    --estimate;
                }

                sign = -1;

                //If we overshot, reduce step and reset estimate
                if (estimate < value) {
                    step += (estimate - value);
                    estimate = value;
                }
            }

            // Smooth out oscillations
            if ((estimate - value) * sign < 0 && step > 1) {
                step = 1;
            }

            // Prevent step from growing more negative than necessary
            if (step < -1000) {
                step = -1000;
            }
        }


    }

    /**
     * @return Current estimate
     */
    public long getValue() {
        return this.estimate;
    }

    /**
     * @param value estimate to manually set
     */
    public void setValue(long value) {
        this.estimate = value;
    }

    /**
     * @return Quantile that is being estimated
     */
    public long getQuantile() {
        return this.quantile;
    }

    /**
     * This function merges another FrugalQuantile estimator with the
     * current estimator.  Uses a naive average of the two quantiles to
     * determine the merged histogram.
     *
     * @param frugal Another FrugalQuantile to merge with current estimator
     * @return The current estimator post-merge
     */
    public FrugalQuantile merge(FrugalQuantile frugal) {

        this.estimate += frugal.getValue();
        this.estimate /= 2;

        return this;
    }
}
