package org.elasticsearch.common.metrics;


import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.test.ElasticSearchTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

import java.util.*;


public class FrugalQuantileTests extends ElasticSearchTestCase {

    @Test
    public void test100Quantile() {
        FrugalQuantile quantile = new FrugalQuantile(100);
        quantile.offer(1000L);
        assertThat(quantile.getValue(), equalTo(1000L));
    }

    @Test
    public void testQuantileAccuracyUniform() {
        Random rand = new Random();
        int numValues = 10000;

        long[] values = new long[numValues];
        long rVal;

        for (int quantIndex = 25; quantIndex <= 100; quantIndex += 25) {
            FrugalQuantile quantile = new FrugalQuantile(quantIndex);

            int quantileIndex = (int) (numValues * ((float)quantIndex / 100));
            if (quantileIndex == numValues) {
                --quantileIndex;
            }

            for (int i = 0; i < numValues; ++i) {
                rVal = rand.nextInt(100);
                values[i] = rVal;
                quantile.offer(rVal);
            }

            long estimate = quantile.getValue();
            Arrays.sort(values);

            double err = Math.abs((estimate - values[quantileIndex]) / (values[quantileIndex]+0.01));
            assertThat(err, lessThan(0.4));
        }

    }

    @Test
    public void testQuantileAccuracyGaussian() {
        Random rand = new Random();
        int numValues = 10000;

        long[] values = new long[numValues];
        long rVal;

        for (int quantIndex = 25; quantIndex <= 100; quantIndex += 25) {
            FrugalQuantile quantile = new FrugalQuantile(quantIndex);

            int quantileIndex = (int) (numValues * ((float)quantIndex / 100));
            if (quantileIndex == numValues) {
                --quantileIndex;
            }

            for (int i = 0; i < numValues; ++i) {
                rVal = (long) rand.nextGaussian() * 20 + 50;
                values[i] = rVal;
                quantile.offer(rVal);
            }

            long estimate = quantile.getValue();
            Arrays.sort(values);

            double err = Math.abs((estimate - values[quantileIndex]) / (values[quantileIndex]+0.01));
            assertThat(err, lessThan(0.4));
        }

    }
}
