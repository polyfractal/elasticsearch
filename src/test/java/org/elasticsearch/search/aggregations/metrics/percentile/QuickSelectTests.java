package org.elasticsearch.search.aggregations.metrics.percentile;

import org.elasticsearch.search.aggregations.metrics.percentile.providers.frugal.QuickSelect;
import org.elasticsearch.test.ElasticsearchTestCase;


public class QuickSelectTests extends ElasticsearchTestCase {

    public void testOrderedEven() {

        double[] data = new double[] {1,2,3,4,5,6,7,8,9,10};

        for (int i = 0; i < data.length -1; i++) {
            double selected = QuickSelect.quickSelect(data, 0, 9, i);
            assertEquals(i+1, (int)selected);
        }
    }

    public void testOrderedOdd() {

        double[] data = new double[] {1,2,3,4,5,6,7,8,9};

        for (int i = 0; i < data.length -1; i++) {
            double selected = QuickSelect.quickSelect(data, 0, 8, i);
            assertEquals(i+1, (int)selected);
        }
    }

    public void testUnorderedEven() {

        double[] data = new double[] {9,8,10,7,6,5,1,2,3,4};

        for (int i = 0; i < data.length -1; i++) {
            double selected = QuickSelect.quickSelect(data, 0, 9, i);
            assertEquals(i+1, (int)selected);
        }
    }

    public void testUnorderedOdd() {

        double[] data = new double[] {9,8,7,6,5,1,2,3,4};

        for (int i = 0; i < data.length -1; i++) {
            double selected = QuickSelect.quickSelect(data, 0, 8, i);
            assertEquals(i+1, (int)selected);
        }
    }



}
