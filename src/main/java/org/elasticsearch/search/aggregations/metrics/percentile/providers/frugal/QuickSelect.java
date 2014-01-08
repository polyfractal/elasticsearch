package org.elasticsearch.search.aggregations.metrics.percentile.providers.frugal;


import java.util.Random;

public class QuickSelect {

    private static final Random rand = new Random();

    public static double quickSelect(double[] list, int left, int right, int k) {

        if (left == right) {
            return list[left];
        }

        while (true) {
            int pivot = left + rand.nextInt(right - left + 1);

            pivot = partition(list, left, right, pivot);

            if (pivot - left == k) {
                return list[pivot];
            } else if (pivot - left < k) {
                k -= pivot - left + 1;
                left = pivot + 1;
            } else {
                right = pivot -1;
            }
        }
    }

    private static int partition(double[] list, int left, int right, int pivot) {
        double pivotValue = list[pivot];

        swap(list, pivot, right);

        int stored = left;

        for (int i = left; i < right; ++i) {
            if (list[i] <= pivotValue) {
                swap (list, stored, i);
                ++stored;
            }
        }

        swap (list, right, stored);
        return stored;

    }

    private static void swap(double[] list, int left, int right)
    {
        double temp = list[left];
        list[left] = list[right];
        list[right] = temp;
    }
}
