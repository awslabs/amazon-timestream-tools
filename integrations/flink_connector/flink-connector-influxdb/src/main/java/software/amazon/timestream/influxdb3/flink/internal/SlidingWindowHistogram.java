/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.internal;

import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;

import java.util.Arrays;

/** Small dependency-free histogram reservoir for connector latency metrics. */
final class SlidingWindowHistogram implements Histogram {
    private final long[] values;
    private long count;
    private int nextIndex;

    SlidingWindowHistogram(int windowSize) {
        if (windowSize <= 0) {
            throw new IllegalArgumentException("windowSize must be positive");
        }
        this.values = new long[windowSize];
    }

    @Override
    public synchronized void update(long value) {
        values[nextIndex] = value;
        nextIndex = (nextIndex + 1) % values.length;
        count++;
    }

    @Override
    public synchronized long getCount() {
        return count;
    }

    @Override
    public synchronized HistogramStatistics getStatistics() {
        int size = (int) Math.min(count, values.length);
        return new Snapshot(Arrays.copyOf(values, size));
    }

    private static final class Snapshot extends HistogramStatistics {
        private final long[] values;
        private final long[] sortedValues;
        private final double mean;
        private final double standardDeviation;

        private Snapshot(long[] values) {
            this.values = values;
            this.sortedValues = Arrays.copyOf(values, values.length);
            Arrays.sort(sortedValues);

            double sum = 0;
            for (long value : values) {
                sum += value;
            }
            this.mean = values.length == 0 ? 0 : sum / values.length;

            double squaredDeviation = 0;
            for (long value : values) {
                double deviation = value - mean;
                squaredDeviation += deviation * deviation;
            }
            this.standardDeviation =
                    values.length == 0 ? 0 : Math.sqrt(squaredDeviation / values.length);
        }

        @Override
        public double getQuantile(double quantile) {
            if (quantile < 0 || quantile > 1) {
                throw new IllegalArgumentException("quantile must be between 0.0 and 1.0");
            }
            if (sortedValues.length == 0) {
                return 0;
            }
            int index = (int) Math.floor(quantile * (sortedValues.length - 1));
            return sortedValues[index];
        }

        @Override
        public long[] getValues() {
            return Arrays.copyOf(values, values.length);
        }

        @Override
        public int size() {
            return values.length;
        }

        @Override
        public double getMean() {
            return mean;
        }

        @Override
        public double getStdDev() {
            return standardDeviation;
        }

        @Override
        public long getMax() {
            return sortedValues.length == 0 ? 0 : sortedValues[sortedValues.length - 1];
        }

        @Override
        public long getMin() {
            return sortedValues.length == 0 ? 0 : sortedValues[0];
        }
    }
}
