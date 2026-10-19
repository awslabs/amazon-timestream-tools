/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.internal;

import java.time.Duration;
import java.util.Arrays;
import java.util.function.LongSupplier;

/**
 * Quantiles over the values recorded within a recent time window.
 *
 * <p>Unlike {@link SlidingWindowHistogram}, which keeps a fixed number of values regardless of
 * their age, values older than the window are excluded, so quantiles follow the current write
 * rate. At most {@code capacity} of the newest values are kept.
 */
final class TimeWindowedQuantiles {
    private final long windowNanos;
    private final LongSupplier nanoClock;
    private final long[] timestamps;
    private final long[] values;
    private int nextIndex;
    private int size;

    TimeWindowedQuantiles(Duration window, int capacity) {
        this(window, capacity, System::nanoTime);
    }

    TimeWindowedQuantiles(Duration window, int capacity, LongSupplier nanoClock) {
        if (window == null || window.isNegative() || window.isZero()) {
            throw new IllegalArgumentException("window must be positive");
        }
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        this.windowNanos = window.toNanos();
        this.nanoClock = nanoClock;
        this.timestamps = new long[capacity];
        this.values = new long[capacity];
    }

    synchronized void update(long value) {
        timestamps[nextIndex] = nanoClock.getAsLong();
        values[nextIndex] = value;
        nextIndex = (nextIndex + 1) % values.length;
        size = Math.min(size + 1, values.length);
    }

    /**
     * @param quantile quantile from {@code 0.0} to {@code 1.0}
     * @return the quantile of values recorded within the window, or {@code 0} when there are none
     */
    double getQuantile(double quantile) {
        if (quantile < 0 || quantile > 1) {
            throw new IllegalArgumentException("quantile must be between 0.0 and 1.0");
        }
        long[] recent = recentValues();
        if (recent.length == 0) {
            return 0;
        }
        Arrays.sort(recent);
        return recent[(int) Math.floor(quantile * (recent.length - 1))];
    }

    private synchronized long[] recentValues() {
        long cutoff = nanoClock.getAsLong() - windowNanos;
        long[] recent = new long[size];
        int count = 0;
        // Newest first; values are recorded in clock order, so the first expired value ends
        // the scan. Subtraction keeps the comparison correct across nanoTime overflow.
        for (int offset = 1; offset <= size; offset++) {
            int index = Math.floorMod(nextIndex - offset, values.length);
            if (timestamps[index] - cutoff < 0) {
                break;
            }
            recent[count++] = values[index];
        }
        return Arrays.copyOf(recent, count);
    }
}
