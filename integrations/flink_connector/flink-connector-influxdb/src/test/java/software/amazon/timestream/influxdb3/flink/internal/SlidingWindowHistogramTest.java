/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.internal;

import org.apache.flink.metrics.HistogramStatistics;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SlidingWindowHistogramTest {

    @Test
    void reportsStatisticsForTheLatestWindow() {
        SlidingWindowHistogram histogram = new SlidingWindowHistogram(3);
        histogram.update(10);
        histogram.update(20);
        histogram.update(30);
        histogram.update(40);

        HistogramStatistics statistics = histogram.getStatistics();

        assertThat(histogram.getCount()).isEqualTo(4);
        assertThat(statistics.size()).isEqualTo(3);
        assertThat(statistics.getValues()).containsExactlyInAnyOrder(40, 20, 30);
        assertThat(statistics.getMin()).isEqualTo(20);
        assertThat(statistics.getMax()).isEqualTo(40);
        assertThat(statistics.getMean()).isEqualTo(30);
        assertThat(statistics.getQuantile(0.5)).isEqualTo(30);
        assertThat(statistics.getStdDev()).isGreaterThan(0);
    }

    @Test
    void handlesEmptyWindowsAndRejectsInvalidInputs() {
        SlidingWindowHistogram histogram = new SlidingWindowHistogram(2);
        HistogramStatistics statistics = histogram.getStatistics();

        assertThat(statistics.size()).isZero();
        assertThat(statistics.getMin()).isZero();
        assertThat(statistics.getMax()).isZero();
        assertThat(statistics.getMean()).isZero();
        assertThat(statistics.getStdDev()).isZero();
        assertThat(statistics.getQuantile(0.5)).isZero();
        assertThatThrownBy(() -> statistics.getQuantile(-0.1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SlidingWindowHistogram(0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
