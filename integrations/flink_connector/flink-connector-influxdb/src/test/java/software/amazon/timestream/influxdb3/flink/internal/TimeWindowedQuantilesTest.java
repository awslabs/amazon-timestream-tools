/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.internal;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TimeWindowedQuantilesTest {

    private final AtomicLong nanos = new AtomicLong();

    @Test
    void reportsQuantilesOfValuesWithinTheWindow() {
        TimeWindowedQuantiles quantiles = newQuantiles(Duration.ofSeconds(60), 100);
        for (long value = 1; value <= 100; value++) {
            quantiles.update(value);
        }

        assertThat(quantiles.getQuantile(0.50)).isEqualTo(50);
        assertThat(quantiles.getQuantile(0.99)).isEqualTo(99);
        assertThat(quantiles.getQuantile(1.0)).isEqualTo(100);
    }

    @Test
    void excludesValuesOlderThanTheWindow() {
        TimeWindowedQuantiles quantiles = newQuantiles(Duration.ofSeconds(60), 100);
        quantiles.update(1_000);
        advance(Duration.ofSeconds(30));
        quantiles.update(10);
        quantiles.update(20);

        assertThat(quantiles.getQuantile(1.0)).isEqualTo(1_000);

        advance(Duration.ofSeconds(31));

        assertThat(quantiles.getQuantile(1.0)).isEqualTo(20);
        assertThat(quantiles.getQuantile(0.0)).isEqualTo(10);

        advance(Duration.ofSeconds(30));

        assertThat(quantiles.getQuantile(0.99)).isZero();
    }

    @Test
    void keepsOnlyTheNewestValuesAtCapacity() {
        TimeWindowedQuantiles quantiles = newQuantiles(Duration.ofSeconds(60), 3);
        quantiles.update(100);
        quantiles.update(1);
        quantiles.update(2);
        quantiles.update(3);

        assertThat(quantiles.getQuantile(1.0)).isEqualTo(3);
        assertThat(quantiles.getQuantile(0.0)).isEqualTo(1);
    }

    @Test
    void handlesClockOverflow() {
        nanos.set(Long.MAX_VALUE - Duration.ofSeconds(10).toNanos());
        TimeWindowedQuantiles quantiles = newQuantiles(Duration.ofSeconds(60), 10);
        quantiles.update(7);
        advance(Duration.ofSeconds(20));

        assertThat(quantiles.getQuantile(0.5)).isEqualTo(7);
    }

    @Test
    void rejectsInvalidArguments() {
        TimeWindowedQuantiles quantiles = newQuantiles(Duration.ofSeconds(1), 1);

        assertThatThrownBy(() -> quantiles.getQuantile(1.1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> newQuantiles(Duration.ZERO, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> newQuantiles(Duration.ofSeconds(1), 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private TimeWindowedQuantiles newQuantiles(Duration window, int capacity) {
        return new TimeWindowedQuantiles(window, capacity, nanos::get);
    }

    private void advance(Duration duration) {
        nanos.addAndGet(duration.toNanos());
    }
}
