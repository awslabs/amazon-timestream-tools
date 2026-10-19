/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.internal;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class RetryDelayCalculatorTest {

    @Test
    void calculatesCappedExponentialBackoff() {
        Duration initial = Duration.ofMillis(100);
        Duration maximum = Duration.ofMillis(250);

        assertThat(RetryDelayCalculator.calculate(0, initial, maximum, 0)).isZero();
        assertThat(RetryDelayCalculator.calculate(1, initial, maximum, 0))
                .isEqualTo(Duration.ofMillis(100));
        assertThat(RetryDelayCalculator.calculate(2, initial, maximum, 0))
                .isEqualTo(Duration.ofMillis(200));
        assertThat(RetryDelayCalculator.calculate(3, initial, maximum, 0))
                .isEqualTo(maximum);
        assertThat(RetryDelayCalculator.calculate(63, initial, maximum, 0))
                .isEqualTo(maximum);
    }

    @Test
    void appliesJitterWithoutExceedingConfiguredMaximum() {
        for (int iteration = 0; iteration < 100; iteration++) {
            Duration delay =
                    RetryDelayCalculator.calculate(
                            1, Duration.ofMillis(100), Duration.ofMillis(110), 0.2);
            assertThat(delay).isBetween(Duration.ofMillis(80), Duration.ofMillis(110));
        }
    }

    @Test
    void returnsZeroWhenMaximumBackoffIsZeroEvenWithJitter() {
        assertThat(
                        RetryDelayCalculator.calculate(
                                1, Duration.ofMillis(100), Duration.ZERO, 0.2))
                .isZero();
    }
}
