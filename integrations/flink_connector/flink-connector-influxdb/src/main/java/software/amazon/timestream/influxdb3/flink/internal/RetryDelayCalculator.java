/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.internal;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

final class RetryDelayCalculator {
    private RetryDelayCalculator() {}

    static Duration calculate(
            int retryAttempt, Duration initialBackoff, Duration maxBackoff, double jitter) {
        if (retryAttempt <= 0 || initialBackoff.isZero()) {
            return Duration.ZERO;
        }

        long initialMillis = initialBackoff.toMillis();
        long maxMillis = maxBackoff.toMillis();
        int shift = Math.min(retryAttempt - 1, 62);
        long exponential;
        if (initialMillis > (Long.MAX_VALUE >> shift)) {
            exponential = Long.MAX_VALUE;
        } else {
            exponential = initialMillis << shift;
        }
        long capped = Math.min(exponential, maxMillis);
        if (jitter == 0.0 || capped == 0) {
            return Duration.ofMillis(capped);
        }
        double multiplier = ThreadLocalRandom.current().nextDouble(1.0 - jitter, 1.0 + jitter);
        return Duration.ofMillis(Math.min(maxMillis, Math.max(0L, (long) (capped * multiplier))));
    }
}
