/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.internal;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InfluxDB3WriteRequestTest {

    @Test
    void rejectsInvalidLineProtocol() {
        assertThatThrownBy(() -> InfluxDB3WriteRequest.initial(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("lineProtocol");
        assertThatThrownBy(() -> InfluxDB3WriteRequest.initial(" \t"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly one");
        assertThatThrownBy(() -> InfluxDB3WriteRequest.initial("cpu value=1i\ncpu value=2i"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly one");
        assertThatThrownBy(() -> InfluxDB3WriteRequest.initial("cpu value=1i\rcpu value=2i"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly one");
    }

    @Test
    void rejectsNegativeRetryMetadata() {
        assertThatThrownBy(() -> new InfluxDB3WriteRequest("cpu value=1i", -1, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryAttempt");
        assertThatThrownBy(() -> new InfluxDB3WriteRequest("cpu value=1i", 0, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("authenticationRefreshes");
    }

    @Test
    void rejectsRetryMetadataOverflow() {
        InfluxDB3WriteRequest retryOverflow =
                new InfluxDB3WriteRequest("cpu value=1i", Integer.MAX_VALUE, 0);
        InfluxDB3WriteRequest refreshOverflow =
                new InfluxDB3WriteRequest("cpu value=1i", 0, Integer.MAX_VALUE);

        assertThatThrownBy(retryOverflow::nextRetry)
                .isInstanceOf(ArithmeticException.class);
        assertThatThrownBy(refreshOverflow::afterAuthenticationRefresh)
                .isInstanceOf(ArithmeticException.class);
    }

    @Test
    void representsLocalValidationFailuresWithoutMakingThemWritable() {
        InfluxDB3WriteRequest request =
                InfluxDB3WriteRequest.validationFailure("converter returned null");

        assertThat(request.isValidationFailure()).isTrue();
        assertThat(request.getLineProtocol()).isNull();
        assertThat(request.getValidationFailureMessage()).isEqualTo("converter returned null");
        assertThatThrownBy(request::nextRetry)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("validation failure");
        assertThatThrownBy(request::afterAuthenticationRefresh)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("validation failure");
    }
}
