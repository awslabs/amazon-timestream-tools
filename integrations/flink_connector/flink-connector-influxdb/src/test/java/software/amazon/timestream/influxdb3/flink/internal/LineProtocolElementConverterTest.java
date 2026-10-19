/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.internal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LineProtocolElementConverterTest {
    private static final long MAX_RECORD_SIZE_IN_BYTES = 1_024;

    @Test
    void convertsExactlyOneLine() {
        LineProtocolElementConverter<String> converter =
                new LineProtocolElementConverter<>(
                        value -> "cpu,host=a value=" + value,
                        true,
                        false,
                        MAX_RECORD_SIZE_IN_BYTES);

        InfluxDB3WriteRequest request = converter.apply("1i", null);

        assertThat(request.getLineProtocol()).isEqualTo("cpu,host=a value=1i");
        assertThat(request.getRetryAttempt()).isZero();
        assertThat(request.getAuthenticationRefreshes()).isZero();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidLineProtocols")
    void rejectsInvalidLineProtocols(InvalidLineProtocol testCase) {
        assertThatThrownBy(
                        () ->
                                new LineProtocolElementConverter<String>(
                                                value -> testCase.lineProtocol(),
                                                true,
                                                false,
                                                MAX_RECORD_SIZE_IN_BYTES)
                                        .apply("input", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(testCase.expectedMessage());
    }

    @Test
    void representsValidationFailuresWhenConfiguredNotToFail() {
        LineProtocolElementConverter<String> nullConverter =
                new LineProtocolElementConverter<>(
                        value -> null, false, false, MAX_RECORD_SIZE_IN_BYTES);
        LineProtocolElementConverter<String> multilineConverter =
                new LineProtocolElementConverter<>(
                        value -> "cpu value=1i\ncpu value=2i",
                        false,
                        false,
                        MAX_RECORD_SIZE_IN_BYTES);
        LineProtocolElementConverter<String> throwingConverter =
                new LineProtocolElementConverter<>(
                        value -> {
                            throw new IllegalArgumentException("bad input");
                        },
                        false,
                        false,
                        MAX_RECORD_SIZE_IN_BYTES);

        assertThat(nullConverter.apply("input", null))
                .satisfies(
                        request -> {
                            assertThat(request.isValidationFailure()).isTrue();
                            assertThat(request.getLineProtocol()).isNull();
                            assertThat(request.getValidationFailureMessage())
                                    .contains("blank line");
                        });
        assertThat(multilineConverter.apply("input", null))
                .satisfies(
                        request -> {
                            assertThat(request.isValidationFailure()).isTrue();
                            assertThat(request.getLineProtocol()).isNull();
                        });
        assertThat(throwingConverter.apply("input", null))
                .satisfies(
                        request -> {
                            assertThat(request.isValidationFailure()).isTrue();
                            assertThat(request.getValidationFailureMessage())
                                    .contains("threw an exception");
                        });
    }

    @Test
    void doesNotRetainARejectedPayloadInTheValidationSentinel() {
        String invalidPayload =
                "private_measurement,customer=example value=1i".repeat(100_000)
                        + "\nsecond value=2i";
        LineProtocolElementConverter<String> converter =
                new LineProtocolElementConverter<>(
                        ignored -> invalidPayload,
                        false,
                        false,
                        MAX_RECORD_SIZE_IN_BYTES);

        InfluxDB3WriteRequest request = converter.apply("input", null);

        assertThat(request.isValidationFailure()).isTrue();
        assertThat(request.getLineProtocol()).isNull();
        assertThat(request.getValidationFailureMessage()).contains("exactly one");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void appliesValidationPolicyToOversizedUtf8Records(boolean failOnValidation) {
        String lineProtocol = "cpu value=\"é\"";
        long maximumSize = lineProtocol.getBytes(StandardCharsets.UTF_8).length - 1L;
        LineProtocolElementConverter<String> converter =
                new LineProtocolElementConverter<>(
                        ignored -> lineProtocol,
                        failOnValidation,
                        false,
                        maximumSize);

        if (failOnValidation) {
            assertThatThrownBy(() -> converter.apply("input", null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("maximum encoded record size");
        } else {
            assertThat(converter.apply("input", null))
                    .satisfies(
                            request -> {
                                assertThat(request.isValidationFailure()).isTrue();
                                assertThat(request.getLineProtocol()).isNull();
                                assertThat(request.getValidationFailureMessage())
                                        .contains("maximum encoded record size");
                            });
        }
    }

    private static Stream<InvalidLineProtocol> invalidLineProtocols() {
        return Stream.of(
                new InvalidLineProtocol("null output", null, "blank"),
                new InvalidLineProtocol("blank output", " \t", "blank"),
                new InvalidLineProtocol(
                        "newline-delimited output",
                        "cpu value=1i\ncpu value=2i",
                        "exactly one"),
                new InvalidLineProtocol(
                        "carriage-return-delimited output",
                        "cpu value=1i\rcpu value=2i",
                        "exactly one"),
                new InvalidLineProtocol("comment output", "#cpu value=1i", "comment line"),
                new InvalidLineProtocol(
                        "indented comment output", "  #cpu value=1i", "comment line"));
    }

    private record InvalidLineProtocol(
            String description, String lineProtocol, String expectedMessage) {

        @Override
        public String toString() {
            return description;
        }
    }
}
