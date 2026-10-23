/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.internal;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FailureLogUtilsTest {

    @Test
    void escapesControlCharactersWithoutRedactingValues() {
        assertThat(
                        FailureLogUtils.escapeAndTruncateForLog(
                                "token=secret\nfield=\"value\"\r\t\u0000\u2028\u2029"))
                .isEqualTo(
                        "token=secret\\u000afield=\"value\"\\u000d\\u0009\\u0000"
                                + "\\u2028\\u2029");
        assertThat(FailureLogUtils.escapeAndTruncateForLog(null)).isEqualTo("<null>");
    }

    @Test
    void truncatesLongValuesAtTheDocumentedLimit() {
        String value = "x".repeat(FailureLogUtils.MAX_LOGGED_CHARACTERS + 100);

        assertThat(FailureLogUtils.escapeAndTruncateForLog(value))
                .isEqualTo(
                        "x".repeat(FailureLogUtils.MAX_LOGGED_CHARACTERS - 3) + "...");
        assertThat(
                        FailureLogUtils.escapeAndTruncateForLog(
                                "x".repeat(FailureLogUtils.MAX_LOGGED_CHARACTERS)))
                .hasSize(FailureLogUtils.MAX_LOGGED_CHARACTERS)
                .doesNotEndWith("...");
    }

    @Test
    void keepsEscapedControlCharacterOutputWithinTheLimit() {
        String escaped =
                FailureLogUtils.escapeAndTruncateForLog(
                        "\n".repeat(FailureLogUtils.MAX_LOGGED_CHARACTERS));

        assertThat(escaped)
                .hasSizeLessThanOrEqualTo(FailureLogUtils.MAX_LOGGED_CHARACTERS)
                .doesNotContain("\n")
                .endsWith("...");
    }
}
