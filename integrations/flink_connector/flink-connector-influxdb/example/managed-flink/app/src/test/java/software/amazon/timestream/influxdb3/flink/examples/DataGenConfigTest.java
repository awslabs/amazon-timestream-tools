/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.examples;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.api.Test;

import java.util.Properties;

class DataGenConfigTest {

    @Test
    void appliesDefaultsWhenTheGroupIsMissing() {
        assertThat(DataGenConfig.from(null))
                .isEqualTo(
                        new DataGenConfig(
                                0, Long.MAX_VALUE, 1_000, 4, "loadtest", "default"));
    }

    @Test
    void parsesEveryProperty() {
        Properties properties = new Properties();
        properties.setProperty("ratePerSecond", "5000");
        properties.setProperty("totalRecords", "1000000");
        properties.setProperty("seriesCardinality", "20");
        properties.setProperty("fieldsPerLine", " 8 ");
        properties.setProperty("measurement", "cpu_load");
        properties.setProperty("runId", "p4-b10k");

        assertThat(DataGenConfig.from(properties))
                .isEqualTo(new DataGenConfig(5_000, 1_000_000, 20, 8, "cpu_load", "p4-b10k"));
    }

    @ParameterizedTest(name = "{0}={1}")
    @CsvSource({
        "ratePerSecond, -1, must not be negative",
        "ratePerSecond, fast, must be an integer, but was 'fast'",
        "totalRecords, 0, must be positive",
        "seriesCardinality, 0, must be between 1 and 10000000",
        "seriesCardinality, 10000001, must be between 1 and 10000000",
        "fieldsPerLine, 101, must be between 1 and 100",
        "measurement, cpu load, must contain 1 to 64 letters",
        "runId, 'r1,host=x', must contain 1 to 64 letters",
        "rate, 10, Unsupported property 'rate'"
    })
    void rejectsInvalidProperties(String key, String value, String message) {
        Properties properties = new Properties();
        properties.setProperty(key, value);

        assertThatIllegalArgumentException()
                .isThrownBy(() -> DataGenConfig.from(properties))
                .withMessageContaining(message)
                .withMessageContaining("'DataGen'");
    }
}
