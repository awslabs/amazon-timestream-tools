/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.examples;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

class LineProtocolGeneratorTest {

    private static final long START = 1_760_000_000_000_000_000L;

    @Test
    void generatesDeterministicLineProtocol() {
        LineProtocolGenerator generator = generator(1_000, 100, 2);

        assertThat(generator.map(42L))
                .isEqualTo("loadtest,run=r1,host=host-42 f0=13.02,f1=92.21 1760000000042000000");
        assertThat(generator.map(42L)).isEqualTo(generator.map(42L));
    }

    @Test
    void padsFieldValuesToTwoDecimalPlaces() {
        LineProtocolGenerator generator = generator(1_000, 100, 1);

        assertThat(generator.map(0L)).contains(" f0=0.00 ");
    }

    @Test
    void cyclesTheHostTagThroughTheSeriesCardinality() {
        LineProtocolGenerator generator = generator(1_000, 100, 1);

        assertThat(generator.map(142L)).startsWith("loadtest,run=r1,host=host-42 ");
        assertThat(generator.map(99L)).startsWith("loadtest,run=r1,host=host-99 ");
        assertThat(generator.map(100L)).startsWith("loadtest,run=r1,host=host-0 ");
    }

    @Test
    void spacesTimestampsByTheConfiguredRate() {
        assertThat(timestamp(generator(1_000, 10, 1).map(1L))).isEqualTo(START + 1_000_000L);
        assertThat(timestamp(generator(3, 10, 1).map(1L))).isEqualTo(START + 333_333_333L);
        assertThat(timestamp(generator(2_000_000_000L, 10, 1).map(1L))).isEqualTo(START + 1L);
        assertThat(timestamp(generator(0, 10, 1).map(1L)))
                .isEqualTo(START + LineProtocolGenerator.UNLIMITED_RATE_SPACING_NANOS);
    }

    @Test
    void givesEveryRowOfASeriesAUniqueTimestamp() {
        LineProtocolGenerator generator = generator(0, 7, 3);
        Set<String> seriesAndTimestamps = new HashSet<>();

        for (long sequence = 0; sequence < 10_000; sequence++) {
            String line = generator.map(sequence);
            String series = line.substring(0, line.indexOf(' '));
            assertThat(seriesAndTimestamps.add(series + "@" + timestamp(line))).isTrue();
        }
    }

    private static LineProtocolGenerator generator(
            long ratePerSecond, int seriesCardinality, int fieldsPerLine) {
        return new LineProtocolGenerator(
                new DataGenConfig(
                        ratePerSecond,
                        Long.MAX_VALUE,
                        seriesCardinality,
                        fieldsPerLine,
                        "loadtest",
                        "r1"),
                START);
    }

    private static long timestamp(String line) {
        return Long.parseLong(line.substring(line.lastIndexOf(' ') + 1));
    }
}
