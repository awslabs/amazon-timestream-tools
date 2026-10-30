/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.examples;

import org.apache.flink.connector.datagen.source.GeneratorFunction;

/**
 * Generates one line protocol row per sequence number for the DATAGEN pipeline.
 *
 * <p>Rows look like {@code loadtest,run=r1,host=host-0042 f0=12.34,f1=56.78 1760385600000001000}.
 * The {@code host} tag cycles through {@code seriesCardinality} values. Field values and
 * timestamps derive only from the sequence number, so a row regenerated after a job restart is
 * identical and overwrites the earlier write instead of adding a point.
 *
 * <p>Timestamps start at {@code startEpochNanos} and advance by a fixed spacing per sequence
 * number, so every row has a unique timestamp across all subtasks. At a configured rate the
 * spacing is one second divided by the rate, which keeps timestamps near wall-clock time.
 */
final class LineProtocolGenerator implements GeneratorFunction<Long, String> {
    private static final long serialVersionUID = 1L;

    /** Spacing used when the rate is unlimited, equivalent to one million rows per second. */
    static final long UNLIMITED_RATE_SPACING_NANOS = 1_000L;

    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    private final String seriesPrefix;
    private final int seriesCardinality;
    private final int fieldsPerLine;
    private final long startEpochNanos;
    private final long spacingNanos;

    LineProtocolGenerator(DataGenConfig config, long startEpochNanos) {
        this.seriesPrefix = config.measurement() + ",run=" + config.runId() + ",host=host-";
        this.seriesCardinality = config.seriesCardinality();
        this.fieldsPerLine = config.fieldsPerLine();
        this.startEpochNanos = startEpochNanos;
        this.spacingNanos =
                config.ratePerSecond() > 0
                        ? Math.max(1L, NANOS_PER_SECOND / config.ratePerSecond())
                        : UNLIMITED_RATE_SPACING_NANOS;
    }

    @Override
    public String map(Long sequence) {
        long value = sequence;
        StringBuilder line = new StringBuilder(seriesPrefix.length() + 24 + fieldsPerLine * 12);
        line.append(seriesPrefix).append(value % seriesCardinality).append(' ');
        for (int field = 0; field < fieldsPerLine; field++) {
            if (field > 0) {
                line.append(',');
            }
            // Deterministic value from 0.00 to 99.99, varied per field.
            long hundredths = Math.floorMod(value * 31 + field * 7919L, 10_000L);
            line.append('f')
                    .append(field)
                    .append('=')
                    .append(hundredths / 100)
                    .append('.')
                    .append(hundredths % 100 < 10 ? "0" : "")
                    .append(hundredths % 100);
        }
        return line.append(' ').append(startEpochNanos + value * spacingNanos).toString();
    }
}
