/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.examples;

import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Settings of the DATAGEN pipeline, read from the optional {@code DataGen} runtime property
 * group. Every property has a default.
 */
record DataGenConfig(
        long ratePerSecond,
        long totalRecords,
        int seriesCardinality,
        int fieldsPerLine,
        String measurement,
        String runId) {

    static final long DEFAULT_RATE_PER_SECOND = 0;
    static final long DEFAULT_TOTAL_RECORDS = Long.MAX_VALUE;
    static final int DEFAULT_SERIES_CARDINALITY = 1_000;
    static final int DEFAULT_FIELDS_PER_LINE = 4;
    static final String DEFAULT_MEASUREMENT = "loadtest";
    static final String DEFAULT_RUN_ID = "default";

    static final int MAX_SERIES_CARDINALITY = 10_000_000;
    static final int MAX_FIELDS_PER_LINE = 100;

    static final List<String> PROPERTY_KEYS =
            List.of(
                    "ratePerSecond",
                    "totalRecords",
                    "seriesCardinality",
                    "fieldsPerLine",
                    "measurement",
                    "runId");

    private static final Pattern NAME_PATTERN = Pattern.compile("[A-Za-z0-9_-]{1,64}");

    static DataGenConfig from(Properties properties) {
        Properties group = properties == null ? new Properties() : properties;
        for (String key : group.stringPropertyNames()) {
            if (!PROPERTY_KEYS.contains(key)) {
                throw new IllegalArgumentException(
                        "Unsupported property '"
                                + key
                                + "' in runtime property group '"
                                + ApplicationConfig.DATAGEN_PROPERTY_GROUP
                                + "'; supported properties are "
                                + PROPERTY_KEYS);
            }
        }

        long ratePerSecond = longProperty(group, "ratePerSecond", DEFAULT_RATE_PER_SECOND);
        long totalRecords = longProperty(group, "totalRecords", DEFAULT_TOTAL_RECORDS);
        long seriesCardinality =
                longProperty(group, "seriesCardinality", DEFAULT_SERIES_CARDINALITY);
        long fieldsPerLine = longProperty(group, "fieldsPerLine", DEFAULT_FIELDS_PER_LINE);
        String measurement = nameProperty(group, "measurement", DEFAULT_MEASUREMENT);
        String runId = nameProperty(group, "runId", DEFAULT_RUN_ID);

        require(ratePerSecond >= 0, "ratePerSecond", "must not be negative; 0 is unlimited");
        require(totalRecords > 0, "totalRecords", "must be positive");
        require(
                seriesCardinality >= 1 && seriesCardinality <= MAX_SERIES_CARDINALITY,
                "seriesCardinality",
                "must be between 1 and " + MAX_SERIES_CARDINALITY);
        require(
                fieldsPerLine >= 1 && fieldsPerLine <= MAX_FIELDS_PER_LINE,
                "fieldsPerLine",
                "must be between 1 and " + MAX_FIELDS_PER_LINE);

        return new DataGenConfig(
                ratePerSecond,
                totalRecords,
                (int) seriesCardinality,
                (int) fieldsPerLine,
                measurement,
                runId);
    }

    private static long longProperty(Properties group, String key, long defaultValue) {
        String value = ApplicationConfig.optionalProperty(group, key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw invalid(key, "must be an integer, but was '" + value + "'", e);
        }
    }

    private static String nameProperty(Properties group, String key, String defaultValue) {
        String value = ApplicationConfig.optionalProperty(group, key);
        if (value == null) {
            return defaultValue;
        }
        require(
                NAME_PATTERN.matcher(value).matches(),
                key,
                "must contain 1 to 64 letters, digits, hyphens, or underscores");
        return value;
    }

    private static void require(boolean condition, String key, String message) {
        if (!condition) {
            throw invalid(key, message, null);
        }
    }

    private static IllegalArgumentException invalid(
            String key, String message, Exception cause) {
        return new IllegalArgumentException(
                "Property '"
                        + key
                        + "' in runtime property group '"
                        + ApplicationConfig.DATAGEN_PROPERTY_GROUP
                        + "' "
                        + message,
                cause);
    }
}
