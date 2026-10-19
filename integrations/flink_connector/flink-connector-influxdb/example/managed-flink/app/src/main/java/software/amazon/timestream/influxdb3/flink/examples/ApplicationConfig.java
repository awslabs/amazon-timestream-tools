/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.examples;

import org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.InitialPosition;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

record ApplicationConfig(
        Pipeline pipeline,
        String streamArn,
        String kinesisRegion,
        InitialPosition initialPosition,
        String initialTimestamp,
        String endpoint,
        String database,
        String secretId,
        String secretJsonField,
        String secretRegion,
        Map<String, String> sinkOptions,
        DataGenConfig dataGen) {

    static final String APPLICATION_PROPERTY_GROUP = "Application";
    static final String KINESIS_PROPERTY_GROUP = "KinesisSource";
    static final String INFLUXDB3_PROPERTY_GROUP = "InfluxDB3";
    static final String SINK_PROPERTY_GROUP = "Sink";
    static final String DATAGEN_PROPERTY_GROUP = "DataGen";

    /**
     * Optional sink tuning keys accepted in the {@value #SINK_PROPERTY_GROUP} group. They use the
     * SQL sink option names; unset keys keep the connector defaults.
     */
    static final List<String> SINK_OPTION_KEYS =
            List.of(
                    "sink.batch.max-size",
                    "sink.requests.max-buffered",
                    "sink.requests.max-inflight",
                    "sink.flush-buffer.size",
                    "sink.flush-buffer.timeout",
                    "sink.max-retries");

    /** Pipeline run by the application. */
    enum Pipeline {
        /** Forwards line protocol records through the DataStream API sink. */
        DATASTREAM,
        /** Writes JSON readings through the Flink SQL sink. */
        SQL,
        /** Generates synthetic line protocol in the job, without Kinesis, for load tests. */
        DATAGEN
    }

    static ApplicationConfig from(Map<String, Properties> propertyGroups) {
        Pipeline pipeline = parsePipeline(propertyGroups);
        // The DATAGEN pipeline has no Kinesis source; the other pipelines require one.
        Properties kinesis =
                pipeline == Pipeline.DATAGEN
                        ? null
                        : requireGroup(propertyGroups, KINESIS_PROPERTY_GROUP);
        Properties influx = requireGroup(propertyGroups, INFLUXDB3_PROPERTY_GROUP);

        String streamArn = null;
        String kinesisRegion = null;
        InitialPosition initialPosition = null;
        String initialTimestamp = null;
        if (kinesis != null) {
            streamArn = requireProperty(kinesis, KINESIS_PROPERTY_GROUP, "streamArn");
            kinesisRegion = requireProperty(kinesis, KINESIS_PROPERTY_GROUP, "region");
            initialPosition = parseInitialPosition(optionalProperty(kinesis, "initialPosition"));
            initialTimestamp = optionalProperty(kinesis, "initialTimestamp");
            if (initialPosition == InitialPosition.AT_TIMESTAMP && initialTimestamp == null) {
                throw new IllegalArgumentException(
                        "Property 'initialTimestamp' is required in runtime property group '"
                                + KINESIS_PROPERTY_GROUP
                                + "' when initialPosition is AT_TIMESTAMP");
            }
        }

        // Without a Kinesis region, an unset secret region uses the AWS SDK region chain.
        String secretRegion = optionalProperty(influx, "region");
        if (secretRegion == null) {
            secretRegion = kinesisRegion;
        }

        if (optionalProperty(influx, "token") != null) {
            throw new IllegalArgumentException(
                    "Property 'token' is not supported in runtime property group '"
                            + INFLUXDB3_PROPERTY_GROUP
                            + "'; store the token in Secrets Manager and configure 'secretId'");
        }

        return new ApplicationConfig(
                pipeline,
                streamArn,
                kinesisRegion,
                initialPosition,
                initialTimestamp,
                requireProperty(influx, INFLUXDB3_PROPERTY_GROUP, "endpoint"),
                requireProperty(influx, INFLUXDB3_PROPERTY_GROUP, "database"),
                requireProperty(influx, INFLUXDB3_PROPERTY_GROUP, "secretId"),
                optionalProperty(influx, "secretJsonField"),
                secretRegion,
                parseSinkOptions(propertyGroups.get(SINK_PROPERTY_GROUP)),
                pipeline == Pipeline.DATAGEN
                        ? DataGenConfig.from(propertyGroups.get(DATAGEN_PROPERTY_GROUP))
                        : null);
    }

    /** Returns the configured value of an integer sink option, or {@code null} when unset. */
    Integer sinkIntOption(String key) {
        return sinkNumberOption(key, Integer::valueOf);
    }

    /** Returns the configured value of a long sink option, or {@code null} when unset. */
    Long sinkLongOption(String key) {
        return sinkNumberOption(key, Long::valueOf);
    }

    private <T> T sinkNumberOption(String key, Function<String, T> parser) {
        String value = sinkOptions.get(key);
        if (value == null) {
            return null;
        }
        try {
            return parser.apply(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Property '"
                            + key
                            + "' in runtime property group '"
                            + SINK_PROPERTY_GROUP
                            + "' must be an integer, but was '"
                            + value
                            + "'",
                    e);
        }
    }

    private static Map<String, String> parseSinkOptions(Properties sink) {
        if (sink == null) {
            return Map.of();
        }
        Map<String, String> options = new LinkedHashMap<>();
        for (String key : sink.stringPropertyNames()) {
            if (!SINK_OPTION_KEYS.contains(key)) {
                throw new IllegalArgumentException(
                        "Unsupported property '"
                                + key
                                + "' in runtime property group '"
                                + SINK_PROPERTY_GROUP
                                + "'; supported properties are "
                                + SINK_OPTION_KEYS);
            }
        }
        for (String key : SINK_OPTION_KEYS) {
            String value = optionalProperty(sink, key);
            if (value != null) {
                options.put(key, value);
            }
        }
        return Collections.unmodifiableMap(options);
    }

    private static Pipeline parsePipeline(Map<String, Properties> propertyGroups) {
        Properties application =
                propertyGroups == null ? null : propertyGroups.get(APPLICATION_PROPERTY_GROUP);
        String value = application == null ? null : optionalProperty(application, "pipeline");
        if (value == null) {
            return Pipeline.DATASTREAM;
        }
        try {
            return Pipeline.valueOf(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Property 'pipeline' in runtime property group '"
                            + APPLICATION_PROPERTY_GROUP
                            + "' must be DATASTREAM, SQL, or DATAGEN",
                    e);
        }
    }

    private static Properties requireGroup(
            Map<String, Properties> propertyGroups, String groupName) {
        if (propertyGroups == null) {
            throw new IllegalArgumentException("Runtime property groups must not be null");
        }
        Properties group = propertyGroups.get(groupName);
        if (group == null) {
            throw new IllegalArgumentException(
                    "Missing runtime property group '" + groupName + "'");
        }
        return group;
    }

    private static String requireProperty(
            Properties properties, String groupName, String propertyName) {
        String value = optionalProperty(properties, propertyName);
        if (value == null) {
            throw new IllegalArgumentException(
                    "Missing required property '"
                            + propertyName
                            + "' in runtime property group '"
                            + groupName
                            + "'");
        }
        return value;
    }

    static String optionalProperty(Properties properties, String propertyName) {
        String value = properties.getProperty(propertyName);
        if (value == null || value.isBlank()) {
            return null;
        }
        return value.trim();
    }

    private static InitialPosition parseInitialPosition(String value) {
        if (value == null) {
            return InitialPosition.LATEST;
        }
        try {
            return InitialPosition.valueOf(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Property 'initialPosition' in runtime property group '"
                            + KINESIS_PROPERTY_GROUP
                            + "' must be LATEST, TRIM_HORIZON, or AT_TIMESTAMP",
                    e);
        }
    }
}
