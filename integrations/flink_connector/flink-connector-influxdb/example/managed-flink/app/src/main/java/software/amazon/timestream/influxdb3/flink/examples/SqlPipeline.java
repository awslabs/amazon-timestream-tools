/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.examples;

import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * Flink SQL pipeline that writes each JSON CPU reading from Kinesis as one InfluxDB 3 point
 * through the {@code timestream-influxdb3} SQL sink.
 *
 * <p>Each Kinesis record must contain one JSON reading, for example:
 *
 * <pre>{@code
 * {"host":"web-1","region":"us-east-1","usage":41.7,"ts":"2026-10-08T19:00:04.120Z"}
 * }</pre>
 *
 * <p>Points are written to the {@code cpu} measurement, named after the sink table.
 */
final class SqlPipeline {

    static final String JOB_NAME = "Kinesis to Timestream for InfluxDB 3 (SQL)";

    private SqlPipeline() {}

    /** Creates the tables and submits the {@code INSERT} job without waiting for it to finish. */
    static TableResult execute(StreamTableEnvironment tableEnvironment, ApplicationConfig config) {
        configure(tableEnvironment);
        createTables(tableEnvironment, config);
        return tableEnvironment.executeSql(insertStatement());
    }

    static void configure(StreamTableEnvironment tableEnvironment) {
        tableEnvironment.getConfig().set(PipelineOptions.NAME, JOB_NAME);
    }

    static void createTables(StreamTableEnvironment tableEnvironment, ApplicationConfig config) {
        tableEnvironment.executeSql(sourceTableStatement(config));
        tableEnvironment.executeSql(sinkTableStatement(config));
    }

    static String sourceTableStatement(ApplicationConfig config) {
        List<String> options = new ArrayList<>();
        options.add(option("connector", "kinesis"));
        options.add(option("stream.arn", config.streamArn()));
        options.add(option("aws.region", config.kinesisRegion()));
        options.add(option("source.init.position", config.initialPosition().name()));
        if (config.initialTimestamp() != null) {
            options.add(option("source.init.timestamp", config.initialTimestamp()));
        }
        options.add(option("format", "json"));
        options.add(option("json.timestamp-format.standard", "ISO-8601"));

        return "CREATE TABLE readings (\n"
                + "    host STRING,\n"
                + "    region STRING,\n"
                + "    usage DOUBLE,\n"
                + "    ts TIMESTAMP_LTZ(3)\n"
                + ") WITH (\n"
                + String.join(",\n", options)
                + "\n)";
    }

    static String sinkTableStatement(ApplicationConfig config) {
        List<String> options = new ArrayList<>();
        options.add(option("connector", "timestream-influxdb3"));
        options.add(option("endpoint", config.endpoint()));
        options.add(option("database", config.database()));
        options.add(option("tag-columns", "host,region"));
        options.add(option("timestamp-column", "time"));
        options.add(option("precision", "ms"));
        options.add(option("token.secret-id", config.secretId()));
        if (config.secretJsonField() != null) {
            options.add(option("token.secret-json-field", config.secretJsonField()));
        }
        options.add(option("token.secret-region", config.secretRegion()));
        options.add(option("sink.metrics.managed-flink.enabled", "true"));
        config.sinkOptions().forEach((key, value) -> options.add(option(key, value)));

        return "CREATE TABLE cpu (\n"
                + "    host STRING,\n"
                + "    region STRING,\n"
                + "    usage DOUBLE,\n"
                + "    `time` TIMESTAMP_LTZ(3)\n"
                + ") WITH (\n"
                + String.join(",\n", options)
                + "\n)";
    }

    /** Returns the job's {@code INSERT}, which writes one point per reading. */
    static String insertStatement() {
        return "INSERT INTO cpu SELECT host, region, usage, ts FROM readings";
    }

    private static String option(String key, String value) {
        return "    " + literal(key) + " = " + literal(value);
    }

    private static String literal(String value) {
        return "'" + value.replace("'", "''") + "'";
    }
}
