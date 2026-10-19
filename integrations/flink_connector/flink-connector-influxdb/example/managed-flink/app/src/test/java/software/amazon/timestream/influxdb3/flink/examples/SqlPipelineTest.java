/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.examples;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

class SqlPipelineTest {

    @Test
    void plansAOnePointPerReadingInsertIntoTheInfluxDB3SqlSink() {
        StreamTableEnvironment tableEnvironment = tableEnvironment();
        SqlPipeline.configure(tableEnvironment);
        SqlPipeline.createTables(tableEnvironment, config(Map.of()));

        String plan = tableEnvironment.explainSql(SqlPipeline.insertStatement());

        assertThat(plan)
                .contains("TableSourceScan(table=[[default_catalog, default_database, readings]]")
                .contains("Sink(table=[default_catalog.default_database.cpu]")
                .doesNotContain("Aggregate");
    }

    @Test
    void rejectsAnUpdatingQueryBecauseTheSinkIsInsertOnly() {
        StreamTableEnvironment tableEnvironment = tableEnvironment();
        SqlPipeline.createTables(tableEnvironment, config(Map.of()));

        assertThatThrownBy(
                        () ->
                                tableEnvironment.explainSql(
                                        "INSERT INTO cpu SELECT host, region, AVG(usage), "
                                                + "MAX(ts) FROM readings GROUP BY host, region"))
                .isInstanceOf(TableException.class)
                .hasMessageContaining("doesn't support consuming update");
    }

    @Test
    void writesRuntimePropertiesIntoTheTableOptions() {
        ApplicationConfig config =
                config(Map.of("database", "it's-quoted"));

        assertThat(SqlPipeline.sourceTableStatement(config))
                .contains("'stream.arn' = '" + streamArn() + "'")
                .contains("'aws.region' = 'us-east-1'")
                .contains("'source.init.position' = 'LATEST'")
                .doesNotContain("source.init.timestamp");
        assertThat(SqlPipeline.sinkTableStatement(config))
                .contains("'connector' = 'timestream-influxdb3'")
                .doesNotContain("'measurement'")
                .contains("'database' = 'it''s-quoted'")
                .contains("'token.secret-id' = 'test/influxdb3/token'")
                .contains("'token.secret-json-field' = 'token'")
                .contains("'token.secret-region' = 'us-east-1'")
                .contains("'sink.metrics.managed-flink.enabled' = 'true'")
                .doesNotContain("'token' =");
    }

    @Test
    void passesTheInitialTimestampToTheKinesisSource() {
        ApplicationConfig config =
                ApplicationConfig.from(
                        Map.of(
                                ApplicationConfig.APPLICATION_PROPERTY_GROUP,
                                properties("pipeline", "sql"),
                                ApplicationConfig.KINESIS_PROPERTY_GROUP,
                                properties(
                                        "streamArn", streamArn(),
                                        "region", "us-east-1",
                                        "initialPosition", "AT_TIMESTAMP",
                                        "initialTimestamp", "2026-10-08T19:00:00.000Z"),
                                ApplicationConfig.INFLUXDB3_PROPERTY_GROUP,
                                influxProperties(Map.of())));

        assertThat(SqlPipeline.sourceTableStatement(config))
                .contains("'source.init.position' = 'AT_TIMESTAMP'")
                .contains("'source.init.timestamp' = '2026-10-08T19:00:00.000Z'");
    }

    @Test
    void passesSinkTuningPropertiesToTheSqlSink() {
        ApplicationConfig config =
                ApplicationConfig.from(
                        Map.of(
                                ApplicationConfig.APPLICATION_PROPERTY_GROUP,
                                properties("pipeline", "sql"),
                                ApplicationConfig.KINESIS_PROPERTY_GROUP,
                                properties("streamArn", streamArn(), "region", "us-east-1"),
                                ApplicationConfig.INFLUXDB3_PROPERTY_GROUP,
                                influxProperties(Map.of()),
                                ApplicationConfig.SINK_PROPERTY_GROUP,
                                properties(
                                        "sink.batch.max-size", "50000",
                                        "sink.requests.max-buffered", "100000",
                                        "sink.flush-buffer.timeout", "1000")));

        assertThat(SqlPipeline.sinkTableStatement(config))
                .contains("'sink.batch.max-size' = '50000'")
                .contains("'sink.requests.max-buffered' = '100000'")
                .contains("'sink.flush-buffer.timeout' = '1000'")
                .doesNotContain("sink.requests.max-inflight");

        StreamTableEnvironment tableEnvironment = tableEnvironment();
        SqlPipeline.createTables(tableEnvironment, config);
        assertThat(tableEnvironment.explainSql(SqlPipeline.insertStatement()))
                .contains("Sink(table=[default_catalog.default_database.cpu]");
    }

    private static StreamTableEnvironment tableEnvironment() {
        return StreamTableEnvironment.create(
                StreamExecutionEnvironment.getExecutionEnvironment());
    }

    private static ApplicationConfig config(Map<String, String> influxOverrides) {
        return ApplicationConfig.from(
                Map.of(
                        ApplicationConfig.APPLICATION_PROPERTY_GROUP,
                        properties("pipeline", "sql"),
                        ApplicationConfig.KINESIS_PROPERTY_GROUP,
                        properties("streamArn", streamArn(), "region", "us-east-1"),
                        ApplicationConfig.INFLUXDB3_PROPERTY_GROUP,
                        influxProperties(influxOverrides)));
    }

    private static Properties influxProperties(Map<String, String> overrides) {
        Properties properties =
                properties(
                        "endpoint", "https://example.test:8181",
                        "database", "sensors",
                        "secretId", "test/influxdb3/token",
                        "secretJsonField", "token");
        properties.putAll(overrides);
        return properties;
    }

    private static String streamArn() {
        return "arn:aws:kinesis:us-east-1:123456789012:stream/influxdb3-example-input";
    }

    private static Properties properties(String... values) {
        Properties properties = new Properties();
        for (int index = 0; index < values.length; index += 2) {
            properties.setProperty(values[index], values[index + 1]);
        }
        return properties;
    }
}
