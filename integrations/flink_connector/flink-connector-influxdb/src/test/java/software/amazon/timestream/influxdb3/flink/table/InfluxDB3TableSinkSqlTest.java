/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.table;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import software.amazon.timestream.influxdb3.flink.FailOnceAfterCheckpointMap;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InfluxDB3TableSinkSqlTest {
    private WireMockServer server;
    private TableEnvironment tableEnvironment;

    @BeforeEach
    void setUp() {
        server = new WireMockServer(wireMockConfig().dynamicPort());
        server.start();
        server.stubFor(
                post(urlPathEqualTo("/api/v3/write_lp")).willReturn(aResponse().withStatus(204)));

        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        tableEnvironment =
                TableEnvironment.create(
                        EnvironmentSettings.newInstance()
                                .inStreamingMode()
                                .withConfiguration(configuration)
                                .build());
        tableEnvironment.getConfig().set("parallelism.default", "1");
    }

    @AfterEach
    void stopServer() {
        server.stop();
    }

    @Test
    void writesRowsFromInsertInto() throws Exception {
        createSinkTable("'tag-columns' = 'host,region'");

        tableEnvironment
                .executeSql(
                        "INSERT INTO cpu VALUES "
                                + "('web 1', 'us-east-1', 0.5, 3, TRUE, 'ok', "
                                + "TIMESTAMP '2025-01-01 00:00:00.001'), "
                                + "('web-2', CAST(NULL AS STRING), CAST(NULL AS DOUBLE), 4, "
                                + "FALSE, 'say \"hi\"', TIMESTAMP '2025-01-01 00:00:00.002')")
                .await();

        server.verify(
                postRequestedFor(urlPathEqualTo("/api/v3/write_lp"))
                        .withQueryParam("db", equalTo("metrics"))
                        .withQueryParam("precision", equalTo("millisecond"))
                        .withHeader("Authorization", equalTo("Bearer test-token")));
        assertThat(writtenLines())
                .containsExactly(
                        "cpu,host=web\\ 1,region=us-east-1 usage=0.5,cores=3i,healthy=true,"
                                + "status=\"ok\" 1735689600001",
                        "cpu,host=web-2 cores=4i,healthy=false,status=\"say \\\"hi\\\"\" "
                                + "1735689600002");
    }

    @Test
    void failsTheJobOnValidationFailuresByDefault() {
        createSinkTable("'tag-columns' = 'host'");

        assertThatThrownBy(
                        () ->
                                tableEnvironment
                                        .executeSql(
                                                "INSERT INTO cpu VALUES ('a', 'r', 1.0, 1, TRUE,"
                                                        + " 'ok', CAST(NULL AS TIMESTAMP(3)))")
                                        .await())
                .hasStackTraceContaining("Timestamp column 'ts' is null");
        assertThat(writtenLines()).isEmpty();
    }

    @Test
    void dropsInvalidRowsWhenValidationFailuresAreNotFatal() throws Exception {
        createSinkTable("'tag-columns' = 'host', 'sink.fail-on-validation' = 'false'");

        tableEnvironment
                .executeSql(
                        "INSERT INTO cpu VALUES "
                                + "('a', 'r', 1.0, 1, TRUE, 'ok', CAST(NULL AS TIMESTAMP(3))), "
                                + "('b', CAST(NULL AS STRING), CAST(NULL AS DOUBLE), "
                                + "CAST(NULL AS INT), "
                                + "CAST(NULL AS BOOLEAN), CAST(NULL AS STRING), "
                                + "TIMESTAMP '2025-01-01 00:00:00.003'), "
                                + "('c', 'r', 2.0, 2, TRUE, 'ok', "
                                + "TIMESTAMP '2025-01-01 00:00:00.004')")
                .await();

        assertThat(writtenLines())
                .containsExactly(
                        "cpu,host=c region=\"r\",usage=2.0,cores=2i,healthy=true,status=\"ok\" "
                                + "1735689600004");
    }

    @Test
    void rejectsUpdatingQueriesBecauseTheSinkIsInsertOnly() {
        createSinkTable("'tag-columns' = 'host'");
        tableEnvironment.executeSql(
                "CREATE TEMPORARY VIEW readings AS SELECT * FROM (VALUES "
                        + "('a', 1.0), ('a', 2.0)) AS t(host, usage)");

        assertThatThrownBy(
                        () ->
                                tableEnvironment.executeSql(
                                        "INSERT INTO cpu SELECT host, 'r', SUM(usage), 1, TRUE, "
                                                + "'ok', TIMESTAMP '2025-01-01 00:00:00' "
                                                + "FROM readings GROUP BY host"))
                .hasStackTraceContaining("doesn't support consuming update changes");
    }

    @Test
    void reportsInvalidTablesWhenPlanningAnInsert() {
        createSinkTable("'tag-columns' = 'missing'");

        assertThatThrownBy(
                        () ->
                                tableEnvironment.executeSql(
                                        "INSERT INTO cpu VALUES ('a', 'r', 1.0, 1, TRUE, 'ok', "
                                                + "TIMESTAMP '2025-01-01 00:00:00')"))
                .hasStackTraceContaining("Tag column 'missing' does not exist in the table.");
    }

    @Test
    void restoresCheckpointedRowsAfterAFailureWithoutLosingThem(@TempDir Path temporaryDirectory)
            throws Exception {
        Path failureMarker = temporaryDirectory.resolve("sql-failure.marker");
        int recordCount = 12;
        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMillis(10));
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.createLocalEnvironment(1, configuration);
        environment.enableCheckpointing(50);
        environment.getCheckpointConfig().setCheckpointTimeout(10_000);
        environment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        StreamTableEnvironment streamTables = StreamTableEnvironment.create(environment);
        streamTables.createTemporaryView(
                "ids",
                environment
                        .fromSource(
                                new DataGeneratorSource<>(
                                        index -> index,
                                        recordCount,
                                        RateLimiterStrategy.perSecond(20),
                                        Types.LONG),
                                WatermarkStrategy.noWatermarks(),
                                "sql-checkpointing-number-source")
                        .map(
                                new FailOnceAfterCheckpointMap<Long>(
                                        failureMarker.toString(), record -> record),
                                Types.LONG));
        // Keep rows buffered across the checkpoint so recovery restores sink state.
        createSinkTable(
                streamTables,
                "'tag-columns' = 'host', 'sink.batch.max-size' = '10', "
                        + "'sink.requests.max-buffered' = '20', "
                        + "'sink.flush-buffer.timeout' = '60000'");

        streamTables
                .executeSql(
                        "INSERT INTO cpu SELECT 'h', 'r', CAST(f0 AS DOUBLE), CAST(f0 AS INT), "
                                + "TRUE, 'ok', TIMESTAMPADD(SECOND, CAST(f0 AS INT), "
                                + "TIMESTAMP '2025-01-01 00:00:00') FROM ids")
                .await();

        List<Integer> writtenIds =
                writtenLines().stream()
                        .map(line -> line.substring(line.indexOf("cores=") + 6))
                        .map(value -> Integer.parseInt(value.substring(0, value.indexOf('i'))))
                        .collect(Collectors.toList());
        assertThat(Files.exists(failureMarker))
                .as("the pipeline failed once after a completed checkpoint")
                .isTrue();
        assertThat(writtenIds)
                .as("at-least-once recovery may duplicate rows but must not lose them")
                .containsAll(IntStream.range(0, recordCount).boxed().toList());
    }

    private void createSinkTable(String extraOptions) {
        createSinkTable(tableEnvironment, "'sink.flush-buffer.timeout' = '50', " + extraOptions);
    }

    private void createSinkTable(TableEnvironment environment, String extraOptions) {
        environment.executeSql(
                "CREATE TABLE cpu ("
                        + " host STRING,"
                        + " region STRING,"
                        + " usage DOUBLE,"
                        + " cores INT,"
                        + " healthy BOOLEAN,"
                        + " status STRING,"
                        + " ts TIMESTAMP(3)"
                        + ") WITH ("
                        + " 'connector' = 'timestream-influxdb3',"
                        + " 'endpoint' = '"
                        + server.baseUrl()
                        + "',"
                        + " 'database' = 'metrics',"
                        + " 'timestamp-column' = 'ts',"
                        + " 'precision' = 'ms',"
                        + " 'token' = 'test-token',"
                        + " 'gzip.enabled' = 'false',"
                        + extraOptions
                        + ")");
    }

    private List<String> writtenLines() {
        return server.findAll(postRequestedFor(urlPathEqualTo("/api/v3/write_lp"))).stream()
                .map(LoggedRequest::getBody)
                .map(body -> new String(body, StandardCharsets.UTF_8))
                .flatMap(String::lines)
                .collect(Collectors.toList());
    }
}
