/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink;

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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.influxdb.v3.client.write.WritePrecision;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import software.amazon.timestream.influxdb3.flink.auth.TokenProvider;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Tag("container")
@Testcontainers
class InfluxDB3SinkContainerIT {

    private static final String DEFAULT_IMAGE = "influxdb:3.11.2-core";
    private static final String IMAGE_PROPERTY = "influxdb3.test.image";
    private static final String ADMIN_TOKEN_FILE = "/tmp/flink-e2e-admin-token.json";
    private static final String ADMIN_TOKEN_JSON_ENV = "FLINK_E2E_ADMIN_TOKEN_JSON";
    private static final String MEASUREMENT = "flink_sink_e2e";
    private static final String PARTIAL_WRITE_MEASUREMENT = "flink_sink_partial_write";
    private static final String NO_SYNC_MEASUREMENT = "flink_sink_no_sync";
    private static final String RATE_LIMIT_MEASUREMENT = "flink_sink_rate_limit";
    private static final String AUTH_ROTATION_MEASUREMENT = "flink_sink_auth_rotation";
    private static final String CHECKPOINT_MEASUREMENT = "flink_sink_checkpoint";
    private static final String SQL_MEASUREMENT = "flink sql,e2e";
    private static final String SQL_CHECKPOINT_MEASUREMENT = "flink_sql_checkpoint";
    private static final long SQL_CHECKPOINT_TIMESTAMP_BASE = 1_735_689_600_500_000_000L;
    // InfluxDB Core allows five databases; tests isolate records by measurement and run ID.
    private static final String DATABASE = "flink_e2e_" + uniqueIdentifier();
    private static final int INFLUXDB_PORT = 8181;
    private static final long NANOS_PER_SECOND = 1_000_000_000L;
    private static final long CHECKPOINT_TIMESTAMP_BASE = 1_735_689_600_400_000_000L;
    private static final Duration API_READINESS_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration QUERY_VISIBILITY_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(250);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final HttpClient HTTP_CLIENT =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private static final String ADMIN_TOKEN = generateAdminToken();

    @Container
    private static final GenericContainer<?> INFLUXDB = createInfluxDbContainer();

    @TempDir Path temporaryDirectory;

    @BeforeAll
    static void createTestDatabase() throws InterruptedException {
        createDatabase(influxDbEndpoint(), DATABASE);
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void writesFullBatchAndFinalUnderfilledBatchToInfluxDb3() throws Exception {
        URI endpoint = influxDbEndpoint();
        String runId = uniqueIdentifier();

        List<ExpectedRow> expectedRows =
                List.of(
                        new ExpectedRow("sensor-a", 21.5, 1, true, 1_735_689_600_000_000_001L),
                        new ExpectedRow("sensor-b", 22.25, 2, false, 1_735_689_600_000_000_002L),
                        new ExpectedRow("sensor-c", 23.75, 3, true, 1_735_689_600_000_000_003L));
        List<String> lineProtocolRows =
                expectedRows.stream().map(row -> row.toLineProtocol(runId)).toList();

        InfluxDB3Sink<String> sink =
                InfluxDB3Sink.<String>builder()
                        .setEndpoint(endpoint)
                        .setDatabase(DATABASE)
                        .setToken(ADMIN_TOKEN)
                        .setConverter(value -> value)
                        .setWritePrecision(WritePrecision.NS)
                        .setMaxBatchSize(2)
                        .setMaxBufferedRequests(4)
                        .setMaxInFlightRequests(1)
                        .setMaxTimeInBufferMS(Duration.ofMinutes(10).toMillis())
                        .setGzipEnabled(true)
                        .build();

        executeSinkJob(sink, lineProtocolRows, "influxdb3-container-e2e");

        List<JsonNode> actualRows =
                awaitQueryRows(
                        endpoint,
                        DATABASE,
                        "SELECT time, run_id, sensor, reading, reading_id, healthy"
                                + " FROM "
                                + MEASUREMENT
                                + " WHERE run_id = '"
                                + runId
                                + "' ORDER BY time",
                        expectedRows.size());
        assertRows(actualRows, expectedRows, runId);
    }

    @ParameterizedTest(name = "failOnRejected={0}")
    @ValueSource(booleans = {false, true})
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void handlesSchemaConflictsAccordingToPartialWritePolicy(boolean failOnRejected)
            throws Exception {
        URI endpoint = influxDbEndpoint();
        String runId = uniqueIdentifier();

        writeLineProtocol(
                endpoint,
                DATABASE,
                PARTIAL_WRITE_MEASUREMENT
                        + ",seed=true value=0i 1735689600100000000");

        List<String> lineProtocolRows =
                List.of(
                        PARTIAL_WRITE_MEASUREMENT
                                + ",run_id="
                                + runId
                                + " value=1i 1735689600100000001",
                        PARTIAL_WRITE_MEASUREMENT
                                + ",run_id="
                                + runId
                                + " value=\"schema-conflict\" 1735689600100000002",
                        PARTIAL_WRITE_MEASUREMENT
                                + ",run_id="
                                + runId
                                + " value=2i 1735689600100000003");
        InfluxDB3Sink<String> sink =
                InfluxDB3Sink.<String>builder()
                        .setEndpoint(endpoint)
                        .setDatabase(DATABASE)
                        .setToken(ADMIN_TOKEN)
                        .setConverter(value -> value)
                        .setWritePrecision(WritePrecision.NS)
                        .setMaxBatchSize(3)
                        .setMaxBufferedRequests(4)
                        .setMaxInFlightRequests(1)
                        .setAcceptPartial(true)
                        .setFailOnRejected(failOnRejected)
                        .build();

        if (failOnRejected) {
            assertThatThrownBy(
                            () ->
                                    executeSinkJob(
                                            sink,
                                            lineProtocolRows,
                                            "influxdb3-partial-write-fail-e2e"))
                    .hasStackTraceContaining(
                            "InfluxDB write failed with a non-retryable error");
        } else {
            executeSinkJob(
                    sink,
                    lineProtocolRows,
                    "influxdb3-partial-write-drop-e2e");
        }

        List<JsonNode> actualRows =
                awaitQueryRows(
                        endpoint,
                        DATABASE,
                        "SELECT time, run_id, value FROM "
                                + PARTIAL_WRITE_MEASUREMENT
                                + " WHERE run_id = '"
                                + runId
                                + "' ORDER BY time",
                        2);

        assertThat(actualRows).hasSize(2);
        assertThat(actualRows)
                .extracting(row -> requiredColumn(row, "run_id").asText())
                .containsOnly(runId);
        assertThat(actualRows)
                .extracting(row -> requiredColumn(row, "value").asLong())
                .containsExactly(1L, 2L);
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void writesQueryableDataWithNoSyncEnabled() throws Exception {
        URI endpoint = influxDbEndpoint();
        String runId = uniqueIdentifier();

        String lineProtocol =
                NO_SYNC_MEASUREMENT
                        + ",run_id="
                        + runId
                        + " value=1i 1735689600200000000";
        InfluxDB3Sink<String> sink =
                InfluxDB3Sink.<String>builder()
                        .setEndpoint(endpoint)
                        .setDatabase(DATABASE)
                        .setToken(ADMIN_TOKEN)
                        .setConverter(value -> value)
                        .setWritePrecision(WritePrecision.NS)
                        .setMaxBatchSize(1)
                        .setMaxBufferedRequests(2)
                        .setMaxInFlightRequests(1)
                        .setNoSync(true)
                        .build();

        executeSinkJob(
                sink,
                List.of(lineProtocol),
                "influxdb3-no-sync-e2e");

        List<JsonNode> actualRows =
                awaitQueryRows(
                        endpoint,
                        DATABASE,
                        "SELECT run_id, value FROM "
                                + NO_SYNC_MEASUREMENT
                                + " WHERE run_id = '"
                                + runId
                                + "'",
                        1);

        assertThat(actualRows).singleElement()
                .satisfies(
                        row -> {
                            assertThat(requiredColumn(row, "run_id").asText())
                                    .isEqualTo(runId);
                            assertThat(requiredColumn(row, "value").asLong()).isEqualTo(1L);
                        });
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void retriesProxySimulatedRateLimitAndPersistsDataToInfluxDb3() throws Exception {
        URI endpoint = influxDbEndpoint();
        String runId = uniqueIdentifier();

        try (OneShotRateLimitingProxy proxy = new OneShotRateLimitingProxy(endpoint)) {
            String lineProtocol =
                    RATE_LIMIT_MEASUREMENT
                            + ",run_id="
                            + runId
                            + " value=1i 1735689600300000000";
            InfluxDB3Sink<String> sink =
                    InfluxDB3Sink.<String>builder()
                            .setEndpoint(proxy.endpoint())
                            .setDatabase(DATABASE)
                            .setToken(ADMIN_TOKEN)
                            .setConverter(value -> value)
                            .setWritePrecision(WritePrecision.NS)
                            .setMaxBatchSize(1)
                            .setMaxBufferedRequests(2)
                            .setMaxInFlightRequests(1)
                            .setGzipEnabled(false)
                            .setMaxRetries(2)
                            .setRetryBackoff(
                                    Duration.ofMillis(10),
                                    Duration.ofMillis(10),
                                    0)
                            .build();

            executeSinkJob(
                    sink,
                    List.of(lineProtocol),
                    "influxdb3-rate-limit-e2e");

            assertThat(proxy.writeAttempts()).isEqualTo(2);
        }

        List<JsonNode> actualRows =
                awaitQueryRows(
                        endpoint,
                        DATABASE,
                        "SELECT run_id, value FROM "
                                + RATE_LIMIT_MEASUREMENT
                                + " WHERE run_id = '"
                                + runId
                                + "'",
                        1);
        assertThat(actualRows).singleElement()
                .satisfies(
                        row -> {
                            assertThat(requiredColumn(row, "run_id").asText())
                                    .isEqualTo(runId);
                            assertThat(requiredColumn(row, "value").asLong()).isEqualTo(1L);
                        });
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void refreshesARejectedTokenAndPersistsDataToInfluxDb3() throws Exception {
        URI endpoint = influxDbEndpoint();
        String runId = uniqueIdentifier();
        String staleToken = "stale-" + uniqueIdentifier();

        try (OneShotAuthenticationProxy proxy =
                new OneShotAuthenticationProxy(endpoint, staleToken, ADMIN_TOKEN)) {
            String lineProtocol =
                    AUTH_ROTATION_MEASUREMENT
                            + ",run_id="
                            + runId
                            + " value=1i 1735689600350000000";
            InfluxDB3Sink<String> sink =
                    InfluxDB3Sink.<String>builder()
                            .setEndpoint(proxy.endpoint())
                            .setDatabase(DATABASE)
                            .setTokenProvider(
                                    new RotatingIntegrationTokenProvider(
                                            staleToken, ADMIN_TOKEN))
                            .setConverter(value -> value)
                            .setWritePrecision(WritePrecision.NS)
                            .setMaxBatchSize(1)
                            .setMaxBufferedRequests(2)
                            .setMaxInFlightRequests(1)
                            .setGzipEnabled(false)
                            .build();

            executeSinkJob(
                    sink,
                    List.of(lineProtocol),
                    "influxdb3-auth-rotation-e2e");

            assertThat(proxy.writeAttempts()).isEqualTo(2);
            assertThat(proxy.authorizationHeaders())
                    .containsExactly(
                            "Bearer " + staleToken,
                            "Bearer " + ADMIN_TOKEN);
        }

        List<JsonNode> actualRows =
                awaitQueryRows(
                        endpoint,
                        DATABASE,
                        "SELECT run_id, value FROM "
                                + AUTH_ROTATION_MEASUREMENT
                                + " WHERE run_id = '"
                                + runId
                                + "'",
                        1);
        assertThat(actualRows).singleElement()
                .satisfies(
                        row -> {
                            assertThat(requiredColumn(row, "run_id").asText())
                                    .isEqualTo(runId);
                            assertThat(requiredColumn(row, "value").asLong()).isEqualTo(1L);
                        });
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void restoresCheckpointedRecordsAfterFailureAgainstInfluxDb3() throws Exception {
        URI endpoint = influxDbEndpoint();
        String runId = uniqueIdentifier();
        Path failureMarker = temporaryDirectory.resolve("container-checkpoint-failure.marker");
        int recordCount = 12;

        InfluxDB3Sink<String> sink =
                InfluxDB3Sink.<String>builder()
                        .setEndpoint(endpoint)
                        .setDatabase(DATABASE)
                        .setToken(ADMIN_TOKEN)
                        .setConverter(value -> value)
                        .setWritePrecision(WritePrecision.NS)
                        .setMaxBatchSize(10)
                        .setMaxBufferedRequests(20)
                        .setMaxInFlightRequests(1)
                        .setMaxTimeInBufferMS(60_000)
                        .setRetryBackoff(Duration.ZERO, Duration.ZERO, 0)
                        .build();

        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY,
                Duration.ofMillis(10));
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.createLocalEnvironment(1, configuration);
        environment.setParallelism(1);
        environment.enableCheckpointing(50);
        environment.getCheckpointConfig().setCheckpointTimeout(10_000);
        environment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        environment
                .fromSource(
                        new DataGeneratorSource<>(
                                index -> index,
                                recordCount,
                                RateLimiterStrategy.perSecond(20),
                                Types.LONG),
                        WatermarkStrategy.noWatermarks(),
                        "container-checkpointing-number-source")
                .uid("container-checkpointing-number-source")
                .map(
                        new FailOnceAfterCheckpointMap<>(
                                failureMarker.toString(),
                                record ->
                                        CHECKPOINT_MEASUREMENT
                                                + ",record_id="
                                                + record
                                                + ",run_id="
                                                + runId
                                                + " value="
                                                + record
                                                + "i "
                                                + (CHECKPOINT_TIMESTAMP_BASE + record)),
                        Types.STRING)
                .name("container-fail-once-after-checkpoint")
                .uid("container-fail-once-after-checkpoint")
                .sinkTo(sink)
                .name("influxdb3-container-recovery-test")
                .uid("influxdb3-container-recovery-test");

        environment.execute("influxdb3-container-checkpoint-recovery-e2e");

        List<JsonNode> actualRows =
                awaitQueryRows(
                        endpoint,
                        DATABASE,
                        "SELECT run_id, record_id, value FROM "
                                + CHECKPOINT_MEASUREMENT
                                + " WHERE run_id = '"
                                + runId
                                + "'",
                        recordCount);
        List<Long> expectedIds = LongStream.range(0, recordCount).boxed().toList();

        assertThat(Files.exists(failureMarker))
                .as("the pipeline failed once after a completed checkpoint")
                .isTrue();
        assertThat(actualRows).hasSize(recordCount);
        assertThat(actualRows)
                .extracting(row -> Long.parseLong(requiredColumn(row, "record_id").asText()))
                .containsExactlyInAnyOrderElementsOf(expectedIds);
        assertThat(actualRows)
                .extracting(row -> requiredColumn(row, "run_id").asText())
                .containsOnly(runId);
        assertThat(actualRows)
                .extracting(row -> requiredColumn(row, "value").asLong())
                .containsExactlyInAnyOrderElementsOf(expectedIds);
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void writesSqlRowsWithTypedAndEscapedValuesToInfluxDb3() throws Exception {
        URI endpoint = influxDbEndpoint();
        String runId = uniqueIdentifier();
        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        TableEnvironment tableEnvironment =
                TableEnvironment.create(
                        EnvironmentSettings.newInstance()
                                .inStreamingMode()
                                .withConfiguration(configuration)
                                .build());
        tableEnvironment.getConfig().set("parallelism.default", "1");

        tableEnvironment.executeSql(
                "CREATE TABLE `"
                        + SQL_MEASUREMENT
                        + "` ("
                        + " run_id STRING,"
                        + " `host name` STRING,"
                        + " flag BOOLEAN,"
                        + " tiny TINYINT,"
                        + " num INT,"
                        + " big BIGINT,"
                        + " ratio FLOAT,"
                        + " reading DOUBLE,"
                        + " amount DECIMAL(10, 3),"
                        + " `a=b,c` STRING,"
                        + " missing DOUBLE,"
                        + " ts TIMESTAMP_LTZ(3)"
                        + ") WITH ("
                        + " 'connector' = 'timestream-influxdb3',"
                        + " 'endpoint' = '"
                        + endpoint
                        + "',"
                        + " 'database' = '"
                        + DATABASE
                        + "',"
                        + " 'tag-columns' = 'run_id, host name',"
                        + " 'timestamp-column' = 'ts',"
                        + " 'token' = '"
                        + ADMIN_TOKEN
                        + "'"
                        + ")");
        tableEnvironment
                .executeSql(
                        "INSERT INTO `"
                                + SQL_MEASUREMENT
                                + "` VALUES ('"
                                + runId
                                + "', 'a\\b,c=d e', TRUE, CAST(-1 AS TINYINT), 42, "
                                + "4000000000, CAST(0.25 AS FLOAT), 1.0E20, 12.345, "
                                + "'say \"hi\" \\ bye', CAST(NULL AS DOUBLE), "
                                + "TO_TIMESTAMP_LTZ(1735689600500, 3))")
                .await();

        JsonNode row =
                awaitQueryRows(
                                endpoint,
                                DATABASE,
                                "SELECT * FROM \""
                                        + SQL_MEASUREMENT
                                        + "\" WHERE run_id = '"
                                        + runId
                                        + "'",
                                1)
                        .get(0);
        assertThat(requiredColumn(row, "host name").asText()).isEqualTo("a\\b,c=d e");
        assertThat(requiredColumn(row, "flag").asBoolean()).isTrue();
        assertThat(requiredColumn(row, "tiny").asLong()).isEqualTo(-1L);
        assertThat(requiredColumn(row, "num").asLong()).isEqualTo(42L);
        assertThat(requiredColumn(row, "big").asLong()).isEqualTo(4_000_000_000L);
        assertThat(requiredColumn(row, "ratio").asDouble()).isEqualTo(0.25);
        assertThat(requiredColumn(row, "reading").asDouble()).isEqualTo(1.0e20);
        assertThat(requiredColumn(row, "amount").asDouble()).isEqualTo(12.345);
        assertThat(requiredColumn(row, "a=b,c").asText()).isEqualTo("say \"hi\" \\ bye");
        assertThat(row.has("missing")).isFalse();
        assertThat(parseTimestamp(requiredColumn(row, "time").asText()))
                .isEqualTo(Instant.parse("2025-01-01T00:00:00.500Z"));

        List<JsonNode> columns =
                awaitQueryRows(
                        endpoint,
                        DATABASE,
                        "SELECT column_name, data_type FROM information_schema.columns"
                                + " WHERE table_name = '"
                                + SQL_MEASUREMENT
                                + "' AND column_name <> 'time'",
                        10);
        assertThat(columns)
                .extracting(
                        column ->
                                requiredColumn(column, "column_name").asText()
                                        + ":"
                                        + requiredColumn(column, "data_type").asText())
                .containsExactlyInAnyOrder(
                        "run_id:Dictionary(Int32, Utf8)",
                        "host name:Dictionary(Int32, Utf8)",
                        "flag:Boolean",
                        "tiny:Int64",
                        "num:Int64",
                        "big:Int64",
                        "ratio:Float64",
                        "reading:Float64",
                        "amount:Float64",
                        "a=b,c:Utf8");
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void restoresCheckpointedSqlRowsAfterFailureAgainstInfluxDb3() throws Exception {
        URI endpoint = influxDbEndpoint();
        String runId = uniqueIdentifier();
        Path failureMarker = temporaryDirectory.resolve("container-sql-failure.marker");
        int recordCount = 12;

        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY,
                Duration.ofMillis(10));
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.createLocalEnvironment(1, configuration);
        environment.enableCheckpointing(50);
        environment.getCheckpointConfig().setCheckpointTimeout(10_000);
        environment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        tableEnvironment.createTemporaryView(
                "ids",
                environment
                        .fromSource(
                                new DataGeneratorSource<>(
                                        index -> index,
                                        recordCount,
                                        RateLimiterStrategy.perSecond(20),
                                        Types.LONG),
                                WatermarkStrategy.noWatermarks(),
                                "container-sql-checkpointing-number-source")
                        .map(
                                new FailOnceAfterCheckpointMap<Long>(
                                        failureMarker.toString(), record -> record),
                                Types.LONG));
        tableEnvironment.executeSql(
                "CREATE TABLE "
                        + SQL_CHECKPOINT_MEASUREMENT
                        + " (run_id STRING, record_id BIGINT, ts BIGINT) WITH ("
                        + " 'connector' = 'timestream-influxdb3',"
                        + " 'endpoint' = '"
                        + endpoint
                        + "',"
                        + " 'database' = '"
                        + DATABASE
                        + "',"
                        + " 'tag-columns' = 'run_id',"
                        + " 'timestamp-column' = 'ts',"
                        + " 'token' = '"
                        + ADMIN_TOKEN
                        + "',"
                        + " 'sink.batch.max-size' = '10',"
                        + " 'sink.requests.max-buffered' = '20',"
                        + " 'sink.requests.max-inflight' = '1',"
                        + " 'sink.flush-buffer.timeout' = '60000'"
                        + ")");

        tableEnvironment
                .executeSql(
                        "INSERT INTO "
                                + SQL_CHECKPOINT_MEASUREMENT
                                + " SELECT '"
                                + runId
                                + "', f0, "
                                + SQL_CHECKPOINT_TIMESTAMP_BASE
                                + " + f0 FROM ids")
                .await();

        List<JsonNode> actualRows =
                awaitQueryRows(
                        endpoint,
                        DATABASE,
                        "SELECT record_id FROM \""
                                + SQL_CHECKPOINT_MEASUREMENT
                                + "\" WHERE run_id = '"
                                + runId
                                + "' ORDER BY record_id",
                        recordCount);
        assertThat(Files.exists(failureMarker))
                .as("the pipeline failed once after a completed checkpoint")
                .isTrue();
        assertThat(actualRows)
                .as("replayed rows overwrite the same points, so each record appears once")
                .extracting(row -> requiredColumn(row, "record_id").asLong())
                .containsExactlyElementsOf(LongStream.range(0, recordCount).boxed().toList());
    }

    private static void executeSinkJob(
            InfluxDB3Sink<String> sink, List<String> lineProtocolRows, String jobName)
            throws Exception {
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.createLocalEnvironment(1);
        environment.setParallelism(1);
        environment
                .fromData(lineProtocolRows.toArray(String[]::new))
                .sinkTo(sink)
                .name(jobName);
        environment.execute(jobName);
    }

    private static void writeLineProtocol(URI endpoint, String database, String lineProtocol)
            throws IOException, InterruptedException {
        URI writeEndpoint =
                URI.create(
                        endpoint
                                + "/api/v3/write_lp?db="
                                + database
                                + "&precision=nanosecond"
                                + "&accept_partial=false&no_sync=false");
        HttpRequest request =
                HttpRequest.newBuilder(writeEndpoint)
                        .timeout(Duration.ofSeconds(10))
                        .header("Authorization", "Bearer " + ADMIN_TOKEN)
                        .header("Content-Type", "text/plain; charset=UTF-8")
                        .POST(HttpRequest.BodyPublishers.ofString(lineProtocol))
                        .build();
        HttpResponse<String> response =
                HTTP_CLIENT.send(
                        request,
                        HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));

        assertThat(isSuccessful(response.statusCode()))
                .as("seed line-protocol write: %s", describe(response))
                .isTrue();
    }

    private static GenericContainer<?> createInfluxDbContainer() {
        DockerImageName image =
                DockerImageName.parse(System.getProperty(IMAGE_PROPERTY, DEFAULT_IMAGE));
        GenericContainer<?> container = new GenericContainer<>(image);
        container.withExposedPorts(INFLUXDB_PORT);
        container.withEnv(ADMIN_TOKEN_JSON_ENV, createAdminTokenFile());
        container.withCreateContainerCmdModifier(
                command -> command.withEntrypoint("/bin/sh"));
        container.withCommand(
                "-c",
                "set -eu; umask 077; "
                        + "printf '%s' \"$"
                        + ADMIN_TOKEN_JSON_ENV
                        + "\" > "
                        + ADMIN_TOKEN_FILE
                        + "; unset "
                        + ADMIN_TOKEN_JSON_ENV
                        + "; exec influxdb3 serve"
                        + " --node-id=node0"
                        + " --object-store=memory"
                        + " --admin-token-file="
                        + ADMIN_TOKEN_FILE);
        container.waitingFor(
                Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(2)));
        return container;
    }

    private static URI influxDbEndpoint() {
        try {
            return new URI(
                    "http",
                    null,
                    INFLUXDB.getHost(),
                    INFLUXDB.getMappedPort(INFLUXDB_PORT),
                    null,
                    null,
                    null);
        } catch (URISyntaxException error) {
            throw new IllegalStateException("Unable to construct the InfluxDB endpoint", error);
        }
    }

    private static void createDatabase(URI endpoint, String database)
            throws InterruptedException {
        ObjectNode requestBody = OBJECT_MAPPER.createObjectNode().put("db", database);
        Instant deadline = Instant.now().plus(API_READINESS_TIMEOUT);
        String lastOutcome = "InfluxDB did not respond";

        while (Instant.now().isBefore(deadline)) {
            try {
                HttpResponse<String> response =
                        postJson(endpoint.resolve("/api/v3/configure/database"), requestBody);
                if (isSuccessful(response.statusCode())) {
                    return;
                }
                lastOutcome = describe(response);
            } catch (IOException error) {
                lastOutcome = error.toString();
            }
            Thread.sleep(POLL_INTERVAL.toMillis());
        }

        throw new AssertionError(
                "InfluxDB did not create database "
                        + database
                        + " within "
                        + API_READINESS_TIMEOUT
                        + "; last outcome: "
                        + lastOutcome);
    }

    private static List<JsonNode> awaitQueryRows(
            URI endpoint, String database, String query, int expectedCount)
            throws InterruptedException {
        ObjectNode requestBody =
                OBJECT_MAPPER
                        .createObjectNode()
                        .put("db", database)
                        .put("q", query)
                        .put("format", "jsonl");
        Instant deadline = Instant.now().plus(QUERY_VISIBILITY_TIMEOUT);
        List<JsonNode> lastRows = List.of();
        String lastOutcome = "InfluxDB did not return a query response";

        while (Instant.now().isBefore(deadline)) {
            try {
                HttpResponse<String> response =
                        postJson(endpoint.resolve("/api/v3/query_sql"), requestBody);
                if (isSuccessful(response.statusCode())) {
                    try {
                        lastRows = parseJsonLines(response.body());
                        if (lastRows.size() >= expectedCount) {
                            return lastRows;
                        }
                        lastOutcome = "query returned " + lastRows.size() + " rows";
                    } catch (IOException error) {
                        lastOutcome = "invalid JSONL response: " + error.getMessage();
                    }
                } else {
                    lastOutcome = describe(response);
                }
            } catch (IOException error) {
                lastOutcome = error.toString();
            }
            Thread.sleep(POLL_INTERVAL.toMillis());
        }

        throw new AssertionError(
                "Expected "
                        + expectedCount
                        + " rows within "
                        + QUERY_VISIBILITY_TIMEOUT
                        + " but last received "
                        + lastRows.size()
                        + "; last outcome: "
                        + lastOutcome);
    }

    private static HttpResponse<String> postJson(URI uri, JsonNode requestBody)
            throws IOException, InterruptedException {
        HttpRequest request =
                HttpRequest.newBuilder(uri)
                        .timeout(Duration.ofSeconds(10))
                        .header("Authorization", "Bearer " + ADMIN_TOKEN)
                        .header("Content-Type", "application/json")
                        .header("Accept", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(requestBody.toString()))
                        .build();
        return HTTP_CLIENT.send(
                request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }

    private static List<JsonNode> parseJsonLines(String responseBody) throws IOException {
        List<JsonNode> rows = new ArrayList<>();
        for (String line : responseBody.split("\\R")) {
            if (!line.isBlank()) {
                rows.add(OBJECT_MAPPER.readTree(line));
            }
        }
        return rows;
    }

    private static void assertRows(
            List<JsonNode> actualRows, List<ExpectedRow> expectedRows, String runId) {
        assertThat(actualRows).hasSize(expectedRows.size());
        for (int index = 0; index < expectedRows.size(); index++) {
            JsonNode actual = actualRows.get(index);
            ExpectedRow expected = expectedRows.get(index);

            assertThat(actual.isObject()).as("row %s", index).isTrue();
            assertThat(requiredColumn(actual, "run_id").asText()).isEqualTo(runId);
            assertThat(requiredColumn(actual, "sensor").asText())
                    .isEqualTo(expected.sensor());
            assertThat(requiredColumn(actual, "reading").asDouble())
                    .isEqualTo(expected.reading());
            assertThat(requiredColumn(actual, "reading_id").asLong())
                    .isEqualTo(expected.readingId());
            assertThat(requiredColumn(actual, "healthy").asBoolean())
                    .isEqualTo(expected.healthy());
            assertThat(parseTimestamp(requiredColumn(actual, "time").asText()))
                    .isEqualTo(toInstant(expected.timestampNanos()));
        }
    }

    private static JsonNode requiredColumn(JsonNode row, String column) {
        JsonNode value = row.get(column);
        assertThat(value).as("column %s in row %s", column, row).isNotNull();
        assertThat(value.isNull()).as("column %s in row %s", column, row).isFalse();
        return value;
    }

    private static Instant parseTimestamp(String timestamp) {
        try {
            return Instant.parse(timestamp);
        } catch (DateTimeParseException ignored) {
            try {
                return OffsetDateTime.parse(timestamp).toInstant();
            } catch (DateTimeParseException noOffset) {
                return LocalDateTime.parse(timestamp).toInstant(ZoneOffset.UTC);
            }
        }
    }

    private static Instant toInstant(long epochNanos) {
        return Instant.ofEpochSecond(
                Math.floorDiv(epochNanos, NANOS_PER_SECOND),
                Math.floorMod(epochNanos, NANOS_PER_SECOND));
    }

    private static String createAdminTokenFile() {
        return OBJECT_MAPPER
                .createObjectNode()
                .put("token", ADMIN_TOKEN)
                .put("name", "flink-e2e-admin")
                .put("description", "Ephemeral token for the Flink connector E2E test")
                .toString();
    }

    private static String generateAdminToken() {
        byte[] randomBytes = new byte[32];
        new SecureRandom().nextBytes(randomBytes);
        return "apiv3_" + Base64.getUrlEncoder().withoutPadding().encodeToString(randomBytes);
    }

    private static String uniqueIdentifier() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    private static boolean isSuccessful(int statusCode) {
        return statusCode >= 200 && statusCode < 300;
    }

    private static String describe(HttpResponse<String> response) {
        String body = response.body();
        if (body.length() > 500) {
            body = body.substring(0, 500) + "...";
        }
        return "HTTP " + response.statusCode() + ": " + body;
    }

    private static void forwardWrite(
            URI upstream, HttpExchange exchange, byte[] requestBody)
            throws IOException, InterruptedException {
        HttpRequest.Builder request =
                HttpRequest.newBuilder(
                                upstream.resolve(
                                        exchange.getRequestURI().toString()))
                        .timeout(Duration.ofSeconds(10))
                        .method(
                                exchange.getRequestMethod(),
                                HttpRequest.BodyPublishers.ofByteArray(requestBody));
        copyHeader(exchange, request, "Authorization");
        copyHeader(exchange, request, "Content-Type");
        copyHeader(exchange, request, "Content-Encoding");
        copyHeader(exchange, request, "Accept");

        HttpResponse<byte[]> response =
                HTTP_CLIENT.send(request.build(), HttpResponse.BodyHandlers.ofByteArray());
        respond(
                exchange,
                response.statusCode(),
                response.body(),
                response.headers().firstValue("Content-Type").orElse(null));
    }

    private static void copyHeader(
            HttpExchange exchange, HttpRequest.Builder request, String name) {
        String value = exchange.getRequestHeaders().getFirst(name);
        if (value != null) {
            request.header(name, value);
        }
    }

    private static void respond(
            HttpExchange exchange,
            int statusCode,
            byte[] responseBody,
            String contentType)
            throws IOException {
        if (contentType != null) {
            exchange.getResponseHeaders().set("Content-Type", contentType);
        }
        if (responseBody.length == 0) {
            exchange.sendResponseHeaders(statusCode, -1);
            return;
        }
        exchange.sendResponseHeaders(statusCode, responseBody.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(responseBody);
        }
    }

    private abstract static class ForwardingWriteProxy implements AutoCloseable {
        private static final byte[] INTERRUPTED_BODY =
                "{\"error\":\"integration-test proxy interrupted\"}"
                        .getBytes(StandardCharsets.UTF_8);
        private static final byte[] FAILED_BODY =
                "{\"error\":\"integration-test proxy failed\"}"
                        .getBytes(StandardCharsets.UTF_8);

        private final URI upstream;
        private final HttpServer server;
        private final AtomicInteger writeAttempts = new AtomicInteger();

        private ForwardingWriteProxy(URI upstream, String threadName)
                throws IOException {
            this.upstream = upstream;
            this.server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            this.server.createContext("/api/v3/write_lp", this::handleWrite);
            this.server.setExecutor(
                    command -> {
                        Thread thread = new Thread(command, threadName);
                        thread.setDaemon(true);
                        thread.start();
                    });
            this.server.start();
        }

        final URI endpoint() {
            return URI.create("http://127.0.0.1:" + server.getAddress().getPort());
        }

        final int writeAttempts() {
            return writeAttempts.get();
        }

        private void handleWrite(HttpExchange exchange) throws IOException {
            byte[] requestBody = exchange.getRequestBody().readAllBytes();
            try {
                int attempt = writeAttempts.incrementAndGet();
                if (!intercept(exchange, attempt)) {
                    forwardWrite(upstream, exchange, requestBody);
                }
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                respond(exchange, 502, INTERRUPTED_BODY, "application/json");
            } catch (RuntimeException error) {
                respond(exchange, 502, FAILED_BODY, "application/json");
            } finally {
                exchange.close();
            }
        }

        protected abstract boolean intercept(HttpExchange exchange, int attempt)
                throws IOException;

        @Override
        public final void close() {
            server.stop(0);
        }
    }

    private static final class OneShotRateLimitingProxy extends ForwardingWriteProxy {
        private static final byte[] RATE_LIMIT_BODY =
                "{\"error\":\"rate limited by integration-test proxy\"}"
                        .getBytes(StandardCharsets.UTF_8);

        private OneShotRateLimitingProxy(URI upstream) throws IOException {
            super(upstream, "influxdb3-rate-limit-proxy");
        }

        @Override
        protected boolean intercept(HttpExchange exchange, int attempt)
                throws IOException {
            if (attempt != 1) {
                return false;
            }
            exchange.getResponseHeaders().set("Retry-After", "0");
            respond(exchange, 429, RATE_LIMIT_BODY, "application/json");
            return true;
        }
    }

    private static final class OneShotAuthenticationProxy extends ForwardingWriteProxy {
        private static final byte[] UNAUTHORIZED_BODY =
                "{\"error\":\"rejected stale integration-test token\"}"
                        .getBytes(StandardCharsets.UTF_8);

        private final String staleAuthorization;
        private final String refreshedAuthorization;
        private final List<String> authorizationHeaders = new CopyOnWriteArrayList<>();

        private OneShotAuthenticationProxy(
                URI upstream, String staleToken, String refreshedToken)
                throws IOException {
            super(upstream, "influxdb3-auth-rotation-proxy");
            this.staleAuthorization = "Bearer " + staleToken;
            this.refreshedAuthorization = "Bearer " + refreshedToken;
        }

        private List<String> authorizationHeaders() {
            return List.copyOf(authorizationHeaders);
        }

        @Override
        protected boolean intercept(HttpExchange exchange, int attempt)
                throws IOException {
            String authorization = exchange.getRequestHeaders().getFirst("Authorization");
            authorizationHeaders.add(authorization);
            if (attempt == 1 && staleAuthorization.equals(authorization)) {
                respond(exchange, 401, UNAUTHORIZED_BODY, "application/json");
                return true;
            }
            if (refreshedAuthorization.equals(authorization)) {
                return false;
            }
            respond(exchange, 401, UNAUTHORIZED_BODY, "application/json");
            return true;
        }
    }

    private record RotatingIntegrationTokenProvider(
            String initialToken, String refreshedToken) implements TokenProvider {
        private static final long serialVersionUID = 1L;

        @Override
        public char[] getToken() {
            return initialToken.toCharArray();
        }

        @Override
        public char[] refreshToken() {
            return refreshedToken.toCharArray();
        }
    }

    private record ExpectedRow(
            String sensor,
            double reading,
            long readingId,
            boolean healthy,
            long timestampNanos) {

        private String toLineProtocol(String runId) {
            return String.format(
                    Locale.ROOT,
                    "%s,run_id=%s,sensor=%s reading=%s,reading_id=%di,healthy=%s %d",
                    MEASUREMENT,
                    runId,
                    sensor,
                    reading,
                    readingId,
                    healthy,
                    timestampNanos);
        }
    }
}
