/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;

import com.influxdb.v3.client.write.WritePrecision;
import software.amazon.timestream.influxdb3.flink.InfluxDB3SinkTestAccess;
import software.amazon.timestream.influxdb3.flink.auth.SecretsManagerTokenProvider;
import software.amazon.timestream.influxdb3.flink.auth.StaticTokenProvider;
import software.amazon.timestream.influxdb3.flink.internal.InfluxDB3SinkConfig;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InfluxDB3DynamicTableSinkFactoryTest {

    private static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(
                    Column.physical("host", DataTypes.STRING()),
                    Column.physical("usage", DataTypes.DOUBLE()),
                    Column.physical("ts", DataTypes.TIMESTAMP(3)));

    @Test
    void isDiscoverableThroughServiceLoading() {
        DynamicTableSinkFactory factory =
                FactoryUtil.discoverFactory(
                        getClass().getClassLoader(),
                        DynamicTableSinkFactory.class,
                        "timestream-influxdb3");

        assertThat(factory).isInstanceOf(InfluxDB3DynamicTableSinkFactory.class);
        assertThat(InfluxDB3ConnectorOptions.class.isAnnotationPresent(PublicEvolving.class))
                .isTrue();
    }

    @Test
    void appliesBuilderDefaultsForUnsetOptions() {
        InfluxDB3DynamicTableSink sink = createSink(requiredOptions());
        InfluxDB3SinkConfig config = InfluxDB3SinkTestAccess.configurationOf(sink.getSink());

        assertThat(sink.getParallelism()).isNull();
        assertThat(sink.getChangelogMode(ChangelogMode.all()))
                .isEqualTo(ChangelogMode.insertOnly());
        assertThat(sink.asSummaryString()).isEqualTo("TimestreamInfluxDB3");
        assertThat(config.getEndpoint().toString()).isEqualTo("https://example.com:8086");
        assertThat(config.getDatabase()).isEqualTo("metrics");
        assertThat(config.getTokenProvider()).isInstanceOf(StaticTokenProvider.class);
        assertThat(config.getWritePrecision()).isEqualTo(WritePrecision.NS);
        assertThat(config.isFailOnValidation()).isTrue();
        assertThat(config.isFailOnRetryExhaustion()).isTrue();
        assertThat(config.isGzipEnabled()).isTrue();
        assertThat(config.isAcceptPartial()).isTrue();
        assertThat(config.isNoSync()).isFalse();
        assertThat(config.getRequestTimeout()).isEqualTo(Duration.ofSeconds(60));
        assertThat(config.getMaxBatchSize()).isEqualTo(10_000);
        assertThat(config.getMaxRetries()).isEqualTo(8);
        assertThat(config.isManagedFlinkMetricsEnabled()).isFalse();
        assertThat(config.getManagedFlinkMetricsSinkName()).isNull();
    }

    @Test
    void mapsEveryOptionToTheSinkBuilder() {
        Map<String, String> options = requiredOptions();
        options.put("tag-columns", " host ");
        options.put("precision", "ms");
        options.put("sink.fail-on-validation", "false");
        options.put("sink.fail-on-rejected", "true");
        options.put("sink.fail-on-error", "false");
        options.put("gzip.enabled", "false");
        options.put("accept-partial", "false");
        options.put("no-sync", "true");
        options.put("request-timeout", "2 min");
        options.put("sink.max-retries", "3");
        options.put("sink.parallelism", "3");
        options.put("sink.batch.max-size", "100");
        options.put("sink.requests.max-inflight", "2");
        options.put("sink.requests.max-buffered", "500");
        options.put("sink.flush-buffer.size", "2097152");
        options.put("sink.flush-buffer.timeout", "250");
        options.put("sink.metrics.managed-flink.enabled", "true");
        options.put("sink.metrics.managed-flink.sink-name", "readings");

        InfluxDB3DynamicTableSink sink = createSink(options);
        InfluxDB3SinkConfig config = InfluxDB3SinkTestAccess.configurationOf(sink.getSink());

        assertThat(sink.getParallelism()).isEqualTo(3);
        assertThat(sink.getSinkRuntimeProvider(null))
                .isInstanceOfSatisfying(
                        SinkV2Provider.class,
                        provider -> {
                            assertThat(provider.createSink()).isSameAs(sink.getSink());
                            assertThat(provider.getParallelism()).contains(3);
                        });
        assertThat(config.getWritePrecision()).isEqualTo(WritePrecision.MS);
        assertThat(config.isFailOnValidation()).isFalse();
        assertThat(config.isFailOnRetryExhaustion()).isFalse();
        assertThat(config.isGzipEnabled()).isFalse();
        assertThat(config.isAcceptPartial()).isFalse();
        assertThat(config.isNoSync()).isTrue();
        assertThat(config.getRequestTimeout()).isEqualTo(Duration.ofMinutes(2));
        assertThat(config.getMaxRetries()).isEqualTo(3);
        assertThat(config.getMaxBatchSize()).isEqualTo(100);
        assertThat(config.getMaxInFlightRequests()).isEqualTo(2);
        assertThat(config.getMaxBufferedRequests()).isEqualTo(500);
        assertThat(config.getMaxBatchSizeInBytes()).isEqualTo(2_097_152L);
        assertThat(config.getMaxTimeInBufferMillis()).isEqualTo(250L);
        assertThat(config.isManagedFlinkMetricsEnabled()).isTrue();
        assertThat(config.getManagedFlinkMetricsSinkName()).isEqualTo("readings");

        assertThat(sink.copy())
                .isInstanceOfSatisfying(
                        InfluxDB3DynamicTableSink.class,
                        copy -> {
                            assertThat(copy.getSink()).isSameAs(sink.getSink());
                            assertThat(copy.getParallelism()).isEqualTo(3);
                        });
    }

    @Test
    void usesSecretsManagerWhenASecretIdIsConfigured() {
        Map<String, String> options = requiredOptions();
        options.remove("token");
        options.put("token.secret-id", "arn:aws:secretsmanager:us-west-2:111122223333:secret:t");
        options.put("token.secret-json-field", "token");
        options.put("token.secret-region", "us-west-2");

        InfluxDB3SinkConfig config =
                InfluxDB3SinkTestAccess.configurationOf(createSink(options).getSink());

        assertThat(config.getTokenProvider())
                .isInstanceOf(SecretsManagerTokenProvider.class)
                .hasToString(
                        SecretsManagerTokenProvider.builder()
                                .setSecretId(
                                        "arn:aws:secretsmanager:us-west-2:111122223333:secret:t")
                                .setJsonField("token")
                                .setRegion("us-west-2")
                                .build()
                                .toString());
    }

    @Test
    void requiresExactlyOneTokenSource() {
        Map<String, String> neither = requiredOptions();
        neither.remove("token");
        assertInvalid(neither, "Exactly one of 'token' or 'token.secret-id' must be set.");

        Map<String, String> both = requiredOptions();
        both.put("token.secret-id", "secret");
        assertInvalid(both, "Exactly one of 'token' or 'token.secret-id' must be set.");

        Map<String, String> orphanJsonField = requiredOptions();
        orphanJsonField.put("token.secret-json-field", "token");
        assertInvalid(
                orphanJsonField, "Option 'token.secret-json-field' requires 'token.secret-id'.");

        Map<String, String> orphanRegion = requiredOptions();
        orphanRegion.put("token.secret-region", "us-east-1");
        assertInvalid(orphanRegion, "Option 'token.secret-region' requires 'token.secret-id'.");
    }

    @Test
    void rejectsMissingAndUnknownOptions() {
        for (String required : List.of("endpoint", "database", "timestamp-column")) {
            Map<String, String> options = requiredOptions();
            options.remove(required);
            assertInvalid(options, required);
        }

        Map<String, String> unknown = requiredOptions();
        unknown.put("sink.batch-size", "10");
        assertInvalid(unknown, "Unsupported options");

        // The measurement is always the sink table name.
        Map<String, String> measurement = requiredOptions();
        measurement.put("measurement", "cpu");
        assertInvalid(measurement, "Unsupported options");
    }

    @Test
    void rejectsMalformedTagColumnLists() {
        Map<String, String> options = requiredOptions();
        options.put("tag-columns", "host,,region");

        assertInvalid(
                options, "Option 'tag-columns' must be a comma-separated list of column names.");
    }

    @Test
    void reportsBuilderValidationErrorsAsValidationExceptions() {
        Map<String, String> endpoint = requiredOptions();
        endpoint.put("endpoint", "example.com");
        assertInvalid(endpoint, "Invalid 'timestream-influxdb3' sink configuration:");

        Map<String, String> timeout = requiredOptions();
        timeout.put("request-timeout", "5 s");
        assertInvalid(timeout, "requestTimeout must exceed writeTimeout plus maxRetryBackoff");

        Map<String, String> retries = requiredOptions();
        retries.put("sink.max-retries", "-1");
        assertInvalid(retries, "maxRetries must not be negative");

        Map<String, String> blankSecret = requiredOptions();
        blankSecret.remove("token");
        blankSecret.put("token.secret-id", " ");
        assertInvalid(blankSecret, "Invalid 'timestream-influxdb3' sink configuration:");

        Map<String, String> sinkNameWithoutMetrics = requiredOptions();
        sinkNameWithoutMetrics.put("sink.metrics.managed-flink.sink-name", "readings");
        assertInvalid(
                sinkNameWithoutMetrics,
                "managedFlinkMetricsSinkName requires managedFlinkMetricsEnabled");
    }

    @Test
    void rejectsSchemaErrorsFromTheConverter() {
        Map<String, String> options = requiredOptions();
        options.put("tag-columns", "usage");

        assertInvalid(options, "Tag column 'usage' has unsupported type DOUBLE.");
    }

    @Test
    void rejectsPrimaryKeysAndPartitionedTables() {
        ResolvedSchema withPrimaryKey =
                new ResolvedSchema(
                        List.of(
                                Column.physical("host", DataTypes.STRING().notNull()),
                                Column.physical("usage", DataTypes.DOUBLE()),
                                Column.physical("ts", DataTypes.TIMESTAMP(3))),
                        List.of(),
                        UniqueConstraint.primaryKey("pk", List.of("host")),
                        List.of());
        assertInvalid(
                () -> createSink(withPrimaryKey, List.of(), requiredOptions()),
                "does not support PRIMARY KEY declarations");
        assertInvalid(
                () -> createSink(SCHEMA, List.of("host"), requiredOptions()),
                "does not support partitioned tables");
    }

    private static void assertInvalid(Map<String, String> options, String message) {
        assertInvalid(() -> createSink(options), message);
    }

    private static void assertInvalid(ThrowingCallable creation, String message) {
        assertThatThrownBy(creation)
                .isInstanceOf(ValidationException.class)
                .satisfies(error -> assertThat(fullMessage(error)).contains(message));
    }

    private static String fullMessage(Throwable error) {
        StringBuilder message = new StringBuilder();
        for (Throwable current = error; current != null; current = current.getCause()) {
            message.append(current.getMessage()).append('\n');
        }
        return message.toString();
    }

    private static Map<String, String> requiredOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "timestream-influxdb3");
        options.put("endpoint", "https://example.com:8086");
        options.put("database", "metrics");
        options.put("timestamp-column", "ts");
        options.put("token", "test-token");
        return options;
    }

    private static InfluxDB3DynamicTableSink createSink(Map<String, String> options) {
        return createSink(SCHEMA, List.of(), options);
    }

    private static InfluxDB3DynamicTableSink createSink(
            ResolvedSchema schema, List<String> partitionKeys, Map<String, String> options) {
        CatalogTable table =
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().fromResolvedSchema(schema).build())
                        .partitionKeys(partitionKeys)
                        .options(options)
                        .build();
        DynamicTableSink sink =
                FactoryUtil.createDynamicTableSink(
                        null,
                        ObjectIdentifier.of("catalog", "database", "table"),
                        new ResolvedCatalogTable(table, schema),
                        Map.of(),
                        new Configuration(),
                        InfluxDB3DynamicTableSinkFactoryTest.class.getClassLoader(),
                        false);
        assertThat(sink).isInstanceOf(InfluxDB3DynamicTableSink.class);
        return (InfluxDB3DynamicTableSink) sink;
    }
}
