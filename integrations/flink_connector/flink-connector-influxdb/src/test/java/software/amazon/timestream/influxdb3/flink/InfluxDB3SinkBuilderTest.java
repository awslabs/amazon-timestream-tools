/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink;

import com.influxdb.v3.client.write.WritePrecision;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.RequestEntryWrapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.timestream.influxdb3.flink.auth.StaticTokenProvider;
import software.amazon.timestream.influxdb3.flink.failure.DefaultWriteFailureHandler;
import software.amazon.timestream.influxdb3.flink.failure.FailureAction;
import software.amazon.timestream.influxdb3.flink.internal.InfluxDB3SinkConfig;
import software.amazon.timestream.influxdb3.flink.internal.InfluxDB3WriteRequest;
import software.amazon.timestream.influxdb3.flink.internal.InfluxDB3WriterStateSerializer;
import software.amazon.timestream.influxdb3.flink.internal.InfluxDBWriteClient;
import software.amazon.timestream.influxdb3.flink.internal.WriterTestContext;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InfluxDB3SinkBuilderTest {

    @Test
    void keepsRuntimeConfigurationOutOfThePublicSinkApi() throws Exception {
        assertThat(InfluxDB3SinkConfig.class.isAnnotationPresent(Internal.class)).isTrue();
        assertThat(InfluxDB3SinkConfig.class.getPackageName()).endsWith(".internal");
        assertThat(
                        Modifier.isPublic(
                                InfluxDB3Sink.class
                                        .getDeclaredMethod("getConfiguration")
                                        .getModifiers()))
                .isFalse();
    }

    @Test
    void marksPublicApiStabilityConsistently() {
        assertThat(
                        List.of(
                                FailureAction.class,
                                DefaultWriteFailureHandler.class,
                                InfluxDB3SinkException.class))
                .allMatch(type -> type.isAnnotationPresent(PublicEvolving.class));
    }

    @Test
    void appliesDocumentedDefaults() {
        InfluxDB3SinkConfig config = validBuilder().build().getConfiguration();

        assertThat(config.getEndpoint()).isEqualTo(URI.create("https://example.com:8086"));
        assertThat(config.getDatabase()).isEqualTo("metrics");
        assertThat(config.getTokenProvider()).isInstanceOf(StaticTokenProvider.class);
        assertThat(config.getFailureHandler()).isSameAs(DefaultWriteFailureHandler.INSTANCE);
        assertThat(config.getWritePrecision())
                .isEqualTo(InfluxDB3SinkBuilder.DEFAULT_WRITE_PRECISION);
        assertThat(config.getMaxBatchSize())
                .isEqualTo(InfluxDB3SinkBuilder.DEFAULT_MAX_BATCH_SIZE);
        assertThat(config.getMaxInFlightRequests())
                .isEqualTo(InfluxDB3SinkBuilder.DEFAULT_MAX_IN_FLIGHT_REQUESTS);
        assertThat(config.getMaxBufferedRequests())
                .isEqualTo(InfluxDB3SinkBuilder.DEFAULT_MAX_BUFFERED_REQUESTS);
        assertThat(config.getMaxBatchSizeInBytes())
                .isEqualTo(InfluxDB3SinkBuilder.DEFAULT_MAX_BATCH_SIZE_IN_BYTES);
        assertThat(config.getMaxBufferedBytes())
                .isEqualTo(InfluxDB3SinkBuilder.DEFAULT_MAX_BUFFERED_BYTES);
        assertThat(config.getMaxTimeInBufferMillis())
                .isEqualTo(InfluxDB3SinkBuilder.DEFAULT_MAX_TIME_IN_BUFFER_MILLIS);
        assertThat(config.getMaxRecordSizeInBytes())
                .isEqualTo(InfluxDB3SinkBuilder.DEFAULT_MAX_RECORD_SIZE_IN_BYTES);
        assertThat(config.getConnectionTimeout())
                .isEqualTo(InfluxDB3SinkBuilder.DEFAULT_CONNECTION_TIMEOUT);
        assertThat(config.getWriteTimeout())
                .isEqualTo(InfluxDB3SinkBuilder.DEFAULT_WRITE_TIMEOUT);
        assertThat(config.getRequestTimeout())
                .isEqualTo(InfluxDB3SinkBuilder.DEFAULT_REQUEST_TIMEOUT);
        assertThat(config.isFailOnRequestTimeout())
                .isEqualTo(InfluxDB3SinkBuilder.DEFAULT_FAIL_ON_REQUEST_TIMEOUT);
        assertThat(config.isGzipEnabled())
                .isEqualTo(InfluxDB3SinkBuilder.DEFAULT_GZIP_ENABLED);
        assertThat(config.isAcceptPartial())
                .isEqualTo(InfluxDB3SinkBuilder.DEFAULT_ACCEPT_PARTIAL);
        assertThat(config.isNoSync())
                .isEqualTo(InfluxDB3SinkBuilder.DEFAULT_NO_SYNC);
        assertThat(config.isFailOnValidation())
                .isEqualTo(InfluxDB3SinkBuilder.DEFAULT_FAIL_ON_VALIDATION);
        assertThat(config.isFailOnRetryExhaustion()).isTrue();
        assertThat(config.isPrintFailedRequests())
                .isEqualTo(InfluxDB3SinkBuilder.DEFAULT_PRINT_FAILED_REQUESTS);
        assertThat(config.getMaxRetries()).isEqualTo(InfluxDB3SinkBuilder.DEFAULT_MAX_RETRIES);
        assertThat(config.getInitialRetryBackoff())
                .isEqualTo(InfluxDB3SinkBuilder.DEFAULT_INITIAL_RETRY_BACKOFF);
        assertThat(config.getMaxRetryBackoff())
                .isEqualTo(InfluxDB3SinkBuilder.DEFAULT_MAX_RETRY_BACKOFF);
        assertThat(config.getRetryJitter())
                .isEqualTo(InfluxDB3SinkBuilder.DEFAULT_RETRY_JITTER);
        assertThat(config.isManagedFlinkMetricsEnabled())
                .isEqualTo(InfluxDB3SinkBuilder.DEFAULT_MANAGED_FLINK_METRICS_ENABLED);
        assertThat(config.getManagedFlinkMetricsSinkName()).isNull();
    }

    @Test
    void appliesManagedFlinkMetricsSettings() {
        InfluxDB3SinkConfig config =
                validBuilder()
                        .setManagedFlinkMetricsEnabled(true)
                        .setManagedFlinkMetricsSinkName("sensors_sink_1")
                        .build()
                        .getConfiguration();

        assertThat(config.isManagedFlinkMetricsEnabled()).isTrue();
        assertThat(config.getManagedFlinkMetricsSinkName()).isEqualTo("sensors_sink_1");
    }

    @Test
    void appliesBufferAndWriteOverrides() {
        InfluxDB3SinkConfig config =
                validBuilder()
                        .setMaxBatchSize(25)
                        .setMaxInFlightRequests(3)
                        .setMaxBufferedRequests(50)
                        .setMaxBatchSizeInBytes(2048)
                        .setMaxBufferedBytes(4096)
                        .setMaxRecordSizeInBytes(1024)
                        .setMaxTimeInBufferMS(250)
                        .setWritePrecision(WritePrecision.MS)
                        .setGzipEnabled(false)
                        .setAcceptPartial(false)
                        .setNoSync(true)
                        .setFailOnRejected(true)
                        .setFailOnValidation(false)
                        .setFailOnError(false)
                        .setPrintFailedRequests(true)
                        .setMaxRetries(2)
                        .setRetryBackoff(Duration.ofMillis(10), Duration.ofSeconds(1), 0.1)
                        .build()
                        .getConfiguration();

        assertThat(config.getMaxBatchSize()).isEqualTo(25);
        assertThat(config.getMaxInFlightRequests()).isEqualTo(3);
        assertThat(config.getMaxBufferedRequests()).isEqualTo(50);
        assertThat(config.getMaxBatchSizeInBytes()).isEqualTo(2048);
        assertThat(config.getMaxBufferedBytes()).isEqualTo(4096);
        assertThat(config.getMaxRecordSizeInBytes()).isEqualTo(1024);
        assertThat(config.getMaxTimeInBufferMillis()).isEqualTo(250);
        assertThat(config.getWritePrecision()).isEqualTo(WritePrecision.MS);
        assertThat(config.isGzipEnabled()).isFalse();
        assertThat(config.isAcceptPartial()).isFalse();
        assertThat(config.isNoSync()).isTrue();
        assertThat(config.isFailOnValidation()).isFalse();
        assertThat(config.isFailOnRetryExhaustion()).isFalse();
        assertThat(config.isPrintFailedRequests()).isTrue();
        assertThat(config.getMaxRetries()).isEqualTo(2);
        assertThat(config.getRetryJitter()).isEqualTo(0.1);
    }

    @Test
    void appliesEndpointAndTimeoutOverrides() {
        URI endpoint = URI.create("http://localhost:8181");

        InfluxDB3SinkConfig config =
                validBuilder()
                        .setEndpoint(endpoint)
                        .setConnectionTimeout(Duration.ofMillis(500))
                        .setWriteTimeout(Duration.ofSeconds(1))
                        .setRequestTimeout(Duration.ofSeconds(4))
                        .setFailOnRequestTimeout(false)
                        .setRetryBackoff(Duration.ZERO, Duration.ofSeconds(2), 1.0)
                        .build()
                        .getConfiguration();

        assertThat(config.getEndpoint()).isEqualTo(endpoint);
        assertThat(config.getConnectionTimeout()).isEqualTo(Duration.ofMillis(500));
        assertThat(config.getWriteTimeout()).isEqualTo(Duration.ofSeconds(1));
        assertThat(config.getRequestTimeout()).isEqualTo(Duration.ofSeconds(4));
        assertThat(config.isFailOnRequestTimeout()).isFalse();
        assertThat(config.getInitialRetryBackoff()).isZero();
        assertThat(config.getMaxRetryBackoff()).isEqualTo(Duration.ofSeconds(2));
        assertThat(config.getRetryJitter()).isEqualTo(1.0);
    }

    @Test
    void acceptsMinimumRequestTimeoutHeadroom() {
        InfluxDB3SinkConfig config =
                validBuilder()
                        .setRetryBackoff(Duration.ZERO, Duration.ofMillis(5), 0)
                        .setWriteTimeout(Duration.ofMillis(10))
                        .setRequestTimeout(Duration.ofMillis(17))
                        .build()
                        .getConfiguration();

        assertThat(config.getRequestTimeout()).isEqualTo(Duration.ofMillis(17));
    }

    @Test
    void createsAndRestoresWritersWithConfiguredClientFactory() throws Exception {
        AtomicInteger clientsCreated = new AtomicInteger();
        InfluxDB3Sink<String> sink =
                validBuilder()
                        .setClientFactory(
                                (config, token) -> {
                                    clientsCreated.incrementAndGet();
                                    return new InfluxDBWriteClient() {
                                        @Override
                                        public void writeRecords(List<String> records) {}

                                        @Override
                                        public void close() {}
                                    };
                                })
                        .build();

        StatefulSinkWriter<String, BufferedRequestState<InfluxDB3WriteRequest>> writer =
                sink.createWriter(WriterTestContext.create());
        writer.close();
        StatefulSinkWriter<String, BufferedRequestState<InfluxDB3WriteRequest>> restored =
                sink.restoreWriter(WriterTestContext.create(), List.of());
        restored.close();

        assertThat(clientsCreated).hasValue(2);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidConfigurations")
    void rejectsInvalidConfigurations(InvalidConfiguration invalidConfiguration) {
        assertThatThrownBy(() -> invalidConfiguration.builder().get().build())
                .isInstanceOf(invalidConfiguration.exceptionType())
                .hasMessageContaining(invalidConfiguration.messageFragment());
    }

    @Test
    void defersMalformedStringEndpointValidationUntilBuild() {
        InfluxDB3SinkBuilder<String> builder = validBuilder();

        assertThatCode(() -> builder.setEndpoint("https://[invalid"))
                .doesNotThrowAnyException();
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("endpoint must be a valid URI")
                .hasCauseInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void supportsCustomFailureHandlerWithConnectorOwnedSettings() {
        InfluxDB3SinkConfig config =
                validBuilder()
                        .setFailOnValidation(false)
                        .setPrintFailedRequests(true)
                        .setFailureHandler(failure -> FailureAction.DROP)
                        .build()
                        .getConfiguration();

        assertThat(config.isFailOnRetryExhaustion()).isTrue();
        assertThat(config.isFailOnValidation()).isFalse();
        assertThat(config.isPrintFailedRequests()).isTrue();
    }

    @Test
    void sinkAndConfigurationAreSerializableWithoutExposingToken() throws Exception {
        InfluxDB3Sink<String> sink = validBuilder().build();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(sink);
        }

        InfluxDB3Sink<?> restored;
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (InfluxDB3Sink<?>) input.readObject();
        }

        assertThat(restored.getConfiguration().getDatabase()).isEqualTo("metrics");
        assertThat(restored.getConfiguration().toString()).doesNotContain("super-secret");
        assertThat(restored.getConfiguration().getTokenProvider().toString())
                .isEqualTo("StaticTokenProvider{token=[REDACTED]}");
        assertThat(restored.getWriterStateSerializer())
                .isInstanceOf(InfluxDB3WriterStateSerializer.class);
        assertThat(new InfluxDB3SinkException("expected")).hasMessage("expected");
    }

    @Test
    void writerStateSerializerUsesConfiguredRecordSizeLimit() {
        InfluxDB3Sink<String> sink =
                validBuilder().setMaxRecordSizeInBytes(5).build();
        InfluxDB3WriteRequest request = InfluxDB3WriteRequest.initial("m f=1i");
        BufferedRequestState<InfluxDB3WriteRequest> state =
                new BufferedRequestState<>(
                        List.of(new RequestEntryWrapper<>(request, 6)));

        assertThatThrownBy(() -> sink.getWriterStateSerializer().serialize(state))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("line protocol exceeds the maximum encoded length");
    }

    private static Stream<InvalidConfiguration> invalidConfigurations() {
        return Stream.of(
                invalid(
                        "null endpoint",
                        () -> validBuilder().setEndpoint((String) null),
                        NullPointerException.class,
                        "endpoint"),
                invalid(
                        "missing endpoint",
                        () ->
                                InfluxDB3Sink.<String>builder()
                                        .setDatabase("metrics")
                                        .setToken("token")
                                        .setConverter(value -> value),
                        NullPointerException.class,
                        "endpoint"),
                invalid(
                        "unsupported endpoint scheme",
                        () -> validBuilder().setEndpoint("ftp://example.com"),
                        IllegalArgumentException.class,
                        "http or https"),
                invalid(
                        "blank database",
                        () -> validBuilder().setDatabase(" "),
                        IllegalArgumentException.class,
                        "database"),
                invalid(
                        "null database",
                        () -> validBuilder().setDatabase(null),
                        IllegalArgumentException.class,
                        "database"),
                invalid(
                        "blank token",
                        () -> validBuilder().setToken(" "),
                        IllegalArgumentException.class,
                        "token"),
                invalid(
                        "managed Flink metrics sink name without managed Flink metrics",
                        () -> validBuilder().setManagedFlinkMetricsSinkName("sensors"),
                        IllegalArgumentException.class,
                        "requires managedFlinkMetricsEnabled"),
                invalid(
                        "managed Flink metrics sink name with unsupported characters",
                        () ->
                                validBuilder()
                                        .setManagedFlinkMetricsEnabled(true)
                                        .setManagedFlinkMetricsSinkName("sensors sink"),
                        IllegalArgumentException.class,
                        "managedFlinkMetricsSinkName"),
                invalid(
                        "empty managed Flink metrics sink name",
                        () ->
                                validBuilder()
                                        .setManagedFlinkMetricsEnabled(true)
                                        .setManagedFlinkMetricsSinkName(""),
                        IllegalArgumentException.class,
                        "managedFlinkMetricsSinkName"),
                invalid(
                        "batch size equals buffered request limit",
                        () -> validBuilder().setMaxBatchSize(10).setMaxBufferedRequests(10),
                        IllegalArgumentException.class,
                        "maxBufferedRequests"),
                invalid(
                        "record size exceeds batch byte limit",
                        () ->
                                validBuilder()
                                        .setMaxBatchSizeInBytes(100)
                                        .setMaxRecordSizeInBytes(101),
                        IllegalArgumentException.class,
                        "maxBatchSizeInBytes"),
                invalid(
                        "buffer byte limit cannot hold the configured request buffer",
                        () ->
                                validBuilder()
                                        .setMaxBatchSizeInBytes(1024)
                                        .setMaxRecordSizeInBytes(512)
                                        .setMaxBufferedBytes(1023),
                        IllegalArgumentException.class,
                        "maxBufferedBytes"),
                invalid(
                        "request timeout is too short for write and retry timeouts",
                        () ->
                                validBuilder()
                                        .setWriteTimeout(Duration.ofSeconds(40))
                                        .setRequestTimeout(Duration.ofSeconds(60)),
                        IllegalArgumentException.class,
                        "requestTimeout"),
                invalid(
                        "sub-millisecond write timeout",
                        () -> validBuilder().setWriteTimeout(Duration.ofNanos(1)),
                        IllegalArgumentException.class,
                        "at least 1 millisecond"),
                invalid(
                        "sub-millisecond request timeout margin",
                        () ->
                                validBuilder()
                                        .setRetryBackoff(Duration.ZERO, Duration.ZERO, 0)
                                        .setWriteTimeout(Duration.ofMillis(1))
                                        .setRequestTimeout(Duration.ofMillis(1).plusNanos(1)),
                        IllegalArgumentException.class,
                        "requestTimeout"),
                invalid(
                        "request timeout headroom below minimum",
                        () ->
                                validBuilder()
                                        .setRetryBackoff(Duration.ZERO, Duration.ofMillis(5), 0)
                                        .setWriteTimeout(Duration.ofMillis(10))
                                        .setRequestTimeout(Duration.ofMillis(16)),
                        IllegalArgumentException.class,
                        "by at least 2 ms"),
                invalid(
                        "timeout exceeds supported millisecond range",
                        () ->
                                validBuilder()
                                        .setConnectionTimeout(Duration.ofSeconds(Long.MAX_VALUE)),
                        IllegalArgumentException.class,
                        "supported millisecond range"),
                invalid(
                        "initial retry backoff exceeds maximum",
                        () ->
                                validBuilder()
                                        .setRetryBackoff(
                                                Duration.ofSeconds(2),
                                                Duration.ofSeconds(1),
                                                0),
                        IllegalArgumentException.class,
                        "initialRetryBackoff"),
                invalid(
                        "negative initial retry backoff",
                        () ->
                                validBuilder()
                                        .setRetryBackoff(
                                                Duration.ofMillis(-1), Duration.ZERO, 0),
                        IllegalArgumentException.class,
                        "initialRetryBackoff"),
                invalid(
                        "negative retry jitter",
                        () ->
                                validBuilder()
                                        .setRetryBackoff(Duration.ZERO, Duration.ZERO, -0.01),
                        IllegalArgumentException.class,
                        "retryJitter"),
                invalid(
                        "NaN retry jitter",
                        () ->
                                validBuilder()
                                        .setRetryBackoff(
                                                Duration.ZERO, Duration.ZERO, Double.NaN),
                        IllegalArgumentException.class,
                        "retryJitter"),
                invalid(
                        "infinite retry jitter",
                        () ->
                                validBuilder()
                                        .setRetryBackoff(
                                                Duration.ZERO,
                                                Duration.ZERO,
                                                Double.POSITIVE_INFINITY),
                        IllegalArgumentException.class,
                        "retryJitter"),
                invalid(
                        "retry jitter greater than one",
                        () ->
                                validBuilder()
                                        .setRetryBackoff(Duration.ZERO, Duration.ZERO, 1.01),
                        IllegalArgumentException.class,
                        "retryJitter"),
                invalid(
                        "negative retry limit",
                        () -> validBuilder().setMaxRetries(-1),
                        IllegalArgumentException.class,
                        "maxRetries"),
                invalid(
                        "custom failure handler with fail-on-rejected policy",
                        () ->
                                validBuilder()
                                        .setFailureHandler(failure -> FailureAction.DROP)
                                        .setFailOnRejected(false),
                        IllegalArgumentException.class,
                        "custom failureHandler"),
                invalid(
                        "custom failure handler with fail-on-error policy",
                        () ->
                                validBuilder()
                                        .setFailOnError(true)
                                        .setFailureHandler(failure -> FailureAction.DROP),
                        IllegalArgumentException.class,
                        "custom failureHandler"),
                invalid(
                        "relative endpoint",
                        () -> validBuilder().setEndpoint(URI.create("http:relative")),
                        IllegalArgumentException.class,
                        "host"),
                invalid(
                        "endpoint with user info",
                        () -> validBuilder().setEndpoint("https://user@example.com"),
                        IllegalArgumentException.class,
                        "user info"),
                invalid(
                        "endpoint with query",
                        () -> validBuilder().setEndpoint("https://example.com?debug=true"),
                        IllegalArgumentException.class,
                        "query"),
                invalid(
                        "endpoint with fragment",
                        () -> validBuilder().setEndpoint("https://example.com#fragment"),
                        IllegalArgumentException.class,
                        "fragment"),
                invalid(
                        "endpoint includes write API path",
                        () -> validBuilder().setEndpoint("https://example.com/api/v3/write_lp"),
                        IllegalArgumentException.class,
                        "API path"),
                invalid(
                        "zero batch size",
                        () -> validBuilder().setMaxBatchSize(0),
                        IllegalArgumentException.class,
                        "maxBatchSize"),
                invalid(
                        "zero write timeout",
                        () -> validBuilder().setWriteTimeout(Duration.ZERO),
                        IllegalArgumentException.class,
                        "writeTimeout"),
                invalid(
                        "negative write timeout",
                        () -> validBuilder().setWriteTimeout(Duration.ofMillis(-1)),
                        IllegalArgumentException.class,
                        "writeTimeout"));
    }

    private static InvalidConfiguration invalid(
            String description,
            Supplier<InfluxDB3SinkBuilder<String>> builder,
            Class<? extends Throwable> exceptionType,
            String messageFragment) {
        return new InvalidConfiguration(description, builder, exceptionType, messageFragment);
    }

    private static InfluxDB3SinkBuilder<String> validBuilder() {
        return InfluxDB3Sink.<String>builder()
                .setEndpoint("https://example.com:8086")
                .setDatabase("metrics")
                .setToken("super-secret")
                .setConverter(value -> value);
    }

    private record InvalidConfiguration(
            String description,
            Supplier<InfluxDB3SinkBuilder<String>> builder,
            Class<? extends Throwable> exceptionType,
            String messageFragment) {

        @Override
        public String toString() {
            return description;
        }
    }
}
