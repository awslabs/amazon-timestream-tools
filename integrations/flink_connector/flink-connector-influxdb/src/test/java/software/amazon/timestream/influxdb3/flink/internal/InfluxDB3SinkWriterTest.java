/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.internal;

import org.apache.flink.connector.base.sink.writer.ResultHandler;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.RequestEntryWrapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import com.influxdb.v3.client.InfluxDBApiHttpException;
import com.influxdb.v3.client.InfluxDBPartialWriteException;
import com.influxdb.v3.client.InfluxDBPartialWriteException.LineError;
import software.amazon.timestream.influxdb3.flink.InfluxDB3Sink;
import software.amazon.timestream.influxdb3.flink.InfluxDB3SinkBuilder;
import software.amazon.timestream.influxdb3.flink.InfluxDB3SinkTestAccess;
import software.amazon.timestream.influxdb3.flink.InfluxDB3SinkException;
import software.amazon.timestream.influxdb3.flink.auth.StaticTokenProvider;
import software.amazon.timestream.influxdb3.flink.auth.TokenProvider;
import software.amazon.timestream.influxdb3.flink.failure.FailureAction;
import software.amazon.timestream.influxdb3.flink.failure.WriteFailureHandler;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.SocketException;
import java.net.http.HttpHeaders;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InfluxDB3SinkWriterTest {

    @Test
    void reportsExactUtf8LineProtocolSize() throws Exception {
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        new StaticTokenProvider("token"),
                        (config, token) -> new SequenceClient(),
                        null);
        try {
            String lineProtocol = "weather,city=Z\u00fcrich temperature=20.5";

            assertThat(writer.getSizeInBytes(InfluxDB3WriteRequest.initial(lineProtocol)))
                    .isEqualTo(lineProtocol.getBytes(StandardCharsets.UTF_8).length);
            assertThat(
                            writer.getSizeInBytes(
                                    InfluxDB3WriteRequest.validationFailure("invalid line")))
                    .isZero();
        } finally {
            writer.close();
        }
    }

    @Test
    void registersManagedFlinkMetricsFromSinkConfiguration() throws Exception {
        WriterTestContext.CapturedContext metrics = WriterTestContext.capture();
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        new StaticTokenProvider("token"),
                        (config, token) -> new SequenceClient(),
                        null,
                        builder ->
                                builder.setManagedFlinkMetricsEnabled(true)
                                        .setManagedFlinkMetricsSinkName("sensors"),
                        metrics);
        try {
            assertThat(
                            metrics.counter(
                                    "KinesisAnalytics.Connector=TimestreamInfluxDB3.Sink=sensors."
                                            + "numRecordsDropped"))
                    .isSameAs(metrics.counter("numRecordsDropped"));
        } finally {
            writer.close();
        }
    }

    @Test
    void recordsSuccessfulBatchCountAndLatestPayloadSize() throws Exception {
        WriterTestContext.CapturedContext metrics = WriterTestContext.capture();
        SequenceClient client = new SequenceClient();
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        new StaticTokenProvider("token"),
                        (config, token) -> client,
                        null,
                        builder -> {},
                        metrics);
        String first = "weather,city=Z\u00fcrich temperature=20.5";
        String second = "cpu value=2i";
        try {
            CapturingResultHandler handler = new CapturingResultHandler();
            writer.submitRequestEntries(
                    List.of(
                            InfluxDB3WriteRequest.initial(first),
                            InfluxDB3WriteRequest.initial(second)),
                    handler);

            handler.await();

            assertThat(handler.completed).isTrue();
            assertThat(metrics.counter("numRecordsWritten").getCount()).isEqualTo(2);
            assertThat(metrics.counter("numRecordsSendErrors").getCount()).isZero();
            assertThat(metrics.counter("numRecordsDropped").getCount()).isZero();
            assertThat(metrics.counter("numRetries").getCount()).isZero();
            assertThat(metrics.counter("numPartialWriteFailures").getCount()).isZero();
            assertThat(metrics.counter("numAuthenticationRefreshes").getCount()).isZero();
            assertThat(metrics.counter("numThrottledRequests").getCount()).isZero();
            assertThat(metrics.counter("numWriteTimeouts").getCount()).isZero();
            assertThat(metrics.counter("numBatchesSuccess").getCount()).isEqualTo(1);
            assertThat(metrics.counter("numBatchesCompletedWithDrops").getCount()).isZero();
            assertThat(metrics.counter("numBatchesRetried").getCount()).isZero();
            assertThat(metrics.counter("numBatchesFailed").getCount()).isZero();
            assertThat(metrics.histogram("writeLatencyMillis").getCount()).isEqualTo(1);
            assertThat(metrics.gaugeValue("lastWriteLatencyMillis"))
                    .isGreaterThanOrEqualTo(0);
            assertThat(metrics.gaugeValue("batchSizeLines")).isEqualTo(2);
            assertThat(metrics.gaugeValue("batchSizeBytes"))
                    .isEqualTo(
                            first.getBytes(StandardCharsets.UTF_8).length
                                    + 1L
                                    + second.getBytes(StandardCharsets.UTF_8).length);
            assertThat(metrics.counter("numBytesWritten").getCount())
                    .isEqualTo(metrics.gaugeValue("batchSizeBytes"));
            assertThat(metrics.counter("numRetryDelayMillis").getCount()).isZero();
            assertThat(metrics.counter("numServerErrors").getCount()).isZero();
            assertThat(metrics.counter("numClientErrors").getCount()).isZero();
            assertThat(metrics.counter("numConnectionErrors").getCount()).isZero();
            assertThat(metrics.gaugeValue("maxRetryAfterMillis")).isZero();
            assertThat(metrics.gaugeValue("numBufferedBytes")).isZero();
            awaitGaugeValue(metrics, "numInFlightWriteRequests", 0L);
            assertThat(metrics.gaugeValue("numActiveClientGenerations")).isEqualTo(1);
        } finally {
            writer.close();
        }
    }

    @Test
    void countsARetriedBatchOnceRegardlessOfRecordCount() throws Exception {
        WriterTestContext.CapturedContext metrics = WriterTestContext.capture();
        SequenceClient client =
                new SequenceClient(new InfluxDBApiHttpException("unavailable", null, 503));
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        new StaticTokenProvider("token"),
                        (config, token) -> client,
                        null,
                        builder -> {},
                        metrics);
        try {
            CapturingResultHandler handler = new CapturingResultHandler();
            writer.submitRequestEntries(
                    List.of(
                            InfluxDB3WriteRequest.initial("cpu value=1i"),
                            InfluxDB3WriteRequest.initial("cpu value=2i")),
                    handler);

            handler.await();

            assertThat(handler.retryEntries).hasSize(2);
            assertThat(metrics.counter("numServerErrors").getCount()).isEqualTo(1);
            assertThat(metrics.counter("numClientErrors").getCount()).isZero();
            assertThat(metrics.counter("numConnectionErrors").getCount()).isZero();
            assertThat(metrics.counter("numBytesWritten").getCount()).isZero();
            assertThat(metrics.counter("numRecordsWritten").getCount()).isZero();
            assertThat(metrics.counter("numRecordsSendErrors").getCount()).isEqualTo(2);
            assertThat(metrics.counter("numRetries").getCount()).isEqualTo(2);
            assertThat(metrics.counter("numBatchesSuccess").getCount()).isZero();
            assertThat(metrics.counter("numBatchesCompletedWithDrops").getCount()).isZero();
            assertThat(metrics.counter("numBatchesRetried").getCount()).isEqualTo(1);
            assertThat(metrics.counter("numBatchesFailed").getCount()).isZero();
            assertThat(metrics.histogram("writeLatencyMillis").getCount()).isEqualTo(1);
        } finally {
            writer.close();
        }
    }

    @Test
    void countsATerminalBatchFailureOnce() throws Exception {
        WriterTestContext.CapturedContext metrics = WriterTestContext.capture();
        SequenceClient client =
                new SequenceClient(new InfluxDBApiHttpException("too large", null, 413));
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        new StaticTokenProvider("token"),
                        (config, token) -> client,
                        null,
                        builder -> {},
                        metrics);
        try {
            CapturingResultHandler handler = new CapturingResultHandler();
            writer.submitRequestEntries(
                    List.of(InfluxDB3WriteRequest.initial("cpu value=1i")), handler);

            handler.await();

            assertThat(handler.failure).isInstanceOf(InfluxDB3SinkException.class);
            assertThat(metrics.counter("numBatchesSuccess").getCount()).isZero();
            assertThat(metrics.counter("numBatchesCompletedWithDrops").getCount()).isZero();
            assertThat(metrics.counter("numBatchesRetried").getCount()).isZero();
            assertThat(metrics.counter("numBatchesFailed").getCount()).isEqualTo(1);
        } finally {
            writer.close();
        }
    }

    @Test
    void retriesTransientFailuresAndPreservesRetryMetadata() throws Exception {
        SequenceClient client =
                new SequenceClient(new InfluxDBApiHttpException("unavailable", null, 503));
        InfluxDB3SinkWriter<String> writer =
                newWriter(new StaticTokenProvider("token"), (config, token) -> client, null);
        try {
            CapturingResultHandler first = new CapturingResultHandler();
            writer.submitRequestEntries(
                    List.of(InfluxDB3WriteRequest.initial("cpu value=1i")), first);

            first.await();
            assertThat(first.retryEntries).singleElement()
                    .satisfies(
                            request -> {
                                assertThat(request.getLineProtocol()).isEqualTo("cpu value=1i");
                                assertThat(request.getRetryAttempt()).isEqualTo(1);
                                assertThat(request.getAuthenticationRefreshes()).isZero();
                            });

            CapturingResultHandler second = new CapturingResultHandler();
            writer.submitRequestEntries(first.retryEntries, second);

            second.await();
            assertThat(second.completed).isTrue();
            assertThat(client.writeCalls).isEqualTo(2);
        } finally {
            writer.close();
        }
    }

    @Test
    void capsRetryAfterAtConfiguredMaximumBackoff() throws Exception {
        HttpHeaders headers =
                HttpHeaders.of(Map.of("Retry-After", List.of("120")), (name, value) -> true);
        WriterTestContext.CapturedContext metrics = WriterTestContext.capture();
        SequenceClient client =
                new SequenceClient(new InfluxDBApiHttpException("rate limited", headers, 429));
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        new StaticTokenProvider("token"),
                        (config, token) -> client,
                        null,
                        builder -> {},
                        metrics);
        try {
            CapturingResultHandler handler = new CapturingResultHandler();
            writer.submitRequestEntries(
                    List.of(InfluxDB3WriteRequest.initial("cpu value=1i")), handler);

            handler.await();

            assertThat(handler.retryEntries).singleElement()
                    .satisfies(request -> assertThat(request.getRetryAttempt()).isEqualTo(1));
            assertThat(metrics.counter("numRecordsSendErrors").getCount()).isEqualTo(1);
            assertThat(metrics.counter("numRetries").getCount()).isEqualTo(1);
            assertThat(metrics.counter("numThrottledRequests").getCount()).isEqualTo(1);
            assertThat(metrics.counter("numBatchesRetried").getCount()).isEqualTo(1);
            assertThat(metrics.gaugeValue("maxRetryAfterMillis")).isEqualTo(120_000);
            assertThat(metrics.counter("numRetryDelayMillis").getCount()).isZero();
            assertThat(metrics.counter("numServerErrors").getCount()).isZero();
            assertThat(metrics.counter("numClientErrors").getCount()).isZero();
        } finally {
            writer.close();
        }
    }

    @Test
    void retriesOnlyRejectedLineWhenServerNormalizesOriginalLine() throws Exception {
        InfluxDBPartialWriteException partialWrite =
                new InfluxDBPartialWriteException(
                        "partial write",
                        null,
                        400,
                        List.of(new LineError(2, "field type conflict", "cpu value=bad")));
        SequenceClient client = new SequenceClient(partialWrite);
        WriteFailureHandler retryPartial = failure -> FailureAction.RETRY;
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        new StaticTokenProvider("token"),
                        (config, token) -> client,
                        retryPartial);
        try {
            CapturingResultHandler handler = new CapturingResultHandler();
            writer.submitRequestEntries(
                    List.of(
                            InfluxDB3WriteRequest.initial("cpu value=1i"),
                            InfluxDB3WriteRequest.initial("cpu value=\"bad\"")),
                    handler);

            handler.await();

            assertThat(handler.retryEntries).singleElement()
                    .satisfies(
                            request -> {
                                assertThat(request.getLineProtocol())
                                        .isEqualTo("cpu value=\"bad\"");
                                assertThat(request.getRetryAttempt()).isEqualTo(1);
                            });
        } finally {
            writer.close();
        }
    }

    @Test
    void prefersExactOriginalLineWhenItDisagreesWithLineNumber() throws Exception {
        InfluxDBPartialWriteException partialWrite =
                new InfluxDBPartialWriteException(
                        "partial write",
                        null,
                        400,
                        List.of(new LineError(1, "field type conflict", "cpu value=2i")));
        SequenceClient client = new SequenceClient(partialWrite);
        List<String> rejectedLines = new ArrayList<>();
        WriteFailureHandler captureAndDrop =
                failure -> {
                    rejectedLines.add(failure.getLineProtocol());
                    return FailureAction.DROP;
                };
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        new StaticTokenProvider("token"),
                        (config, token) -> client,
                        captureAndDrop);
        try {
            CapturingResultHandler handler = new CapturingResultHandler();
            writer.submitRequestEntries(
                    List.of(
                            InfluxDB3WriteRequest.initial("cpu value=1i"),
                            InfluxDB3WriteRequest.initial("cpu value=2i")),
                    handler);

            handler.await();

            assertThat(handler.completed).isTrue();
            assertThat(rejectedLines).containsExactly("cpu value=2i");
        } finally {
            writer.close();
        }
    }

    @Test
    void dropsRejectedPartialWriteLinesByDefault() throws Exception {
        WriterTestContext.CapturedContext metrics = WriterTestContext.capture();
        SequenceClient client =
                new SequenceClient(
                        new InfluxDBPartialWriteException(
                                "partial write",
                                null,
                                400,
                                List.of(new LineError(1, "invalid line", "invalid"))));
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        new StaticTokenProvider("token"),
                        (config, token) -> client,
                        null,
                        builder -> {},
                        metrics);
        try {
            CapturingResultHandler handler = new CapturingResultHandler();
            writer.submitRequestEntries(
                    List.of(
                            InfluxDB3WriteRequest.initial("invalid"),
                            InfluxDB3WriteRequest.initial("cpu value=2i")),
                    handler);

            handler.await();

            assertThat(handler.completed).isTrue();
            assertThat(handler.retryEntries).isNull();
            assertThat(handler.failure).isNull();
            assertThat(metrics.counter("numRecordsWritten").getCount()).isEqualTo(1);
            assertThat(metrics.counter("numRecordsSendErrors").getCount()).isEqualTo(1);
            assertThat(metrics.counter("numRecordsDropped").getCount()).isEqualTo(1);
            assertThat(metrics.counter("numPartialWriteFailures").getCount()).isEqualTo(1);
            assertThat(metrics.counter("numBatchesSuccess").getCount()).isZero();
            assertThat(metrics.counter("numBatchesCompletedWithDrops").getCount()).isEqualTo(1);
            assertThat(metrics.counter("numBatchesRetried").getCount()).isZero();
            assertThat(metrics.counter("numBatchesFailed").getCount()).isZero();
            assertThat(metrics.counter("numBytesWritten").getCount())
                    .isEqualTo("cpu value=2i".length());
            assertThat(metrics.counter("numClientErrors").getCount()).isEqualTo(1);
        } finally {
            writer.close();
        }
    }

    @Test
    void countsSuccessfulWritesWithLocalValidationDropsAsCompletedWithDrops()
            throws Exception {
        WriterTestContext.CapturedContext metrics = WriterTestContext.capture();
        SequenceClient client = new SequenceClient();
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        new StaticTokenProvider("token"),
                        (config, token) -> client,
                        null,
                        builder -> builder.setFailOnValidation(false),
                        metrics);
        try {
            CapturingResultHandler handler = new CapturingResultHandler();
            writer.submitRequestEntries(
                    List.of(
                            InfluxDB3WriteRequest.validationFailure("invalid line"),
                            InfluxDB3WriteRequest.initial("cpu value=2i")),
                    handler);

            handler.await();

            assertThat(handler.completed).isTrue();
            assertThat(client.lastRecords).containsExactly("cpu value=2i");
            assertThat(metrics.counter("numRecordsWritten").getCount()).isEqualTo(1);
            assertThat(metrics.counter("numRecordsSendErrors").getCount()).isEqualTo(1);
            assertThat(metrics.counter("numRecordsDropped").getCount()).isEqualTo(1);
            assertThat(metrics.counter("numBatchesSuccess").getCount()).isZero();
            assertThat(metrics.counter("numBatchesCompletedWithDrops").getCount()).isEqualTo(1);
            assertThat(metrics.counter("numBatchesRetried").getCount()).isZero();
            assertThat(metrics.counter("numBatchesFailed").getCount()).isZero();
        } finally {
            writer.close();
        }
    }

    @Test
    void failsRejectedPartialWriteLinesWhenConfigured() throws Exception {
        SequenceClient client =
                new SequenceClient(
                        new InfluxDBPartialWriteException(
                                "partial write",
                                null,
                                400,
                                List.of(new LineError(1, "invalid line", "invalid"))));
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        new StaticTokenProvider("token"),
                        (config, token) -> client,
                        null,
                        builder -> builder.setFailOnRejected(true));
        try {
            CapturingResultHandler handler = new CapturingResultHandler();
            writer.submitRequestEntries(
                    List.of(InfluxDB3WriteRequest.initial("invalid")), handler);

            handler.await();

            assertThat(handler.failure)
                    .isInstanceOf(InfluxDB3SinkException.class)
                    .hasMessageContaining("non-retryable");
        } finally {
            writer.close();
        }
    }

    @Test
    void refreshesAuthenticationOnceAndUsesNewClient() throws Exception {
        WriterTestContext.CapturedContext metrics = WriterTestContext.capture();
        RotatingTokenProvider provider = new RotatingTokenProvider();
        SequenceClient staleClient =
                new SequenceClient(new InfluxDBApiHttpException("unauthorized", null, 401));
        SequenceClient refreshedClient = new SequenceClient();
        List<String> createdWithTokens = new ArrayList<>();
        InfluxDBClientFactory factory =
                (config, token) -> {
                    String value = new String(token);
                    createdWithTokens.add(value);
                    return "old-token".equals(value) ? staleClient : refreshedClient;
                };
        InfluxDB3SinkWriter<String> writer =
                newWriter(provider, factory, null, builder -> {}, metrics);
        try {
            CapturingResultHandler first = new CapturingResultHandler();
            writer.submitRequestEntries(
                    List.of(InfluxDB3WriteRequest.initial("cpu value=1i")), first);

            first.await();
            assertThat(first.retryEntries).singleElement()
                    .satisfies(
                            request -> {
                                assertThat(request.getRetryAttempt()).isZero();
                                assertThat(request.getAuthenticationRefreshes()).isEqualTo(1);
                            });

            CapturingResultHandler second = new CapturingResultHandler();
            writer.submitRequestEntries(first.retryEntries, second);
            second.await();

            assertThat(second.completed).isTrue();
            assertThat(provider.refreshes.get()).isEqualTo(1);
            assertThat(metrics.counter("numRecordsSendErrors").getCount()).isEqualTo(1);
            assertThat(metrics.counter("numAuthenticationRefreshes").getCount()).isEqualTo(1);
            assertThat(createdWithTokens).containsExactly("old-token", "new-token");
            assertThat(staleClient.closeCalls).isEqualTo(1);
            assertThat(refreshedClient.closeCalls).isZero();
            assertThat(refreshedClient.writeCalls).isEqualTo(1);
            assertThat(
                            fieldValue(
                                            writer,
                                            "clientManager",
                                            InfluxDBClientManager.class)
                                    .clientCount())
                    .isEqualTo(1);
        } finally {
            writer.close();
        }
        assertThat(refreshedClient.closeCalls).isEqualTo(1);
    }

    @Test
    void failsWhenRefreshedAuthenticationIsRejected() throws Exception {
        RotatingTokenProvider provider = new RotatingTokenProvider();
        InfluxDBClientFactory factory =
                (config, token) ->
                        new SequenceClient(
                                new InfluxDBApiHttpException("unauthorized", null, 401));
        InfluxDB3SinkWriter<String> writer = newWriter(provider, factory, null);
        try {
            CapturingResultHandler first = new CapturingResultHandler();
            writer.submitRequestEntries(
                    List.of(InfluxDB3WriteRequest.initial("cpu value=1i")), first);
            first.await();

            CapturingResultHandler second = new CapturingResultHandler();
            writer.submitRequestEntries(first.retryEntries, second);
            second.await();

            assertThat(second.failure)
                    .isInstanceOf(InfluxDB3SinkException.class)
                    .hasMessageContaining("after one token refresh");
            assertThat(provider.refreshes.get()).isEqualTo(1);
        } finally {
            writer.close();
        }
    }

    @Test
    void failsImmediatelyForPermanentBatchErrors() throws Exception {
        SequenceClient client =
                new SequenceClient(new InfluxDBApiHttpException("bad request", null, 400));
        InfluxDB3SinkWriter<String> writer =
                newWriter(new StaticTokenProvider("token"), (config, token) -> client, null);
        try {
            CapturingResultHandler handler = new CapturingResultHandler();
            writer.submitRequestEntries(
                    List.of(InfluxDB3WriteRequest.initial("cpu value=1i")), handler);

            handler.await();

            assertThat(handler.failure)
                    .isInstanceOf(InfluxDB3SinkException.class)
                    .hasMessageContaining("non-retryable");
        } finally {
            writer.close();
        }
    }

    @Test
    void dropsLocalAndHttp400Or422ValidationWhenConfigured() throws Exception {
        AtomicInteger handlerCalls = new AtomicInteger();
        WriteFailureHandler customHandler =
                failure -> {
                    handlerCalls.incrementAndGet();
                    return FailureAction.FAIL;
                };
        SequenceClient client =
                new SequenceClient(
                        new InfluxDBApiHttpException("bad request", null, 400),
                        new InfluxDBApiHttpException("unprocessable entity", null, 422));
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        new StaticTokenProvider("token"),
                        (config, token) -> client,
                        customHandler,
                        builder -> builder.setFailOnValidation(false));
        try {
            CapturingResultHandler localValidation = new CapturingResultHandler();
            writer.submitRequestEntries(
                    List.of(
                            InfluxDB3WriteRequest.validationFailure(
                                    "converter returned multiple lines")),
                    localValidation);
            localValidation.await();

            CapturingResultHandler http400Validation = new CapturingResultHandler();
            writer.submitRequestEntries(
                    List.of(InfluxDB3WriteRequest.initial("cpu value=\"bad\"")),
                    http400Validation);
            http400Validation.await();

            CapturingResultHandler http422Validation = new CapturingResultHandler();
            writer.submitRequestEntries(
                    List.of(InfluxDB3WriteRequest.initial("cpu value=\"also-bad\"")),
                    http422Validation);
            http422Validation.await();

            assertThat(localValidation.completed).isTrue();
            assertThat(http400Validation.completed).isTrue();
            assertThat(http422Validation.completed).isTrue();
            assertThat(client.writeCalls).isEqualTo(2);
            assertThat(client.lastRecords).containsExactly("cpu value=\"also-bad\"");
            assertThat(handlerCalls).hasValue(0);
        } finally {
            writer.close();
        }
    }

    @Test
    void keepsHttp413FatalWithCustomHandler() throws Exception {
        SequenceClient client =
                new SequenceClient(new InfluxDBApiHttpException("too large", null, 413));
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        new StaticTokenProvider("token"),
                        (config, token) -> client,
                        failure -> FailureAction.DROP);
        try {
            CapturingResultHandler handler = new CapturingResultHandler();
            writer.submitRequestEntries(
                    List.of(InfluxDB3WriteRequest.initial("cpu value=1i")), handler);

            handler.await();

            assertThat(handler.failure)
                    .isInstanceOf(InfluxDB3SinkException.class)
                    .hasMessageContaining("HTTP 413");
        } finally {
            writer.close();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("fatalClientErrors")
    void keepsClientErrorsFatalWithoutInvokingCustomHandler(Error clientError) throws Exception {
        AtomicInteger handlerCalls = new AtomicInteger();
        InfluxDBWriteClient client =
                new InfluxDBWriteClient() {
                    @Override
                    public void writeRecords(List<String> records) {
                        throw clientError;
                    }

                    @Override
                    public void close() {}
                };
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        new StaticTokenProvider("token"),
                        (config, token) -> client,
                        failure -> {
                            handlerCalls.incrementAndGet();
                            return FailureAction.DROP;
                        });
        try {
            CapturingResultHandler handler = new CapturingResultHandler();
            writer.submitRequestEntries(
                    List.of(InfluxDB3WriteRequest.initial("cpu value=1i")), handler);

            handler.await();

            assertThat(handler.failure)
                    .isInstanceOf(InfluxDB3SinkException.class)
                    .hasMessageContaining("Unexpected error");
            assertThat(handler.failure.getCause()).isSameAs(clientError);
            assertThat(handlerCalls).hasValue(0);
        } finally {
            writer.close();
        }
    }

    @Test
    void snapshotsRecordsThatHaveNotReachedTheFlushThreshold() throws Exception {
        WriterTestContext.CapturedContext metrics = WriterTestContext.capture();
        SequenceClient client = new SequenceClient();
        InfluxDB3SinkConfig config =
                InfluxDB3SinkTestAccess.configurationOf(
                        InfluxDB3Sink.<String>builder()
                                .setEndpoint("http://localhost:8086")
                                .setDatabase("metrics")
                                .setToken("token")
                                .setConverter(value -> value)
                                .setMaxBatchSize(2)
                                .setMaxBufferedRequests(3)
                                .setMaxInFlightRequests(1)
                                .build());
        InfluxDB3SinkWriter<String> writer =
                new InfluxDB3SinkWriter<>(
                        new LineProtocolElementConverter<>(
                                value -> value,
                                true,
                                false,
                                config.getMaxRecordSizeInBytes()),
                        metrics.context(),
                        config,
                        (ignoredConfig, ignoredToken) -> client,
                        List.of());
        try {
            String lineProtocol = "cpu value=1i";
            writer.write(lineProtocol, null);

            List<BufferedRequestState<InfluxDB3WriteRequest>> state =
                    writer.snapshotState(42L);

            assertThat(state).singleElement()
                    .satisfies(
                            buffered ->
                                    assertThat(buffered.getBufferedRequestEntries())
                                            .singleElement()
                                            .satisfies(
                                                    entry ->
                                                            assertThat(
                                                                            entry.getRequestEntry()
                                                                                    .getLineProtocol())
                                                                    .isEqualTo(lineProtocol)));
            assertThat(client.writeCalls).isZero();
            assertThat(metrics.gaugeValue("numBufferedBytes"))
                    .isEqualTo(lineProtocol.getBytes(StandardCharsets.UTF_8).length);
        } finally {
            writer.close();
        }
    }

    @Test
    void flushesBeforeAddingARecordThatCouldExceedTheBufferedByteLimit() throws Exception {
        SequenceClient client = new SequenceClient();
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        new StaticTokenProvider("token"),
                        (config, token) -> client,
                        null,
                        builder ->
                                builder.setMaxBatchSize(10)
                                        .setMaxBufferedRequests(11)
                                        .setMaxBatchSizeInBytes(10)
                                        .setMaxBufferedBytes(10)
                                        .setMaxRecordSizeInBytes(6)
                                        .setMaxTimeInBufferMS(60_000),
                        WriterTestContext.captureWithQueuedMailbox());
        try {
            writer.write("abcde", null);
            assertThat(client.writeCalls).isZero();

            writer.write("fgh", null);

            assertThat(client.writeCalls).isEqualTo(1);
            assertThat(client.lastRecords).containsExactly("abcde");
            assertThat(writer.snapshotState(42L)).singleElement()
                    .satisfies(
                            state ->
                                    assertThat(state.getBufferedRequestEntries())
                                            .singleElement()
                                            .satisfies(
                                                    entry ->
                                                            assertThat(
                                                                            entry.getRequestEntry()
                                                                                    .getLineProtocol())
                                                                    .isEqualTo("fgh")));
        } finally {
            writer.close();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("restoredFailureLimitCases")
    void preservesFailureLimitsFromRestoredState(RestoredFailureLimitCase testCase)
            throws Exception {
        RotatingTokenProvider provider = new RotatingTokenProvider();
        SequenceClient client =
                new SequenceClient(
                        new InfluxDBApiHttpException(
                                "restored request failure", null, testCase.statusCode()));
        InfluxDB3WriteRequest recoveredRequest =
                new InfluxDB3WriteRequest(
                        "cpu value=1i",
                        testCase.retryAttempt(),
                        testCase.authenticationRefreshes());
        BufferedRequestState<InfluxDB3WriteRequest> recoveredState =
                new BufferedRequestState<>(
                        List.of(
                                new RequestEntryWrapper<>(
                                        recoveredRequest,
                                        recoveredRequest
                                                .getLineProtocol()
                                                .getBytes(StandardCharsets.UTF_8)
                                                .length)));
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        provider,
                        (config, token) -> client,
                        null,
                        builder -> builder.setMaxRetries(testCase.maxRetries()),
                        WriterTestContext.capture(),
                        List.of(recoveredState));
        try {
            List<InfluxDB3WriteRequest> restoredRequests =
                    writer.snapshotState(42L).get(0).getBufferedRequestEntries().stream()
                            .map(RequestEntryWrapper::getRequestEntry)
                            .toList();
            assertThat(restoredRequests).singleElement()
                    .satisfies(
                            request -> {
                                assertThat(request.getRetryAttempt())
                                        .isEqualTo(testCase.retryAttempt());
                                assertThat(request.getAuthenticationRefreshes())
                                        .isEqualTo(testCase.authenticationRefreshes());
                            });

            CapturingResultHandler handler = new CapturingResultHandler();
            writer.submitRequestEntries(restoredRequests, handler);
            handler.await();

            assertThat(handler.failure)
                    .isInstanceOf(InfluxDB3SinkException.class)
                    .hasMessageContaining(testCase.expectedMessage());
            assertThat(provider.refreshes).hasValue(0);
            assertThat(client.writeCalls).isEqualTo(1);
        } finally {
            writer.close();
        }
    }

    @Test
    void oversizedLocalValidationFailuresBypassPayloadByteLimitsWhenDropped() throws Exception {
        String invalidPayload = "private-value".repeat(100_000);
        SequenceClient client = new SequenceClient();
        InfluxDB3SinkConfig config =
                InfluxDB3SinkTestAccess.configurationOf(
                        InfluxDB3Sink.<String>builder()
                                .setEndpoint("http://localhost:8086")
                                .setDatabase("metrics")
                                .setToken("token")
                                .setConverter(ignored -> invalidPayload)
                                .setFailOnValidation(false)
                                .setMaxBatchSizeInBytes(64)
                                .setMaxBufferedBytes(64)
                                .setMaxRecordSizeInBytes(32)
                                .build());
        InfluxDB3SinkWriter<String> writer =
                new InfluxDB3SinkWriter<>(
                        new LineProtocolElementConverter<>(
                                ignored -> invalidPayload,
                                false,
                                false,
                                config.getMaxRecordSizeInBytes()),
                        WriterTestContext.create(),
                        config,
                        (ignoredConfig, ignoredToken) -> client,
                        List.of());
        try {
            writer.write("input", null);

            assertThat(writer.snapshotState(42L)).singleElement()
                    .satisfies(
                            state ->
                                    assertThat(state.getBufferedRequestEntries())
                                            .singleElement()
                                            .satisfies(
                                                    entry -> {
                                                        assertThat(entry.getSize()).isZero();
                                                        assertThat(
                                                                        entry.getRequestEntry()
                                                                                .isValidationFailure())
                                                                .isTrue();
                                                        assertThat(
                                                                        entry.getRequestEntry()
                                                                                .getLineProtocol())
                                                                .isNull();
                                                    }));
            assertThat(client.writeCalls).isZero();
        } finally {
            writer.close();
        }
    }

    @Test
    void rejectsWritesAfterCloseAndAllowsRepeatedClose() throws Exception {
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        new StaticTokenProvider("token"),
                        (config, token) -> new SequenceClient(),
                        null);

        writer.close();
        writer.close();
        CapturingResultHandler handler = new CapturingResultHandler();
        writer.submitRequestEntries(
                List.of(InfluxDB3WriteRequest.initial("cpu value=1i")), handler);

        handler.await();
        assertThat(handler.failure)
                .isInstanceOf(InfluxDB3SinkException.class)
                .hasMessageContaining("after the sink writer closed");
    }

    @Test
    void reportsWriteExecutorRejections() throws Exception {
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        new StaticTokenProvider("token"),
                        (config, token) -> new SequenceClient(),
                        null);
        try {
            BlockingWriteExecutor executor =
                    fieldValue(
                            writer,
                            "writeExecutor",
                            BlockingWriteExecutor.class);
            fieldValue(executor, "writeExecutor", ThreadPoolExecutor.class)
                    .shutdownNow();
            CapturingResultHandler handler = new CapturingResultHandler();

            writer.submitRequestEntries(
                    List.of(InfluxDB3WriteRequest.initial("cpu value=1i")), handler);

            handler.await();
            assertThat(handler.failure)
                    .isInstanceOf(InfluxDB3SinkException.class)
                    .hasMessageContaining("write executor rejected");
        } finally {
            writer.close();
        }
    }

    @Test
    void reportsSchedulerRejections() throws Exception {
        WriterTestContext.CapturedContext metrics = WriterTestContext.capture();
        BlockingRetryableClient client = new BlockingRetryableClient();
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        new StaticTokenProvider("token"),
                        (config, token) -> client,
                        null,
                        builder -> {},
                        metrics);
        try {
            CapturingResultHandler handler = new CapturingResultHandler();
            writer.submitRequestEntries(
                    List.of(InfluxDB3WriteRequest.initial("cpu value=1i")), handler);
            client.awaitStarted();

            BlockingWriteExecutor executor =
                    fieldValue(
                            writer,
                            "writeExecutor",
                            BlockingWriteExecutor.class);
            fieldValue(
                            executor,
                            "retryScheduler",
                            ScheduledThreadPoolExecutor.class)
                    .shutdownNow();
            client.release();

            handler.await();
            assertThat(handler.failure)
                    .isInstanceOf(InfluxDB3SinkException.class)
                    .hasMessageContaining("Unable to schedule an InfluxDB write retry");
            assertThat(metrics.counter("numRetries").getCount()).isZero();
        } finally {
            client.release();
            writer.close();
        }
    }

    @Test
    void retryCallbacksDoNotDelayWriteTimeouts() throws Exception {
        BlockingClient client = new BlockingClient();
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        new StaticTokenProvider("token"),
                        (config, token) -> client,
                        null,
                        builder ->
                                builder.setMaxInFlightRequests(2)
                                        .setMaxRetries(0)
                                        .setWriteTimeout(Duration.ofMillis(25))
                                        .setRequestTimeout(Duration.ofMillis(250))
                                        .setRetryBackoff(
                                                Duration.ofMillis(1),
                                                Duration.ofMillis(1),
                                                0));
        CountDownLatch retryStarted = new CountDownLatch(1);
        CountDownLatch releaseRetry = new CountDownLatch(1);
        try {
            BlockingWriteExecutor executor =
                    fieldValue(
                            writer,
                            "writeExecutor",
                            BlockingWriteExecutor.class);
            ScheduledThreadPoolExecutor timeoutScheduler =
                    fieldValue(
                            executor,
                            "timeoutScheduler",
                            ScheduledThreadPoolExecutor.class);
            ScheduledThreadPoolExecutor retryScheduler =
                    fieldValue(
                            executor,
                            "retryScheduler",
                            ScheduledThreadPoolExecutor.class);
            assertThat(timeoutScheduler).isNotSameAs(retryScheduler);
            assertThat(timeoutScheduler.getCorePoolSize()).isEqualTo(2);

            ResultHandler<InfluxDB3WriteRequest> blockingRetryHandler =
                    new ResultHandler<>() {
                        @Override
                        public void complete() {
                            throw new AssertionError("unexpected completion");
                        }

                        @Override
                        public void completeExceptionally(Exception exception) {
                            throw new AssertionError(
                                    "unexpected exceptional completion", exception);
                        }

                        @Override
                        public void retryForEntries(
                                List<InfluxDB3WriteRequest> requests) {
                            retryStarted.countDown();
                            try {
                                releaseRetry.await();
                            } catch (InterruptedException error) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    };
            executor.scheduleRetry(
                    List.of(InfluxDB3WriteRequest.initial("blocked value=1i")),
                    Duration.ZERO,
                    new SingleCompletionResultHandler<>(
                            blockingRetryHandler, ignored -> {}));
            assertThat(retryStarted.await(5, TimeUnit.SECONDS))
                    .as("blocking retry callback")
                    .isTrue();

            CapturingResultHandler handler = new CapturingResultHandler();
            writer.submitRequestEntries(
                    List.of(InfluxDB3WriteRequest.initial("cpu value=1i")), handler);
            client.awaitStarted();

            assertThat(handler.completedLatch.await(1, TimeUnit.SECONDS))
                    .as("write timeout while retry callback is blocked")
                    .isTrue();
            assertThat(handler.failure)
                    .isInstanceOf(InfluxDB3SinkException.class)
                    .hasMessageContaining("exhausted 0 retries");
            client.awaitInterrupted();
        } finally {
            releaseRetry.countDown();
            writer.close();
        }
    }

    @Test
    void closeWaitsForPendingDelayedRetryCallbacks() throws Exception {
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        new StaticTokenProvider("token"),
                        (config, token) -> new SequenceClient(),
                        null);
        CapturingResultHandler handler = new CapturingResultHandler();
        List<InfluxDB3WriteRequest> retry =
                List.of(InfluxDB3WriteRequest.initial("cpu value=1i").nextRetry());
        try {
            BlockingWriteExecutor executor =
                    fieldValue(
                            writer,
                            "writeExecutor",
                            BlockingWriteExecutor.class);
            executor.scheduleRetry(
                    retry,
                    Duration.ofMillis(25),
                    new SingleCompletionResultHandler<>(handler, ignored -> {}));

            writer.close();

            handler.await();
            assertThat(handler.retryEntries).containsExactlyElementsOf(retry);
            assertThat(handler.callbackCount).hasValue(1);
        } finally {
            writer.close();
        }
    }

    @Test
    void closeSharesOneDeadlineAcrossWriteAndRetryShutdown() throws Exception {
        long requestTimeoutMillis = 1_000L;
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        new StaticTokenProvider("token"),
                        (config, token) -> new SequenceClient(),
                        null,
                        builder ->
                                builder.setWriteTimeout(Duration.ofMillis(10))
                                        .setRequestTimeout(Duration.ofMillis(requestTimeoutMillis))
                                        .setRetryBackoff(
                                                Duration.ofMillis(10),
                                                Duration.ofMillis(10),
                                                0));
        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch taskBlocker = new CountDownLatch(1);
        CapturingResultHandler handler = new CapturingResultHandler();
        List<InfluxDB3WriteRequest> retry =
                List.of(InfluxDB3WriteRequest.initial("cpu value=1i").nextRetry());
        try {
            BlockingWriteExecutor executor =
                    fieldValue(
                            writer,
                            "writeExecutor",
                            BlockingWriteExecutor.class);
            ThreadPoolExecutor rawWriteExecutor =
                    fieldValue(
                            executor,
                            "writeExecutor",
                            ThreadPoolExecutor.class);
            rawWriteExecutor.execute(
                    () -> {
                        taskStarted.countDown();
                        try {
                            taskBlocker.await();
                        } catch (InterruptedException error) {
                            // Scheduled after the write executor used the whole close budget.
                            executor.scheduleRetry(
                                    retry,
                                    Duration.ofSeconds(5),
                                    new SingleCompletionResultHandler<>(
                                            handler, ignored -> {}));
                            Thread.currentThread().interrupt();
                        }
                    });
            assertThat(taskStarted.await(5, TimeUnit.SECONDS))
                    .as("write executor task")
                    .isTrue();

            long startNanos = System.nanoTime();
            assertThatThrownBy(writer::close)
                    .isInstanceOf(InfluxDB3SinkException.class)
                    .hasRootCauseMessage(
                            "InfluxDB retry scheduler did not terminate before its close timeout");
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

            // An independent per-executor budget would take about 2 x requestTimeout here.
            assertThat(elapsedMillis).isLessThan(requestTimeoutMillis + 600L);
            assertThat(handler.callbackCount).hasValue(0);
        } finally {
            taskBlocker.countDown();
            writer.close();
        }
    }

    @Test
    void closeAllowsRunningWriteToScheduleRetryBeforeSchedulerShutdown() throws Exception {
        BlockingRetryableClient client = new BlockingRetryableClient();
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        new StaticTokenProvider("token"),
                        (config, token) -> client,
                        null,
                        builder ->
                                builder.setWriteTimeout(Duration.ofSeconds(1))
                                        .setRequestTimeout(Duration.ofSeconds(2))
                                        .setRetryBackoff(
                                                Duration.ofMillis(10),
                                                Duration.ofMillis(10),
                                                0));
        CapturingResultHandler handler = new CapturingResultHandler();
        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        Thread closeThread =
                new Thread(
                        () -> {
                            try {
                                writer.close();
                            } catch (Throwable error) {
                                closeFailure.set(error);
                            }
                        },
                        "influxdb3-writer-close-test");
        closeThread.setDaemon(true);
        try {
            writer.submitRequestEntries(
                    List.of(InfluxDB3WriteRequest.initial("cpu value=1i")), handler);
            client.awaitStarted();

            closeThread.start();
            awaitTimedWaiting(closeThread);
            client.release();
            closeThread.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(closeThread.isAlive()).isFalse();
            assertThat(closeFailure.get()).isNull();
            handler.await();
            assertThat(handler.retryEntries).singleElement()
                    .satisfies(
                            retry ->
                                    assertThat(retry.getRetryAttempt())
                                            .isEqualTo(1));
            assertThat(handler.failure).isNull();
        } finally {
            client.release();
            closeThread.join(TimeUnit.SECONDS.toMillis(5));
            writer.close();
        }
    }

    @Test
    void refreshesOnlyOnceForConcurrentFailuresFromTheSameClient() throws Exception {
        RotatingTokenProvider provider = new RotatingTokenProvider();
        CoordinatedUnauthorizedClient staleClient = new CoordinatedUnauthorizedClient(2);
        SequenceClient refreshedClient = new SequenceClient();
        InfluxDBClientFactory factory =
                (config, token) ->
                        "old-token".equals(new String(token)) ? staleClient : refreshedClient;
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        provider,
                        factory,
                        null,
                        builder -> builder.setMaxInFlightRequests(2));
        try {
            CapturingResultHandler first = new CapturingResultHandler();
            CapturingResultHandler second = new CapturingResultHandler();

            writer.submitRequestEntries(
                    List.of(InfluxDB3WriteRequest.initial("cpu value=1i")), first);
            writer.submitRequestEntries(
                    List.of(InfluxDB3WriteRequest.initial("cpu value=2i")), second);

            first.await();
            second.await();

            assertThat(first.retryEntries).hasSize(1);
            assertThat(second.retryEntries).hasSize(1);
            assertThat(provider.refreshes).hasValue(1);
        } finally {
            writer.close();
        }
    }

    @Test
    void retainsTimedOutClientLeaseUntilNativeWriteExitsAfterAuthenticationRefresh()
            throws Exception {
        WriterTestContext.CapturedContext metrics = WriterTestContext.capture();
        RotatingTokenProvider provider = new RotatingTokenProvider();
        BlockingThenUnauthorizedClient staleClient = new BlockingThenUnauthorizedClient();
        SequenceClient refreshedClient = new SequenceClient();
        InfluxDBClientFactory factory =
                (config, token) ->
                        "old-token".equals(new String(token)) ? staleClient : refreshedClient;
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        provider,
                        factory,
                        null,
                        builder ->
                                builder.setMaxInFlightRequests(2)
                                        .setWriteTimeout(Duration.ofMillis(200))
                                        .setRequestTimeout(Duration.ofMillis(500)),
                        metrics);
        CapturingResultHandler timedOut = new CapturingResultHandler();
        CapturingResultHandler unauthorized = new CapturingResultHandler();
        try {
            writer.submitRequestEntries(
                    List.of(InfluxDB3WriteRequest.initial("cpu value=1i")), timedOut);
            staleClient.awaitBlockingWriteStarted();
            writer.submitRequestEntries(
                    List.of(InfluxDB3WriteRequest.initial("cpu value=2i")), unauthorized);

            unauthorized.await();
            assertThat(unauthorized.retryEntries).hasSize(1);
            assertThat(provider.refreshes).hasValue(1);

            timedOut.await();
            assertThat(timedOut.failure)
                    .isInstanceOf(InfluxDB3SinkException.class)
                    .hasMessageContaining("did not terminate");
            assertThat(staleClient.closeCalls).hasValue(0);
            assertThat(metrics.gaugeValue("numInFlightWriteRequests")).isEqualTo(1L);

            staleClient.releaseBlockingWrite();
            staleClient.awaitBlockingWriteExited();
            staleClient.awaitClosed();
            awaitGaugeValue(metrics, "numInFlightWriteRequests", 0L);
            assertThat(staleClient.closeCalls).hasValue(1);
        } finally {
            staleClient.releaseBlockingWrite();
            writer.close();
        }
    }

    @Test
    void serializesFailureHandlerCallbacksPerWriter() throws Exception {
        CoordinatedFailureClient client = new CoordinatedFailureClient(2, 404);
        AtomicInteger activeCallbacks = new AtomicInteger();
        AtomicInteger maximumConcurrentCallbacks = new AtomicInteger();
        WriteFailureHandler statefulHandler =
                failure -> {
                    int active = activeCallbacks.incrementAndGet();
                    maximumConcurrentCallbacks.accumulateAndGet(active, Math::max);
                    try {
                        Thread.sleep(100);
                        return FailureAction.DROP;
                    } catch (InterruptedException error) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(
                                "Interrupted while classifying a test failure", error);
                    } finally {
                        activeCallbacks.decrementAndGet();
                    }
                };
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        new StaticTokenProvider("token"),
                        (config, token) -> client,
                        statefulHandler,
                        builder -> builder.setMaxInFlightRequests(2));
        try {
            CapturingResultHandler first = new CapturingResultHandler();
            CapturingResultHandler second = new CapturingResultHandler();

            writer.submitRequestEntries(
                    List.of(InfluxDB3WriteRequest.initial("cpu value=1i")), first);
            writer.submitRequestEntries(
                    List.of(InfluxDB3WriteRequest.initial("cpu value=2i")), second);

            first.await();
            second.await();

            assertThat(first.completed).isTrue();
            assertThat(second.completed).isTrue();
            assertThat(maximumConcurrentCallbacks).hasValue(1);
        } finally {
            writer.close();
        }
    }

    @Test
    void usesServerRetryDelayWhenItExceedsConfiguredBackoff() throws Exception {
        HttpHeaders headers =
                HttpHeaders.of(Map.of("Retry-After", List.of("1")), (name, value) -> true);
        WriterTestContext.CapturedContext metrics = WriterTestContext.capture();
        SequenceClient client =
                new SequenceClient(new InfluxDBApiHttpException("rate limited", headers, 429));
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        new StaticTokenProvider("token"),
                        (config, token) -> client,
                        null,
                        builder ->
                                builder.setRetryBackoff(
                                        Duration.ZERO, Duration.ofMillis(10), 0),
                        metrics);
        try {
            CapturingResultHandler handler = new CapturingResultHandler();
            writer.submitRequestEntries(
                    List.of(InfluxDB3WriteRequest.initial("cpu value=1i")), handler);

            handler.await();

            assertThat(handler.retryEntries).hasSize(1);
            assertThat(metrics.counter("numRetryDelayMillis").getCount()).isEqualTo(10);
            assertThat(metrics.gaugeValue("maxRetryAfterMillis")).isEqualTo(1_000);
        } finally {
            writer.close();
        }
    }

    @Test
    void handlesPartialWriteExceptionsAsValidationFailuresWhenDisabled() throws Exception {
        SequenceClient client =
                new SequenceClient(
                        new InfluxDBPartialWriteException(
                                "partial write",
                                null,
                                400,
                                List.of(new LineError(1, "invalid", "cpu value=1i"))));
        List<Boolean> partialWriteFlags = new ArrayList<>();
        WriteFailureHandler captureAndDrop =
                failure -> {
                    partialWriteFlags.add(failure.isPartialWrite());
                    return FailureAction.DROP;
                };
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        new StaticTokenProvider("token"),
                        (config, token) -> client,
                        captureAndDrop,
                        builder -> builder.setAcceptPartial(false));
        try {
            CapturingResultHandler handler = new CapturingResultHandler();
            writer.submitRequestEntries(
                    List.of(InfluxDB3WriteRequest.initial("cpu value=1i")), handler);

            handler.await();

            assertThat(handler.failure)
                    .isInstanceOf(InfluxDB3SinkException.class)
                    .hasMessageContaining("non-retryable");
            assertThat(partialWriteFlags).isEmpty();
        } finally {
            writer.close();
        }
    }

    @Test
    void failsAuthenticationRefreshWithoutInvokingCustomHandler() throws Exception {
        TokenProvider provider =
                new TokenProvider() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public char[] getToken() {
                        return "old-token".toCharArray();
                    }

                    @Override
                    public char[] refreshToken() throws IOException {
                        throw new IOException("refresh unavailable");
                    }
                };
        SequenceClient client =
                new SequenceClient(new InfluxDBApiHttpException("unauthorized", null, 403));
        AtomicInteger handlerCalls = new AtomicInteger();
        WriteFailureHandler customHandler =
                failure -> {
                    handlerCalls.incrementAndGet();
                    return FailureAction.DROP;
                };
        InfluxDB3SinkWriter<String> writer =
                newWriter(provider, (config, token) -> client, customHandler);
        try {
            CapturingResultHandler handler = new CapturingResultHandler();
            writer.submitRequestEntries(
                    List.of(InfluxDB3WriteRequest.initial("cpu value=1i")), handler);

            handler.await();

            assertThat(handler.failure)
                    .isInstanceOf(InfluxDB3SinkException.class)
                    .hasMessageContaining("refresh the InfluxDB authentication token")
                    .hasRootCauseMessage("refresh unavailable");
            assertThat(handlerCalls).hasValue(0);
        } finally {
            writer.close();
        }
    }

    @Test
    void rejectsMalformedPartialWriteDetails() throws Exception {
        assertMalformedPartialWrite(
                List.of(), "partial write without line-level error details");
        assertMalformedPartialWrite(
                List.of(new LineError(99, "invalid line", null)),
                "Unable to map");
        assertMalformedPartialWrite(
                List.of(
                        new LineError(1, "first error", null),
                        new LineError(1, "ambiguous second error", null)),
                "Unable to map");
    }

    @Test
    void mapsPartialWritesByOriginalLineAndMergesDuplicateMessages() throws Exception {
        InfluxDBPartialWriteException partialWrite =
                new InfluxDBPartialWriteException(
                        "partial write",
                        null,
                        400,
                        List.of(
                                new LineError(99, "fallback match", "cpu value=2i"),
                                new LineError(1, "first problem", "cpu value=1i"),
                                new LineError(1, "second problem", "cpu value=1i")));
        SequenceClient client = new SequenceClient(partialWrite);
        List<String> serverMessages = new ArrayList<>();
        WriteFailureHandler dropAndCapture =
                failure -> {
                    serverMessages.add(failure.getServerMessage());
                    return FailureAction.DROP;
                };
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        new StaticTokenProvider("token"),
                        (config, token) -> client,
                        dropAndCapture);
        try {
            CapturingResultHandler handler = new CapturingResultHandler();
            writer.submitRequestEntries(
                    List.of(
                            InfluxDB3WriteRequest.initial("cpu value=1i"),
                            InfluxDB3WriteRequest.initial("cpu value=2i")),
                    handler);

            handler.await();

            assertThat(handler.completed).isTrue();
            assertThat(serverMessages)
                    .containsExactly(
                            "fallback match", "first problem; second problem");
        } finally {
            writer.close();
        }
    }

    @Test
    void substitutesPlaceholderForMissingPartialWriteErrorMessage() throws Exception {
        InfluxDBPartialWriteException partialWrite =
                new InfluxDBPartialWriteException(
                        "partial write",
                        null,
                        400,
                        List.of(new LineError(1, null, "cpu value=1i")));
        SequenceClient client = new SequenceClient(partialWrite);
        List<String> serverMessages = new ArrayList<>();
        WriteFailureHandler dropAndCapture =
                failure -> {
                    serverMessages.add(failure.getServerMessage());
                    return FailureAction.DROP;
                };
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        new StaticTokenProvider("token"),
                        (config, token) -> client,
                        dropAndCapture);
        try {
            CapturingResultHandler handler = new CapturingResultHandler();
            writer.submitRequestEntries(
                    List.of(InfluxDB3WriteRequest.initial("cpu value=1i")), handler);

            handler.await();

            assertThat(handler.completed).isTrue();
            assertThat(handler.failure).isNull();
            assertThat(serverMessages).containsExactly("unknown partial-write error");
        } finally {
            writer.close();
        }
    }

    @Test
    void handlesLineNumberFallbacksWhenMappingPartialWrites() throws Exception {
        InfluxDBPartialWriteException partialWrite =
                new InfluxDBPartialWriteException(
                        "partial write",
                        null,
                        400,
                        List.of(
                                new LineError(null, "missing number", "cpu value=1i"),
                                new LineError(0, "zero number", "cpu value=2i"),
                                new LineError(-1, "negative number", "cpu value=3i"),
                                new LineError(4, "missing original", null),
                                new LineError(99, "duplicate original", "cpu value=1i")));
        SequenceClient client = new SequenceClient(partialWrite);
        InfluxDB3SinkWriter<String> writer =
                newWriter(new StaticTokenProvider("token"), (config, token) -> client, null);
        try {
            CapturingResultHandler handler = new CapturingResultHandler();
            writer.submitRequestEntries(
                    List.of(
                            InfluxDB3WriteRequest.initial("cpu value=1i"),
                            InfluxDB3WriteRequest.initial("cpu value=2i"),
                            InfluxDB3WriteRequest.initial("cpu value=3i"),
                            InfluxDB3WriteRequest.initial("cpu value=4i")),
                    handler);

            handler.await();

            assertThat(handler.failure)
                    .isInstanceOf(InfluxDB3SinkException.class)
                    .hasMessageContaining("Unable to map");
        } finally {
            writer.close();
        }
    }

    @Test
    void reportsFailureHandlerErrorsAndNullDecisions() throws Exception {
        assertFailureHandlerError(
                failure -> {
                    throw new IllegalStateException("handler failed");
                },
                "handler failed");
        assertFailureHandlerError(
                failure -> null, "WriteFailureHandler returned null");
    }

    @Test
    void failsWhenRetryLimitIsExhausted() throws Exception {
        SequenceClient client =
                new SequenceClient(new InfluxDBApiHttpException("unavailable", null, 503));
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        new StaticTokenProvider("token"),
                        (config, token) -> client,
                        null,
                        builder -> builder.setMaxRetries(0));
        try {
            CapturingResultHandler handler = new CapturingResultHandler();
            writer.submitRequestEntries(
                    List.of(InfluxDB3WriteRequest.initial("cpu value=1i")), handler);

            handler.await();

            assertThat(handler.failure)
                    .isInstanceOf(InfluxDB3SinkException.class)
                    .hasMessageContaining("exhausted 0 retries");
        } finally {
            writer.close();
        }
    }

    @Test
    void dropsWhenRetryLimitIsExhaustedAndFailOnErrorIsDisabled() throws Exception {
        WriterTestContext.CapturedContext metrics = WriterTestContext.capture();
        SequenceClient client =
                new SequenceClient(new InfluxDBApiHttpException("unavailable", null, 503));
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        new StaticTokenProvider("token"),
                        (config, token) -> client,
                        null,
                        builder -> builder.setMaxRetries(0).setFailOnError(false),
                        metrics);
        try {
            CapturingResultHandler handler = new CapturingResultHandler();
            writer.submitRequestEntries(
                    List.of(InfluxDB3WriteRequest.initial("cpu value=1i")), handler);

            handler.await();

            assertThat(handler.completed).isTrue();
            assertThat(handler.failure).isNull();
            assertThat(handler.retryEntries).isNull();
            assertThat(metrics.counter("numBatchesSuccess").getCount()).isZero();
            assertThat(metrics.counter("numBatchesCompletedWithDrops").getCount()).isEqualTo(1);
            assertThat(metrics.counter("numBatchesRetried").getCount()).isZero();
            assertThat(metrics.counter("numBatchesFailed").getCount()).isZero();
        } finally {
            writer.close();
        }
    }

    @Test
    void retriesTransportFailuresWithoutAnHttpStatus() throws Exception {
        SequenceClient client =
                new SequenceClient(
                        new RuntimeException(new SocketException("connection reset")));
        WriterTestContext.CapturedContext metrics = WriterTestContext.capture();
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        new StaticTokenProvider("token"),
                        (config, token) -> client,
                        null,
                        builder -> {},
                        metrics);
        try {
            CapturingResultHandler handler = new CapturingResultHandler();
            writer.submitRequestEntries(
                    List.of(InfluxDB3WriteRequest.initial("cpu value=1i")), handler);

            handler.await();

            assertThat(handler.retryEntries).singleElement()
                    .satisfies(request -> assertThat(request.getRetryAttempt()).isEqualTo(1));
            assertThat(metrics.counter("numConnectionErrors").getCount()).isEqualTo(1);
            assertThat(metrics.counter("numServerErrors").getCount()).isZero();
        } finally {
            writer.close();
        }
    }

    @ParameterizedTest(name = "Retry-After: {0}")
    @MethodSource("retryAfterValuesTreatedAsNoDelay")
    void treatsPastAndInvalidRetryAfterValuesAsNoDelay(String retryAfter) throws Exception {
        HttpHeaders headers =
                HttpHeaders.of(
                        Map.of("Retry-After", List.of(retryAfter)),
                        (name, value) -> true);
        SequenceClient client =
                new SequenceClient(
                        new InfluxDBApiHttpException("rate limited", headers, 429));
        InfluxDB3SinkWriter<String> writer =
                newWriter(new StaticTokenProvider("token"), (config, token) -> client, null);
        try {
            CapturingResultHandler handler = new CapturingResultHandler();
            writer.submitRequestEntries(
                    List.of(InfluxDB3WriteRequest.initial("cpu value=1i")), handler);

            handler.await();

            assertThat(handler.retryEntries).hasSize(1);
        } finally {
            writer.close();
        }
    }

    @Test
    void cleansUpAfterInitializationFailures() {
        TokenProvider failingProvider =
                new TokenProvider() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public char[] getToken() throws IOException {
                        throw new IOException("token unavailable");
                    }

                    @Override
                    public void close() throws IOException {
                        throw new IOException("provider close failed");
                    }
                };

        assertThatThrownBy(
                        () ->
                                newWriter(
                                        failingProvider,
                                        (config, token) -> new SequenceClient(),
                                        null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("token unavailable")
                .satisfies(
                        error ->
                                assertThat(error.getSuppressed())
                                        .singleElement()
                                        .satisfies(
                                                suppressed ->
                                                        assertThat(suppressed)
                                                                .hasMessage(
                                                                        "provider close failed")));

        assertThatThrownBy(
                        () ->
                                newWriter(
                                        new StaticTokenProvider("token"),
                                        (config, token) -> {
                                            throw new IllegalStateException("factory failed");
                                        },
                                        null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unable to initialize")
                .hasRootCauseMessage("factory failed");
    }

    @Test
    void interruptsAWriteAtTheConfiguredWriteTimeout() throws Exception {
        WriterTestContext.CapturedContext metrics = WriterTestContext.capture();
        BlockingClient client = new BlockingClient();
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        new StaticTokenProvider("token"),
                        (config, token) -> client,
                        null,
                        builder ->
                                builder.setWriteTimeout(Duration.ofMillis(10))
                                        .setRequestTimeout(Duration.ofMillis(20)),
                        metrics);
        CapturingResultHandler handler = new CapturingResultHandler();
        writer.submitRequestEntries(
                List.of(InfluxDB3WriteRequest.initial("cpu value=1i")), handler);
        client.awaitStarted();
        assertThat(metrics.gaugeValue("numInFlightWriteRequests")).isEqualTo(1);

        client.awaitInterrupted();
        handler.await();
        assertThat(handler.retryEntries).hasSize(1);
        assertThat(metrics.counter("numRecordsSendErrors").getCount()).isEqualTo(1);
        assertThat(metrics.counter("numRetries").getCount()).isEqualTo(1);
        assertThat(metrics.counter("numWriteTimeouts").getCount()).isEqualTo(1);
        assertThat(metrics.counter("numConnectionErrors").getCount()).isZero();
        assertThat(metrics.counter("numBatchesRetried").getCount()).isEqualTo(1);
        assertThat(metrics.histogram("writeLatencyMillis").getCount()).isEqualTo(1);
        awaitGaugeValue(metrics, "numInFlightWriteRequests", 0L);
        writer.close();
    }

    @Test
    void keepsCancellationConfirmationPositiveWithMinimumTimeoutHeadroom() throws Exception {
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        new StaticTokenProvider("token"),
                        (config, token) -> new SequenceClient(),
                        null,
                        builder ->
                                builder.setWriteTimeout(Duration.ofMillis(1))
                                        .setRequestTimeout(Duration.ofMillis(3)));
        try {
            BlockingWriteExecutor executor =
                    fieldValue(
                            writer,
                            "writeExecutor",
                            BlockingWriteExecutor.class);

            assertThat(fieldValue(executor, "cancellationConfirmationMillis", Long.class))
                    .isEqualTo(1L);
        } finally {
            writer.close();
        }
    }

    @Test
    void failsWhenTheNativeClientDoesNotStopAfterCancellation() throws Exception {
        WriterTestContext.CapturedContext metrics = WriterTestContext.capture();
        UninterruptibleBlockingClient client = new UninterruptibleBlockingClient();
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        new StaticTokenProvider("token"),
                        (config, token) -> client,
                        null,
                        builder ->
                                builder.setWriteTimeout(Duration.ofMillis(10))
                                        .setRequestTimeout(Duration.ofMillis(30)),
                        metrics);
        CapturingResultHandler handler = new CapturingResultHandler();
        try {
            writer.submitRequestEntries(
                    List.of(InfluxDB3WriteRequest.initial("cpu value=1i")), handler);
            client.awaitStarted();

            handler.await();

            assertThat(handler.failure)
                    .isInstanceOf(InfluxDB3SinkException.class)
                    .hasMessageContaining("did not terminate");
            assertThat(handler.callbackCount).hasValue(1);
            assertThat(metrics.gaugeValue("numInFlightWriteRequests")).isEqualTo(1L);
        } finally {
            client.release();
            client.awaitExited();
            awaitGaugeValue(metrics, "numInFlightWriteRequests", 0L);
            writer.close();
        }
        assertThat(handler.callbackCount).hasValue(1);
    }

    @Test
    void cancelsTimedOutQueuedWriteWithoutInvokingTheClient() throws Exception {
        UninterruptibleBlockingClient client = new UninterruptibleBlockingClient();
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        new StaticTokenProvider("token"),
                        (config, token) -> client,
                        null,
                        builder ->
                                builder.setWriteTimeout(Duration.ofMillis(10))
                                        .setRequestTimeout(Duration.ofMillis(30)));
        CapturingResultHandler running = new CapturingResultHandler();
        CapturingResultHandler queued = new CapturingResultHandler();
        try {
            writer.submitRequestEntries(
                    List.of(InfluxDB3WriteRequest.initial("cpu value=1i")), running);
            client.awaitStarted();
            writer.submitRequestEntries(
                    List.of(InfluxDB3WriteRequest.initial("cpu value=2i")), queued);

            queued.await();
            running.await();

            assertThat(queued.retryEntries).hasSize(1);
            assertThat(queued.callbackCount).hasValue(1);
            assertThat(running.failure)
                    .isInstanceOf(InfluxDB3SinkException.class)
                    .hasMessageContaining("did not terminate");
            assertThat(client.writeCalls).hasValue(1);
        } finally {
            client.release();
            client.awaitExited();
            writer.close();
        }
        assertThat(running.callbackCount).hasValue(1);
        assertThat(queued.callbackCount).hasValue(1);
    }

    @Test
    void preservesInterruptWhenCloseIsInterrupted() throws Exception {
        BlockingClient client = new BlockingClient();
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        new StaticTokenProvider("token"),
                        (config, token) -> client,
                        null);
        CapturingResultHandler handler = new CapturingResultHandler();
        writer.submitRequestEntries(
                List.of(InfluxDB3WriteRequest.initial("cpu value=1i")), handler);
        client.awaitStarted();

        InfluxDB3SinkException closeFailure = null;
        Thread.currentThread().interrupt();
        try {
            writer.close();
        } catch (InfluxDB3SinkException error) {
            closeFailure = error;
        } finally {
            Thread.interrupted();
        }

        assertThat(closeFailure)
                .isNotNull()
                .hasMessageContaining("Unable to close")
                .hasCauseInstanceOf(InterruptedException.class);
        client.awaitInterrupted();
        handler.await();
    }

    @Test
    void aggregatesFailuresFromMultipleClients() throws Exception {
        RotatingTokenProvider provider = new RotatingTokenProvider();
        InfluxDBWriteClient staleClient =
                closeFailingClient(
                        new InfluxDBApiHttpException("unauthorized", null, 401),
                        "stale client close failed");
        InfluxDBWriteClient refreshedClient =
                closeFailingClient(null, "refreshed client close failed");
        InfluxDBClientFactory factory =
                (config, token) ->
                        "old-token".equals(new String(token)) ? staleClient : refreshedClient;
        InfluxDB3SinkWriter<String> writer = newWriter(provider, factory, null);
        CapturingResultHandler handler = new CapturingResultHandler();
        writer.submitRequestEntries(
                List.of(InfluxDB3WriteRequest.initial("cpu value=1i")), handler);
        handler.await();
        assertThat(handler.retryEntries).hasSize(1);

        assertThatThrownBy(writer::close)
                .isInstanceOf(InfluxDB3SinkException.class)
                .satisfies(
                        error -> {
                            List<String> closeFailureMessages = new ArrayList<>();
                            closeFailureMessages.add(error.getCause().getMessage());
                            Arrays.stream(error.getCause().getSuppressed())
                                    .map(Throwable::getMessage)
                                    .forEach(closeFailureMessages::add);
                            assertThat(closeFailureMessages)
                                    .containsExactlyInAnyOrder(
                                            "stale client close failed",
                                            "refreshed client close failed");
                        });
    }

    @Test
    void reportsTokenProviderCloseFailureByItself() throws Exception {
        TokenProvider provider =
                new TokenProvider() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public char[] getToken() {
                        return "token".toCharArray();
                    }

                    @Override
                    public void close() throws IOException {
                        throw new IOException("provider close failed");
                    }
                };
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        provider,
                        (config, token) -> new SequenceClient(),
                        null);

        assertThatThrownBy(writer::close)
                .isInstanceOf(InfluxDB3SinkException.class)
                .hasCauseInstanceOf(IOException.class)
                .hasRootCauseMessage("provider close failed");
    }

    @Test
    void aggregatesClientAndTokenProviderCloseFailures() throws Exception {
        TokenProvider provider =
                new TokenProvider() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public char[] getToken() {
                        return "token".toCharArray();
                    }

                    @Override
                    public void close() throws IOException {
                        throw new IOException("provider close failed");
                    }
                };
        InfluxDBWriteClient client =
                new InfluxDBWriteClient() {
                    @Override
                    public void writeRecords(List<String> records) {}

                    @Override
                    public void close() throws Exception {
                        throw new IOException("client close failed");
                    }
                };
        InfluxDB3SinkWriter<String> writer =
                newWriter(provider, (config, token) -> client, null);

        assertThatThrownBy(writer::close)
                .isInstanceOf(InfluxDB3SinkException.class)
                .hasMessageContaining("Unable to close")
                .satisfies(
                        error -> {
                            assertThat(error.getCause()).hasMessage("client close failed");
                            assertThat(error.getCause().getSuppressed())
                                    .singleElement()
                                    .satisfies(
                                            suppressed ->
                                                    assertThat(suppressed)
                                                            .hasMessage(
                                                                    "provider close failed"));
                        });
        writer.close();
    }

    private static InfluxDB3SinkWriter<String> newWriter(
            TokenProvider tokenProvider,
            InfluxDBClientFactory factory,
            WriteFailureHandler failureHandler)
            throws IOException {
        return newWriter(tokenProvider, factory, failureHandler, builder -> {});
    }

    private static InfluxDB3SinkWriter<String> newWriter(
            TokenProvider tokenProvider,
            InfluxDBClientFactory factory,
            WriteFailureHandler failureHandler,
            Consumer<InfluxDB3SinkBuilder<String>> customizer)
            throws IOException {
        return newWriter(
                tokenProvider,
                factory,
                failureHandler,
                customizer,
                WriterTestContext.capture());
    }

    private static InfluxDB3SinkWriter<String> newWriter(
            TokenProvider tokenProvider,
            InfluxDBClientFactory factory,
            WriteFailureHandler failureHandler,
            Consumer<InfluxDB3SinkBuilder<String>> customizer,
            WriterTestContext.CapturedContext testContext)
            throws IOException {
        return newWriter(
                tokenProvider,
                factory,
                failureHandler,
                customizer,
                testContext,
                List.of());
    }

    private static InfluxDB3SinkWriter<String> newWriter(
            TokenProvider tokenProvider,
            InfluxDBClientFactory factory,
            WriteFailureHandler failureHandler,
            Consumer<InfluxDB3SinkBuilder<String>> customizer,
            WriterTestContext.CapturedContext testContext,
            Collection<BufferedRequestState<InfluxDB3WriteRequest>> recoveredState)
            throws IOException {
        InfluxDB3SinkBuilder<String> builder =
                InfluxDB3Sink.<String>builder()
                        .setEndpoint("http://localhost:8086")
                        .setDatabase("metrics")
                        .setTokenProvider(tokenProvider)
                        .setConverter(value -> value)
                        .setMaxBatchSize(1)
                        .setMaxBufferedRequests(2)
                        .setMaxInFlightRequests(1)
                        .setRetryBackoff(Duration.ZERO, Duration.ZERO, 0);
        customizer.accept(builder);
        if (failureHandler != null) {
            builder.setFailureHandler(failureHandler);
        }
        InfluxDB3SinkConfig config =
                InfluxDB3SinkTestAccess.configurationOf(builder.build());
        return new InfluxDB3SinkWriter<>(
                new LineProtocolElementConverter<>(
                        value -> value,
                        config.isFailOnValidation(),
                        config.isPrintFailedRequests(),
                        config.getMaxRecordSizeInBytes()),
                testContext.context(),
                config,
                factory,
                recoveredState);
    }

    private static void assertMalformedPartialWrite(
            List<LineError> lineErrors, String expectedMessage) throws Exception {
        SequenceClient client =
                new SequenceClient(
                        new InfluxDBPartialWriteException(
                                "partial write", null, 400, lineErrors));
        InfluxDB3SinkWriter<String> writer =
                newWriter(new StaticTokenProvider("token"), (config, token) -> client, null);
        try {
            CapturingResultHandler handler = new CapturingResultHandler();
            writer.submitRequestEntries(
                    List.of(InfluxDB3WriteRequest.initial("cpu value=1i")), handler);

            handler.await();

            assertThat(handler.failure)
                    .isInstanceOf(InfluxDB3SinkException.class)
                    .hasMessageContaining(expectedMessage);
        } finally {
            writer.close();
        }
    }

    private static void assertFailureHandlerError(
            WriteFailureHandler failureHandler, String expectedRootMessage) throws Exception {
        SequenceClient client = new SequenceClient(new IllegalStateException("write failed"));
        InfluxDB3SinkWriter<String> writer =
                newWriter(
                        new StaticTokenProvider("token"),
                        (config, token) -> client,
                        failureHandler);
        try {
            CapturingResultHandler handler = new CapturingResultHandler();
            writer.submitRequestEntries(
                    List.of(InfluxDB3WriteRequest.initial("cpu value=1i")), handler);

            handler.await();

            assertThat(handler.failure)
                    .isInstanceOf(InfluxDB3SinkException.class)
                    .hasMessageContaining("WriteFailureHandler failed")
                    .hasRootCauseMessage(expectedRootMessage);
        } finally {
            writer.close();
        }
    }

    private static Stream<String> retryAfterValuesTreatedAsNoDelay() {
        String futureDate =
                ZonedDateTime.now(ZoneOffset.UTC)
                        .plusDays(1)
                        .format(DateTimeFormatter.RFC_1123_DATE_TIME);
        return Stream.of(
                "Wed, 21 Oct 2015 07:28:00 GMT",
                "not-a-date",
                "0",
                futureDate);
    }

    private static Stream<Error> fatalClientErrors() {
        return Stream.of(
                new AssertionError("client assertion failed"),
                new LinkageError("client linkage failed"));
    }

    private static Stream<RestoredFailureLimitCase> restoredFailureLimitCases() {
        return Stream.of(
                new RestoredFailureLimitCase(
                        "retry limit survives restore",
                        1,
                        0,
                        503,
                        1,
                        "exhausted 1 retries"),
                new RestoredFailureLimitCase(
                        "authentication refresh limit survives restore",
                        0,
                        1,
                        401,
                        3,
                        "after one token refresh"));
    }

    private record RestoredFailureLimitCase(
            String description,
            int retryAttempt,
            int authenticationRefreshes,
            int statusCode,
            int maxRetries,
            String expectedMessage) {

        @Override
        public String toString() {
            return description;
        }
    }

    private static InfluxDBWriteClient closeFailingClient(
            RuntimeException writeFailure, String closeMessage) {
        return new InfluxDBWriteClient() {
            @Override
            public void writeRecords(List<String> records) {
                if (writeFailure != null) {
                    throw writeFailure;
                }
            }

            @Override
            public void close() throws Exception {
                throw new IOException(closeMessage);
            }
        };
    }

    private static <T> T fieldValue(Object instance, String fieldName, Class<T> fieldType) {
        try {
            Field field = instance.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return fieldType.cast(field.get(instance));
        } catch (ReflectiveOperationException error) {
            throw new AssertionError("Unable to access " + fieldName, error);
        }
    }

    private static void awaitGaugeValue(
            WriterTestContext.CapturedContext metrics, String name, long expected)
            throws InterruptedException {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (metrics.gaugeValue(name) != expected && System.nanoTime() < deadlineNanos) {
            Thread.sleep(1L);
        }
        assertThat(metrics.gaugeValue(name)).isEqualTo(expected);
    }

    private static void awaitTimedWaiting(Thread thread) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (thread.isAlive()
                && thread.getState() != Thread.State.TIMED_WAITING
                && System.nanoTime() < deadlineNanos) {
            Thread.sleep(1L);
        }
        assertThat(thread.getState()).isEqualTo(Thread.State.TIMED_WAITING);
    }

    private static final class CoordinatedUnauthorizedClient implements InfluxDBWriteClient {
        private final CountDownLatch requestsStarted;

        private CoordinatedUnauthorizedClient(int expectedRequests) {
            this.requestsStarted = new CountDownLatch(expectedRequests);
        }

        @Override
        public void writeRecords(List<String> records) {
            requestsStarted.countDown();
            try {
                if (!requestsStarted.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Concurrent requests did not start");
                }
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while coordinating requests", error);
            }
            throw new InfluxDBApiHttpException("unauthorized", null, 401);
        }

        @Override
        public void close() {}
    }

    private static final class BlockingThenUnauthorizedClient implements InfluxDBWriteClient {
        private final CountDownLatch blockingWriteStarted = new CountDownLatch(1);
        private final CountDownLatch blockingWriteRelease = new CountDownLatch(1);
        private final CountDownLatch blockingWriteExited = new CountDownLatch(1);
        private final CountDownLatch closed = new CountDownLatch(1);
        private final AtomicInteger writeCalls = new AtomicInteger();
        private final AtomicInteger closeCalls = new AtomicInteger();

        @Override
        public void writeRecords(List<String> records) {
            if (writeCalls.incrementAndGet() > 1) {
                throw new InfluxDBApiHttpException("unauthorized", null, 401);
            }
            blockingWriteStarted.countDown();
            try {
                boolean released = false;
                while (!released) {
                    try {
                        blockingWriteRelease.await();
                        released = true;
                    } catch (InterruptedException ignored) {
                        // Deliberately model a dependency that does not honor cancellation.
                    }
                }
            } finally {
                blockingWriteExited.countDown();
            }
        }

        @Override
        public void close() {
            closeCalls.incrementAndGet();
            closed.countDown();
        }

        private void awaitBlockingWriteStarted() throws InterruptedException {
            assertThat(blockingWriteStarted.await(5, TimeUnit.SECONDS)).isTrue();
        }

        private void releaseBlockingWrite() {
            blockingWriteRelease.countDown();
        }

        private void awaitBlockingWriteExited() throws InterruptedException {
            assertThat(blockingWriteExited.await(5, TimeUnit.SECONDS)).isTrue();
        }

        private void awaitClosed() throws InterruptedException {
            assertThat(closed.await(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static final class CoordinatedFailureClient implements InfluxDBWriteClient {
        private final CountDownLatch requestsStarted;
        private final int statusCode;

        private CoordinatedFailureClient(int expectedRequests, int statusCode) {
            this.requestsStarted = new CountDownLatch(expectedRequests);
            this.statusCode = statusCode;
        }

        @Override
        public void writeRecords(List<String> records) {
            requestsStarted.countDown();
            try {
                if (!requestsStarted.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Concurrent requests did not start");
                }
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "Interrupted while coordinating requests", error);
            }
            throw new InfluxDBApiHttpException("coordinated failure", null, statusCode);
        }

        @Override
        public void close() {}
    }

    private static final class BlockingClient implements InfluxDBWriteClient {
        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch blocker = new CountDownLatch(1);
        private final CountDownLatch interrupted = new CountDownLatch(1);

        @Override
        public void writeRecords(List<String> records) {
            started.countDown();
            try {
                blocker.await();
            } catch (InterruptedException error) {
                interrupted.countDown();
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void close() {}

        private void awaitStarted() throws InterruptedException {
            assertThat(started.await(5, TimeUnit.SECONDS))
                    .as("blocking client invocation")
                    .isTrue();
        }

        private void awaitInterrupted() throws InterruptedException {
            assertThat(interrupted.await(5, TimeUnit.SECONDS))
                    .as("blocking client interruption")
                    .isTrue();
        }
    }

    private static final class BlockingRetryableClient implements InfluxDBWriteClient {
        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch blocker = new CountDownLatch(1);

        @Override
        public void writeRecords(List<String> records) {
            started.countDown();
            try {
                blocker.await();
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "Interrupted while waiting to return a retryable failure", error);
            }
            throw new InfluxDBApiHttpException("unavailable", null, 503);
        }

        @Override
        public void close() {}

        private void awaitStarted() throws InterruptedException {
            assertThat(started.await(5, TimeUnit.SECONDS))
                    .as("retryable client invocation")
                    .isTrue();
        }

        private void release() {
            blocker.countDown();
        }
    }

    private static final class UninterruptibleBlockingClient implements InfluxDBWriteClient {
        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch blocker = new CountDownLatch(1);
        private final CountDownLatch exited = new CountDownLatch(1);
        private final AtomicInteger writeCalls = new AtomicInteger();

        @Override
        public void writeRecords(List<String> records) {
            writeCalls.incrementAndGet();
            started.countDown();
            try {
                boolean released = false;
                while (!released) {
                    try {
                        blocker.await();
                        released = true;
                    } catch (InterruptedException ignored) {
                        // Deliberately model a dependency that does not honor cancellation.
                    }
                }
            } finally {
                exited.countDown();
            }
        }

        @Override
        public void close() {}

        private void awaitStarted() throws InterruptedException {
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
        }

        private void release() {
            blocker.countDown();
        }

        private void awaitExited() throws InterruptedException {
            assertThat(exited.await(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static final class SequenceClient implements InfluxDBWriteClient {
        private final Deque<RuntimeException> failures = new ArrayDeque<>();
        private int writeCalls;
        private int closeCalls;
        private List<String> lastRecords = List.of();

        private SequenceClient(RuntimeException... failures) {
            this.failures.addAll(Arrays.asList(failures));
        }

        @Override
        public synchronized void writeRecords(List<String> records) {
            writeCalls++;
            lastRecords = List.copyOf(records);
            if (!failures.isEmpty()) {
                throw failures.removeFirst();
            }
        }

        @Override
        public synchronized void close() {
            closeCalls++;
        }
    }

    private static final class RotatingTokenProvider implements TokenProvider {
        private static final long serialVersionUID = 1L;
        private final AtomicInteger refreshes = new AtomicInteger();

        @Override
        public char[] getToken() {
            return "old-token".toCharArray();
        }

        @Override
        public char[] refreshToken() {
            refreshes.incrementAndGet();
            return "new-token".toCharArray();
        }
    }

    private static final class CapturingResultHandler
            implements ResultHandler<InfluxDB3WriteRequest> {
        private final CountDownLatch completedLatch = new CountDownLatch(1);
        private volatile boolean completed;
        private volatile Exception failure;
        private volatile List<InfluxDB3WriteRequest> retryEntries;
        private final AtomicInteger callbackCount = new AtomicInteger();

        @Override
        public void complete() {
            callbackCount.incrementAndGet();
            completed = true;
            completedLatch.countDown();
        }

        @Override
        public void completeExceptionally(Exception exception) {
            callbackCount.incrementAndGet();
            failure = exception;
            completedLatch.countDown();
        }

        @Override
        public void retryForEntries(List<InfluxDB3WriteRequest> requests) {
            callbackCount.incrementAndGet();
            retryEntries = List.copyOf(requests);
            completedLatch.countDown();
        }

        private void await() throws InterruptedException {
            assertThat(completedLatch.await(5, TimeUnit.SECONDS))
                    .as("writer result callback")
                    .isTrue();
        }
    }
}
