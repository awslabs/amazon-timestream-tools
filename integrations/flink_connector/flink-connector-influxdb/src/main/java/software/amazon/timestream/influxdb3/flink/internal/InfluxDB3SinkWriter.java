/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.ResultHandler;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import org.apache.flink.connector.base.sink.writer.strategy.AIMDScalingStrategy;
import org.apache.flink.connector.base.sink.writer.strategy.CongestionControlRateLimitingStrategy;
import org.apache.flink.connector.base.sink.writer.strategy.RateLimitingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.timestream.influxdb3.flink.InfluxDB3SinkException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** Flink-facing writer that coordinates buffering, write execution, and failure handling. */
@Internal
public final class InfluxDB3SinkWriter<InputT>
        extends AsyncSinkWriter<InputT, InfluxDB3WriteRequest> {
    private static final Logger LOG = LoggerFactory.getLogger(InfluxDB3SinkWriter.class);

    private final InfluxDB3SinkConfig config;
    private final PayloadSizeTrackingRequestBuffer<InfluxDB3WriteRequest> requestBuffer;
    private final InfluxDBSinkMetrics metrics;
    private final InfluxDBClientManager clientManager;
    private final BlockingWriteExecutor writeExecutor;
    private final InfluxDBFailureProcessor failureProcessor;

    public InfluxDB3SinkWriter(
            ElementConverter<InputT, InfluxDB3WriteRequest> elementConverter,
            WriterInitContext context,
            InfluxDB3SinkConfig config,
            InfluxDBClientFactory clientFactory,
            Collection<BufferedRequestState<InfluxDB3WriteRequest>> recoveredState)
            throws IOException {
        this(
                elementConverter,
                context,
                Objects.requireNonNull(config, "config"),
                clientFactory,
                recoveredState,
                new PayloadSizeTrackingRequestBuffer<>());
    }

    private InfluxDB3SinkWriter(
            ElementConverter<InputT, InfluxDB3WriteRequest> elementConverter,
            WriterInitContext context,
            InfluxDB3SinkConfig config,
            InfluxDBClientFactory clientFactory,
            Collection<BufferedRequestState<InfluxDB3WriteRequest>> recoveredState,
            PayloadSizeTrackingRequestBuffer<InfluxDB3WriteRequest> requestBuffer)
            throws IOException {
        super(
                elementConverter,
                context,
                writerConfiguration(config),
                recoveredState,
                new LineProtocolBatchCreator(config.getMaxBatchSizeInBytes()),
                requestBuffer);
        this.config = config;
        this.requestBuffer = requestBuffer;
        this.metrics =
                new InfluxDBSinkMetrics(
                        context,
                        requestBuffer,
                        config.isManagedFlinkMetricsEnabled(),
                        config.getManagedFlinkMetricsSinkName());
        this.clientManager =
                new InfluxDBClientManager(
                        config,
                        clientFactory,
                        metrics,
                        getFatalExceptionCons()::accept);
        this.writeExecutor = new BlockingWriteExecutor(config, clientManager, metrics);
        this.failureProcessor =
                new InfluxDBFailureProcessor(
                        config,
                        clientManager,
                        writeExecutor::scheduleRetry,
                        metrics,
                        LOG);

        LOG.info(
                "InfluxDB sink payload limits per subtask: maxBufferedBytes={}, "
                        + "maxInFlightRequests={}, maxBatchSizeInBytes={}, "
                        + "estimatedRawPayloadUpperBoundBytes={}",
                config.getMaxBufferedBytes(),
                config.getMaxInFlightRequests(),
                config.getMaxBatchSizeInBytes(),
                estimatedRawPayloadUpperBoundBytes(config));
        try {
            clientManager.initialize();
        } catch (IOException error) {
            writeExecutor.shutdownImmediately();
            throw error;
        }
    }

    @Override
    public void write(InputT element, SinkWriter.Context context)
            throws IOException, InterruptedException {
        long flushThreshold =
                config.getMaxBufferedBytes() - config.getMaxRecordSizeInBytes();
        if (!requestBuffer.isEmpty() && requestBuffer.totalSizeInBytes() > flushThreshold) {
            super.flush(true);
        }
        super.write(element, context);
    }

    @Override
    protected void submitRequestEntries(
            List<InfluxDB3WriteRequest> requestEntries,
            ResultHandler<InfluxDB3WriteRequest> resultHandler) {
        SingleCompletionResultHandler<InfluxDB3WriteRequest> guardedResultHandler =
                new SingleCompletionResultHandler<>(
                        resultHandler,
                        metrics::batchOutcome);
        if (clientManager.isClosed()) {
            guardedResultHandler.completeExceptionally(
                    new InfluxDB3SinkException(
                            "Cannot submit writes after the sink writer closed"));
            return;
        }

        List<InfluxDB3WriteRequest> writableRequests =
                handleValidationFailures(requestEntries, guardedResultHandler);
        if (writableRequests == null) {
            return;
        }
        if (writableRequests.isEmpty()) {
            guardedResultHandler.complete();
            return;
        }

        List<String> records =
                writableRequests.stream()
                        .map(InfluxDB3WriteRequest::getLineProtocol)
                        .collect(Collectors.toUnmodifiableList());
        writeExecutor.submit(
                writableRequests,
                records,
                guardedResultHandler,
                failureProcessor::handleWriteFailure);
    }

    @Override
    protected long getSizeInBytes(InfluxDB3WriteRequest requestEntry) {
        if (requestEntry.isValidationFailure()) {
            return 0L;
        }
        return requestEntry.getLineProtocol().getBytes(StandardCharsets.UTF_8).length;
    }

    private List<InfluxDB3WriteRequest> handleValidationFailures(
            List<InfluxDB3WriteRequest> requests,
            SingleCompletionResultHandler<InfluxDB3WriteRequest> resultHandler) {
        List<InfluxDB3WriteRequest> validationFailures =
                requests.stream()
                        .filter(InfluxDB3WriteRequest::isValidationFailure)
                        .collect(Collectors.toList());
        if (validationFailures.isEmpty()) {
            return requests;
        }

        metrics.recordsSendErrors(validationFailures.size());
        if (config.isFailOnValidation()) {
            resultHandler.completeExceptionally(
                    new InfluxDB3SinkException(
                            "Line protocol validation failed: "
                                    + validationFailures
                                            .get(0)
                                            .getValidationFailureMessage()));
            return null;
        }

        metrics.recordsDropped(validationFailures.size());
        resultHandler.markRecordsDropped();
        return requests.stream()
                .filter(request -> !request.isValidationFailure())
                .collect(Collectors.toList());
    }

    @Override
    public void close() {
        if (!clientManager.beginClose()) {
            return;
        }

        Exception failure = writeExecutor.close();
        failure = clientManager.closeResources(failure);
        super.close();
        if (failure != null) {
            throw new InfluxDB3SinkException(
                    "Unable to close the InfluxDB sink writer", failure);
        }
    }

    private static long estimatedRawPayloadUpperBoundBytes(InfluxDB3SinkConfig config) {
        try {
            return Math.addExact(
                    config.getMaxBufferedBytes(),
                    Math.multiplyExact(
                            (long) config.getMaxInFlightRequests(),
                            config.getMaxBatchSizeInBytes()));
        } catch (ArithmeticException ignored) {
            return Long.MAX_VALUE;
        }
    }

    static AsyncSinkWriterConfiguration writerConfiguration(InfluxDB3SinkConfig config) {
        return AsyncSinkWriterConfiguration.builder()
                .setMaxBatchSize(config.getMaxBatchSize())
                .setMaxBatchSizeInBytes(config.getMaxBatchSizeInBytes())
                .setMaxInFlightRequests(config.getMaxInFlightRequests())
                .setMaxBufferedRequests(config.getMaxBufferedRequests())
                .setMaxTimeInBufferMS(config.getMaxTimeInBufferMillis())
                .setMaxRecordSizeInBytes(config.getMaxRecordSizeInBytes())
                .setRequestTimeoutMS(config.getRequestTimeout().toMillis())
                .setFailOnTimeout(config.isFailOnRequestTimeout())
                .setRateLimitingStrategy(rateLimitingStrategy(config))
                .build();
    }

    /**
     * Allows {@code maxInFlightRequests} full batches in flight from the start.
     *
     * <p>Flink's default strategy starts at one batch and adds ten records per successful
     * request, so a writer needs thousands of requests to reach its configured concurrency. This
     * strategy starts at the configured concurrency, halves it when a request has failed or
     * retried records, such as on HTTP 429, and adds one batch per successful request.
     */
    static RateLimitingStrategy rateLimitingStrategy(InfluxDB3SinkConfig config) {
        int maxInFlightMessages =
                (int)
                        Math.min(
                                Integer.MAX_VALUE,
                                (long) config.getMaxBatchSize()
                                        * config.getMaxInFlightRequests());
        return CongestionControlRateLimitingStrategy.builder()
                .setMaxInFlightRequests(config.getMaxInFlightRequests())
                .setInitialMaxInFlightMessages(maxInFlightMessages)
                .setScalingStrategy(
                        AIMDScalingStrategy.builder(maxInFlightMessages)
                                .setIncreaseRate(config.getMaxBatchSize())
                                .build())
                .build();
    }
}
