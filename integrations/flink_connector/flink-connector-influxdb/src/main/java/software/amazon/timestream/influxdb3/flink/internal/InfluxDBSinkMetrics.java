/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.internal;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import com.influxdb.v3.client.InfluxDBApiHttpException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** Metric registration and mailbox-safe updates for one sink writer. */
final class InfluxDBSinkMetrics {
    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBSinkMetrics.class);

    /** Metric group that Managed Flink publishes to CloudWatch. */
    static final String MANAGED_FLINK_METRIC_GROUP = "KinesisAnalytics";

    static final String CONNECTOR_DIMENSION = "Connector";
    static final String CONNECTOR_DIMENSION_VALUE = "TimestreamInfluxDB3";
    static final String SINK_DIMENSION = "Sink";

    /** Time window of the write latency percentiles published to Managed Flink. */
    static final Duration MANAGED_FLINK_LATENCY_WINDOW = Duration.ofSeconds(60);

    private static final int MANAGED_FLINK_LATENCY_CAPACITY = 4_096;

    private final MailboxExecutor mailboxExecutor;
    private final AtomicLong lastWriteLatencyMillis = new AtomicLong();
    private final AtomicInteger lastBatchSizeLines = new AtomicInteger();
    private final AtomicLong lastBatchSizeBytes = new AtomicLong();
    private final AtomicInteger inFlightWriteRequests = new AtomicInteger();
    private final AtomicInteger activeClientGenerations = new AtomicInteger();
    private final AtomicLong maxRetryAfterMillis = new AtomicLong();

    private final Counter numRecordsWritten;
    private final Counter numRecordsSendErrors;
    private final Counter numRecordsDropped;
    private final Counter numRetries;
    private final Counter numPartialWriteFailures;
    private final Counter numAuthenticationRefreshes;
    private final Counter numThrottledRequests;
    private final Counter numWriteTimeouts;
    private final Counter numBatchesSuccess;
    private final Counter numBatchesCompletedWithDrops;
    private final Counter numBatchesRetried;
    private final Counter numBatchesFailed;
    private final Counter numBytesWritten;
    private final Counter numBytesOut;
    private final Counter numRetryDelayMillis;
    private final Counter numServerErrors;
    private final Counter numClientErrors;
    private final Counter numConnectionErrors;
    private final Histogram writeLatencyMillis;
    /** Recent write latencies for the published percentiles; {@code null} when not published. */
    private final TimeWindowedQuantiles recentWriteLatencyMillis;

    InfluxDBSinkMetrics(
            WriterInitContext context,
            PayloadSizeTrackingRequestBuffer<InfluxDB3WriteRequest> requestBuffer,
            boolean managedFlinkMetricsEnabled,
            String managedFlinkMetricsSinkName) {
        this.mailboxExecutor =
                Objects.requireNonNull(
                        context.getMailboxExecutor(), "context mailbox executor");
        SinkWriterMetricGroup metrics = context.metricGroup();
        this.numRecordsWritten = metrics.counter("numRecordsWritten");
        this.numRecordsSendErrors = metrics.getNumRecordsSendErrorsCounter();
        this.numRecordsDropped = metrics.counter("numRecordsDropped");
        this.numRetries = metrics.counter("numRetries");
        this.numPartialWriteFailures = metrics.counter("numPartialWriteFailures");
        this.numAuthenticationRefreshes = metrics.counter("numAuthenticationRefreshes");
        this.numThrottledRequests = metrics.counter("numThrottledRequests");
        this.numWriteTimeouts = metrics.counter("numWriteTimeouts");
        this.numBatchesSuccess = metrics.counter("numBatchesSuccess");
        this.numBatchesCompletedWithDrops =
                metrics.counter("numBatchesCompletedWithDrops");
        this.numBatchesRetried = metrics.counter("numBatchesRetried");
        this.numBatchesFailed = metrics.counter("numBatchesFailed");
        this.numBytesWritten = metrics.counter("numBytesWritten");
        this.numBytesOut = metrics.getIOMetricGroup().getNumBytesOutCounter();
        this.numRetryDelayMillis = metrics.counter("numRetryDelayMillis");
        this.numServerErrors = metrics.counter("numServerErrors");
        this.numClientErrors = metrics.counter("numClientErrors");
        this.numConnectionErrors = metrics.counter("numConnectionErrors");
        this.writeLatencyMillis =
                metrics.histogram("writeLatencyMillis", new SlidingWindowHistogram(1_024));
        metrics.gauge("lastWriteLatencyMillis", lastWriteLatencyMillis::get);
        metrics.gauge("batchSizeLines", lastBatchSizeLines::get);
        metrics.gauge("batchSizeBytes", lastBatchSizeBytes::get);
        metrics.gauge("numBufferedBytes", requestBuffer::totalSizeInBytes);
        metrics.gauge("numInFlightWriteRequests", inFlightWriteRequests::get);
        metrics.gauge("numActiveClientGenerations", activeClientGenerations::get);
        metrics.gauge("maxRetryAfterMillis", maxRetryAfterMillis::get);
        this.recentWriteLatencyMillis =
                managedFlinkMetricsEnabled
                        ? new TimeWindowedQuantiles(
                                MANAGED_FLINK_LATENCY_WINDOW, MANAGED_FLINK_LATENCY_CAPACITY)
                        : null;
        if (managedFlinkMetricsEnabled) {
            registerManagedFlinkMetrics(
                    managedFlinkMetricGroup(metrics, managedFlinkMetricsSinkName),
                    requestBuffer);
        }
    }

    /**
     * Registers the operational metrics a second time where Managed Flink publishes them.
     *
     * <p>The same metric instances are registered, so both groups always report equal values.
     * CloudWatch does not accept histograms, so write latency is published as percentile gauges
     * over the writes of the last {@link #MANAGED_FLINK_LATENCY_WINDOW}. A fixed-count window
     * would cover hours of history at low batch rates.
     */
    private void registerManagedFlinkMetrics(
            MetricGroup group,
            PayloadSizeTrackingRequestBuffer<InfluxDB3WriteRequest> requestBuffer) {
        group.counter("numRecordsWritten", numRecordsWritten);
        group.counter("numRecordsSendErrors", numRecordsSendErrors);
        group.counter("numRecordsDropped", numRecordsDropped);
        group.counter("numRetries", numRetries);
        group.counter("numPartialWriteFailures", numPartialWriteFailures);
        group.counter("numAuthenticationRefreshes", numAuthenticationRefreshes);
        group.counter("numThrottledRequests", numThrottledRequests);
        group.counter("numWriteTimeouts", numWriteTimeouts);
        group.counter("numBatchesFailed", numBatchesFailed);
        group.counter("numBatchesSuccess", numBatchesSuccess);
        group.counter("numBatchesRetried", numBatchesRetried);
        group.counter("numBatchesCompletedWithDrops", numBatchesCompletedWithDrops);
        group.counter("numBytesWritten", numBytesWritten);
        group.counter("numBytesOut", numBytesOut);
        group.counter("numRetryDelayMillis", numRetryDelayMillis);
        group.counter("numServerErrors", numServerErrors);
        group.counter("numClientErrors", numClientErrors);
        group.counter("numConnectionErrors", numConnectionErrors);
        group.meter("numRecordsWrittenPerSecond", new MeterView(numRecordsWritten));
        group.meter("numBytesWrittenPerSecond", new MeterView(numBytesWritten));
        group.meter("numRecordsDroppedPerSecond", new MeterView(numRecordsDropped));
        group.meter("numRecordsSendErrorsPerSecond", new MeterView(numRecordsSendErrors));
        group.meter("numBatchesFailedPerSecond", new MeterView(numBatchesFailed));
        group.gauge("writeLatencyP50Millis", writeLatencyQuantile(0.50));
        group.gauge("writeLatencyP95Millis", writeLatencyQuantile(0.95));
        group.gauge("writeLatencyP99Millis", writeLatencyQuantile(0.99));
        group.gauge("lastWriteLatencyMillis", lastWriteLatencyMillis::get);
        group.gauge("batchSizeLines", lastBatchSizeLines::get);
        group.gauge("batchSizeBytes", lastBatchSizeBytes::get);
        group.gauge("numBufferedBytes", requestBuffer::totalSizeInBytes);
        group.gauge("numInFlightWriteRequests", inFlightWriteRequests::get);
        group.gauge("maxRetryAfterMillis", maxRetryAfterMillis::get);
    }

    private static MetricGroup managedFlinkMetricGroup(MetricGroup metrics, String sinkName) {
        MetricGroup group =
                metrics.addGroup(MANAGED_FLINK_METRIC_GROUP)
                        .addGroup(CONNECTOR_DIMENSION, CONNECTOR_DIMENSION_VALUE);
        return sinkName == null ? group : group.addGroup(SINK_DIMENSION, sinkName);
    }

    private Gauge<Double> writeLatencyQuantile(double quantile) {
        return () -> recentWriteLatencyMillis.getQuantile(quantile);
    }

    void recordsWritten(long count) {
        increment(numRecordsWritten, count);
    }

    void bytesWritten(long bytes) {
        increment(numBytesWritten, bytes);
    }

    void retryScheduled(long delayMillis, long retryAfterMillis) {
        increment(numRetryDelayMillis, delayMillis);
        maxRetryAfterMillis.accumulateAndGet(retryAfterMillis, Math::max);
    }

    /**
     * Counts a write attempt that the InfluxDB client failed, by response class. HTTP 429 is
     * counted separately as throttling; write-deadline timeouts never reach this method.
     */
    void writeAttemptFailed(Exception error) {
        InfluxDBApiHttpException httpError = InfluxDBFailureProcessor.findHttpError(error);
        if (httpError == null) {
            increment(numConnectionErrors, 1L);
            return;
        }
        int statusCode = httpError.statusCode();
        if (statusCode >= 500) {
            increment(numServerErrors, 1L);
        } else if (statusCode >= 400 && statusCode != 429) {
            increment(numClientErrors, 1L);
        }
    }

    void recordsSendErrors(long count) {
        increment(numRecordsSendErrors, count);
    }

    void recordsDropped(long count) {
        increment(numRecordsDropped, count);
    }

    void retries(long count) {
        increment(numRetries, count);
    }

    void partialWriteFailures(long count) {
        increment(numPartialWriteFailures, count);
    }

    void authenticationRefresh() {
        increment(numAuthenticationRefreshes, 1L);
    }

    void throttledRequest() {
        increment(numThrottledRequests, 1L);
    }

    void writeTimeout() {
        increment(numWriteTimeouts, 1L);
    }

    void writeRequestStarted() {
        inFlightWriteRequests.incrementAndGet();
    }

    void writeRequestFinished() {
        inFlightWriteRequests.decrementAndGet();
    }

    void clientGenerationOpened() {
        activeClientGenerations.incrementAndGet();
    }

    void clientGenerationClosed() {
        activeClientGenerations.decrementAndGet();
    }

    /** @return the batch's UTF-8 line protocol payload size in bytes */
    long batchSubmitted(List<String> records) {
        long payloadSizeInBytes = lineProtocolPayloadSizeInBytes(records);
        lastBatchSizeLines.set(records.size());
        lastBatchSizeBytes.set(payloadSizeInBytes);
        return payloadSizeInBytes;
    }

    void writeFinished(long startNanos) {
        writeLatency(TimeUnit.NANOSECONDS.toMillis(Math.max(0L, System.nanoTime() - startNanos)));
    }

    void writeLatency(long latencyMillis) {
        lastWriteLatencyMillis.set(latencyMillis);
        writeLatencyMillis.update(latencyMillis);
        if (recentWriteLatencyMillis != null) {
            recentWriteLatencyMillis.update(latencyMillis);
        }
    }

    void batchOutcome(SingleCompletionResultHandler.Outcome outcome) {
        switch (outcome) {
            case SUCCESS:
                increment(numBatchesSuccess, 1L);
                break;
            case COMPLETED_WITH_DROPS:
                increment(numBatchesCompletedWithDrops, 1L);
                break;
            case RETRIED:
                increment(numBatchesRetried, 1L);
                break;
            case FAILED:
                increment(numBatchesFailed, 1L);
                break;
        }
    }

    private void increment(Counter counter, long delta) {
        if (delta == 0L) {
            return;
        }
        try {
            mailboxExecutor.execute(
                    () -> counter.inc(delta),
                    "Update InfluxDB sink metric");
        } catch (RuntimeException error) {
            LOG.debug("Unable to enqueue an InfluxDB sink metric update", error);
        }
    }

    static long lineProtocolPayloadSizeInBytes(List<String> records) {
        long payloadSizeInBytes = 0L;
        for (int index = 0; index < records.size(); index++) {
            if (index > 0) {
                payloadSizeInBytes = Math.addExact(payloadSizeInBytes, 1L);
            }
            payloadSizeInBytes =
                    Math.addExact(
                            payloadSizeInBytes,
                            records.get(index).getBytes(StandardCharsets.UTF_8).length);
        }
        return payloadSizeInBytes;
    }
}
