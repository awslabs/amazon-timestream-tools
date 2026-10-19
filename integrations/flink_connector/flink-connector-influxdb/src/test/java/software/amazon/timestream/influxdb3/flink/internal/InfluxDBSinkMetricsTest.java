/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.internal;

import org.apache.flink.metrics.MeterView;
import org.junit.jupiter.api.Test;

import com.influxdb.v3.client.InfluxDBApiHttpException;

import java.net.SocketException;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class InfluxDBSinkMetricsTest {

    private static final String MANAGED_FLINK_PREFIX =
            "KinesisAnalytics.Connector=TimestreamInfluxDB3.";

    private static final Set<String> MANAGED_FLINK_METRICS =
            Set.of(
                    "numRecordsWritten",
                    "numRecordsSendErrors",
                    "numRecordsDropped",
                    "numRetries",
                    "numPartialWriteFailures",
                    "numAuthenticationRefreshes",
                    "numThrottledRequests",
                    "numWriteTimeouts",
                    "numBatchesFailed",
                    "numBatchesSuccess",
                    "numBatchesRetried",
                    "numBatchesCompletedWithDrops",
                    "numBytesWritten",
                    "numBytesOut",
                    "numRetryDelayMillis",
                    "numServerErrors",
                    "numClientErrors",
                    "numConnectionErrors",
                    "numRecordsWrittenPerSecond",
                    "numBytesWrittenPerSecond",
                    "numRecordsDroppedPerSecond",
                    "numRecordsSendErrorsPerSecond",
                    "numBatchesFailedPerSecond",
                    "writeLatencyP50Millis",
                    "writeLatencyP95Millis",
                    "writeLatencyP99Millis",
                    "lastWriteLatencyMillis",
                    "batchSizeLines",
                    "batchSizeBytes",
                    "numBufferedBytes",
                    "numInFlightWriteRequests",
                    "maxRetryAfterMillis");

    @Test
    void registersNothingUnderManagedFlinkGroupWhenDisabled() {
        WriterTestContext.CapturedContext context = WriterTestContext.capture();

        newMetrics(context, false, null);

        assertThat(context.metricNames())
                .isNotEmpty()
                .noneMatch(name -> name.startsWith("KinesisAnalytics"));
    }

    @Test
    void registersOperationalMetricsUnderManagedFlinkGroupWhenEnabled() {
        WriterTestContext.CapturedContext context = WriterTestContext.capture();

        newMetrics(context, true, null);

        assertThat(managedFlinkMetricNames(context, MANAGED_FLINK_PREFIX))
                .isEqualTo(MANAGED_FLINK_METRICS);
        assertThat(context.metricNames())
                .doesNotContain(MANAGED_FLINK_PREFIX + "writeLatencyMillis");
    }

    @Test
    void addsSinkDimensionWhenSinkNameIsConfigured() {
        WriterTestContext.CapturedContext context = WriterTestContext.capture();

        newMetrics(context, true, "sensors");

        assertThat(managedFlinkMetricNames(context, MANAGED_FLINK_PREFIX + "Sink=sensors."))
                .isEqualTo(MANAGED_FLINK_METRICS);
        assertThat(managedFlinkMetricNames(context, MANAGED_FLINK_PREFIX)).isEmpty();
    }

    @Test
    void sharesMetricInstancesWithTheSinkWriterGroup() {
        WriterTestContext.CapturedContext context = WriterTestContext.capture();
        InfluxDBSinkMetrics metrics = newMetrics(context, true, null);

        metrics.recordsWritten(3);
        metrics.recordsDropped(2);
        metrics.batchSubmitted(List.of("cpu value=1i", "cpu value=2i"));

        assertThat(context.counter(MANAGED_FLINK_PREFIX + "numRecordsWritten"))
                .isSameAs(context.counter("numRecordsWritten"));
        assertThat(context.counter(MANAGED_FLINK_PREFIX + "numRecordsDropped").getCount())
                .isEqualTo(2);
        assertThat(context.gaugeValue(MANAGED_FLINK_PREFIX + "batchSizeLines")).isEqualTo(2);
        assertThat(context.meter(MANAGED_FLINK_PREFIX + "numRecordsDroppedPerSecond"))
                .isInstanceOf(MeterView.class);
    }

    @Test
    void publishesRecentWriteLatencyPercentiles() {
        WriterTestContext.CapturedContext context = WriterTestContext.capture();
        InfluxDBSinkMetrics metrics = newMetrics(context, true, null);

        assertThat(context.gaugeDouble(MANAGED_FLINK_PREFIX + "writeLatencyP99Millis")).isZero();

        for (long latency = 1; latency <= 100; latency++) {
            metrics.writeLatency(latency);
        }

        assertThat(context.histogram("writeLatencyMillis").getCount()).isEqualTo(100);
        assertThat(context.gaugeValue(MANAGED_FLINK_PREFIX + "lastWriteLatencyMillis"))
                .isEqualTo(100);

        assertThat(context.gaugeDouble(MANAGED_FLINK_PREFIX + "writeLatencyP50Millis"))
                .isEqualTo(50);
        assertThat(context.gaugeDouble(MANAGED_FLINK_PREFIX + "writeLatencyP95Millis"))
                .isEqualTo(95);
        assertThat(context.gaugeDouble(MANAGED_FLINK_PREFIX + "writeLatencyP99Millis"))
                .isEqualTo(99);
    }

    @Test
    void publishesTheFlinkBytesOutCounter() {
        WriterTestContext.CapturedContext context = WriterTestContext.capture();
        newMetrics(context, true, null);

        assertThat(context.counter(MANAGED_FLINK_PREFIX + "numBytesOut"))
                .isSameAs(context.counter("numBytesOut"));
    }

    @Test
    void classifiesFailedWriteAttemptsByResponse() {
        WriterTestContext.CapturedContext context = WriterTestContext.capture();
        InfluxDBSinkMetrics metrics = newMetrics(context, false, null);

        metrics.writeAttemptFailed(new InfluxDBApiHttpException("unavailable", null, 503));
        metrics.writeAttemptFailed(
                new RuntimeException(new InfluxDBApiHttpException("bad gateway", null, 502)));
        metrics.writeAttemptFailed(new InfluxDBApiHttpException("bad request", null, 400));
        metrics.writeAttemptFailed(new InfluxDBApiHttpException("rate limited", null, 429));
        metrics.writeAttemptFailed(new RuntimeException(new SocketException("reset")));

        assertThat(context.counter("numServerErrors").getCount()).isEqualTo(2);
        assertThat(context.counter("numClientErrors").getCount()).isEqualTo(1);
        assertThat(context.counter("numConnectionErrors").getCount()).isEqualTo(1);
    }

    @Test
    void tracksTotalRetryDelayAndLargestRetryAfter() {
        WriterTestContext.CapturedContext context = WriterTestContext.capture();
        InfluxDBSinkMetrics metrics = newMetrics(context, false, null);

        metrics.retryScheduled(200, 0);
        metrics.retryScheduled(5_000, 5_000);
        metrics.retryScheduled(400, 2_000);

        assertThat(context.counter("numRetryDelayMillis").getCount()).isEqualTo(5_600);
        assertThat(context.gaugeValue("maxRetryAfterMillis")).isEqualTo(5_000);
    }

    @Test
    void returnsAndCountsBatchPayloadBytes() {
        WriterTestContext.CapturedContext context = WriterTestContext.capture();
        InfluxDBSinkMetrics metrics = newMetrics(context, false, null);

        long payloadBytes = metrics.batchSubmitted(List.of("cpu value=1i", "cpu value=2i"));
        metrics.bytesWritten(payloadBytes);

        assertThat(payloadBytes).isEqualTo(25);
        assertThat(context.counter("numBytesWritten").getCount()).isEqualTo(25);
    }

    private static InfluxDBSinkMetrics newMetrics(
            WriterTestContext.CapturedContext context, boolean enabled, String sinkName) {
        return new InfluxDBSinkMetrics(
                context.context(), new PayloadSizeTrackingRequestBuffer<>(), enabled, sinkName);
    }

    private static Set<String> managedFlinkMetricNames(
            WriterTestContext.CapturedContext context, String prefix) {
        return context.metricNames().stream()
                .filter(name -> name.startsWith(prefix))
                .map(name -> name.substring(prefix.length()))
                .filter(name -> !name.contains("."))
                .collect(Collectors.toSet());
    }
}
