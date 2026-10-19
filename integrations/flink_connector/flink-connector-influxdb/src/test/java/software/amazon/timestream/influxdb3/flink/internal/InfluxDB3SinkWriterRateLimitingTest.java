/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.internal;

import org.apache.flink.connector.base.sink.writer.strategy.BasicRequestInfo;
import org.apache.flink.connector.base.sink.writer.strategy.BasicResultInfo;
import org.apache.flink.connector.base.sink.writer.strategy.CongestionControlRateLimitingStrategy;
import org.apache.flink.connector.base.sink.writer.strategy.RateLimitingStrategy;
import org.junit.jupiter.api.Test;

import software.amazon.timestream.influxdb3.flink.InfluxDB3Sink;
import software.amazon.timestream.influxdb3.flink.InfluxDB3SinkTestAccess;

import static org.assertj.core.api.Assertions.assertThat;

class InfluxDB3SinkWriterRateLimitingTest {

    @Test
    void allowsTheConfiguredConcurrencyFromTheFirstRequest() {
        RateLimitingStrategy strategy = strategy(10_000, 8);

        for (int request = 0; request < 8; request++) {
            BasicRequestInfo batch = new BasicRequestInfo(10_000);
            assertThat(strategy.shouldBlock(batch)).as("request %d", request).isFalse();
            strategy.registerInFlightRequest(batch);
        }
        assertThat(strategy.shouldBlock(new BasicRequestInfo(10_000))).isTrue();
        assertThat(maxInFlightMessages(strategy)).isEqualTo(80_000);
    }

    @Test
    void halvesAfterFailedRecordsAndRecoversOneBatchPerSuccess() {
        RateLimitingStrategy strategy = strategy(1_000, 8);

        complete(strategy, 1_000, 1);
        assertThat(maxInFlightMessages(strategy)).isEqualTo(4_000);

        complete(strategy, 1_000, 0);
        assertThat(maxInFlightMessages(strategy)).isEqualTo(5_000);

        for (int request = 0; request < 10; request++) {
            complete(strategy, 1_000, 0);
        }
        assertThat(maxInFlightMessages(strategy)).isEqualTo(8_000);
    }

    @Test
    void capsTheInFlightRecordLimitAtIntegerMaxValue() {
        assertThat(maxInFlightMessages(strategy(Integer.MAX_VALUE / 2, 8)))
                .isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void usesTheStrategyInTheWriterConfiguration() {
        InfluxDB3SinkConfig config = config(500, 4);

        assertThat(InfluxDB3SinkWriter.writerConfiguration(config).getRateLimitingStrategy())
                .isInstanceOfSatisfying(
                        CongestionControlRateLimitingStrategy.class,
                        strategy -> assertThat(strategy.getMaxBatchSize())
                                .isEqualTo(2_000));
    }

    private static void complete(RateLimitingStrategy strategy, int batchSize, int failed) {
        BasicRequestInfo request = new BasicRequestInfo(batchSize);
        strategy.registerInFlightRequest(request);
        strategy.registerCompletedRequest(new BasicResultInfo(failed, batchSize));
    }

    /** Flink reports the current in-flight record allowance as its maximum batch size. */
    private static int maxInFlightMessages(RateLimitingStrategy strategy) {
        return ((CongestionControlRateLimitingStrategy) strategy).getMaxBatchSize();
    }

    private static RateLimitingStrategy strategy(int maxBatchSize, int maxInFlightRequests) {
        return InfluxDB3SinkWriter.rateLimitingStrategy(config(maxBatchSize, maxInFlightRequests));
    }

    private static InfluxDB3SinkConfig config(int maxBatchSize, int maxInFlightRequests) {
        return InfluxDB3SinkTestAccess.configurationOf(
                InfluxDB3Sink.<String>builder()
                        .setEndpoint("http://localhost:8086")
                        .setDatabase("metrics")
                        .setToken("token")
                        .setConverter(value -> value)
                        .setMaxBatchSize(maxBatchSize)
                        .setMaxBufferedRequests(
                                (int) Math.min(Integer.MAX_VALUE, (long) maxBatchSize + 1))
                        .setMaxBatchSizeInBytes(Long.MAX_VALUE / 4)
                        .setMaxBufferedBytes(Long.MAX_VALUE / 4)
                        .setMaxInFlightRequests(maxInFlightRequests)
                        .build());
    }
}
