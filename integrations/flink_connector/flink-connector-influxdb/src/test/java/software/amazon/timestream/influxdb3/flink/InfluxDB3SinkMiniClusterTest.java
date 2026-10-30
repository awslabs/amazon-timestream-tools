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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import software.amazon.timestream.influxdb3.flink.internal.InfluxDBClientFactory;
import software.amazon.timestream.influxdb3.flink.internal.InfluxDB3SinkConfig;
import software.amazon.timestream.influxdb3.flink.internal.InfluxDBWriteClient;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class InfluxDB3SinkMiniClusterTest {

    @TempDir Path temporaryDirectory;

    @Test
    void writesAndFlushesThroughTheFlinkSinkLifecycle() throws Exception {
        Path output = temporaryDirectory.resolve("writes.lp");
        InfluxDB3Sink<String> sink =
                InfluxDB3Sink.<String>builder()
                        .setEndpoint("https://example.com:8181")
                        .setDatabase("metrics")
                        .setToken("token")
                        .setConverter(value -> value)
                        .setMaxBatchSize(2)
                        .setMaxBufferedRequests(4)
                        .setMaxInFlightRequests(1)
                        .setMaxTimeInBufferMS(50)
                        .setRetryBackoff(Duration.ZERO, Duration.ZERO, 0)
                        // Registers the Managed Flink metric group with a real Flink registry.
                        .setManagedFlinkMetricsEnabled(true)
                        .setManagedFlinkMetricsSinkName("minicluster")
                        .setClientFactory(new FileRecordingClientFactory(output.toString()))
                        .build();

        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.createLocalEnvironment(1);
        environment.setParallelism(1);
        environment
                .fromData("cpu value=1i", "cpu value=2i", "cpu value=3i")
                .sinkTo(sink)
                .name("influxdb3-test");

        environment.execute("influxdb3-connector-lifecycle-test");

        assertThat(Files.readAllLines(output, StandardCharsets.UTF_8))
                .containsExactly("cpu value=1i", "cpu value=2i", "cpu value=3i");
    }

    @Test
    void restoresFromCheckpointAfterFailureWithoutLosingRecords() throws Exception {
        Path output = temporaryDirectory.resolve("recovered-writes.lp");
        Path failureMarker = temporaryDirectory.resolve("intentional-failure.marker");
        int recordCount = 12;
        InfluxDB3Sink<String> sink =
                InfluxDB3Sink.<String>builder()
                        .setEndpoint("https://example.com:8181")
                        .setDatabase("metrics")
                        .setToken("token")
                        .setConverter(value -> value)
                        .setMaxBatchSize(10)
                        .setMaxBufferedRequests(20)
                        .setMaxInFlightRequests(1)
                        .setMaxTimeInBufferMS(60_000)
                        .setRetryBackoff(Duration.ZERO, Duration.ZERO, 0)
                        .setClientFactory(new FileRecordingClientFactory(output.toString()))
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
                        "checkpointing-number-source")
                .uid("checkpointing-number-source")
                .map(
                        new FailOnceAfterCheckpointMap<>(
                                failureMarker.toString(),
                                record ->
                                        "checkpoint_metric,id="
                                                + record
                                                + " value="
                                                + record
                                                + "i"),
                        Types.STRING)
                .name("fail-once-after-checkpoint")
                .uid("fail-once-after-checkpoint")
                .sinkTo(sink)
                .name("influxdb3-recovery-test")
                .uid("influxdb3-recovery-test");

        environment.execute("influxdb3-connector-checkpoint-recovery-test");

        List<Integer> writtenIds = new ArrayList<>();
        for (String line : Files.readAllLines(output, StandardCharsets.UTF_8)) {
            int idStart = line.indexOf("id=") + 3;
            int idEnd = line.indexOf(' ', idStart);
            writtenIds.add(Integer.parseInt(line.substring(idStart, idEnd)));
        }
        List<Integer> expectedIds =
                IntStream.range(0, recordCount).boxed().toList();

        assertThat(Files.exists(failureMarker))
                .as("the pipeline failed once after a completed checkpoint")
                .isTrue();
        assertThat(writtenIds)
                .as("at-least-once recovery may duplicate records but must not lose them")
                .containsAll(expectedIds);
    }

    private static final class FileRecordingClientFactory
            implements InfluxDBClientFactory, Serializable {
        private static final long serialVersionUID = 1L;
        private final String outputPath;

        private FileRecordingClientFactory(String outputPath) {
            this.outputPath = outputPath;
        }

        @Override
        public InfluxDBWriteClient create(InfluxDB3SinkConfig config, char[] token) {
            return new InfluxDBWriteClient() {
                @Override
                public void writeRecords(List<String> records) {
                    try {
                        Files.write(
                                Path.of(outputPath),
                                records,
                                StandardCharsets.UTF_8,
                                StandardOpenOption.CREATE,
                                StandardOpenOption.APPEND);
                    } catch (IOException error) {
                        throw new IllegalStateException("Unable to record test writes", error);
                    }
                }

                @Override
                public void close() {}
            };
        }
    }
}
