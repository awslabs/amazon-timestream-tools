/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.examples;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.aws.config.AWSConfigOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import software.amazon.timestream.influxdb3.flink.InfluxDB3Sink;
import software.amazon.timestream.influxdb3.flink.InfluxDB3SinkBuilder;
import software.amazon.timestream.influxdb3.flink.auth.SecretsManagerTokenProvider;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/**
 * Managed Flink example that writes Kinesis records to InfluxDB 3.
 *
 * <p>The {@code Application.pipeline} runtime property selects the pipeline: {@code datastream}
 * (default) forwards line protocol records through the DataStream API sink, {@code sql} runs
 * {@link SqlPipeline}, and {@code datagen} writes generated line protocol for load tests without
 * reading Kinesis.
 */
public final class KinesisToInfluxDB3Application {

    static final String LOCAL_PROPERTIES_FILE_ENV = "APPLICATION_PROPERTIES_FILE";

    private KinesisToInfluxDB3Application() {}

    public static void main(String[] args) throws Exception {
        ApplicationConfig config = ApplicationConfig.from(loadApplicationProperties());
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        switch (config.pipeline()) {
            case DATASTREAM -> {
                buildPipeline(environment, config);
                environment.execute("Kinesis to Timestream for InfluxDB 3");
            }
            // executeSql only submits the INSERT; await it so main blocks like execute() above.
            case SQL -> SqlPipeline.execute(StreamTableEnvironment.create(environment), config)
                    .await();
            case DATAGEN -> {
                buildDataGenPipeline(environment, config, currentEpochNanos());
                environment.execute("Generated line protocol to Timestream for InfluxDB 3");
            }
        }
    }

    static void buildPipeline(
            StreamExecutionEnvironment environment, ApplicationConfig config) {
        environment
                .fromSource(
                        createKinesisSource(config),
                        WatermarkStrategy.noWatermarks(),
                        "kinesis-line-protocol-source")
                .returns(Types.STRING)
                .uid("kinesis-line-protocol-source")
                .sinkTo(createInfluxSink(config))
                .name("timestream-influxdb3-sink")
                .uid("timestream-influxdb3-sink");
    }

    /**
     * Builds the load-test pipeline, which generates line protocol in the job instead of reading
     * Kinesis.
     *
     * @param startEpochNanos timestamp of the first generated row; captured once when the job is
     *     built so all subtasks and job restarts generate the same timestamps. An application
     *     update or restart builds the job again with a new value.
     */
    static void buildDataGenPipeline(
            StreamExecutionEnvironment environment,
            ApplicationConfig config,
            long startEpochNanos) {
        DataGenConfig dataGen = config.dataGen();
        DataGeneratorSource<String> source =
                new DataGeneratorSource<>(
                        new LineProtocolGenerator(dataGen, startEpochNanos),
                        dataGen.totalRecords(),
                        dataGen.ratePerSecond() > 0
                                // Divided evenly across the source subtasks.
                                ? RateLimiterStrategy.perSecond(dataGen.ratePerSecond())
                                : RateLimiterStrategy.noOp(),
                        Types.STRING);
        environment
                .fromSource(
                        source, WatermarkStrategy.noWatermarks(), "datagen-line-protocol-source")
                .uid("datagen-line-protocol-source")
                .sinkTo(createInfluxSink(config))
                .name("timestream-influxdb3-sink")
                .uid("timestream-influxdb3-sink");
    }

    private static long currentEpochNanos() {
        Instant now = Instant.now();
        return Math.addExact(
                Math.multiplyExact(now.getEpochSecond(), 1_000_000_000L), now.getNano());
    }

    static KinesisStreamsSource<String> createKinesisSource(ApplicationConfig config) {
        Configuration sourceConfiguration = new Configuration();
        sourceConfiguration.set(AWSConfigOptions.AWS_REGION_OPTION, config.kinesisRegion());
        sourceConfiguration.set(
                KinesisSourceConfigOptions.STREAM_INITIAL_POSITION, config.initialPosition());
        if (config.initialTimestamp() != null) {
            sourceConfiguration.set(
                    KinesisSourceConfigOptions.STREAM_INITIAL_TIMESTAMP,
                    config.initialTimestamp());
        }

        return KinesisStreamsSource.<String>builder()
                .setStreamArn(config.streamArn())
                .setSourceConfig(sourceConfiguration)
                .setDeserializationSchema(new SimpleStringSchema())
                .build();
    }

    static InfluxDB3Sink<String> createInfluxSink(ApplicationConfig config) {
        SecretsManagerTokenProvider.Builder tokenProvider =
                SecretsManagerTokenProvider.builder()
                        .setSecretId(config.secretId())
                        .setRegion(config.secretRegion());
        if (config.secretJsonField() != null) {
            tokenProvider.setJsonField(config.secretJsonField());
        }

        InfluxDB3SinkBuilder<String> builder =
                InfluxDB3Sink.<String>builder()
                        .setEndpoint(config.endpoint())
                        .setDatabase(config.database())
                        .setTokenProvider(tokenProvider.build())
                        .setConverter(KinesisToInfluxDB3Application::requireSingleLine)
                        .setManagedFlinkMetricsEnabled(true);
        Optional.ofNullable(config.sinkIntOption("sink.batch.max-size"))
                .ifPresent(builder::setMaxBatchSize);
        Optional.ofNullable(config.sinkIntOption("sink.requests.max-buffered"))
                .ifPresent(builder::setMaxBufferedRequests);
        Optional.ofNullable(config.sinkIntOption("sink.requests.max-inflight"))
                .ifPresent(builder::setMaxInFlightRequests);
        Optional.ofNullable(config.sinkLongOption("sink.flush-buffer.size"))
                .ifPresent(builder::setMaxBatchSizeInBytes);
        Optional.ofNullable(config.sinkLongOption("sink.flush-buffer.timeout"))
                .ifPresent(builder::setMaxTimeInBufferMS);
        Optional.ofNullable(config.sinkIntOption("sink.max-retries"))
                .ifPresent(builder::setMaxRetries);
        return builder.build();
    }

    static String requireSingleLine(String lineProtocol) {
        if (lineProtocol == null || lineProtocol.isBlank()) {
            throw new IllegalArgumentException("Kinesis record must contain line protocol");
        }
        if (lineProtocol.indexOf('\n') >= 0 || lineProtocol.indexOf('\r') >= 0) {
            throw new IllegalArgumentException(
                    "Each Kinesis record must contain exactly one line protocol row");
        }
        return lineProtocol;
    }

    private static Map<String, Properties> loadApplicationProperties() throws IOException {
        String localPropertiesFile = System.getenv(LOCAL_PROPERTIES_FILE_ENV);
        if (localPropertiesFile != null && !localPropertiesFile.isBlank()) {
            return KinesisAnalyticsRuntime.getApplicationProperties(localPropertiesFile);
        }
        return KinesisAnalyticsRuntime.getApplicationProperties();
    }
}
