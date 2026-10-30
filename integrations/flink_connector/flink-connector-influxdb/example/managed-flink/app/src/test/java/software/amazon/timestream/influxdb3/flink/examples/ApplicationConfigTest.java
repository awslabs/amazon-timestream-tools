/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.examples;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.InitialPosition;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Map;
import java.util.Properties;

class ApplicationConfigTest {

    @Test
    void parsesRuntimePropertiesAndCreatesSink() {
        Properties kinesis = properties("streamArn", streamArn(), "region", "us-east-1");
        Properties influx =
                properties(
                        "endpoint",
                        "https://example.test:8181",
                        "database",
                        "sensors",
                        "secretId",
                        "test/influxdb3/token",
                        "secretJsonField",
                        "token");

        ApplicationConfig config =
                ApplicationConfig.from(
                        Map.of(
                                ApplicationConfig.KINESIS_PROPERTY_GROUP,
                                kinesis,
                                ApplicationConfig.INFLUXDB3_PROPERTY_GROUP,
                                influx));

        assertThat(config.pipeline()).isEqualTo(ApplicationConfig.Pipeline.DATASTREAM);
        assertThat(config.initialPosition()).isEqualTo(InitialPosition.LATEST);
        assertThat(config.secretRegion()).isEqualTo("us-east-1");

        assertThat(config.endpoint()).isEqualTo("https://example.test:8181");
        assertThat(config.database()).isEqualTo("sensors");
        assertThat(config.secretId()).isEqualTo("test/influxdb3/token");
        assertThat(config.sinkOptions()).isEmpty();
        assertThat(KinesisToInfluxDB3Application.createInfluxSink(config)).isNotNull();
    }

    @Test
    void parsesOptionalSinkTuningProperties() {
        ApplicationConfig config =
                withSink(
                        properties(
                                "sink.batch.max-size", " 50000 ",
                                "sink.requests.max-buffered", "100000",
                                "sink.requests.max-inflight", "4",
                                "sink.flush-buffer.size", "20971520",
                                "sink.flush-buffer.timeout", "1000",
                                "sink.max-retries", "3"));

        assertThat(config.sinkOptions())
                .containsExactly(
                        Map.entry("sink.batch.max-size", "50000"),
                        Map.entry("sink.requests.max-buffered", "100000"),
                        Map.entry("sink.requests.max-inflight", "4"),
                        Map.entry("sink.flush-buffer.size", "20971520"),
                        Map.entry("sink.flush-buffer.timeout", "1000"),
                        Map.entry("sink.max-retries", "3"));
        assertThat(config.sinkIntOption("sink.batch.max-size")).isEqualTo(50_000);
        assertThat(config.sinkLongOption("sink.flush-buffer.size")).isEqualTo(20_971_520L);
        assertThat(config.sinkIntOption("sink.requests.max-inflight")).isEqualTo(4);
        assertThat(KinesisToInfluxDB3Application.createInfluxSink(config)).isNotNull();
    }

    @Test
    void rejectsUnknownSinkProperties() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> withSink(properties("sink.batch.max-sizes", "100")))
                .withMessageContaining("Unsupported property 'sink.batch.max-sizes'")
                .withMessageContaining("'Sink'");
    }

    @Test
    void rejectsNonIntegerSinkProperties() {
        ApplicationConfig config = withSink(properties("sink.batch.max-size", "10k"));

        assertThatIllegalArgumentException()
                .isThrownBy(() -> KinesisToInfluxDB3Application.createInfluxSink(config))
                .withMessage(
                        "Property 'sink.batch.max-size' in runtime property group 'Sink' "
                                + "must be an integer, but was '10k'");
    }

    /** Each case is rejected by the connector, which proves the value reached the builder. */
    @ParameterizedTest(name = "{0}={1}")
    @CsvSource({
        "sink.batch.max-size, 0, maxBatchSize",
        "sink.requests.max-buffered, 10000, maxBufferedRequests must be greater than maxBatchSize",
        "sink.requests.max-inflight, 0, maxInFlightRequests",
        "sink.flush-buffer.size, 1024, maxBatchSizeInBytes must be at least maxRecordSizeInBytes",
        "sink.flush-buffer.timeout, 0, maxTimeInBufferMillis",
        "sink.max-retries, -1, maxRetries must not be negative"
    })
    void appliesSinkPropertiesToTheConnector(String key, String value, String message) {
        ApplicationConfig config = withSink(properties(key, value));

        assertThatIllegalArgumentException()
                .isThrownBy(() -> KinesisToInfluxDB3Application.createInfluxSink(config))
                .withMessageContaining(message);
    }

    @Test
    void parsesTrimHorizonCaseInsensitively() {
        Properties kinesis =
                properties(
                        "streamArn",
                        streamArn(),
                        "region",
                        "us-east-1",
                        "initialPosition",
                        "trim_horizon");

        ApplicationConfig config =
                ApplicationConfig.from(
                        Map.of(
                                ApplicationConfig.KINESIS_PROPERTY_GROUP,
                                kinesis,
                                ApplicationConfig.INFLUXDB3_PROPERTY_GROUP,
                                influxProperties()));

        assertThat(config.initialPosition()).isEqualTo(InitialPosition.TRIM_HORIZON);
    }

    @Test
    void parsesTheSqlPipelineCaseInsensitively() {
        ApplicationConfig config =
                ApplicationConfig.from(
                        Map.of(
                                ApplicationConfig.APPLICATION_PROPERTY_GROUP,
                                properties("pipeline", "Sql"),
                                ApplicationConfig.KINESIS_PROPERTY_GROUP,
                                properties("streamArn", streamArn(), "region", "us-east-1"),
                                ApplicationConfig.INFLUXDB3_PROPERTY_GROUP,
                                influxProperties()));

        assertThat(config.pipeline()).isEqualTo(ApplicationConfig.Pipeline.SQL);
    }

    @Test
    void rejectsUnknownPipelines() {
        assertThatIllegalArgumentException()
                .isThrownBy(
                        () ->
                                ApplicationConfig.from(
                                        Map.of(
                                                ApplicationConfig.APPLICATION_PROPERTY_GROUP,
                                                properties("pipeline", "table"),
                                                ApplicationConfig.KINESIS_PROPERTY_GROUP,
                                                properties(
                                                        "streamArn",
                                                        streamArn(),
                                                        "region",
                                                        "us-east-1"),
                                                ApplicationConfig.INFLUXDB3_PROPERTY_GROUP,
                                                influxProperties())))
                .withMessageContaining("must be DATASTREAM, SQL, or DATAGEN");
    }

    @Test
    void rejectsPlaintextTokenRuntimeProperty() {
        Properties kinesis = properties("streamArn", streamArn(), "region", "us-east-1");
        Properties influx =
                properties(
                        "endpoint",
                        "https://example.test:8181",
                        "database",
                        "sensors",
                        "token",
                        "temporary-token");

        assertThatIllegalArgumentException()
                .isThrownBy(
                        () ->
                                ApplicationConfig.from(
                                        Map.of(
                                                ApplicationConfig.KINESIS_PROPERTY_GROUP,
                                                kinesis,
                                                ApplicationConfig.INFLUXDB3_PROPERTY_GROUP,
                                                influx)))
                .withMessageContaining("not supported")
                .withMessageContaining("Secrets Manager");
    }

    @Test
    void rejectsPlaintextTokenEvenWhenSecretIdIsConfigured() {
        Properties kinesis = properties("streamArn", streamArn(), "region", "us-east-1");
        Properties influx =
                properties(
                        "endpoint",
                        "https://example.test:8181",
                        "database",
                        "sensors",
                        "token",
                        "temporary-token",
                        "secretId",
                        "test/influxdb3/token");

        assertThatIllegalArgumentException()
                .isThrownBy(
                        () ->
                                ApplicationConfig.from(
                                        Map.of(
                                                ApplicationConfig.KINESIS_PROPERTY_GROUP,
                                                kinesis,
                                                ApplicationConfig.INFLUXDB3_PROPERTY_GROUP,
                                                influx)))
                .withMessageContaining("not supported");
    }

    @Test
    void requiresSecretId() {
        Properties kinesis = properties("streamArn", streamArn(), "region", "us-east-1");
        Properties influx =
                properties(
                        "endpoint",
                        "https://example.test:8181",
                        "database",
                        "sensors");

        assertThatIllegalArgumentException()
                .isThrownBy(
                        () ->
                                ApplicationConfig.from(
                                        Map.of(
                                                ApplicationConfig.KINESIS_PROPERTY_GROUP,
                                                kinesis,
                                                ApplicationConfig.INFLUXDB3_PROPERTY_GROUP,
                                                influx)))
                .withMessageContaining("secretId");
    }

    @Test
    void requiresTimestampForAtTimestampPosition() {
        Properties kinesis =
                properties(
                        "streamArn",
                        streamArn(),
                        "region",
                        "us-east-1",
                        "initialPosition",
                        "AT_TIMESTAMP");

        assertThatIllegalArgumentException()
                .isThrownBy(
                        () ->
                                ApplicationConfig.from(
                                        Map.of(
                                                ApplicationConfig.KINESIS_PROPERTY_GROUP,
                                                kinesis,
                                                ApplicationConfig.INFLUXDB3_PROPERTY_GROUP,
                                                influxProperties())))
                .withMessageContaining("initialTimestamp");
    }

    @Test
    void reportsMissingPropertyGroup() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> ApplicationConfig.from(Map.of()))
                .withMessage("Missing runtime property group 'KinesisSource'");
    }

    @Test
    void rejectsMultipleLineProtocolRowsInOneRecord() {
        assertThatIllegalArgumentException()
                .isThrownBy(
                        () ->
                                KinesisToInfluxDB3Application.requireSingleLine(
                                        "a value=1\nb value=2"))
                .withMessageContaining("exactly one");
    }

    @Test
    void buildsSourceToSinkJobGraphWithExplicitStringType() {
        ApplicationConfig config =
                ApplicationConfig.from(
                        Map.of(
                                ApplicationConfig.KINESIS_PROPERTY_GROUP,
                                properties(
                                        "streamArn",
                                        streamArn(),
                                        "region",
                                        "us-east-1"),
                                ApplicationConfig.INFLUXDB3_PROPERTY_GROUP,
                                influxProperties()));
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        KinesisToInfluxDB3Application.buildPipeline(environment, config);

        assertThat(environment.getStreamGraph().getStreamNodes()).hasSize(2);
    }

    @Test
    void parsesTheDataGenPipelineWithoutAKinesisSource() {
        Properties influx = influxProperties();
        influx.remove("secretJsonField");
        ApplicationConfig config =
                ApplicationConfig.from(
                        Map.of(
                                ApplicationConfig.APPLICATION_PROPERTY_GROUP,
                                properties("pipeline", "datagen"),
                                ApplicationConfig.INFLUXDB3_PROPERTY_GROUP,
                                influx,
                                ApplicationConfig.DATAGEN_PROPERTY_GROUP,
                                properties("ratePerSecond", "1000", "runId", "smoke")));

        assertThat(config.pipeline()).isEqualTo(ApplicationConfig.Pipeline.DATAGEN);
        assertThat(config.streamArn()).isNull();
        assertThat(config.kinesisRegion()).isNull();
        // Falls back to the AWS SDK region chain at runtime.
        assertThat(config.secretRegion()).isNull();
        assertThat(config.dataGen().ratePerSecond()).isEqualTo(1_000);
        assertThat(config.dataGen().runId()).isEqualTo("smoke");
        assertThat(config.dataGen().seriesCardinality())
                .isEqualTo(DataGenConfig.DEFAULT_SERIES_CARDINALITY);
    }

    @Test
    void ignoresTheDataGenGroupForOtherPipelines() {
        ApplicationConfig config =
                ApplicationConfig.from(
                        Map.of(
                                ApplicationConfig.KINESIS_PROPERTY_GROUP,
                                properties("streamArn", streamArn(), "region", "us-east-1"),
                                ApplicationConfig.INFLUXDB3_PROPERTY_GROUP,
                                influxProperties(),
                                ApplicationConfig.DATAGEN_PROPERTY_GROUP,
                                properties("ratePerSecond", "1000")));

        assertThat(config.dataGen()).isNull();
    }

    @Test
    void buildsGeneratorToSinkJobGraph() {
        ApplicationConfig config =
                ApplicationConfig.from(
                        Map.of(
                                ApplicationConfig.APPLICATION_PROPERTY_GROUP,
                                properties("pipeline", "DATAGEN"),
                                ApplicationConfig.INFLUXDB3_PROPERTY_GROUP,
                                influxProperties(),
                                ApplicationConfig.DATAGEN_PROPERTY_GROUP,
                                properties("ratePerSecond", "100")));
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        KinesisToInfluxDB3Application.buildDataGenPipeline(
                environment, config, 1_760_000_000_000_000_000L);

        assertThat(environment.getStreamGraph().getStreamNodes())
                .hasSize(2)
                .extracting(node -> node.getOperatorName())
                .anySatisfy(name -> assertThat(name).contains("datagen-line-protocol-source"))
                .anySatisfy(name -> assertThat(name).contains("timestream-influxdb3-sink"));
    }

    private static ApplicationConfig withSink(Properties sink) {
        return ApplicationConfig.from(
                Map.of(
                        ApplicationConfig.KINESIS_PROPERTY_GROUP,
                        properties("streamArn", streamArn(), "region", "us-east-1"),
                        ApplicationConfig.INFLUXDB3_PROPERTY_GROUP,
                        influxProperties(),
                        ApplicationConfig.SINK_PROPERTY_GROUP,
                        sink));
    }

    private static Properties influxProperties() {
        return properties(
                "endpoint",
                "https://example.test:8181",
                "database",
                "sensors",
                "secretId",
                "test/influxdb3/token",
                "secretJsonField",
                "token");
    }

    private static String streamArn() {
        return "arn:aws:kinesis:us-east-1:123456789012:stream/influxdb3-example-input";
    }

    private static Properties properties(String... values) {
        Properties properties = new Properties();
        for (int index = 0; index < values.length; index += 2) {
            properties.setProperty(values[index], values[index + 1]);
        }
        return properties;
    }
}
