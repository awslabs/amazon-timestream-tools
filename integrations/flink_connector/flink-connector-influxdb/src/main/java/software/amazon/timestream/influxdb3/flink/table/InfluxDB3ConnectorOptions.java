/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import com.influxdb.v3.client.write.WritePrecision;
import software.amazon.timestream.influxdb3.flink.InfluxDB3SinkBuilder;

import java.time.Duration;

/**
 * Table API options for the {@code timestream-influxdb3} connector.
 *
 * <p>Batching options are shared with other asynchronous sinks through {@link
 * org.apache.flink.connector.base.table.AsyncSinkConnectorOptions}.
 */
@PublicEvolving
public final class InfluxDB3ConnectorOptions {

    /** Base HTTPS cluster endpoint, without {@code /api/v3/write_lp}. */
    public static final ConfigOption<String> ENDPOINT =
            ConfigOptions.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Base HTTPS cluster endpoint, including the port, without "
                                    + "/api/v3/write_lp.");

    /** Target InfluxDB 3 database. */
    public static final ConfigOption<String> DATABASE =
            ConfigOptions.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Existing InfluxDB 3 database to write to.");

    /** Comma-separated list of string columns written as tags. */
    public static final ConfigOption<String> TAG_COLUMNS =
            ConfigOptions.key("tag-columns")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Comma-separated list of string columns written as tags. All other "
                                    + "columns except the timestamp column are written as fields.");

    /** Column providing each row's timestamp. */
    public static final ConfigOption<String> TIMESTAMP_COLUMN =
            ConfigOptions.key("timestamp-column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "TIMESTAMP, TIMESTAMP_LTZ, or BIGINT column providing each row's "
                                    + "timestamp. BIGINT values must already be in 'precision' "
                                    + "units.");

    /** Timestamp precision written to InfluxDB. */
    public static final ConfigOption<WritePrecision> PRECISION =
            ConfigOptions.key("precision")
                    .enumType(WritePrecision.class)
                    .defaultValue(InfluxDB3SinkBuilder.DEFAULT_WRITE_PRECISION)
                    .withDescription(
                            "Timestamp precision: ns, us, ms, or s. Coarser precisions truncate "
                                    + "TIMESTAMP values.");

    /** Static API token. */
    public static final ConfigOption<String> TOKEN =
            ConfigOptions.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Static API token. It is stored in the table definition; prefer "
                                    + "'token.secret-id'. Exactly one of the two is required.");

    /** AWS Secrets Manager secret containing the API token. */
    public static final ConfigOption<String> TOKEN_SECRET_ID =
            ConfigOptions.key("token.secret-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "AWS Secrets Manager secret name or ARN containing the API token.");

    /** Top-level JSON field containing the token in the secret. */
    public static final ConfigOption<String> TOKEN_SECRET_JSON_FIELD =
            ConfigOptions.key("token.secret-json-field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Top-level JSON field containing the token. Omit when the entire "
                                    + "secret value is the token.");

    /** Region containing the secret. */
    public static final ConfigOption<String> TOKEN_SECRET_REGION =
            ConfigOptions.key("token.secret-region")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Region containing the secret. Defaults to the AWS SDK region "
                                    + "provider chain.");

    /** Whether local and server-side validation failures fail the job. */
    public static final ConfigOption<Boolean> FAIL_ON_VALIDATION =
            ConfigOptions.key("sink.fail-on-validation")
                    .booleanType()
                    .defaultValue(InfluxDB3SinkBuilder.DEFAULT_FAIL_ON_VALIDATION)
                    .withDescription(
                            "Fail the job on rows that cannot be converted, such as a null "
                                    + "timestamp or no non-null fields, and on non-partial HTTP "
                                    + "400/422 responses. When false, such rows are dropped and "
                                    + "counted in numRecordsDropped.");

    /** Whether lines rejected in a partial write fail the job. */
    public static final ConfigOption<Boolean> FAIL_ON_REJECTED =
            ConfigOptions.key("sink.fail-on-rejected")
                    .booleanType()
                    .defaultValue(InfluxDB3SinkBuilder.DEFAULT_FAIL_ON_REJECTED)
                    .withDescription(
                            "Fail the job instead of dropping lines rejected in a partial write.");

    /** Whether other terminal errors and retry exhaustion fail the job. */
    public static final ConfigOption<Boolean> FAIL_ON_ERROR =
            ConfigOptions.key("sink.fail-on-error")
                    .booleanType()
                    .defaultValue(InfluxDB3SinkBuilder.DEFAULT_FAIL_ON_ERROR)
                    .withDescription(
                            "Fail the job on other terminal write errors and retry exhaustion.");

    /** Retries after the initial write attempt. */
    public static final ConfigOption<Integer> MAX_RETRIES =
            ConfigOptions.key("sink.max-retries")
                    .intType()
                    .defaultValue(InfluxDB3SinkBuilder.DEFAULT_MAX_RETRIES)
                    .withDescription(
                            "Retries after the initial write attempt for retryable failures, "
                                    + "with exponential backoff and jitter.");

    /** Whether request payloads are gzip-compressed. */
    public static final ConfigOption<Boolean> GZIP_ENABLED =
            ConfigOptions.key("gzip.enabled")
                    .booleanType()
                    .defaultValue(InfluxDB3SinkBuilder.DEFAULT_GZIP_ENABLED)
                    .withDescription("Compress HTTP request payloads with gzip.");

    /** Whether valid rows in a partially invalid batch are committed. */
    public static final ConfigOption<Boolean> ACCEPT_PARTIAL =
            ConfigOptions.key("accept-partial")
                    .booleanType()
                    .defaultValue(InfluxDB3SinkBuilder.DEFAULT_ACCEPT_PARTIAL)
                    .withDescription("Allow valid rows in a partially invalid batch to commit.");

    /** Whether writes are acknowledged before durable synchronization. */
    public static final ConfigOption<Boolean> NO_SYNC =
            ConfigOptions.key("no-sync")
                    .booleanType()
                    .defaultValue(InfluxDB3SinkBuilder.DEFAULT_NO_SYNC)
                    .withDescription(
                            "Acknowledge writes before durable synchronization, trading "
                                    + "durability for latency.");

    /** Deadline for Flink to receive the result of one write attempt. */
    public static final ConfigOption<Duration> REQUEST_TIMEOUT =
            ConfigOptions.key("request-timeout")
                    .durationType()
                    .defaultValue(InfluxDB3SinkBuilder.DEFAULT_REQUEST_TIMEOUT)
                    .withDescription(
                            "Deadline for Flink to receive a write result, including retry "
                                    + "delay. Must exceed the 10 s write timeout plus the 30 s "
                                    + "maximum retry backoff.");

    /** Whether sink metrics are also published to Managed Flink CloudWatch metrics. */
    public static final ConfigOption<Boolean> MANAGED_FLINK_METRICS_ENABLED =
            ConfigOptions.key("sink.metrics.managed-flink.enabled")
                    .booleanType()
                    .defaultValue(InfluxDB3SinkBuilder.DEFAULT_MANAGED_FLINK_METRICS_ENABLED)
                    .withDescription(
                            "Also register sink metrics under the KinesisAnalytics metric group "
                                    + "so Amazon Managed Service for Apache Flink publishes "
                                    + "them to CloudWatch. Published metrics are billed as "
                                    + "CloudWatch custom metrics.");

    /** {@code Sink} dimension of metrics published to Managed Flink CloudWatch metrics. */
    public static final ConfigOption<String> MANAGED_FLINK_METRICS_SINK_NAME =
            ConfigOptions.key("sink.metrics.managed-flink.sink-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Sink dimension of the published CloudWatch metrics. Set a distinct "
                                    + "name for each InfluxDB sink in a job. Requires "
                                    + "'sink.metrics.managed-flink.enabled'.");

    private InfluxDB3ConnectorOptions() {}
}
