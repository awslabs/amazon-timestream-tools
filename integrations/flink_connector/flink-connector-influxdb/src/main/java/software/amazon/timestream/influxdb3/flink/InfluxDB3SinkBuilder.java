/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.sink.AsyncSinkBaseBuilder;

import com.influxdb.v3.client.write.WritePrecision;
import software.amazon.timestream.influxdb3.flink.auth.StaticTokenProvider;
import software.amazon.timestream.influxdb3.flink.auth.TokenProvider;
import software.amazon.timestream.influxdb3.flink.failure.DefaultWriteFailureHandler;
import software.amazon.timestream.influxdb3.flink.failure.WriteFailureHandler;
import software.amazon.timestream.influxdb3.flink.internal.DefaultInfluxDBClientFactory;
import software.amazon.timestream.influxdb3.flink.internal.InfluxDBClientFactory;
import software.amazon.timestream.influxdb3.flink.internal.InfluxDB3SinkConfig;
import software.amazon.timestream.influxdb3.flink.internal.InfluxDB3WriteRequest;

import java.net.URI;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Builder for {@link InfluxDB3Sink}.
 *
 * <p>The endpoint, database, token provider, and converter are required. Optional settings and
 * defaults are documented on their setter methods and exposed through the {@code DEFAULT_*}
 * constants.
 *
 * <p>Configuration is fully validated when {@link #build()} is called. Setters may reject values
 * immediately when constructing public value types that enforce their own invariants.
 *
 * @param <InputT> type of records consumed by the sink
 */
@PublicEvolving
public final class InfluxDB3SinkBuilder<InputT>
        extends AsyncSinkBaseBuilder<
                InputT, InfluxDB3WriteRequest, InfluxDB3SinkBuilder<InputT>> {

    /** Default precision used to interpret line protocol timestamps. */
    public static final WritePrecision DEFAULT_WRITE_PRECISION = WritePrecision.NS;

    /** Default maximum number of records in one write request. */
    public static final int DEFAULT_MAX_BATCH_SIZE = 10_000;

    /** Default maximum number of concurrent write requests. */
    public static final int DEFAULT_MAX_IN_FLIGHT_REQUESTS = 8;

    /** Default maximum number of records buffered by each sink subtask. */
    public static final int DEFAULT_MAX_BUFFERED_REQUESTS = 20_000;

    /** Default maximum uncompressed size of one write request. */
    public static final long DEFAULT_MAX_BATCH_SIZE_IN_BYTES = 10L * 1024 * 1024;

    /** Default maximum uncompressed payload bytes buffered by each sink subtask. */
    public static final long DEFAULT_MAX_BUFFERED_BYTES = 64L * 1024 * 1024;

    /** Default maximum time a record remains buffered before being sent. */
    public static final long DEFAULT_MAX_TIME_IN_BUFFER_MILLIS = 5_000;

    /** Default maximum uncompressed size of one line protocol record. */
    public static final long DEFAULT_MAX_RECORD_SIZE_IN_BYTES = 1024L * 1024;

    /** Default number of retries after the initial write attempt. */
    public static final int DEFAULT_MAX_RETRIES = 8;

    /** Default delay before the first retry. */
    public static final Duration DEFAULT_INITIAL_RETRY_BACKOFF = Duration.ofMillis(200);

    /** Default upper bound for retry delays, including {@code Retry-After}. */
    public static final Duration DEFAULT_MAX_RETRY_BACKOFF = Duration.ofSeconds(30);

    /** Default proportional jitter applied to retry delays. */
    public static final double DEFAULT_RETRY_JITTER = 0.2;

    /** Default timeout for establishing an InfluxDB HTTP connection. */
    public static final Duration DEFAULT_CONNECTION_TIMEOUT = Duration.ofSeconds(10);

    /** Default hard deadline for one blocking InfluxDB HTTP write attempt. */
    public static final Duration DEFAULT_WRITE_TIMEOUT = Duration.ofSeconds(10);

    /** Default timeout for Flink to receive a result for one write attempt. */
    public static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(60);

    /** Default behavior when a Flink request timeout expires. */
    public static final boolean DEFAULT_FAIL_ON_REQUEST_TIMEOUT = true;

    /** Default behavior for gzip request compression. */
    public static final boolean DEFAULT_GZIP_ENABLED = true;

    /** Default behavior for accepting valid rows from a partially invalid batch. */
    public static final boolean DEFAULT_ACCEPT_PARTIAL = true;

    /** Default behavior for acknowledging writes before durable synchronization. */
    public static final boolean DEFAULT_NO_SYNC = false;

    /** Default behavior for permanently rejected lines from partial writes. */
    public static final boolean DEFAULT_FAIL_ON_REJECTED = false;

    /** Default behavior for local and server-side validation failures. */
    public static final boolean DEFAULT_FAIL_ON_VALIDATION = true;

    /** Default behavior for terminal write errors and retry exhaustion. */
    public static final boolean DEFAULT_FAIL_ON_ERROR = true;

    /** Default behavior for logging terminally failed line protocol values. */
    public static final boolean DEFAULT_PRINT_FAILED_REQUESTS = false;

    /** Default behavior for publishing sink metrics to Managed Flink CloudWatch metrics. */
    public static final boolean DEFAULT_MANAGED_FLINK_METRICS_ENABLED = false;

    private static final Pattern MANAGED_FLINK_METRICS_SINK_NAME_PATTERN =
            Pattern.compile("[A-Za-z0-9_]{1,255}");

    /**
     * Minimum margin between {@code requestTimeout} and {@code writeTimeout + maxRetryBackoff}.
     * Leaves room for a cancellation confirmation wait that is strictly shorter than the margin.
     */
    private static final long MIN_REQUEST_TIMEOUT_HEADROOM_MILLIS = 2L;

    private String endpoint;
    private String database;
    private TokenProvider tokenProvider;
    private LineProtocolConverter<InputT> converter;
    private WriteFailureHandler failureHandler;
    private WritePrecision writePrecision = DEFAULT_WRITE_PRECISION;
    private long maxBufferedBytes = DEFAULT_MAX_BUFFERED_BYTES;
    private Duration connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;
    private Duration writeTimeout = DEFAULT_WRITE_TIMEOUT;
    private Duration requestTimeout = DEFAULT_REQUEST_TIMEOUT;
    private boolean failOnRequestTimeout = DEFAULT_FAIL_ON_REQUEST_TIMEOUT;
    private boolean gzipEnabled = DEFAULT_GZIP_ENABLED;
    private boolean acceptPartial = DEFAULT_ACCEPT_PARTIAL;
    private boolean noSync = DEFAULT_NO_SYNC;
    private boolean failOnRejected = DEFAULT_FAIL_ON_REJECTED;
    private boolean failOnValidation = DEFAULT_FAIL_ON_VALIDATION;
    private boolean failOnError = DEFAULT_FAIL_ON_ERROR;
    private boolean printFailedRequests = DEFAULT_PRINT_FAILED_REQUESTS;
    private boolean failOnRejectedConfigured;
    private boolean failOnErrorConfigured;
    private boolean customFailureHandlerConfigured;
    private int maxRetries = DEFAULT_MAX_RETRIES;
    private Duration initialRetryBackoff = DEFAULT_INITIAL_RETRY_BACKOFF;
    private Duration maxRetryBackoff = DEFAULT_MAX_RETRY_BACKOFF;
    private double retryJitter = DEFAULT_RETRY_JITTER;
    private boolean managedFlinkMetricsEnabled = DEFAULT_MANAGED_FLINK_METRICS_ENABLED;
    private String managedFlinkMetricsSinkName;
    private InfluxDBClientFactory clientFactory = DefaultInfluxDBClientFactory.INSTANCE;

    /** Creates a sink builder with the documented defaults. */
    public InfluxDB3SinkBuilder() {}

    /**
     * Sets the base HTTPS endpoint for the InfluxDB 3 cluster.
     *
     * <p>Required; there is no default.
     *
     * <p>The endpoint is parsed and validated when {@link #build()} is called.
     *
     * @param endpoint cluster endpoint, including its port when required
     * @return this builder
     */
    public InfluxDB3SinkBuilder<InputT> setEndpoint(String endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    /**
     * Sets the base HTTPS endpoint for the InfluxDB 3 cluster.
     *
     * <p>Required; there is no default.
     *
     * <p>The endpoint is validated when {@link #build()} is called.
     *
     * @param endpoint cluster endpoint, including its port when required
     * @return this builder
     */
    public InfluxDB3SinkBuilder<InputT> setEndpoint(URI endpoint) {
        this.endpoint = endpoint == null ? null : endpoint.toString();
        return this;
    }

    /**
     * Sets the target InfluxDB 3 database.
     *
     * <p>Required; there is no default.
     *
     * @param database database name
     * @return this builder
     */
    public InfluxDB3SinkBuilder<InputT> setDatabase(String database) {
        this.database = database;
        return this;
    }

    /**
     * Sets a fixed token that will be serialized with the Flink job.
     *
     * <p>This is intended for local development. Deployed jobs should use {@link
     * #setTokenProvider(TokenProvider)}.
     * Either this method or {@link #setTokenProvider(TokenProvider)} is required.
     * The token is validated immediately by {@link StaticTokenProvider}.
     *
     * @param token InfluxDB API token
     * @return this builder
     */
    public InfluxDB3SinkBuilder<InputT> setToken(String token) {
        this.tokenProvider = new StaticTokenProvider(token);
        return this;
    }

    /**
     * Sets the token provider initialized independently in each sink subtask.
     *
     * <p>Either this method or {@link #setToken(String)} is required.
     *
     * @param tokenProvider serializable token provider
     * @return this builder
     */
    public InfluxDB3SinkBuilder<InputT> setTokenProvider(TokenProvider tokenProvider) {
        this.tokenProvider = tokenProvider;
        return this;
    }

    /**
     * Sets the converter that produces one line protocol row per input record.
     *
     * <p>Required; there is no default.
     *
     * @param converter serializable record converter
     * @return this builder
     */
    public InfluxDB3SinkBuilder<InputT> setConverter(LineProtocolConverter<InputT> converter) {
        this.converter = converter;
        return this;
    }

    /**
     * Sets the policy used to classify record-level write failures.
     *
     * <p>A custom handler replaces {@link #setFailOnRejected(boolean)} and {@link
     * #setFailOnError(boolean)}. Explicitly configuring either of those options together with a
     * custom handler is invalid. Validation and failed-request logging remain controlled by
     * {@link #setFailOnValidation(boolean)} and {@link #setPrintFailedRequests(boolean)}.
     * Connector-internal failures and hard-fatal HTTP responses are not delegated to the handler.
     * By default, the built-in handler uses {@link #DEFAULT_FAIL_ON_REJECTED} and {@link
     * #DEFAULT_FAIL_ON_ERROR}.
     *
     * @param failureHandler serializable failure handler
     * @return this builder
     */
    public InfluxDB3SinkBuilder<InputT> setFailureHandler(WriteFailureHandler failureHandler) {
        this.failureHandler = failureHandler;
        this.customFailureHandlerConfigured = true;
        return this;
    }

    /**
     * Sets the precision used to interpret timestamps in line protocol records.
     *
     * <p>Default: {@link #DEFAULT_WRITE_PRECISION}.
     *
     * @param writePrecision timestamp precision
     * @return this builder
     */
    public InfluxDB3SinkBuilder<InputT> setWritePrecision(WritePrecision writePrecision) {
        this.writePrecision = writePrecision;
        return this;
    }

    /**
     * Sets the maximum number of line protocol rows in one HTTP request.
     *
     * <p>Default: {@link #DEFAULT_MAX_BATCH_SIZE}.
     *
     * @param maxBatchSize positive row count
     * @return this builder
     */
    @Override
    public InfluxDB3SinkBuilder<InputT> setMaxBatchSize(int maxBatchSize) {
        return super.setMaxBatchSize(maxBatchSize);
    }

    /**
     * Sets the maximum number of concurrent blocking HTTP writes per sink subtask.
     *
     * <p>This also determines the number of write executor threads.
     * Default: {@link #DEFAULT_MAX_IN_FLIGHT_REQUESTS}.
     *
     * @param maxInFlightRequests positive request count
     * @return this builder
     */
    @Override
    public InfluxDB3SinkBuilder<InputT> setMaxInFlightRequests(int maxInFlightRequests) {
        return super.setMaxInFlightRequests(maxInFlightRequests);
    }

    /**
     * Sets the maximum number of rows buffered by one sink subtask.
     *
     * <p>Backpressure is applied when either this limit or {@link #setMaxBufferedBytes(long)}
     * is reached.
     * Default: at least {@link #DEFAULT_MAX_BUFFERED_REQUESTS}; when the configured batch size
     * is larger, one more than that batch size.
     *
     * @param maxBufferedRequests positive row count, greater than {@code maxBatchSize}
     * @return this builder
     */
    @Override
    public InfluxDB3SinkBuilder<InputT> setMaxBufferedRequests(int maxBufferedRequests) {
        return super.setMaxBufferedRequests(maxBufferedRequests);
    }

    /**
     * Sets the maximum uncompressed payload size of one HTTP request.
     *
     * <p>Default: {@link #DEFAULT_MAX_BATCH_SIZE_IN_BYTES} (10 MiB).
     *
     * @param maxBatchSizeInBytes positive byte count
     * @return this builder
     */
    @Override
    public InfluxDB3SinkBuilder<InputT> setMaxBatchSizeInBytes(long maxBatchSizeInBytes) {
        return super.setMaxBatchSizeInBytes(maxBatchSizeInBytes);
    }

    /**
     * Sets how long the oldest buffered row may wait before a partial batch is sent.
     *
     * <p>Default: {@link #DEFAULT_MAX_TIME_IN_BUFFER_MILLIS}.
     *
     * @param maxTimeInBufferMS positive duration in milliseconds
     * @return this builder
     */
    @Override
    public InfluxDB3SinkBuilder<InputT> setMaxTimeInBufferMS(long maxTimeInBufferMS) {
        return super.setMaxTimeInBufferMS(maxTimeInBufferMS);
    }

    /**
     * Sets the largest accepted uncompressed line protocol row.
     *
     * <p>Default: {@link #DEFAULT_MAX_RECORD_SIZE_IN_BYTES} (1 MiB).
     *
     * @param maxRecordSizeInBytes positive byte count
     * @return this builder
     */
    @Override
    public InfluxDB3SinkBuilder<InputT> setMaxRecordSizeInBytes(long maxRecordSizeInBytes) {
        return super.setMaxRecordSizeInBytes(maxRecordSizeInBytes);
    }

    /**
     * Sets the maximum uncompressed payload bytes buffered by one sink subtask.
     *
     * <p>This limit excludes Java object overhead and payload copies owned by in-flight requests.
     * Default: {@link #DEFAULT_MAX_BUFFERED_BYTES} (64 MiB).
     *
     * @param maxBufferedBytes positive byte count, at least {@code maxBatchSizeInBytes}
     * @return this builder
     */
    public InfluxDB3SinkBuilder<InputT> setMaxBufferedBytes(long maxBufferedBytes) {
        this.maxBufferedBytes = maxBufferedBytes;
        return this;
    }

    /**
     * Sets the timeout for establishing an InfluxDB HTTP connection.
     *
     * <p>The write-attempt deadline remains the outer bound when it is shorter.
     * Default: {@link #DEFAULT_CONNECTION_TIMEOUT}.
     *
     * @param connectionTimeout positive timeout
     * @return this builder
     */
    public InfluxDB3SinkBuilder<InputT> setConnectionTimeout(Duration connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    /**
     * Sets the hard deadline for one blocking InfluxDB HTTP write attempt.
     *
     * <p>The connector interrupts the official client's blocking call when this deadline expires.
     * Default: {@link #DEFAULT_WRITE_TIMEOUT}.
     *
     * @param writeTimeout positive write timeout
     * @return this builder
     */
    public InfluxDB3SinkBuilder<InputT> setWriteTimeout(Duration writeTimeout) {
        this.writeTimeout = writeTimeout;
        return this;
    }

    /**
     * Sets how long Flink waits for a write result, including retry scheduling.
     *
     * <p>Default: {@link #DEFAULT_REQUEST_TIMEOUT}.
     *
     * @param requestTimeout positive request timeout
     * @return this builder
     */
    public InfluxDB3SinkBuilder<InputT> setRequestTimeout(Duration requestTimeout) {
        this.requestTimeout = requestTimeout;
        return this;
    }

    /**
     * Sets whether an expired Flink request timeout fails the sink.
     *
     * <p>Default: {@link #DEFAULT_FAIL_ON_REQUEST_TIMEOUT}.
     *
     * @param failOnRequestTimeout {@code true} to fail the job on timeout
     * @return this builder
     */
    public InfluxDB3SinkBuilder<InputT> setFailOnRequestTimeout(boolean failOnRequestTimeout) {
        this.failOnRequestTimeout = failOnRequestTimeout;
        return this;
    }

    /**
     * Enables or disables gzip request compression.
     *
     * <p>Default: {@link #DEFAULT_GZIP_ENABLED}.
     *
     * @param gzipEnabled {@code true} to gzip write payloads
     * @return this builder
     */
    public InfluxDB3SinkBuilder<InputT> setGzipEnabled(boolean gzipEnabled) {
        this.gzipEnabled = gzipEnabled;
        return this;
    }

    /**
     * Sets whether the server may accept valid rows from a partially invalid batch.
     *
     * <p>Default: {@link #DEFAULT_ACCEPT_PARTIAL}.
     *
     * @param acceptPartial {@code true} to enable partial writes
     * @return this builder
     */
    public InfluxDB3SinkBuilder<InputT> setAcceptPartial(boolean acceptPartial) {
        this.acceptPartial = acceptPartial;
        return this;
    }

    /**
     * Sets whether InfluxDB may acknowledge before synchronizing the write to durable storage.
     *
     * <p>Default: {@link #DEFAULT_NO_SYNC}.
     *
     * @param noSync {@code true} to reduce durability in favor of write latency
     * @return this builder
     */
    public InfluxDB3SinkBuilder<InputT> setNoSync(boolean noSync) {
        this.noSync = noSync;
        return this;
    }

    /**
     * Sets whether a permanently rejected line from a partial write fails the sink.
     *
     * <p>When disabled, only the rejected line is dropped. This option configures the built-in
     * failure handler and cannot be explicitly combined with a custom handler.
     * Default: {@link #DEFAULT_FAIL_ON_REJECTED}.
     *
     * @param failOnRejected {@code true} to fail instead of dropping rejected lines
     * @return this builder
     */
    public InfluxDB3SinkBuilder<InputT> setFailOnRejected(boolean failOnRejected) {
        this.failOnRejected = failOnRejected;
        this.failOnRejectedConfigured = true;
        return this;
    }

    /**
     * Sets whether validation failures fail the sink.
     *
     * <p>This covers invalid output from the line protocol converter and non-partial HTTP 400 or
     * 422 schema or parse failures. When disabled, the invalid record or affected batch is
     * dropped. This connector-owned policy also applies when a custom failure handler is
     * configured.
     * Default: {@link #DEFAULT_FAIL_ON_VALIDATION}.
     *
     * @param failOnValidation {@code true} to fail on validation errors
     * @return this builder
     */
    public InfluxDB3SinkBuilder<InputT> setFailOnValidation(boolean failOnValidation) {
        this.failOnValidation = failOnValidation;
        return this;
    }

    /**
     * Sets whether terminal write errors fail the sink.
     *
     * <p>This applies after retry handling is complete. This option configures the built-in
     * failure handler and cannot be explicitly combined with a custom handler.
     * Default: {@link #DEFAULT_FAIL_ON_ERROR}.
     *
     * @param failOnError {@code true} to fail instead of dropping affected records
     * @return this builder
     */
    public InfluxDB3SinkBuilder<InputT> setFailOnError(boolean failOnError) {
        this.failOnError = failOnError;
        this.failOnErrorConfigured = true;
        return this;
    }

    /**
     * Sets whether terminally dropped or failed line protocol values are logged.
     *
     * <p>Logging is disabled by default because line protocol may contain sensitive tag or field
     * values. Logged values have control characters escaped and are truncated, but they are not
     * redacted. This connector-owned option also applies when a custom failure handler is
     * configured.
     * Default: {@link #DEFAULT_PRINT_FAILED_REQUESTS}.
     *
     * @param printFailedRequests {@code true} to log terminal failed requests
     * @return this builder
     */
    public InfluxDB3SinkBuilder<InputT> setPrintFailedRequests(boolean printFailedRequests) {
        this.printFailedRequests = printFailedRequests;
        return this;
    }

    /**
     * Sets the maximum retries after the initial write attempt.
     *
     * <p>Default: {@link #DEFAULT_MAX_RETRIES}.
     *
     * @param maxRetries non-negative retry count
     * @return this builder
     */
    public InfluxDB3SinkBuilder<InputT> setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return this;
    }

    /**
     * Sets exponential retry backoff parameters.
     *
     * <p>The server's {@code Retry-After} value is honored up to {@code maxBackoff}.
     * Defaults: {@link #DEFAULT_INITIAL_RETRY_BACKOFF}, {@link
     * #DEFAULT_MAX_RETRY_BACKOFF}, and {@link #DEFAULT_RETRY_JITTER} jitter.
     *
     * @param initialBackoff non-negative delay before the first retry
     * @param maxBackoff non-negative upper bound for every retry delay
     * @param jitter proportional jitter from {@code 0.0} to {@code 1.0}
     * @return this builder
     */
    public InfluxDB3SinkBuilder<InputT> setRetryBackoff(
            Duration initialBackoff, Duration maxBackoff, double jitter) {
        this.initialRetryBackoff = initialBackoff;
        this.maxRetryBackoff = maxBackoff;
        this.retryJitter = jitter;
        return this;
    }

    /**
     * Sets whether sink metrics are also published to CloudWatch by Amazon Managed Service for
     * Apache Flink.
     *
     * <p>Managed Flink publishes only metrics registered under the {@code KinesisAnalytics}
     * metric group. When enabled, the sink also registers its operational metrics under {@code
     * KinesisAnalytics} with the dimension {@code Connector=TimestreamInfluxDB3}, plus
     * per-second rates and write latency percentiles. Published metrics are billed as
     * CloudWatch custom metrics. Leave this disabled on other platforms, where the metrics
     * would be reported twice.
     * Default: {@link #DEFAULT_MANAGED_FLINK_METRICS_ENABLED}.
     *
     * @param managedFlinkMetricsEnabled {@code true} to publish metrics to Managed Flink
     *     CloudWatch metrics
     * @return this builder
     */
    public InfluxDB3SinkBuilder<InputT> setManagedFlinkMetricsEnabled(
            boolean managedFlinkMetricsEnabled) {
        this.managedFlinkMetricsEnabled = managedFlinkMetricsEnabled;
        return this;
    }

    /**
     * Sets the {@code Sink} dimension of metrics published to Managed Flink CloudWatch metrics.
     *
     * <p>Set a distinct name for each InfluxDB sink when a job contains more than one, so their
     * CloudWatch metrics do not combine. Requires {@link #setManagedFlinkMetricsEnabled(boolean)}.
     * Default: unset, which publishes metrics without a {@code Sink} dimension.
     *
     * @param sinkName 1 to 255 letters, digits, or underscores
     * @return this builder
     */
    public InfluxDB3SinkBuilder<InputT> setManagedFlinkMetricsSinkName(String sinkName) {
        this.managedFlinkMetricsSinkName = sinkName;
        return this;
    }

    InfluxDB3SinkBuilder<InputT> setClientFactory(InfluxDBClientFactory clientFactory) {
        this.clientFactory = clientFactory;
        return this;
    }

    @Override
    public InfluxDB3Sink<InputT> build() {
        URI resolvedEndpoint = parseEndpoint(endpoint);
        validateEndpoint(resolvedEndpoint);
        requireNonBlank(database, "database");
        Objects.requireNonNull(tokenProvider, "tokenProvider must be configured");
        Objects.requireNonNull(converter, "converter must be configured");
        Objects.requireNonNull(writePrecision, "writePrecision");
        if (customFailureHandlerConfigured) {
            Objects.requireNonNull(failureHandler, "failureHandler");
            if (failOnRejectedConfigured || failOnErrorConfigured) {
                throw new IllegalArgumentException(
                        "A custom failureHandler cannot be combined with explicitly configured "
                                + "failOnRejected or failOnError");
            }
        }
        requirePositiveMillis(connectionTimeout, "connectionTimeout");
        long writeTimeoutMillis = requirePositiveMillis(writeTimeout, "writeTimeout");
        long requestTimeoutMillis = requirePositiveMillis(requestTimeout, "requestTimeout");
        requireNonNegativeMillis(initialRetryBackoff, "initialRetryBackoff");
        long maxRetryBackoffMillis =
                requireNonNegativeMillis(maxRetryBackoff, "maxRetryBackoff");
        if (initialRetryBackoff.compareTo(maxRetryBackoff) > 0) {
            throw new IllegalArgumentException(
                    "initialRetryBackoff must not exceed maxRetryBackoff");
        }
        final long minimumRequestTimeoutMillis;
        try {
            minimumRequestTimeoutMillis =
                    Math.addExact(
                            Math.addExact(writeTimeoutMillis, maxRetryBackoffMillis),
                            MIN_REQUEST_TIMEOUT_HEADROOM_MILLIS);
        } catch (ArithmeticException error) {
            throw new IllegalArgumentException(
                    "writeTimeout plus maxRetryBackoff exceeds the supported millisecond range",
                    error);
        }
        if (requestTimeoutMillis < minimumRequestTimeoutMillis) {
            throw new IllegalArgumentException(
                    "requestTimeout must exceed writeTimeout plus maxRetryBackoff by at least "
                            + MIN_REQUEST_TIMEOUT_HEADROOM_MILLIS
                            + " ms");
        }
        if (!Double.isFinite(retryJitter) || retryJitter < 0.0 || retryJitter > 1.0) {
            throw new IllegalArgumentException("retryJitter must be between 0.0 and 1.0");
        }
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must not be negative");
        }
        if (managedFlinkMetricsSinkName != null) {
            if (!managedFlinkMetricsEnabled) {
                throw new IllegalArgumentException(
                        "managedFlinkMetricsSinkName requires managedFlinkMetricsEnabled");
            }
            if (!MANAGED_FLINK_METRICS_SINK_NAME_PATTERN
                    .matcher(managedFlinkMetricsSinkName)
                    .matches()) {
                throw new IllegalArgumentException(
                        "managedFlinkMetricsSinkName must contain 1 to 255 letters, digits, "
                                + "or underscores");
            }
        }

        int resolvedMaxBatchSize =
                Optional.ofNullable(getMaxBatchSize()).orElse(DEFAULT_MAX_BATCH_SIZE);
        int resolvedMaxInFlightRequests =
                Optional.ofNullable(getMaxInFlightRequests())
                        .orElse(DEFAULT_MAX_IN_FLIGHT_REQUESTS);
        int resolvedMaxBufferedRequests =
                Optional.ofNullable(getMaxBufferedRequests())
                        .orElse(
                                Math.max(
                                        DEFAULT_MAX_BUFFERED_REQUESTS,
                                        resolvedMaxBatchSize + 1));
        long resolvedMaxBatchSizeInBytes =
                Optional.ofNullable(getMaxBatchSizeInBytes())
                        .orElse(DEFAULT_MAX_BATCH_SIZE_IN_BYTES);
        long resolvedMaxTimeInBufferMillis =
                Optional.ofNullable(getMaxTimeInBufferMS())
                        .orElse(DEFAULT_MAX_TIME_IN_BUFFER_MILLIS);
        long resolvedMaxRecordSizeInBytes =
                Optional.ofNullable(getMaxRecordSizeInBytes())
                        .orElse(DEFAULT_MAX_RECORD_SIZE_IN_BYTES);

        requirePositive(resolvedMaxBatchSize, "maxBatchSize");
        requirePositive(resolvedMaxInFlightRequests, "maxInFlightRequests");
        requirePositive(resolvedMaxBufferedRequests, "maxBufferedRequests");
        requirePositive(resolvedMaxBatchSizeInBytes, "maxBatchSizeInBytes");
        requirePositive(maxBufferedBytes, "maxBufferedBytes");
        requirePositive(resolvedMaxTimeInBufferMillis, "maxTimeInBufferMillis");
        requirePositive(resolvedMaxRecordSizeInBytes, "maxRecordSizeInBytes");
        if (resolvedMaxBufferedRequests <= resolvedMaxBatchSize) {
            throw new IllegalArgumentException(
                    "maxBufferedRequests must be greater than maxBatchSize");
        }
        if (resolvedMaxBatchSizeInBytes < resolvedMaxRecordSizeInBytes) {
            throw new IllegalArgumentException(
                    "maxBatchSizeInBytes must be at least maxRecordSizeInBytes");
        }
        if (maxBufferedBytes < resolvedMaxBatchSizeInBytes) {
            throw new IllegalArgumentException(
                    "maxBufferedBytes must be at least maxBatchSizeInBytes");
        }

        WriteFailureHandler resolvedFailureHandler =
                customFailureHandlerConfigured
                        ? failureHandler
                        : defaultFailureHandler(failOnRejected, failOnError);
        boolean failOnRetryExhaustion = customFailureHandlerConfigured || failOnError;
        InfluxDB3SinkConfig config =
                new InfluxDB3SinkConfig(
                        resolvedEndpoint,
                        database,
                        tokenProvider,
                        resolvedFailureHandler,
                        writePrecision,
                        resolvedMaxBatchSize,
                        resolvedMaxInFlightRequests,
                        resolvedMaxBufferedRequests,
                        resolvedMaxBatchSizeInBytes,
                        maxBufferedBytes,
                        resolvedMaxTimeInBufferMillis,
                        resolvedMaxRecordSizeInBytes,
                        connectionTimeout,
                        writeTimeout,
                        requestTimeout,
                        failOnRequestTimeout,
                        gzipEnabled,
                        acceptPartial,
                        noSync,
                        failOnValidation,
                        failOnRetryExhaustion,
                        printFailedRequests,
                        maxRetries,
                        initialRetryBackoff,
                        maxRetryBackoff,
                        retryJitter,
                        managedFlinkMetricsEnabled,
                        managedFlinkMetricsSinkName);
        return new InfluxDB3Sink<>(converter, config, clientFactory);
    }

    private static WriteFailureHandler defaultFailureHandler(
            boolean failOnRejected, boolean failOnError) {
        if (failOnRejected == DEFAULT_FAIL_ON_REJECTED
                && failOnError == DEFAULT_FAIL_ON_ERROR) {
            return DefaultWriteFailureHandler.INSTANCE;
        }
        return new DefaultWriteFailureHandler(failOnRejected, failOnError);
    }

    private static URI parseEndpoint(String endpoint) {
        Objects.requireNonNull(endpoint, "endpoint must be configured");
        try {
            return URI.create(endpoint);
        } catch (IllegalArgumentException error) {
            throw new IllegalArgumentException("endpoint must be a valid URI", error);
        }
    }

    private static void validateEndpoint(URI endpoint) {
        String scheme = endpoint.getScheme();
        // The token is sent with every request, so plaintext HTTP is never allowed.
        if (!"https".equalsIgnoreCase(scheme)) {
            throw new IllegalArgumentException("endpoint scheme must be https");
        }
        if (endpoint.getHost() == null) {
            throw new IllegalArgumentException("endpoint must contain a host");
        }
        if (endpoint.getUserInfo() != null
                || endpoint.getQuery() != null
                || endpoint.getFragment() != null) {
            throw new IllegalArgumentException(
                    "endpoint must not contain user info, query parameters, or a fragment");
        }
        if (endpoint.getPath() != null
                && !endpoint.getPath().isEmpty()
                && !"/".equals(endpoint.getPath())) {
            throw new IllegalArgumentException("endpoint must not contain an API path");
        }
    }

    private static void requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }

    private static long requirePositiveMillis(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        long millis = toMillis(value, name);
        if (millis == 0) {
            throw new IllegalArgumentException(name + " must be at least 1 millisecond");
        }
        return millis;
    }

    private static long requireNonNegativeMillis(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isNegative()) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
        return toMillis(value, name);
    }

    private static void requirePositive(long value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }

    private static long toMillis(Duration value, String name) {
        try {
            return value.toMillis();
        } catch (ArithmeticException error) {
            throw new IllegalArgumentException(
                    name + " exceeds the supported millisecond range", error);
        }
    }
}
