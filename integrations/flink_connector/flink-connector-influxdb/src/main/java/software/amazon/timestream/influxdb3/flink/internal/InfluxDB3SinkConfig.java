/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.internal;

import org.apache.flink.annotation.Internal;

import com.influxdb.v3.client.write.WritePrecision;
import software.amazon.timestream.influxdb3.flink.InfluxDB3Sink;
import software.amazon.timestream.influxdb3.flink.InfluxDB3SinkBuilder;
import software.amazon.timestream.influxdb3.flink.auth.TokenProvider;
import software.amazon.timestream.influxdb3.flink.failure.WriteFailureHandler;

import java.io.Serializable;
import java.net.URI;
import java.time.Duration;

/**
 * Internal immutable runtime configuration for {@link InfluxDB3Sink}.
 *
 * <p>Users configure the connector through {@link InfluxDB3SinkBuilder}.
 */
@Internal
public final class InfluxDB3SinkConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private final URI endpoint;
    private final String database;
    private final TokenProvider tokenProvider;
    private final WriteFailureHandler failureHandler;
    private final WritePrecision writePrecision;
    private final int maxBatchSize;
    private final int maxInFlightRequests;
    private final int maxBufferedRequests;
    private final long maxBatchSizeInBytes;
    private final long maxBufferedBytes;
    private final long maxTimeInBufferMillis;
    private final long maxRecordSizeInBytes;
    private final Duration connectionTimeout;
    private final Duration writeTimeout;
    private final Duration requestTimeout;
    private final boolean failOnRequestTimeout;
    private final boolean gzipEnabled;
    private final boolean acceptPartial;
    private final boolean noSync;
    private final boolean failOnValidation;
    private final boolean failOnRetryExhaustion;
    private final boolean printFailedRequests;
    private final int maxRetries;
    private final Duration initialRetryBackoff;
    private final Duration maxRetryBackoff;
    private final double retryJitter;
    private final boolean managedFlinkMetricsEnabled;
    private final String managedFlinkMetricsSinkName;

    public InfluxDB3SinkConfig(
            URI endpoint,
            String database,
            TokenProvider tokenProvider,
            WriteFailureHandler failureHandler,
            WritePrecision writePrecision,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxBufferedBytes,
            long maxTimeInBufferMillis,
            long maxRecordSizeInBytes,
            Duration connectionTimeout,
            Duration writeTimeout,
            Duration requestTimeout,
            boolean failOnRequestTimeout,
            boolean gzipEnabled,
            boolean acceptPartial,
            boolean noSync,
            boolean failOnValidation,
            boolean failOnRetryExhaustion,
            boolean printFailedRequests,
            int maxRetries,
            Duration initialRetryBackoff,
            Duration maxRetryBackoff,
            double retryJitter,
            boolean managedFlinkMetricsEnabled,
            String managedFlinkMetricsSinkName) {
        this.endpoint = endpoint;
        this.database = database;
        this.tokenProvider = tokenProvider;
        this.failureHandler = failureHandler;
        this.writePrecision = writePrecision;
        this.maxBatchSize = maxBatchSize;
        this.maxInFlightRequests = maxInFlightRequests;
        this.maxBufferedRequests = maxBufferedRequests;
        this.maxBatchSizeInBytes = maxBatchSizeInBytes;
        this.maxBufferedBytes = maxBufferedBytes;
        this.maxTimeInBufferMillis = maxTimeInBufferMillis;
        this.maxRecordSizeInBytes = maxRecordSizeInBytes;
        this.connectionTimeout = connectionTimeout;
        this.writeTimeout = writeTimeout;
        this.requestTimeout = requestTimeout;
        this.failOnRequestTimeout = failOnRequestTimeout;
        this.gzipEnabled = gzipEnabled;
        this.acceptPartial = acceptPartial;
        this.noSync = noSync;
        this.failOnValidation = failOnValidation;
        this.failOnRetryExhaustion = failOnRetryExhaustion;
        this.printFailedRequests = printFailedRequests;
        this.maxRetries = maxRetries;
        this.initialRetryBackoff = initialRetryBackoff;
        this.maxRetryBackoff = maxRetryBackoff;
        this.retryJitter = retryJitter;
        this.managedFlinkMetricsEnabled = managedFlinkMetricsEnabled;
        this.managedFlinkMetricsSinkName = managedFlinkMetricsSinkName;
    }

    /** @return base HTTP or HTTPS cluster endpoint */
    public URI getEndpoint() {
        return endpoint;
    }

    /** @return target database name */
    public String getDatabase() {
        return database;
    }

    /** @return token provider initialized by each sink subtask */
    public TokenProvider getTokenProvider() {
        return tokenProvider;
    }

    /** @return configured record-level failure handler */
    public WriteFailureHandler getFailureHandler() {
        return failureHandler;
    }

    /** @return precision used to interpret line protocol timestamps */
    public WritePrecision getWritePrecision() {
        return writePrecision;
    }

    /** @return maximum number of records in one write request */
    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    /** @return maximum number of concurrent write requests */
    public int getMaxInFlightRequests() {
        return maxInFlightRequests;
    }

    /** @return maximum number of records buffered by one sink subtask */
    public int getMaxBufferedRequests() {
        return maxBufferedRequests;
    }

    /** @return maximum uncompressed size of one write request */
    public long getMaxBatchSizeInBytes() {
        return maxBatchSizeInBytes;
    }

    /** @return maximum uncompressed payload bytes buffered by one sink subtask */
    public long getMaxBufferedBytes() {
        return maxBufferedBytes;
    }

    /** @return maximum buffering time before a request is sent */
    public long getMaxTimeInBufferMillis() {
        return maxTimeInBufferMillis;
    }

    /** @return maximum uncompressed size of one line protocol record */
    public long getMaxRecordSizeInBytes() {
        return maxRecordSizeInBytes;
    }

    /** @return timeout for establishing an InfluxDB HTTP connection */
    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    /** @return hard deadline for one blocking InfluxDB HTTP write attempt */
    public Duration getWriteTimeout() {
        return writeTimeout;
    }

    /** @return timeout for Flink to receive a write result */
    public Duration getRequestTimeout() {
        return requestTimeout;
    }

    /** @return whether an expired request timeout fails the sink */
    public boolean isFailOnRequestTimeout() {
        return failOnRequestTimeout;
    }

    /** @return whether write payloads are gzip compressed */
    public boolean isGzipEnabled() {
        return gzipEnabled;
    }

    /** @return whether the server may accept valid rows from a partially invalid batch */
    public boolean isAcceptPartial() {
        return acceptPartial;
    }

    /** @return whether the server may acknowledge before synchronizing durable storage */
    public boolean isNoSync() {
        return noSync;
    }

    /** @return whether local and non-partial HTTP 400/422 validation failures fail the sink */
    public boolean isFailOnValidation() {
        return failOnValidation;
    }

    /** @return whether retry exhaustion fails the sink instead of dropping affected records */
    public boolean isFailOnRetryExhaustion() {
        return failOnRetryExhaustion;
    }

    /**
     * @return whether terminally dropped or failed line protocol is written, without redaction,
     *     to the connector log
     */
    public boolean isPrintFailedRequests() {
        return printFailedRequests;
    }

    /** @return maximum retries after the initial write attempt */
    public int getMaxRetries() {
        return maxRetries;
    }

    /** @return delay before the first retry */
    public Duration getInitialRetryBackoff() {
        return initialRetryBackoff;
    }

    /** @return maximum retry delay */
    public Duration getMaxRetryBackoff() {
        return maxRetryBackoff;
    }

    /** @return proportional jitter applied to retry delays */
    public double getRetryJitter() {
        return retryJitter;
    }

    /** @return whether metrics are also registered for Managed Flink CloudWatch publication */
    public boolean isManagedFlinkMetricsEnabled() {
        return managedFlinkMetricsEnabled;
    }

    /** @return {@code Sink} dimension for Managed Flink metrics, or {@code null} when unset */
    public String getManagedFlinkMetricsSinkName() {
        return managedFlinkMetricsSinkName;
    }

    @Override
    public String toString() {
        return "InfluxDB3SinkConfig{"
                + "endpoint="
                + endpoint
                + ", database='"
                + database
                + '\''
                + ", tokenProviderType="
                + tokenProvider.getClass().getName()
                + ", writePrecision="
                + writePrecision
                + ", maxBatchSize="
                + maxBatchSize
                + ", maxInFlightRequests="
                + maxInFlightRequests
                + ", maxBufferedRequests="
                + maxBufferedRequests
                + ", maxBatchSizeInBytes="
                + maxBatchSizeInBytes
                + ", maxBufferedBytes="
                + maxBufferedBytes
                + ", maxTimeInBufferMillis="
                + maxTimeInBufferMillis
                + ", maxRecordSizeInBytes="
                + maxRecordSizeInBytes
                + ", connectionTimeout="
                + connectionTimeout
                + ", writeTimeout="
                + writeTimeout
                + ", requestTimeout="
                + requestTimeout
                + ", failOnRequestTimeout="
                + failOnRequestTimeout
                + ", gzipEnabled="
                + gzipEnabled
                + ", acceptPartial="
                + acceptPartial
                + ", noSync="
                + noSync
                + ", failOnValidation="
                + failOnValidation
                + ", failOnRetryExhaustion="
                + failOnRetryExhaustion
                + ", printFailedRequests="
                + printFailedRequests
                + ", maxRetries="
                + maxRetries
                + ", initialRetryBackoff="
                + initialRetryBackoff
                + ", maxRetryBackoff="
                + maxRetryBackoff
                + ", retryJitter="
                + retryJitter
                + ", managedFlinkMetricsEnabled="
                + managedFlinkMetricsEnabled
                + ", managedFlinkMetricsSinkName="
                + managedFlinkMetricsSinkName
                + '}';
    }
}
