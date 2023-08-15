package com.amazonaws.samples.connectors.timestream;


import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
import java.time.Duration;

@Data
@Builder()
@PublicEvolving
public class TimestreamSinkConfig implements Serializable {
    @NonNull
    private final WriteClientConfig writeClientConfig;
    private static final String APP_NAME = "ts-flink.";

    @Builder.Default
    private final int maxBatchSize = 100;
    @Builder.Default
    private final int maxInFlightRequests = 100;
    @Builder.Default
    private final int maxBufferedRequests = 10 * 100;
    @Builder.Default
    private final long maxTimeInBufferMS = 15 * 1000;

    /**
     * Emit sink metrics to Amazon CloudWatch, by adding them to "kinesisanalytics" group.
     * Unfortunately - due to mechanism how the metrics can be exposed in KDA and Flink - this
     * has to be static variable when constructing base sink, rather than a configuration option.
     * (Flink doesn't offer to modify parent metric group, KDA doesn't offer other mechanism of exporting metrics).
     *
     * Therefore, the hack is that TimestreamSinkConfig specifies this as normal option, which is copied before
     * TimestreamSinkWriter creation to static variable.
     *
     * Used mechanism: https://docs.aws.amazon.com/kinesisanalytics/latest/java/monitoring-metrics-custom.html
     * This will extend the default metrics: https://docs.aws.amazon.com/kinesisanalytics/latest/java/metrics-dimensions.html
     */
    @Builder.Default
    private final boolean emitSinkMetricsToCloudWatch = false;

    @Builder.Default
    private final FailureHandlerConfig failureHandlerConfig = FailureHandlerConfig.builder().build();

    @Builder.Default
    private final CredentialProviderType credentialsProviderType = CredentialProviderType.AUTO;

    @Builder.Default
    private final CredentialConfig credentialConfig = null;

    @Data
    @Builder
    public static class FailureHandlerConfig implements Serializable {

        @Builder.Default
        private final String failureHandlerClass = DefaultWriteRequestFailureHandler.class.getCanonicalName();

        @Builder.Default
        // NOTE: Setting this to true will print RECORD data to logs (potentially sensitive information). Use with caution.
        private final boolean printFailedRequests = false;

        @Builder.Default
        // Throw exception out of sink and fail processing on RejectedRecordsException from Timestream (for example:
        // "the record timestamp is outside the time range").
        // Setting this to false will ignore RejectedRecordsExceptions and continue stream processing.
        // Mainly designed to be used for test environment.
        private final boolean failProcessingOnRejectedRecordsException = true;

        @Builder.Default
        // Throw exception out of sink and fail processing on ValidationException from Timestream.
        // Setting this to false will ignore ValidationException and continue stream processing.
        // Mainly designed to be used for test environment.
        private final boolean failProcessingOnValidationException = true;

        @Builder.Default
        // Throw exception on any other, non-retryable exception, like AccessDeniedException or ResourceNotFoundException.
        // This doesn't apply to two exceptions above.
        // Mainly designed to be used for test environment.
        private final boolean failProcessingOnErrorDefault = true;
    }

    @Data
    @Builder
    public static class CredentialConfig implements Serializable {
        /**
         * Required field for credProvider == "PROFILE"
         */
        @Builder.Default
        private final String profileName = null;

        /**
         * Option field for credProvider == "PROFILE"
         */
        @Builder.Default
        private final String profileConfigPath = null;
    }


    @Data
    @Builder
    public static class WriteClientConfig implements Serializable {
        private final String region;
        @Builder.Default
        private final int maxConcurrency = 5000;
        @Builder.Default
        private final Duration requestTimeout = Duration.ofSeconds(20);
        @Builder.Default
        private final int maxErrorRetry = 10;
        @Builder.Default
        private final String endpointOverride = null;
    }

    public enum CredentialProviderType {
        AUTO,
        ENV_VAR,
        SYS_PROP,
        PROFILE
    }

    public static String getApplicationName() {
        return APP_NAME + getVersion();
    }

    private static String getVersion() {
        return TimestreamSinkConfig.class.getPackage().getImplementationVersion();
    }
}
