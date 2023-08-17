package com.amazonaws.samples.connectors.timestream;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.Sink.InitContext;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.InstantiationUtil;

import com.amazonaws.samples.connectors.timestream.metrics.CloudWatchEmittedMetricGroupHelper;
import com.amazonaws.samples.connectors.timestream.metrics.MetricsCollector;
import com.amazonaws.samples.connectors.timestream.metrics.TimestreamSinkMetricGroup;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteAsyncClient;
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteAsyncClientBuilder;
import software.amazon.awssdk.services.timestreamwrite.model.Record;
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsRequest;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

import static com.amazonaws.samples.connectors.timestream.TimestreamSink.MAX_BATCH_SIZE_IN_BYTES;
import static com.amazonaws.samples.connectors.timestream.TimestreamSink.MAX_RECORD_SIZE_IN_BYTES;
import static com.amazonaws.samples.connectors.timestream.TimestreamSinkConfig.getApplicationName;

public class TimestreamSinkWriter<InputT> extends AsyncSinkWriter<InputT, Record> {
    private static final Logger LOG = LoggerFactory.getLogger(TimestreamSinkWriter.class);

    private final BatchConverter batchConverter;
    private final TimestreamWriteAsyncClient client;
    private final WriteRequestFailureHandler failureHandler;
    private final MetricsCollector metricsCollector;

    public TimestreamSinkWriter(
            ElementConverter<InputT, Record> elementConverter,
            BatchConverter batchConverter,
            InitContext context,
            TimestreamSinkConfig timestreamSinkConfig) {
        super(elementConverter,
                context,
                timestreamSinkConfig.getMaxBatchSize(),
                timestreamSinkConfig.getMaxInFlightRequests(),
                timestreamSinkConfig.getMaxBufferedRequests(),
                MAX_BATCH_SIZE_IN_BYTES,
                timestreamSinkConfig.getMaxTimeInBufferMS(),
                MAX_RECORD_SIZE_IN_BYTES);
        this.batchConverter = batchConverter;
        this.client = openAsyncClient(timestreamSinkConfig);
        this.failureHandler = createFailureHandler(timestreamSinkConfig);
        this.metricsCollector = openMetricCollector(context);
    }

    TimestreamSinkMetricGroup createTimestreamSinkMetricGroup(final InitContext context) {
        final MetricGroup metricGroup = CloudWatchEmittedMetricGroupHelper.extendMetricGroup(context.metricGroup());
        return new TimestreamSinkMetricGroup(metricGroup);
    }

    @SneakyThrows
    protected WriteRequestFailureHandler createFailureHandler(TimestreamSinkConfig timestreamSinkConfig) {
        final WriteRequestFailureHandler instance = InstantiationUtil.instantiate(
                timestreamSinkConfig.getFailureHandlerConfig().getFailureHandlerClass(),
                WriteRequestFailureHandler.class,
                Thread.currentThread().getContextClassLoader()
        );
        instance.open(getFatalExceptionCons(), timestreamSinkConfig.getFailureHandlerConfig());
        return instance;
    }

    @VisibleForTesting
    protected MetricsCollector openMetricCollector(InitContext context) {
        return new MetricsCollector(createTimestreamSinkMetricGroup(context));
    }

    @VisibleForTesting
    protected TimestreamWriteAsyncClient openAsyncClient(TimestreamSinkConfig timestreamSinkConfig) {
        TimestreamWriteAsyncClientBuilder asyncClientBuilder = TimestreamWriteAsyncClient.builder()
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .apiCallAttemptTimeout(timestreamSinkConfig.getWriteClientConfig().getRequestTimeout())
                        .advancedOptions(Map.of(SdkAdvancedClientOption.USER_AGENT_PREFIX, getApplicationName()))
                        .retryPolicy(RetryPolicy.builder()
                                .numRetries(timestreamSinkConfig.getWriteClientConfig().getMaxErrorRetry())
                                .build())
                        .build())
                .httpClient(AwsCrtAsyncHttpClient
                        .builder()
                        .maxConcurrency(timestreamSinkConfig.getWriteClientConfig().getMaxConcurrency())
                        .build())
                .region(Region.of(timestreamSinkConfig.getWriteClientConfig().getRegion()))
                .credentialsProvider(getCredentialProvider(
                        timestreamSinkConfig.getCredentialsProviderType(), timestreamSinkConfig.getCredentialConfig()));
        String endpointOverride = timestreamSinkConfig.getWriteClientConfig().getEndpointOverride();
        if (endpointOverride != null) {
            URI endpointOverrideURI = parseEndpointOverride(endpointOverride);
            asyncClientBuilder = asyncClientBuilder.endpointOverride(endpointOverrideURI);
        }
        LOG.debug("AmazonTimestreamWriteAsync client constructed.");
        return asyncClientBuilder.build();
    }

    private AwsCredentialsProvider getCredentialProvider(TimestreamSinkConfig.CredentialProviderType credentialType, TimestreamSinkConfig.CredentialConfig credentialConfig) {
        switch (credentialType) {
            case ENV_VAR:
                return EnvironmentVariableCredentialsProvider.create();
            case SYS_PROP:
                return SystemPropertyCredentialsProvider.create();
            case PROFILE:
                String profileName = credentialConfig.getProfileName();
                String profileConfigPath = credentialConfig.getProfileConfigPath();
                return (profileConfigPath == null)
                        ? ProfileCredentialsProvider.create(profileName)
                        : ProfileCredentialsProvider.builder().profileName(profileName).profileFile(
                                ProfileFile.builder().content(Path.of(profileConfigPath)).build()).build();
            case AUTO:
                return DefaultCredentialsProvider.create();
            default:
                throw new IllegalArgumentException("Credential provider not supported: " + credentialType);
        }
    }

    private URI parseEndpointOverride(String endpointOverride) {
        try {
            return new URI(endpointOverride);
        } catch (URISyntaxException uriSyntaxException) {
            throw new RuntimeException("Invalid EndpointOverride Config: " + endpointOverride);
        }
    }

    /**
     * This method sends requests to Timestream.
     *
     * <p>The method is invoked with a set of request entries according to the buffering hints (and
     * the valid limits of the destination). {@code batchConverter} is used to convert buffered elements to WriteRecordsRequest
     *
     * <p>If fatal, non-retryable exception occurred, invoke @{code getFatalExceptionCons}.
     *
     * @param requestEntries a set of request entries that should be sent to the destination
     * @param requestResult  the {@code accept} method should be called on this Consumer once the
     *                       processing of the {@code requestEntries} are complete. Any entries that encountered
     *                       difficulties in persisting should be re-queued through {@code requestResult} by including
     *                       that element in the collection of {@code RequestEntryT}s passed to the {@code accept}
     */
    @Override
    protected void submitRequestEntries(List<Record> requestEntries, Consumer<List<Record>> requestResult) {
        final WriteRecordsRequest request = batchConverter.apply(requestEntries);
        LOG.debug("Sending WriteRecordsRequest with {} records to Timestream...", request.records().size());
        metricsCollector.collectPreWriteMetrics(request);

        try {
            asyncWriteRecords(requestEntries, requestResult, request);
        } catch (Exception t) {
            // this can happen very infrequently due to a bug in AWS SDK: https://github.com/aws/aws-sdk-java-v2/issues/1812
            // all other exceptions should normally be handled inside @{code asyncWriteRecords} method
            LOG.error("Unexpected exception occurred when sending records to Timestream. Retrying all records.", t);
            metricsCollector.collectExceptionMetrics(t);
            requestResult.accept(requestEntries);
        }
    }

    private void asyncWriteRecords(List<Record> requestEntries, Consumer<List<Record>> requestResult, WriteRecordsRequest request) {
        client.writeRecords(request).whenComplete((response, err) -> {
            if (err != null) {
                if (err instanceof CompletionException) {
                    err = err.getCause(); // unwrap real exception
                }
                if (err instanceof Exception) {
                    final Exception exception = (Exception) err;
                    metricsCollector.collectExceptionMetrics(exception);
                    Consumer<List<Record>> requestResultMetricsWrapped = (List<Record> records) -> {
                        metricsCollector.collectRetries(records);
                        requestResult.accept(records);
                    };
                    Consumer<List<Record>> droppedRecordsMetricsWrapped = (List<Record> records) -> {
                        metricsCollector.collectDropped(records, request);
                    };
                    failureHandler.onWriteError(requestEntries, request, exception,
                            requestResultMetricsWrapped, droppedRecordsMetricsWrapped);
                } else {
                    getFatalExceptionCons().accept(new Exception(err));
                }
            } else {
                LOG.trace("Timestream writeRecordsAsync onSuccess: {} -> {}", request, response);
                metricsCollector.collectSuccessMetrics(request);
                requestResult.accept(Collections.emptyList());
            }
        });
    }

    /**
     * This method allows the getting of the size of a {@code RequestEntryT} in bytes. The size in
     * this case is measured as the total bytes that is written to the destination as a result of
     * persisting this particular {@code RequestEntryT} rather than the serialized length (which may
     * be the same).
     *
     * @param requestEntry the requestEntry for which we want to know the size
     * @return the size of the requestEntry, as defined previously
     */
    @Override
    protected long getSizeInBytes(Record requestEntry) {
        return TimestreamModelUtils.getRecordSizeInBytes(requestEntry);
    }

    @Override
    public List<BufferedRequestState<Record>> snapshotState(long checkpointId) {
        // This sink does not participate in checkpointing, instead we flush the buffer
        try {
            flush(true);
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted while flushing buffer during snapshotState", e);
        }

        return super.snapshotState(checkpointId);
    }
}
