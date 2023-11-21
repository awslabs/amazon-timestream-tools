package software.amazon.timestream.utility;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient;
import software.amazon.timestream.TimestreamSinkConnectorConfig;
import software.amazon.timestream.exception.TimestreamSinkConnectorError;
import software.amazon.timestream.exception.TimestreamSinkConnectorException;
import software.amazon.timestream.exception.TimestreamSinkErrorCodes;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;

/**
 * Class that instantiates AWS service client objects
 * such as objects of {@link S3Client},
 * {@link TimestreamWriteClient} and more that are required for the connector.
 */
@Getter
public final class AWSServiceClientFactory {

    /**
     * Logger object
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(AWSServiceClientFactory.class);
    /**
     * Amazon S3 client object
     */
    private final S3Client s3Client;
    /**
     * Amazon Timestream Write Client object
     */
    private final TimestreamWriteClient timestreamClient;

    /**
     * @param config Timestream SinkConnector Config values
     */
    public AWSServiceClientFactory(final TimestreamSinkConnectorConfig config) {
        LOGGER.info("AWSServiceClientFactory::instantiate for region {}", config.getAWSRegion());
        this.s3Client = S3Client.builder().region(config.getAWSRegion()).build();
        this.timestreamClient = instantiateTimeStreamWriterClient(config);
    }

    /**
     * Instantiates {@link TimestreamWriteClient} object
     * @param config: configuration values
     */
    private TimestreamWriteClient instantiateTimeStreamWriterClient(final TimestreamSinkConnectorConfig config) {
        LOGGER.info("Begin::AWSServiceClientFactory::instantiateTimeStreamWriterClient");
        return buildTimeStreamWriterClient(config);
    }

    /**
     * Method that instantiates an object of {@link TimestreamWriteClient}
     * for the given configuration set in {@link TimestreamSinkConnectorConfig}
     * @param config: configuration values
     * @return timestreamWriteClient
     */
    private TimestreamWriteClient buildTimeStreamWriterClient(final TimestreamSinkConnectorConfig config) {
        LOGGER.info("Begin::AWSServiceClientFactory::buildTimeStreamWriterClient");
        try {
            final ApacheHttpClient.Builder httpClientBuilder =ApacheHttpClient.builder();

            httpClientBuilder.maxConnections(config.getMaxConnections());

            final RetryPolicy.Builder retryPolicy = RetryPolicy.builder();
            retryPolicy.numRetries(config.getNumRetries());

            final ClientOverrideConfiguration.Builder overrideConfig = ClientOverrideConfiguration.builder();
            overrideConfig.apiCallAttemptTimeout(Duration.ofSeconds(config.getMaxTimeoutSeconds()));
            overrideConfig.retryPolicy(retryPolicy.build());

            return TimestreamWriteClient.builder()
                    .httpClientBuilder(httpClientBuilder)
                    .overrideConfiguration(overrideConfig.build())
                    .region(config.getAWSRegion()).endpointOverride(new URI(config.getTimestreamIngestionEndPoint()))
                    .build();
        } catch (URISyntaxException e) {
            LOGGER.error("ERROR::AWSServiceClientFactory::buildTimeStreamWriterClient::", e);
            final TimestreamSinkConnectorError error = new TimestreamSinkConnectorError(TimestreamSinkErrorCodes.INVALID_ENDPOINT, config.getTimestreamIngestionEndPoint());
            throw new TimestreamSinkConnectorException(error, e);
        } catch (SdkException e) {
            LOGGER.error("ERROR::AWSServiceClientFactory::buildTimeStreamWriterClient: while building Timestream client::", e);
            throw new TimestreamSinkConnectorException(e);
        }
    }
}
