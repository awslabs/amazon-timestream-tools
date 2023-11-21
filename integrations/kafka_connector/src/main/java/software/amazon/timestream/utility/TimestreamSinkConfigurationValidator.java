package software.amazon.timestream.utility;

import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.timestream.TimestreamSinkConnectorConfig;
import software.amazon.timestream.TimestreamSinkConstants;
import software.amazon.timestream.exception.TimestreamSinkConnectorError;
import software.amazon.timestream.exception.TimestreamSinkConnectorValidationException;
import software.amazon.timestream.exception.TimestreamSinkErrorCodes;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Class that has methods for validating configuration values supplied
 *
 * @see TimestreamSinkConnectorConfig
 */
public final class TimestreamSinkConfigurationValidator {

    /**
     * Logger object
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(TimestreamSinkConfigurationValidator.class);

    /**
     * list of validation errors
     */
    private static List<TimestreamSinkConnectorError> validationErrors;

    /**
     * private constructor
     */
    private TimestreamSinkConfigurationValidator () {

    }
    /**
     * Method to validate if all the required configuration values
     * are supplied to start the sink connector
     */
    public static void validateInitialConfig(final Map<String, String> config) {

        validationErrors = new ArrayList<>();
        validateRequiredConnectorConfig(config, TimestreamSinkConstants.TASKS_MAX, TimestreamSinkErrorCodes.MISSING_TASKS_MAX);
        validateRequiredConnectorConfig(config, TimestreamSinkConstants.CONNECTOR_CLASS, TimestreamSinkErrorCodes.MISSING_CLASS, TimestreamSinkConstants.CLASS_FQDN);
        validateRequiredConnectorConfig(config, SinkConnector.TOPICS_CONFIG, TimestreamSinkErrorCodes.MISSING_TOPICS);

        validateRequiredConnectorConfig(config, TimestreamSinkConstants.AWS_REGION, TimestreamSinkErrorCodes.MISSING_REGION);
        validateRequiredConnectorConfig(config, TimestreamSinkConstants.S3_BUCKET, TimestreamSinkErrorCodes.MISSING_BUCKET);
        validateRequiredConnectorConfig(config, TimestreamSinkConstants.SCHEMA_S3KEY, TimestreamSinkErrorCodes.MISSING_SCHEMA);
        validateRequiredConnectorConfig(config, TimestreamSinkConstants.DATABASE_NAME, TimestreamSinkErrorCodes.MISSING_DATABASE);
        validateRequiredConnectorConfig(config, TimestreamSinkConstants.TABLE_NAME, TimestreamSinkErrorCodes.MISSING_TABLE);
        validateRequiredConnectorConfig(config, TimestreamSinkConstants.VPC_ENDPOINT, TimestreamSinkErrorCodes.MISSING_ENDPOINT);
        validateURI(config.get(TimestreamSinkConstants.VPC_ENDPOINT));

        AWSServiceClientObjectsValidator.validateAWSRegion(config.get(TimestreamSinkConstants.AWS_REGION), validationErrors);

        if (!validationErrors.isEmpty()) {
            throw new TimestreamSinkConnectorValidationException(validationErrors);
        }
    }

    /**
     * Method to validate if all the given AWS service objects' references
     * and optional configuration values are valid to start the sink connector
     *
     * @param config {@link TimestreamSinkConnectorConfig}
     */
    public static void validateTimestreamSinkConnectorConfig(final AWSServiceClientFactory factory, final TimestreamSinkConnectorConfig config) {

        validationErrors = new ArrayList<>();
        validateAWSServiceObjects(factory, config);
        validateOptionalConfigs(config);
        if (!validationErrors.isEmpty()) {
            throw new TimestreamSinkConnectorValidationException(validationErrors);
        }
    }

    /**
     * Method to validate if all the given AWS service objects' references
     * are valid to start the sink connector
     *
     * @param config {@link TimestreamSinkConnectorConfig}
     */
    private static void validateAWSServiceObjects(final AWSServiceClientFactory clientFactory, final TimestreamSinkConnectorConfig config) {
        // validate S3 resources
        AWSServiceClientObjectsValidator.validateS3Bucket(clientFactory, config.getAWSRegion(), config.getSchemaS3BucketName(), validationErrors);
        AWSServiceClientObjectsValidator.validateS3Object(clientFactory, config.getAWSRegion(), config.getSchemaS3BucketName(), config.getSchmeaS3ObjectPath(), validationErrors);
        // validate Timestream resources
        AWSServiceClientObjectsValidator.validateTimestreamDatabase(clientFactory.getTimestreamClient(), config.getDatabaseName(), validationErrors);
        AWSServiceClientObjectsValidator.validateTimestreamTable(clientFactory.getTimestreamClient(), config.getDatabaseName(), config.getTableName(), validationErrors);
    }

    /**
     * Method to validate if the optional configuration values are valid
     *
     * @param config {@link TimestreamSinkConnectorConfig}
     */
    private static void validateOptionalConfigs(final TimestreamSinkConnectorConfig config) {
        validateTimestreamWriteBatchSize(config);
        validateTimestreamMaxConnections(config);
        validateTimestreamConnectionsRetry(config);
    }

    /**
     * Method to validate if the given timestream batch size is valid
     *
     * @param config {@link TimestreamSinkConnectorConfig}
     */
    private static void validateTimestreamWriteBatchSize(final TimestreamSinkConnectorConfig config) {
        final int batchSize = config.getTimestreamBatchSize();
        if (batchSize < 0 || batchSize > TimestreamSinkConstants.DEFAULT_BATCHSIZE) {
            validationErrors.add(new TimestreamSinkConnectorError(TimestreamSinkErrorCodes.INVALID_BATCH,
                    TimestreamSinkConstants.BATCH_SIZE));
        }
    }

    /**
     * Method to validate if the given timestream max connections is valid
     *
     * @param config {@link TimestreamSinkConnectorConfig}
     */
    private static void validateTimestreamMaxConnections(final TimestreamSinkConnectorConfig config) {
        final int maxConnections = config.getMaxConnections();
        if (maxConnections < TimestreamSinkConstants.DEFAULT_CON_MAX) {
            validationErrors.add(new TimestreamSinkConnectorError(TimestreamSinkErrorCodes.INVALID_MAX_CON,
                    TimestreamSinkConstants.CON_MAX));
        }
    }

    /**
     * Method to validate if the given timestream connections retry is valid
     *
     * @param config {@link TimestreamSinkConnectorConfig}
     */
    private static void validateTimestreamConnectionsRetry(final TimestreamSinkConnectorConfig config) {
        final int retries = config.getNumRetries();
        if (retries < 0 || retries > TimestreamSinkConstants.DEFAULT_RETRIES) {
            validationErrors.add(new TimestreamSinkConnectorError(TimestreamSinkErrorCodes.INVALID_CON_RETRY,
                    TimestreamSinkConstants.CON_RETRIES, TimestreamSinkConstants.DEFAULT_RETRIES));
        }
    }

    /**
     * Method to validate if the required configuration exists
     *
     * @param configMap  Map  that has all the given configuration values loaded
     * @param key        The key to be validated if is present in the given @configMap
     * @param messageKey Key to the error message
     * @param arguments  Arguments to be passed to format the error message
     */
    private static void validateRequiredConnectorConfig(final Map<String, String> configMap, final String key, final String messageKey, final Object... arguments) {
        if (!configMap.containsKey(key)) {
            validationErrors.add(new TimestreamSinkConnectorError(messageKey, key, arguments));
        }
    }

    /**
     * Method to validate if the supplied SinkRecord is of valid type.
     *
     * @param record SinkRecord to be validated for its type
     * @return true, if the sink record is of valid type; otherwise false
     */
    public static boolean isSinkRecordValidType(final SinkRecord record) {
        final boolean isValid = record.value() instanceof Map<?, ?>;
        if (!isValid) {
            LOGGER.error("ERROR::TimestreamSinkConfigurationValidator::isSinkRecordValidType: " +
                    "Unable to convert the sink record, Check if you have configured the JsonConvertor in the worker configuration {}", record.value());
        }
        return isValid;
    }



    /**
     * Method to validate if the supplied ingestionEndpoint is a valid URI
     *
     * @param ingestionEndpoint VPC Endpoint to be validated if is a valid URI
     */
    private static void validateURI(final String ingestionEndpoint) {
        LOGGER.info("Begin::TimestreamSinkTaskValidator::validateURI:: Timestream ingestion endpoint {}", ingestionEndpoint);
        try {
            new URI(ingestionEndpoint);
        } catch (URISyntaxException e) {
            LOGGER.error("ERROR::TimestreamSinkTaskValidator::validateURI", e);
            validationErrors.add(new TimestreamSinkConnectorError(TimestreamSinkErrorCodes.INVALID_ENDPOINT, ingestionEndpoint));
        }
    }
}
