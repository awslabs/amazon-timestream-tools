package software.amazon.timestream.utility.reader;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.timestream.table.schema.SchemaDefinition;
import software.amazon.timestream.TimestreamSinkConnectorConfig;
import software.amazon.timestream.exception.*;
import software.amazon.timestream.utility.AWSServiceClientFactory;

import java.nio.charset.StandardCharsets;
/**
 * Timestream Table schema definition reader from an S3 bucket
 */
public class TimestreamS3SchemaReader implements TimestreamSchemaReader{

    /**
     * Logger object
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(TimestreamS3SchemaReader.class);
    /**
     * GSON object for JSON marshaling/unmarshalling
     */
    private static final Gson GSON = new Gson();
    /**
     * Connector configuration values
     */
    private final TimestreamSinkConnectorConfig connectorConfig;

    /**
     * Connector configuration values
     */
    private final AWSServiceClientFactory clientFactory;

    /**
     * @param config        : Timestream Sink connector configuration
     * @param clientFactory : AWS Service client factory
     */
    public TimestreamS3SchemaReader (final AWSServiceClientFactory clientFactory, final TimestreamSinkConnectorConfig config)  {
        this.connectorConfig = config;
        this.clientFactory = clientFactory;
    }


    /**
     * Method to get the Timestream schema definition from the supplied S3 object
     * @return root
     * @see TimestreamSinkConnectorConfig
     */
    @Override
    public SchemaDefinition getSchemaDefinition() {
        LOGGER.info("Begin::TimestreamSchemaReader::getSchemaDefinition");
        SchemaDefinition schemaDef;
        try {
            final String jsonString = getS3ObjectAsString();
            schemaDef = GSON.fromJson(jsonString, SchemaDefinition.class);
            return schemaDef;
        } catch (JsonSyntaxException je) {
            LOGGER.error("ERROR::TimestreamSchemaReader::getSchemaDefinition", je);
            final TimestreamSinkConnectorError error = new TimestreamSinkConnectorError(TimestreamSinkErrorCodes.INVALID_SCHEMA,
                    this.connectorConfig.getSchmeaS3ObjectPath());
            throw new TimestreamSinkConnectorException(error, je);
        }
    }

    /**
     * Method to get the Timestream table schema definition for the given S3 object
     * @throws TimestreamSinkConnectorException if the S3 object does not exist
     * @see TimestreamSinkConnectorConfig
     * @return S3 object's byte as UTF-8 formatted string.
     */
    private String getS3ObjectAsString() {
        LOGGER.info("Begin::TimestreamSchemaReader::getS3ObjectAsString");
        final GetObjectRequest objectRequest = GetObjectRequest
                .builder()
                .key(this.connectorConfig.getSchmeaS3ObjectPath())
                .bucket(this.connectorConfig.getSchemaS3BucketName())
                .build();
        final ResponseBytes<GetObjectResponse> objectBytes = clientFactory.getS3Client().getObjectAsBytes(objectRequest);
        try {
            final String definition =  new String(objectBytes.asByteArray(), StandardCharsets.UTF_8);
            LOGGER.info("TimestreamSchemaReader::getSchemaDefinition: Successfully read the schema config from s3: {} {}", //NOPMD - suppressed GuardLogStatement - TODO explain reason for suppression
                    this.connectorConfig.getSchemaS3BucketName(), this.connectorConfig.getSchmeaS3ObjectPath());
            return definition;
        } catch (S3Exception e) {
            LOGGER.error("ERROR::TimestreamSchemaReader::getS3ObjectAsString", e);
            final TimestreamSinkConnectorError error = new TimestreamSinkConnectorError(TimestreamSinkErrorCodes.INVALID_S3KEY,
                    this.connectorConfig.getAWSRegion(), this.connectorConfig.getSchemaS3BucketName(),
                    this.connectorConfig.getSchmeaS3ObjectPath());
            throw new TimestreamSinkConnectorException(error, e);
        }
    }
}