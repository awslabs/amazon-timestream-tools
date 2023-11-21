package software.amazon.timestream.utility;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient;
import software.amazon.awssdk.services.timestreamwrite.model.DescribeDatabaseRequest;
import software.amazon.awssdk.services.timestreamwrite.model.DescribeDatabaseResponse;
import software.amazon.awssdk.services.timestreamwrite.model.DescribeTableRequest;
import software.amazon.awssdk.services.timestreamwrite.model.DescribeTableResponse;
import software.amazon.timestream.exception.TimestreamSinkConnectorError;
import software.amazon.timestream.exception.TimestreamSinkErrorCodes;

import java.util.List;

/**
 * Helps in validating connector configurations
 * for accessing AWS service client objects
 */
final class AWSServiceClientObjectsValidator {

    /**
     * Logger object
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(AWSServiceClientObjectsValidator.class);

    /**
     *
     */
    private AWSServiceClientObjectsValidator() {

    }
    /**
     * Method to validate if the given S3 bucket exists in the given AWS Region
     *
     * @param region     AWS Region where the bucket is supposed to be present
     * @param bucketName S3 Bucket name to be validated
     */
    public static void validateS3Bucket(final AWSServiceClientFactory factory, final Region region, final String bucketName, final List<TimestreamSinkConnectorError> validationErrors) {
        LOGGER.info("Begin::AWSServiceClientObjectsValidator::validateS3Bucket:: for region: {} bucket: {}", region, bucketName);
        final HeadBucketRequest headBucketRequest = HeadBucketRequest.builder()
                .bucket(bucketName)
                .build();
        try {
            factory.getS3Client().headBucket(headBucketRequest);
        } catch (S3Exception e) {
            LOGGER.error("ERROR::AWSServiceClientObjectsValidator::validateS3Bucket:", e);
            validationErrors.add(new TimestreamSinkConnectorError(TimestreamSinkErrorCodes.INVALID_S3BUCKET, bucketName, region));
        }
    }

    /**
     * Method to validate if the given S3 object exists
     * in the given S3 bucket and in the given AWS Region
     *
     * @param region     AWS Region where the bucket is supposed to be present
     * @param bucketName S3 Bucket Name where the object is supposed to be present
     * @param objectKey  S3 Object key to be validated
     */
    public static void validateS3Object(final AWSServiceClientFactory factory, final Region region, final String bucketName, final String objectKey, final List<TimestreamSinkConnectorError> validationErrors) {
        LOGGER.info("Begin::AWSServiceClientObjectsValidator::validateS3Object:: for region: {} bucket: {} key: {}", region, bucketName, objectKey);
        final HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .build();
        try {
            factory.getS3Client().headObject(headObjectRequest);
        } catch (S3Exception e) {
            LOGGER.error("ERROR::AWSServiceClientObjectsValidator::validateS3Object", e);
            validationErrors.add(new TimestreamSinkConnectorError(TimestreamSinkErrorCodes.INVALID_S3KEY, region, bucketName, objectKey));
        }
    }

    /**
     * Method to validate if the database exists in Timestream
     *
     * @param timestreamClient Timestream Write Client object
     * @param databaseName          Database name to be validated
     */
    public static void validateTimestreamDatabase(final TimestreamWriteClient timestreamClient, final String databaseName, final List<TimestreamSinkConnectorError> validationErrors) {
        LOGGER.info("Begin::AWSServiceClientObjectsValidator::validateTimestreamDatabase:: Timestream database: {}", databaseName);
        final DescribeDatabaseRequest describeDbRequest = DescribeDatabaseRequest.builder()
                .databaseName(databaseName).build();
        try {
            final DescribeDatabaseResponse response = timestreamClient.describeDatabase(describeDbRequest);
            LOGGER.info("AWSServiceClientObjectsValidator::validateTimestreamDatabase:: database: {} , databaseId: {}", response.database(), response.database().arn());
        } catch (SdkException e) {
            LOGGER.error("ERROR::AWSServiceClientObjectsValidator::validateTimestreamDatabase", e);
            validationErrors.add(new TimestreamSinkConnectorError(TimestreamSinkErrorCodes.INVALID_DATABASE, databaseName));
        }
    }

    /**
     * Method to validate if the given regionString represents a valid AWS Region
     *
     * @param regionString AWS region name that needs to be validated
     */
    public static void validateAWSRegion(final String regionString, final List<TimestreamSinkConnectorError> validationErrors) {
        LOGGER.info("Begin::AWSServiceClientObjectsValidator::validateAWSRegion:: for region: {} ", regionString);
        try {
            if (regionString == null || regionString.isEmpty()) {
                throw new IllegalArgumentException(" AWS Region must not be null or empty");
            }
            Region.of(regionString);
        } catch (SdkException e) {
            LOGGER.error("ERROR::AWSServiceClientObjectsValidator::validateAWSRegion:", e);
            validationErrors.add(new TimestreamSinkConnectorError(TimestreamSinkErrorCodes.INVALID_REGION, regionString));
        }
    }


    /**
     * Method to validate if the given table exists in teh given Timestream database
     *
     * @param timestreamClient Timestream Write Client object
     * @param databaseName          Database name
     * @param tableName             Table name to be validated
     */
    public static void validateTimestreamTable(final TimestreamWriteClient timestreamClient, final String databaseName, final String tableName, final List<TimestreamSinkConnectorError> validationErrors) {

        LOGGER.info("Begin::AWSServiceClientObjectsValidator::validateTimestreamTable:: Timestream database: {}, table: {}", databaseName, tableName);
        final DescribeTableRequest describeTableReq = DescribeTableRequest.builder()
                .databaseName(databaseName).tableName(tableName).build();
        try {
            final DescribeTableResponse response = timestreamClient.describeTable(describeTableReq);
            LOGGER.info("AWSServiceClientObjectsValidator::validateTimestreamTable:: tableName: {} , tableId: {}", tableName, response.table().arn());
        } catch (SdkException e) {
            LOGGER.error("ERROR::AWSServiceClientObjectsValidator::validateTimestreamTable", e);
            validationErrors.add(new TimestreamSinkConnectorError(TimestreamSinkErrorCodes.INVALID_TABLE, tableName, databaseName));
        }
    }
}
