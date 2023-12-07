package software.amazon.timestream;

/**
 * Interface that lists all the constants used by the connector classes
 */
public class TimestreamSinkConstants {
    /**
     * Default measure value if not specified in schema definition
     */
    public static final String DEFAULT_MEASURE = "default_measure";
    /**
     * Custom TimeUnit
     */
    public static final String TIMEUNIT_DATETIME = "DATETIME";

    /**
     * Constant: Java class for the connector
     */
    public static final String CONNECTOR_CLASS = "connector.class";
    /**
     * Value: Java class for the connector
     */
    public static final String CLASS_FQDN = "software.amazon.timestream.TimestreamSinkConnector";
    /**
     * Constant: max tasks for the connector
     */
    public static final String TASKS_MAX = "tasks.max";
    /**
     * Constant:AWS region
     */
    public static final String AWS_REGION = "aws.region";

    // Timestream Sink Connector - Mandatory configurations
    /**
     * Constant: Timestream database name
     */
    public static final String DATABASE_NAME = "timestream.database.name";
    /**
     * Constant: Timestream table name
     */
    public static final String TABLE_NAME = "timestream.table.name";
    /**
     * Constant: S3 bucket name
     */
    public static final String S3_BUCKET = "timestream.schema.s3.bucket.name";
    /**
     * Constant: S3 schema definition object key
     */
    public static final String SCHEMA_S3KEY = "timestream.schema.s3.key";
    /**
     * Constant: VPC endpoint for Timestream
     */
    public static final String VPC_ENDPOINT = "timestream.ingestion.endpoint";

    // Timestream Sink Connector - Optional configurations
    /**
     * Constant: specifies versioning of records required
     */
    public static final String AUTO_VERSIONING = "timestream.record.versioning.auto";
    /**
     * Constant: specifies if empty dimension can be skipped
     */
    public static final String SKIP_DIMENSION = "timestream.record.dimension.skip.empty";
    /**
     * Constant: specifies if empty measure value can be skipped
     */
    public static final String SKIP_MEASURE = "timestream.record.measure.skip.empty";
    /**
     * Constant: specifies records size in a batch write
     */
    public static final String BATCH_SIZE = "timestream.record.batch.size";
    /**
     * Constant: specifies maximum number of allowed
     * concurrently opened HTTP connections to the Timestream service.
     */
    public static final String CON_MAX = "timestream.connections.max";
    /**
     * Constant: The maximum number of retry attempts for retryable errors
     */
    public static final String CON_RETRIES = "timestream.connections.retries";
    /**
     * Constant: The time in seconds
     * the AWS SDK will wait for a query request before timing out.
     * Non-positive value disables request timeout.
     */
    public static final String CON_TIMEOUT = "timestream.connections.timeoutseconds";

    /**
     * Constant: default value for 'timestream.connections.max'
     */
    public static final int DEFAULT_CON_MAX = 5000;
    /**
     * Constant: default value for 'timestream.connections.retries'
     */
    public static final int DEFAULT_RETRIES = 10;
    /**
     * Constant: default value for 'timestream.connections.timeoutseconds'
     */
    public static final int DEFAULT_TIMEOUT = 20;
    /**
     * Constant: default value for 'tasks.max'
     */
    public static final int DEFAULT_MAX_TASKS = 1;
    /**
     * Constant: default value for 'timestream.record.batch.size'
     */
    public static final int DEFAULT_BATCHSIZE = 100;
}