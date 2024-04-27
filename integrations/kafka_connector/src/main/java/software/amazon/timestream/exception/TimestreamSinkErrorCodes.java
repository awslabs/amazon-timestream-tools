package software.amazon.timestream.exception;

/**
 * Interface that lists the error code constants
 */
public class TimestreamSinkErrorCodes {

    /**
     * Error code for validation
     */
    public static final String VALIDATION_ERROR = "VALIDATION_ERROR";
    /**
     * Error code: if 'connector.class' is not specified
     */
    public static final String MISSING_CLASS = "validation.error.missing.timestream.sink.class";
    /**
     * Error code: if 'tasks.max' is not specified
     */
    public static final String MISSING_TASKS_MAX = "validation.error.missing.tasks.max";
    /**
     * Error code: if 'aws.region' is not specified
     */
    public static final String MISSING_REGION = "validation.error.missing.aws.region";
    /**
     * Error code: if 'topics' is not specified
     */
    public static final String MISSING_TOPICS = "validation.error.missing.topics";
    /**
     * Error code: if 'timestream.schema.s3.bucket.name' is not specified
     */
    public static final String MISSING_BUCKET = "validation.error.missing.bucket.name";
    /**
     * Error code: if 'timestream.database.name' is not specified
     */
    public static final String MISSING_DATABASE = "validation.error.missing.timestream.database.name";
    /**
     * Error code: if 'timestream.table.name' is not specified
     */
    public static final String MISSING_TABLE = "validation.error.missing.timestream.table.name";
    /**
     * Error code: if 'timestream.ingestion.endpoint' is not specified
     */
    public static final String MISSING_ENDPOINT = "validation.error.missing.timestream.ingestion.endpoint";
    /**
     * Error code: if 'timestream.schema.s3.key' is not specified
     */
    public static final String MISSING_SCHEMA = "validation.error.missing.timestream.schema.s3.key";
    /**
     * Error code: if value for 'timestream.ingestion.endpoint' is invalid
     */
    public static final String INVALID_ENDPOINT = "invalid.timestream.ingestion.endpoint";
    /**
     * Error code: if value for 'timestream.schema.s3.key' is invalid
     */
    public static final String INVALID_S3KEY = "invalid.timestream.schema.s3.key";
    /**
     * Error code: if value for 'timestream.schema.s3.key' is invalid JSON schema
     */
    public static final String INVALID_SCHEMA = "invalid.timestream.schema.definition";
    /**
     * Error code: if value for 'timestream.schema.s3.bucket.name' is invalid
     */
    public static final String INVALID_S3BUCKET = "invalid.s3.bucket.name";
    /**
     * Error code: if value for 'aws.region' is invalid
     */
    public static final String INVALID_REGION = "invalid.region";
    /**
     * Error code: if value for 'timestream.database.name' is invalid
     */
    public static final String INVALID_DATABASE = "invalid.timestream.database";
    /**
     * Error code: if value for 'timestream.table.name' is invalid
     */
    public static final String INVALID_TABLE = "invalid.timestream.table";
    /**
     * Error code: if value for 'timestream.record.batch.size' is invalid
     */
    public static final String INVALID_BATCH = "invalid.timestream.record.batch.size";
    /**
     * Error code: if value for 'timestream.connections.max' is invalid
     */
    public static final String INVALID_MAX_CON = "invalid.timestream.connections.max";
    /**
     * Error code: if value for 'timestream.connections.retries' is invalid
     */
    public static final String INVALID_CON_RETRY ="invalid.timestream.connections.retries";
    /**
     * Error code: if supplied configuration is unknown
     */
    public static final String UNKNOWN_CONFIG = "Unknown configuration '%s'";

    /**
     * Error code: if the sink record values/ types are invalid
     */
    public static final String INVALID_SINK_RECORD ="invalid.sink.record";

    /**
     * Error code: 'bootstrap.servers' is required when DLQ is enabled
     */
    public static final String MISSING_BOOTSTRAP_SERVER ="validation.error.missing.bootstrap.server";

    /**
     * Error code: 'key.serializer' is required when DLQ is enabled
     */
    public static final String MISSING_KEY_DESER ="validation.error.missing.key.serializer";

    /**
     * Error code: 'value.serializer' is required when DLQ is enabled
     */
    public static final String MISSING_VALUE_DESER ="validation.error.missing.value.serializer";

    /**
     * Error code: if value for a DIMENSION is invalid
     */
    public static final String INVALID_DIMENSION_VALUE ="invalid.dimension.value";

    /**
     * Error code: if value for a MEASURE is invalid
     */
    public static final String INVALID_MEASURE_VALUE ="invalid.measure.value";

    //// INFLUX DB
    // #TODO fix later to add detailed errors
}
