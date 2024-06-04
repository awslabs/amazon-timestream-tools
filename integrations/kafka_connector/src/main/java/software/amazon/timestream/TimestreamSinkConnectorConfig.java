package software.amazon.timestream;


import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.SinkConnector;
import software.amazon.awssdk.regions.Region;
import software.amazon.timestream.exception.TimestreamSinkConnectorError;
import software.amazon.timestream.exception.TimestreamSinkConnectorException;
import software.amazon.timestream.exception.TimestreamSinkErrorCodes;

import java.util.Map;

/**
 * Class that extends {@link AbstractConfig}
 * for preserving the Timestream Sink connector
 * configuration details.
 */
public class TimestreamSinkConnectorConfig extends AbstractConfig {

    /**
     *
     * @param config the definition of the configurations
     * @param parsedConfig configuration properties
     */
    public TimestreamSinkConnectorConfig(final ConfigDef config, final Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    /**
     *
     * @param parsedConfig configuration properties
     */
    public TimestreamSinkConnectorConfig(final Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    /**
     *
     * @return ConfigDef the definition of the configurations
     */
    public static ConfigDef conf() {
        return new ConfigDef()
                .define(TimestreamSinkConstants.TASKS_MAX,
                        ConfigDef.Type.INT,
                        TimestreamSinkConstants.DEFAULT_MAX_TASKS,
                        ConfigDef.Importance.HIGH,
                        "The maximum number of active tasks for a sink connector  ")
                .define(SinkConnector.TOPICS_CONFIG,
                        ConfigDef.Type.STRING,
                        "",
                        ConfigDef.Importance.HIGH,
                        "Name of the Kafka topic which needs to be polled for messages")
                .define(TimestreamSinkConstants.AWS_REGION,
                        ConfigDef.Type.STRING,
                        "",
                        ConfigDef.Importance.HIGH,
                        "The region in which the AWS service resources are provisioned")
                .define(TimestreamSinkConstants.DATABASE_NAME,
                        ConfigDef.Type.STRING,
                        "",
                        ConfigDef.Importance.HIGH,
                        "Name of the Timestream database where the target table is created")
                .define(TimestreamSinkConstants.TABLE_NAME,
                        ConfigDef.Type.STRING,
                        "",
                        ConfigDef.Importance.HIGH,
                        "Name of the Timestream table where the messages are to be ingested as records")
                .define(TimestreamSinkConstants.VPC_ENDPOINT,
                        ConfigDef.Type.STRING,
                        "",
                        ConfigDef.Importance.HIGH,
                        "Ingestion endpoint for Timestream, in URI format")
                .define(TimestreamSinkConstants.S3_BUCKET,
                        ConfigDef.Type.STRING,
                        "",
                        ConfigDef.Importance.HIGH,
                        "Name of the Amazon S3 bucket in which the target Timestream table's schema definition is present")
                .define(TimestreamSinkConstants.SCHEMA_S3KEY,
                        ConfigDef.Type.STRING,
                        "",
                        ConfigDef.Importance.HIGH,
                        "S3 object key of the targeted Timestream table schema  ")
                .define(TimestreamSinkConstants.CON_MAX,
                        ConfigDef.Type.INT,
                        TimestreamSinkConstants.DEFAULT_CON_MAX,
                        ConfigDef.Importance.LOW,
                        "The maximum number of allowed concurrently opened HTTP connections to the Timestream service. ")
                .define(TimestreamSinkConstants.CON_RETRIES,
                        ConfigDef.Type.INT,
                        TimestreamSinkConstants.DEFAULT_RETRIES,
                        ConfigDef.Importance.LOW,
                        "The maximum number of retry attempts for retryable errors with 5XX error codes in the SDK. The value must be non-negative. ")
                .define(TimestreamSinkConstants.CON_TIMEOUT,
                        ConfigDef.Type.INT,
                        TimestreamSinkConstants.DEFAULT_TIMEOUT,
                        ConfigDef.Importance.LOW,
                        "The time in seconds the AWS SDK will wait for a query request before timing out. Non-positive value disables request timeout.")
                .define(TimestreamSinkConstants.BATCH_SIZE,
                        ConfigDef.Type.INT,
                        TimestreamSinkConstants.DEFAULT_BATCHSIZE,
                        ConfigDef.Importance.LOW,
                        "The maximum number of records in a WriteRecords API request. Maxed at 100")
                .define(TimestreamSinkConstants.AUTO_VERSIONING,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.LOW,
                        "Enable if upserts are required. By default the version is set to 1    ")
                .define(TimestreamSinkConstants.SKIP_DIMENSION,
                        ConfigDef.Type.BOOLEAN,
                        true,
                        ConfigDef.Importance.LOW,
                        "When a dimension value is not present/ empty, only that dimension would be skipped by default.")
                .define(TimestreamSinkConstants.SKIP_MEASURE,
                        ConfigDef.Type.BOOLEAN,
                        true,
                        ConfigDef.Importance.LOW,
                        "When a measure value is not present/ empty, only that measure would be skipped by default.")
                .define("errors.deadletterqueue.topic.name",
                        ConfigDef.Type.STRING,
                        "",
                        ConfigDef.Importance.LOW,
                        "DLQ topic name for messages that result in an error when processed by this sink connector")
                .define(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                        ConfigDef.Type.STRING,
                        "",
                        ConfigDef.Importance.LOW,
                        "A list of host/port pairs to use for establishing the initial connection to the Kafka cluster")
                .define(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                        ConfigDef.Type.STRING,
                        "org.apache.kafka.common.serialization.StringSerializer",
                        ConfigDef.Importance.LOW,
                        "Serializer class for key")
                .define(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                        ConfigDef.Type.STRING,
                        "org.apache.kafka.common.serialization.StringSerializer",
                        ConfigDef.Importance.LOW,
                        "Serializer class for Value")
                /// INFLUXDB
                .define(TimestreamSinkConstants.LIVE_ANALYTICS_ENABLE,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.HIGH,
                        "LiveAnalytics Ingestion Enabled")
                .define(TimestreamSinkConstants.INFLUXDB_ENABLE,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.HIGH,
                        "InfluxDB Ingestion Enabled")
                .define(TimestreamSinkConstants.INFLUXDB_BUCKET,
                        ConfigDef.Type.STRING,
                        "",
                        ConfigDef.Importance.HIGH,
                        "InfluxDB Target Bucket")
                .define(TimestreamSinkConstants.INFLUXDB_URL,
                        ConfigDef.Type.STRING,
                        "",
                        ConfigDef.Importance.HIGH,
                        "InfluxDB API URL")
                .define(TimestreamSinkConstants.INFLUXDB_TOKEN,
                        ConfigDef.Type.STRING,
                        "",
                        ConfigDef.Importance.HIGH,
                        "InfluxDB API Token")
                .define(TimestreamSinkConstants.INFLUXDB_ORG,
                        ConfigDef.Type.STRING,
                        "",
                        ConfigDef.Importance.HIGH,
                        "InfluxDB Organization")
                ////////////////////////////////
                ;
    }

    /**
     * @return maximum number HTTP connections to Timestream service.
     */
    public int getMaxConnections() {
        try {
            return getInt(TimestreamSinkConstants.CON_MAX);
        } catch (final ConfigException ex) {
            final TimestreamSinkConnectorError error = new TimestreamSinkConnectorError(TimestreamSinkErrorCodes.UNKNOWN_CONFIG,
                    TimestreamSinkConstants.CON_MAX, ex);
            throw new TimestreamSinkConnectorException(error, ex);
        }
    }

    /**
     * @return AWS region where AWS resources are provisioned
     */
    public Region getAWSRegion() {
        try {
            return Region.of(getString(TimestreamSinkConstants.AWS_REGION));
        } catch (final ConfigException ex) {
            final TimestreamSinkConnectorError error = new TimestreamSinkConnectorError(TimestreamSinkErrorCodes.UNKNOWN_CONFIG,
                    TimestreamSinkConstants.AWS_REGION, ex);
            throw new TimestreamSinkConnectorException(error, ex);
        }
    }

    ////// INFLUXDB Configs
    public boolean isLiveAnalyticsEnabled() {
        try {
            return getBoolean(TimestreamSinkConstants.LIVE_ANALYTICS_ENABLE);
        } catch (final ConfigException ex) {
            final TimestreamSinkConnectorError error = new TimestreamSinkConnectorError(TimestreamSinkErrorCodes.UNKNOWN_CONFIG,
                    TimestreamSinkConstants.LIVE_ANALYTICS_ENABLE, ex);
            throw new TimestreamSinkConnectorException(error, ex);
        }
    }

    public boolean isInfluxDBEnabled() {
        try {
            return getBoolean(TimestreamSinkConstants.INFLUXDB_ENABLE);
        } catch (final ConfigException ex) {
            final TimestreamSinkConnectorError error = new TimestreamSinkConnectorError(TimestreamSinkErrorCodes.UNKNOWN_CONFIG,
                    TimestreamSinkConstants.INFLUXDB_ENABLE, ex);
            throw new TimestreamSinkConnectorException(error, ex);
        }
    }

    public String getInfluxDBBucket() {
        try {
            return getString(TimestreamSinkConstants.INFLUXDB_BUCKET);
        } catch (final ConfigException ex) {
            final TimestreamSinkConnectorError error = new TimestreamSinkConnectorError(TimestreamSinkErrorCodes.UNKNOWN_CONFIG,
                    TimestreamSinkConstants.INFLUXDB_BUCKET, ex);
            throw new TimestreamSinkConnectorException(error, ex);
        }
    }

    public String getInfluxDBUrl() {
        try {
            return getString(TimestreamSinkConstants.INFLUXDB_URL);
        } catch (final ConfigException ex) {
            final TimestreamSinkConnectorError error = new TimestreamSinkConnectorError(TimestreamSinkErrorCodes.UNKNOWN_CONFIG,
                    TimestreamSinkConstants.INFLUXDB_URL, ex);
            throw new TimestreamSinkConnectorException(error, ex);
        }
    }

    public String getInfluxDBToken() {
        try {
            return getString(TimestreamSinkConstants.INFLUXDB_TOKEN);
        } catch (final ConfigException ex) {
            final TimestreamSinkConnectorError error = new TimestreamSinkConnectorError(TimestreamSinkErrorCodes.UNKNOWN_CONFIG,
                    TimestreamSinkConstants.INFLUXDB_TOKEN, ex);
            throw new TimestreamSinkConnectorException(error, ex);
        }
    }
    public String getInfluxDBOrg() {
        try {
            return getString(TimestreamSinkConstants.INFLUXDB_ORG);
        } catch (final ConfigException ex) {
            final TimestreamSinkConnectorError error = new TimestreamSinkConnectorError(TimestreamSinkErrorCodes.UNKNOWN_CONFIG,
                    TimestreamSinkConstants.INFLUXDB_ORG, ex);
            throw new TimestreamSinkConnectorException(error, ex);
        }
    }


    ////////////////////////////////////////////////////////

    /**
     *
     * @return The maximum number of retry attempts for retryable errors.
     */
    public int getNumRetries() {
        try {
            return getInt(TimestreamSinkConstants.CON_RETRIES);
        } catch (final ConfigException ex) {
            final TimestreamSinkConnectorError error = new TimestreamSinkConnectorError(TimestreamSinkErrorCodes.UNKNOWN_CONFIG,
                    TimestreamSinkConstants.CON_RETRIES, ex);
            throw new TimestreamSinkConnectorException(error, ex);
        }
    }

    /**
     * @return The time in seconds
     * the AWS SDK will wait for a query request before timing out.
     */
    public int getMaxTimeoutSeconds() {
        try {
            return getInt(TimestreamSinkConstants.CON_TIMEOUT);
        } catch (final ConfigException ex) {
            final TimestreamSinkConnectorError error = new TimestreamSinkConnectorError(TimestreamSinkErrorCodes.UNKNOWN_CONFIG,
                    TimestreamSinkConstants.CON_TIMEOUT, ex);
            throw new TimestreamSinkConnectorException(error, ex);
        }
    }

    /**
     *
     * @return Timestream database name where the table exists
     */
    public String getDatabaseName() {
        try {
            return getString(TimestreamSinkConstants.DATABASE_NAME);
        } catch (final ConfigException ex) {
            final TimestreamSinkConnectorError error = new TimestreamSinkConnectorError(TimestreamSinkErrorCodes.UNKNOWN_CONFIG,
                    TimestreamSinkConstants.DATABASE_NAME, ex);
            throw new TimestreamSinkConnectorException(error, ex);
        }
    }

    /**
     * @return Timestream table name where the events will be ingested as records
     */
    public String getTableName() {
        try {
            return getString(TimestreamSinkConstants.TABLE_NAME);
        } catch (final ConfigException ex) {
            final TimestreamSinkConnectorError error = new TimestreamSinkConnectorError(TimestreamSinkErrorCodes.UNKNOWN_CONFIG,
                    TimestreamSinkConstants.TABLE_NAME, ex);
            throw new TimestreamSinkConnectorException(error, ex);
        }
    }

    /**
     * @return Amazon S3 bucket name
     * where Timestream table's schema definition is stored
     */
    public String getSchemaS3BucketName() {
        try {
            return getString(TimestreamSinkConstants.S3_BUCKET);
        } catch (final ConfigException ex) {
            final TimestreamSinkConnectorError error = new TimestreamSinkConnectorError(TimestreamSinkErrorCodes.UNKNOWN_CONFIG,
                    TimestreamSinkConstants.S3_BUCKET, ex);
            throw new TimestreamSinkConnectorException(error, ex);
        }
    }

    /**
     * @return Amazon S3 object name in which table schema definition is stored
     */
    public String getSchmeaS3ObjectPath() {
        try {
            return getString(TimestreamSinkConstants.SCHEMA_S3KEY);
        } catch (final ConfigException ex) {
            final TimestreamSinkConnectorError error = new TimestreamSinkConnectorError(TimestreamSinkErrorCodes.UNKNOWN_CONFIG,
                    TimestreamSinkConstants.SCHEMA_S3KEY, ex);
            throw new TimestreamSinkConnectorException(error, ex);
        }
    }

    /**
     * @return Ingestion endpoint for Timestream, in URI format
     */
    public String getTimestreamIngestionEndPoint() {
        try {
            return getString(TimestreamSinkConstants.VPC_ENDPOINT);
        } catch (final ConfigException ex) {
            final TimestreamSinkConnectorError error = new TimestreamSinkConnectorError(TimestreamSinkErrorCodes.UNKNOWN_CONFIG,
                    TimestreamSinkConstants.VPC_ENDPOINT, ex);
            throw new TimestreamSinkConnectorException(error, ex);
        }
    }

    /**
     * @return maximum number of records in a WriteRecords API request.
     */
    public int getTimestreamBatchSize() {
        try {
            return getInt(TimestreamSinkConstants.BATCH_SIZE);
        } catch (final ConfigException ex) {
            final TimestreamSinkConnectorError error = new TimestreamSinkConnectorError(TimestreamSinkErrorCodes.UNKNOWN_CONFIG,
                    TimestreamSinkConstants.BATCH_SIZE, ex);
            throw new TimestreamSinkConnectorException(error, ex);
        }
    }

    /**
     * @return if auto versioning is enabled or not
     */
    public boolean isTimestreamRecordAutoVersioning() {
        try {
            return getBoolean(TimestreamSinkConstants.AUTO_VERSIONING);
        } catch (final ConfigException ex) {
            final TimestreamSinkConnectorError error = new TimestreamSinkConnectorError(TimestreamSinkErrorCodes.UNKNOWN_CONFIG,
                    TimestreamSinkConstants.AUTO_VERSIONING, ex);
            throw new TimestreamSinkConnectorException(error, ex);
        }
    }


    /**
     * @return if skip empty dimensions is enabled or not
     */
    public boolean isSkipEmptyDimensions() {
        try {
            return getBoolean(TimestreamSinkConstants.SKIP_DIMENSION);
        } catch (final ConfigException ex) {
            final TimestreamSinkConnectorError error = new TimestreamSinkConnectorError(TimestreamSinkErrorCodes.UNKNOWN_CONFIG,
                    TimestreamSinkConstants.SKIP_DIMENSION, ex);
            throw new TimestreamSinkConnectorException(error, ex);
        }
    }


    /**
     * @return if skip empty measure is enabled or not
     */
    public boolean isSkipEmptyMeasures() {
        try {
            return getBoolean(TimestreamSinkConstants.SKIP_MEASURE);
        } catch (final ConfigException ex) {
            final TimestreamSinkConnectorError error = new TimestreamSinkConnectorError(TimestreamSinkErrorCodes.UNKNOWN_CONFIG,
                    TimestreamSinkConstants.SKIP_MEASURE, ex);
            throw new TimestreamSinkConnectorException(error, ex);
        }
    }
}