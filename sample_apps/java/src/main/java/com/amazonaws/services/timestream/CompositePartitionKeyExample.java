package com.amazonaws.services.timestream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import org.apache.commons.lang3.RandomStringUtils;

import com.amazonaws.services.timestreamwrite.AmazonTimestreamWrite;
import com.amazonaws.services.timestreamquery.AmazonTimestreamQuery;
import com.amazonaws.services.timestreamwrite.model.Dimension;
import com.amazonaws.services.timestreamwrite.model.MeasureValueType;
import com.amazonaws.services.timestreamwrite.model.MeasureValue;
import com.amazonaws.services.timestreamwrite.model.RejectedRecordsException;
import com.amazonaws.services.timestreamwrite.model.ConflictException;
import com.amazonaws.services.timestreamwrite.model.CreateTableRequest;
import com.amazonaws.services.timestreamwrite.model.DescribeTableRequest;
import com.amazonaws.services.timestreamwrite.model.DescribeTableResult;
import com.amazonaws.services.timestreamwrite.model.Record;
import com.amazonaws.services.timestreamwrite.model.ResourceNotFoundException;
import com.amazonaws.services.timestreamwrite.model.RetentionProperties;
import com.amazonaws.services.timestreamwrite.model.Table;
import com.amazonaws.services.timestreamwrite.model.UpdateTableRequest;
import com.amazonaws.services.timestreamwrite.model.ValidationException;
import com.amazonaws.services.timestreamwrite.model.WriteRecordsRequest;
import com.amazonaws.services.timestreamwrite.model.WriteRecordsResult;
import com.amazonaws.services.timestreamwrite.model.PartitionKey;
import com.amazonaws.services.timestreamwrite.model.PartitionKeyEnforcementLevel;
import com.amazonaws.services.timestreamwrite.model.PartitionKeyType;
import com.amazonaws.services.timestreamwrite.model.Schema;
import com.amazonaws.services.timestreamwrite.model.MagneticStoreWriteProperties;
import com.amazonaws.services.timestreamwrite.model.MagneticStoreRejectedDataLocation;
import com.amazonaws.services.timestreamwrite.model.S3Configuration;
import com.amazonaws.services.timestreamwrite.model.S3EncryptionOption;
import com.amazonaws.services.timestream.utils.Constants;

import static com.amazonaws.services.timestream.utils.Constants.DATABASE_NAME;
import static com.amazonaws.services.timestream.utils.Constants.HT_TTL_HOURS;
import static com.amazonaws.services.timestream.utils.Constants.CT_TTL_DAYS;
import static com.amazonaws.services.timestream.utils.QueryUtil.runQuery;
import com.amazonaws.services.timestream.utils.TimestreamDependencyHelper;
import com.amazonaws.services.timestream.utils.WriteUtil;

public class CompositePartitionKeyExample {
    private static final String PARTITION_KEY_DIMENSION_TABLE_NAME = "host_metrics_dim_pk";
    private static final String PARTITION_KEY_MEASURE_TABLE_NAME = "host_metrics_measure_pk";

    // Composite Partition Keys are most effective when dimension has high cardinality
    // and are frequently accessed in queries.
    // Using dimension name with high cardinality, "hostId"
    private static final String COMPOSITE_PARTITION_KEY_DIM_NAME = "hostId";
    private static final String COMPOSITE_PARTITION_KEY_DIM_VALUE = "host1";
    // Different dimension name to demonstrate enforcement level and record rejection
    private static final String COMPOSITE_PARTITION_KEY_DIFF_NAME = "hostIdDiff";
    public static final String ERROR_CONFIGURATION_S3_BUCKET_NAME_PREFIX = "error-configuration-sample-s3-bucket-";

    private final InputArguments inputArguments;
    private final AmazonTimestreamWrite writeClient;
    private final AmazonTimestreamQuery queryClient;
    private final WriteUtil writeUtil;
    private final TimestreamDependencyHelper timestreamDependencyHelper;
    private String s3ErrorReportBucketName;

    public CompositePartitionKeyExample(InputArguments inputArguments, final AmazonTimestreamWrite writeClient,
                                        final AmazonTimestreamQuery queryClient) {
        this.inputArguments = inputArguments;
        this.writeClient = writeClient;
        this.queryClient = queryClient;
        this.writeUtil = new WriteUtil(writeClient);
        this.timestreamDependencyHelper = new TimestreamDependencyHelper(inputArguments.getRegion());
        this.s3ErrorReportBucketName = ERROR_CONFIGURATION_S3_BUCKET_NAME_PREFIX +
                RandomStringUtils.randomAlphanumeric(5).toLowerCase();
    }

    public void run() throws IOException {
        try {
            // S3 bucket creation to store rejected records for MagneticStoreWrite Upsert
            s3ErrorReportBucketName = timestreamDependencyHelper.createS3Bucket(s3ErrorReportBucketName);
            writeUtil.createDatabase(DATABASE_NAME);

            /* This sample demonstrates workflow using dimension type partition key
             * 1. Create table with dimension type partition key, with optional enforcement.
             * 2. Ingest records with missing partition key. Records will be accepted as enforcement level is optional
             * 3. Update table with required enforcement.
             * 4. Ingest records with same partition key.
             * 5. Ingest records with missing partition key. This will return rejected records as they do not
             *    contain required partition key.
             * 6. Query records with same partition key.
             */
            runSampleWithDimensionPartitionKey(PARTITION_KEY_DIMENSION_TABLE_NAME);

            /* This sample demonstrates workflow using measure name type partition key
             * 1. Create table with measure name type partition key
             * 2. Ingest records and query
             */
            runSampleWithMeasureNamePartitionKey(PARTITION_KEY_MEASURE_TABLE_NAME);

        } catch (Exception e) {
            System.out.println("Output of exception: " + e);
            e.printStackTrace();
        } finally {
            if (!inputArguments.isSkipDeletion()) {
                writeUtil.deleteTable(DATABASE_NAME, PARTITION_KEY_DIMENSION_TABLE_NAME);
                writeUtil.deleteTable(DATABASE_NAME, PARTITION_KEY_MEASURE_TABLE_NAME);
                writeUtil.deleteDatabase(DATABASE_NAME);
                timestreamDependencyHelper.deleteS3Bucket(s3ErrorReportBucketName);
            }
        }
    }

    private void runSampleWithDimensionPartitionKey(String tableName) {
        System.out.println("Starting example for dimension type partition key");

        // Create table with dimension type partition key and OPTIONAL enforcement
        final List<PartitionKey> partitionKeyWithDimensionAndOptionalEnforcement = Collections.singletonList(new PartitionKey()
                .withName(COMPOSITE_PARTITION_KEY_DIM_NAME)
                .withType(PartitionKeyType.DIMENSION)
                .withEnforcementInRecord(PartitionKeyEnforcementLevel.OPTIONAL));
        createTableWithPartitionKey(DATABASE_NAME, tableName, partitionKeyWithDimensionAndOptionalEnforcement);
        describeTable(DATABASE_NAME, tableName);

        System.out.println("Writing records without partition key dimension into table with optional enforcement." +
                "Since the enforcement level is OPTIONAL the records will be ingested");
        writeRecords(DATABASE_NAME, tableName, COMPOSITE_PARTITION_KEY_DIFF_NAME);

        // Update table's partition key enforcement level to REQUIRED
        final List<PartitionKey> partitionKeyWithDimensionAndRequiredEnforcement = Collections.singletonList(new PartitionKey()
                .withName(COMPOSITE_PARTITION_KEY_DIM_NAME)
                .withType(PartitionKeyType.DIMENSION)
                .withEnforcementInRecord(PartitionKeyEnforcementLevel.REQUIRED));
        updateTable(DATABASE_NAME, tableName, partitionKeyWithDimensionAndRequiredEnforcement);
        describeTable(DATABASE_NAME, tableName);

        System.out.println("Writing records with partition key dimension");
        writeRecords(DATABASE_NAME, tableName, COMPOSITE_PARTITION_KEY_DIM_NAME);

        System.out.println("Writing records without partition key dimension into table with required enforcement." +
                "Since the enforcement level is REQUIRED the records will be rejected");
        writeRecords(DATABASE_NAME, tableName, COMPOSITE_PARTITION_KEY_DIFF_NAME);

        // Query with partition key
        runSingleQuery(DATABASE_NAME, tableName, COMPOSITE_PARTITION_KEY_DIM_NAME,
                "'" + COMPOSITE_PARTITION_KEY_DIM_VALUE + "'");
    }

    private void runSampleWithMeasureNamePartitionKey(String tableName) {
        System.out.println("Starting example for measure type partition key");

        // Create table with measure name type partition key
        final List<PartitionKey> partitionKeyWithMeasure = Collections.singletonList(new PartitionKey()
                .withType(PartitionKeyType.MEASURE));
        createTableWithPartitionKey(DATABASE_NAME, tableName, partitionKeyWithMeasure);
        describeTable(DATABASE_NAME, tableName);
        System.out.println("Writing records");
        writeRecords(DATABASE_NAME, tableName, COMPOSITE_PARTITION_KEY_DIM_NAME);
        runSingleQuery(DATABASE_NAME, tableName, "cpu_utilization", "13.5");
    }

    public void createTableWithPartitionKey(String databaseName, String tableName, List<PartitionKey> partitionKeyList) {
        System.out.println("Creating table with table name: " + tableName);

        CreateTableRequest createTableRequest = new CreateTableRequest();
        createTableRequest.setDatabaseName(databaseName);
        createTableRequest.setTableName(tableName);

        // Set RetentionProperties for the table
        final RetentionProperties retentionProperties = new RetentionProperties()
                .withMemoryStoreRetentionPeriodInHours(HT_TTL_HOURS)
                .withMagneticStoreRetentionPeriodInDays(CT_TTL_DAYS);
        createTableRequest.setRetentionProperties(retentionProperties);

        // Set MagneticStoreWriteProperties for the table
        final MagneticStoreRejectedDataLocation magneticStoreRejectedDataLocation = new MagneticStoreRejectedDataLocation()
                .withS3Configuration(new S3Configuration().withBucketName(s3ErrorReportBucketName)
                        .withEncryptionOption(S3EncryptionOption.SSE_S3));
        final MagneticStoreWriteProperties magneticStoreWriteProperties = new MagneticStoreWriteProperties()
                .withEnableMagneticStoreWrites(true)
                .withMagneticStoreRejectedDataLocation(magneticStoreRejectedDataLocation);
        createTableRequest.setMagneticStoreWriteProperties(magneticStoreWriteProperties);

        // Set CompositePartitionKey for the table
        // Can use dimension or measure name as partition key
        // If using type dimension, can specify enforcement level with OPTIONAL or REQUIRED
        Schema schema = new Schema();
        schema.setCompositePartitionKey(partitionKeyList);
        createTableRequest.setSchema(schema);

        try {
            writeClient.createTable(createTableRequest);
            System.out.println("Table [" + tableName + "] successfully created.");
        } catch (ConflictException e) {
            System.out.println("Table [" + tableName + "] exists on database [" + databaseName + "] . Skipping database creation");
        }
    }

    public void updateTable(String databaseName, String tableName, List<PartitionKey> partitionKeyList) {
        System.out.println("Updating table with table name: " + tableName);

        UpdateTableRequest updateTableRequest = new UpdateTableRequest();
        updateTableRequest.setDatabaseName(databaseName);
        updateTableRequest.setTableName(tableName);

        Schema schema = new Schema();
        schema.setCompositePartitionKey(partitionKeyList);
        updateTableRequest.withSchema(schema);

        writeClient.updateTable(updateTableRequest);
        System.out.println("Table updated");
    }

    public void describeTable(String databaseName, String tableName) {
        System.out.println("Describing table with table name: " + tableName);
        final DescribeTableRequest describeTableRequest = new DescribeTableRequest();
        describeTableRequest.setDatabaseName(databaseName);
        describeTableRequest.setTableName(tableName);
        try {
            DescribeTableResult result = writeClient.describeTable(describeTableRequest);
            String tableId = result.getTable().getArn();
            System.out.println("Table " + tableName + " has id " + tableId);
            System.out.println("Table " + tableName + " has partition key " + result.getTable().getSchema().getCompositePartitionKey());
        } catch (final Exception e) {
            System.out.println("Table " + tableName + " doesn't exist = " + e);
            throw e;
        }
    }

    public void writeRecords(String databaseName, String tableName, String compositePartitionKeyDimName) {
        List<Record> records = new ArrayList<>();
        final long time = System.currentTimeMillis();

        List<Dimension> dimensions = new ArrayList<>();
        final Dimension region = new Dimension().withName("region").withValue("us-east-1");
        final Dimension az = new Dimension().withName("az").withValue("az1");
        final Dimension hostId = new Dimension().withName(compositePartitionKeyDimName).withValue(COMPOSITE_PARTITION_KEY_DIM_VALUE);



        dimensions.add(region);
        dimensions.add(az);
        dimensions.add(hostId);

        Record commonAttributes = new Record()
                .withDimensions(dimensions)
                .withTime(String.valueOf(time))
                .withVersion(time);

        Record cpuUtilization = new Record()
                .withMeasureName("cpu_utilization")
                .withMeasureValue("13.5")
                .withMeasureValueType(MeasureValueType.DOUBLE);
        Record memoryUtilization = new Record()
                .withMeasureName("memory_utilization")
                .withMeasureValue("40")
                .withMeasureValueType(MeasureValueType.DOUBLE);

        records.add(cpuUtilization);
        records.add(memoryUtilization);

        // Adding multi record
        MeasureValue cpuUtilizationMulti = new MeasureValue()
                .withName("cpu_utilization")
                .withType(MeasureValueType.DOUBLE)
                .withValue("13.5");
        MeasureValue memoryUtilizationMulti = new MeasureValue()
                .withName("memory_utilization")
                .withType(MeasureValueType.DOUBLE)
                .withValue("40");
        Record computationalResources = new Record()
                .withMeasureName("cpu_memory")
                .withMeasureValues(cpuUtilizationMulti, memoryUtilizationMulti)
                .withMeasureValueType(MeasureValueType.MULTI);

        records.add(computationalResources);

        WriteRecordsRequest writeRecordsRequest = new WriteRecordsRequest()
                .withDatabaseName(databaseName)
                .withTableName(tableName)
                .withCommonAttributes(commonAttributes)
                .withRecords(records);

        try {
            WriteRecordsResult writeRecordsResult = writeClient.writeRecords(writeRecordsRequest);
            System.out.println("WriteRecords Status: " + writeRecordsResult.getSdkHttpMetadata().getHttpStatusCode());
        } catch (RejectedRecordsException e) {
            printRejectedRecordsException(e);
        } catch (Exception e) {
            System.out.println("Error: " + e);
        }
    }

    private void printRejectedRecordsException(RejectedRecordsException e) {
        System.out.println("RejectedRecords: " + e);
        e.getRejectedRecords().forEach(System.out::println);
    }

    private void runSingleQuery(String databaseName, String tableName, String partitionKeyName, String partitionKeyValue) {
        final String QUERY_STRING = String.format("SELECT * FROM \"%s\".\"%s\" WHERE \"%s\"=%s", databaseName, tableName,
                partitionKeyName, partitionKeyValue);
        System.out.println("Running query " + QUERY_STRING);
        runQuery(queryClient, QUERY_STRING);
    }
}