package com.amazonaws.services.timestream;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.commons.lang3.RandomStringUtils;

import software.amazon.awssdk.services.timestreamquery.TimestreamQueryClient;
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient;
import software.amazon.awssdk.services.timestreamwrite.model.ConflictException;
import software.amazon.awssdk.services.timestreamwrite.model.CreateTableRequest;
import software.amazon.awssdk.services.timestreamwrite.model.DescribeTableRequest;
import software.amazon.awssdk.services.timestreamwrite.model.DescribeTableResponse;
import software.amazon.awssdk.services.timestreamwrite.model.Dimension;
import software.amazon.awssdk.services.timestreamwrite.model.MeasureValue;
import software.amazon.awssdk.services.timestreamwrite.model.MeasureValueType;
import software.amazon.awssdk.services.timestreamwrite.model.PartitionKey;
import software.amazon.awssdk.services.timestreamwrite.model.PartitionKeyEnforcementLevel;
import software.amazon.awssdk.services.timestreamwrite.model.PartitionKeyType;
import software.amazon.awssdk.services.timestreamwrite.model.Record;
import software.amazon.awssdk.services.timestreamwrite.model.RejectedRecordsException;
import software.amazon.awssdk.services.timestreamwrite.model.RetentionProperties;
import software.amazon.awssdk.services.timestreamwrite.model.Schema;
import software.amazon.awssdk.services.timestreamwrite.model.UpdateTableRequest;
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsRequest;
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsResponse;
import software.amazon.awssdk.services.timestreamwrite.model.MagneticStoreRejectedDataLocation;
import software.amazon.awssdk.services.timestreamwrite.model.MagneticStoreWriteProperties;
import software.amazon.awssdk.services.timestreamwrite.model.S3Configuration;
import software.amazon.awssdk.services.timestreamwrite.model.S3EncryptionOption;
import com.amazonaws.services.timestream.utils.WriteUtil;

import static com.amazonaws.services.timestream.utils.Constants.DATABASE_NAME;
import static com.amazonaws.services.timestream.utils.Constants.HT_TTL_HOURS;
import static com.amazonaws.services.timestream.utils.Constants.CT_TTL_DAYS;
import static com.amazonaws.services.timestream.utils.QueryUtil.runQuery;
import com.amazonaws.services.timestream.utils.TimestreamDependencyHelper;

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
    private static final String ERROR_CONFIGURATION_S3_BUCKET_NAME_PREFIX = "error-configuration-sample-s3-bucket-";

    private final InputArguments inputArguments;
    private final TimestreamWriteClient writeClient;
    private final TimestreamQueryClient queryClient;
    private final WriteUtil writeUtil;
    private String s3ErrorReportBucketName;
    private final TimestreamDependencyHelper timestreamDependencyHelper;


    public CompositePartitionKeyExample(InputArguments inputArguments, final TimestreamWriteClient writeClient,
                                        final TimestreamQueryClient queryClient) {
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
            timestreamDependencyHelper.createS3Bucket(s3ErrorReportBucketName);
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
        System.out.println("Starting example for partition key type dimension");

        // Create table with partition key type dimension and OPTIONAL enforcement
        final List<PartitionKey> partitionKeyWithDimensionAndOptionalEnforcement = Collections.singletonList(PartitionKey
                .builder()
                .name(COMPOSITE_PARTITION_KEY_DIM_NAME)
                .type(PartitionKeyType.DIMENSION)
                .enforcementInRecord(PartitionKeyEnforcementLevel.OPTIONAL)
                .build());
        createTableWithPartitionKey(DATABASE_NAME, tableName, partitionKeyWithDimensionAndOptionalEnforcement);
        describeTable(DATABASE_NAME, tableName);

        System.out.println("Writing records without partition key dimension into table with optional enforcement." +
                "Since the enforcement level is OPTIONAL the records will be ingested");
        writeRecords(DATABASE_NAME, tableName, COMPOSITE_PARTITION_KEY_DIFF_NAME);

        // Update partition key enforcement level to REQUIRED
        final List<PartitionKey> partitionKeyWithDimensionAndRequiredEnforcement = Collections.singletonList(PartitionKey
                .builder()
                .name(COMPOSITE_PARTITION_KEY_DIM_NAME)
                .type(PartitionKeyType.DIMENSION)
                .enforcementInRecord(PartitionKeyEnforcementLevel.REQUIRED)
                .build());
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
        final List<PartitionKey> partitionKeyWithMeasure = Collections.singletonList(PartitionKey.builder()
                .type(PartitionKeyType.MEASURE)
                .build());
        createTableWithPartitionKey(DATABASE_NAME, tableName, partitionKeyWithMeasure);
        describeTable(DATABASE_NAME, tableName);
        System.out.println("Writing records");
        writeRecords(DATABASE_NAME, tableName, COMPOSITE_PARTITION_KEY_DIM_NAME);
        runSingleQuery(DATABASE_NAME, tableName, "cpu_utilization", "13.5");
    }

    public void createTableWithPartitionKey(String databaseName, String tableName, List<PartitionKey> partitionKeyList) {
        System.out.println("Creating table with table name: " + tableName);

        // Set RetentionProperties for the table
        final RetentionProperties retentionProperties = RetentionProperties.builder()
                .memoryStoreRetentionPeriodInHours(HT_TTL_HOURS)
                .magneticStoreRetentionPeriodInDays(CT_TTL_DAYS)
                .build();

        // Set MagneticStoreWriteProperties for the table
        final MagneticStoreRejectedDataLocation magneticStoreRejectedDataLocation = MagneticStoreRejectedDataLocation.builder()
                .s3Configuration(S3Configuration.builder().bucketName(s3ErrorReportBucketName)
                        .encryptionOption(S3EncryptionOption.SSE_S3)
                        .build()).build();
        final MagneticStoreWriteProperties magneticStoreWriteProperties =
                MagneticStoreWriteProperties.builder()
                        .enableMagneticStoreWrites(true)
                        .magneticStoreRejectedDataLocation(magneticStoreRejectedDataLocation)
                        .build();

        // Set CompositePartitionKey for the table
        // Can use dimension or measure name as partition key
        // If using type dimension, can specify enforcement level with OPTIONAL or REQUIRED
        final Schema schema = Schema.builder()
                .compositePartitionKey(partitionKeyList).build();

        final CreateTableRequest createTableRequest = CreateTableRequest.builder()
                .databaseName(databaseName)
                .tableName(tableName)
                .retentionProperties(retentionProperties)
                .magneticStoreWriteProperties(magneticStoreWriteProperties)
                .schema(schema)
                .build();

        try {
            writeClient.createTable(createTableRequest);
            System.out.println("Table [" + tableName + "] successfully created.");
        } catch (ConflictException e) {
            System.out.println("Table [" + tableName + "] exists on database [" + databaseName + "] . Skipping database creation");
        }
    }

    public void updateTable(String databaseName, String tableName, List<PartitionKey> partitionKeyList) {
        System.out.println("Updating table with table name: " + tableName);
        final Schema schema = Schema.builder()
                .compositePartitionKey(partitionKeyList).build();
        final UpdateTableRequest updateTableRequest = UpdateTableRequest.builder()
                .databaseName(databaseName).tableName(tableName).schema(schema).build();

        writeClient.updateTable(updateTableRequest);
        System.out.println("Table updated");
    }

    public void describeTable(String databaseName, String tableName) {
        System.out.println("Describing table with table name: " + tableName);
        final DescribeTableRequest describeTableRequest = DescribeTableRequest.builder()
                .databaseName(databaseName).tableName(tableName).build();
        try {
            DescribeTableResponse response = writeClient.describeTable(describeTableRequest);
            String tableId = response.table().arn();
            System.out.println("Table " + tableName + " has id " + tableId);
            System.out.println("Table " + tableName + " has partition key " + response.table().schema().compositePartitionKey());
        } catch (final Exception e) {
            System.out.println("Table " + tableName + " doesn't exist = " + e);
            throw e;
        }
    }

    public void writeRecords(String databaseName, String tableName, String compositePartitionKeyDimName) {
        List<Record> records = new ArrayList<>();
        final long time = System.currentTimeMillis();

        List<Dimension> dimensions = new ArrayList<>();
        final Dimension region = Dimension.builder().name("region").value("us-east-1").build();
        final Dimension az = Dimension.builder().name("az").value("az1").build();
        final Dimension hostname = Dimension.builder()
                .name(compositePartitionKeyDimName).value(COMPOSITE_PARTITION_KEY_DIM_VALUE).build();

        dimensions.add(region);
        dimensions.add(az);
        dimensions.add(hostname);

        Record commonAttributes = Record.builder()
                .dimensions(dimensions)
                .time(String.valueOf(time))
                .version(time)
                .build();

        Record cpuUtilization = Record.builder()
                .measureValueType(MeasureValueType.DOUBLE)
                .measureName("cpu_utilization")
                .measureValue("13.5")
                .build();

        Record memoryUtilization = Record.builder()
                .measureValueType(MeasureValueType.DOUBLE)
                .measureName("memory_utilization")
                .measureValue("40")
                .build();

        records.add(cpuUtilization);
        records.add(memoryUtilization);

        // Adding multi record
        MeasureValue cpuUtilizationMulti = MeasureValue.builder()
                .name("cpu_utilization")
                .type(MeasureValueType.DOUBLE)
                .value("13.5")
                .build();
        MeasureValue memoryUtilizationMulti = MeasureValue.builder()
                .name("memory_utilization")
                .type(MeasureValueType.DOUBLE)
                .value("40")
                .build();
        Record computationalResources = Record.builder()
                .measureName("cpu_memory")
                .measureValues(cpuUtilizationMulti, memoryUtilizationMulti)
                .measureValueType(MeasureValueType.MULTI)
                .build();

        records.add(computationalResources);

        WriteRecordsRequest writeRecordsRequest = WriteRecordsRequest.builder()
                .databaseName(databaseName)
                .tableName(tableName)
                .records(records)
                .commonAttributes(commonAttributes)
                .build();

        try {
            WriteRecordsResponse writeRecordsResponse = writeClient.writeRecords(writeRecordsRequest);
            System.out.println("WriteRecords Status: " + writeRecordsResponse.sdkHttpResponse().statusCode());
        } catch (RejectedRecordsException e) {
            printRejectedRecordsException(e);
        } catch (Exception e) {
            System.out.println("Error: " + e);
        }
    }

    private void printRejectedRecordsException(RejectedRecordsException e) {
        System.out.println("RejectedRecords: " + e);
        e.rejectedRecords().forEach(System.out::println);
    }

    private void runSingleQuery(String databaseName, String tableName, String partitionKeyName, String partitionKeyValue) {
        final String QUERY_STRING = String.format("SELECT * FROM \"%s\".\"%s\" WHERE \"%s\"=%s", databaseName, tableName,
                partitionKeyName, partitionKeyValue);
        System.out.println("Running query " + QUERY_STRING);
        runQuery(queryClient, QUERY_STRING);
    }
}