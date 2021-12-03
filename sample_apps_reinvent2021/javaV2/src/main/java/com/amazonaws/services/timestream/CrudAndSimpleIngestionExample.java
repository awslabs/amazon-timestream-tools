package com.amazonaws.services.timestream;

import static com.amazonaws.services.timestream.Main.DATABASE_NAME;
import static com.amazonaws.services.timestream.Main.TABLE_NAME;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.RandomStringUtils;
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient;
import software.amazon.awssdk.services.timestreamwrite.model.ConflictException;
import software.amazon.awssdk.services.timestreamwrite.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.timestreamwrite.model.CreateTableRequest;
import software.amazon.awssdk.services.timestreamwrite.model.Database;
import software.amazon.awssdk.services.timestreamwrite.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.timestreamwrite.model.DeleteDatabaseResponse;
import software.amazon.awssdk.services.timestreamwrite.model.DeleteTableRequest;
import software.amazon.awssdk.services.timestreamwrite.model.DeleteTableResponse;
import software.amazon.awssdk.services.timestreamwrite.model.DescribeDatabaseRequest;
import software.amazon.awssdk.services.timestreamwrite.model.DescribeDatabaseResponse;
import software.amazon.awssdk.services.timestreamwrite.model.DescribeTableRequest;
import software.amazon.awssdk.services.timestreamwrite.model.DescribeTableResponse;
import software.amazon.awssdk.services.timestreamwrite.model.Dimension;
import software.amazon.awssdk.services.timestreamwrite.model.ListDatabasesRequest;
import software.amazon.awssdk.services.timestreamwrite.model.ListDatabasesResponse;
import software.amazon.awssdk.services.timestreamwrite.model.ListTablesRequest;
import software.amazon.awssdk.services.timestreamwrite.model.ListTablesResponse;
import software.amazon.awssdk.services.timestreamwrite.model.MeasureValueType;
import software.amazon.awssdk.services.timestreamwrite.model.Record;
import software.amazon.awssdk.services.timestreamwrite.model.RejectedRecordsException;
import software.amazon.awssdk.services.timestreamwrite.model.ResourceNotFoundException;
import software.amazon.awssdk.services.timestreamwrite.model.RetentionProperties;
import software.amazon.awssdk.services.timestreamwrite.model.Table;
import software.amazon.awssdk.services.timestreamwrite.model.UpdateDatabaseRequest;
import software.amazon.awssdk.services.timestreamwrite.model.UpdateTableRequest;
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsRequest;
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsResponse;
import software.amazon.awssdk.services.timestreamwrite.paginators.ListDatabasesIterable;
import software.amazon.awssdk.services.timestreamwrite.paginators.ListTablesIterable;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.timestreamwrite.model.MagneticStoreRejectedDataLocation;
import software.amazon.awssdk.services.timestreamwrite.model.MagneticStoreWriteProperties;
import software.amazon.awssdk.services.timestreamwrite.model.S3Configuration;
import software.amazon.awssdk.services.timestreamwrite.model.S3EncryptionOption;

import static com.amazonaws.services.timestream.TimestreamDependencyHelper.ERROR_CONFIGURATION_S3_BUCKET_NAME_PREFIX;

public class CrudAndSimpleIngestionExample {
    public static final long HT_TTL_HOURS = 24L;
    public static final long CT_TTL_DAYS = 7L;

    TimestreamWriteClient timestreamWriteClient;
    S3Client s3Client;
    TimestreamDependencyHelper timestreamDependencyHelper = new TimestreamDependencyHelper();

    public CrudAndSimpleIngestionExample(TimestreamWriteClient client, S3Client s3Client) {
        this.timestreamWriteClient = client;
        this.s3Client = s3Client;
    }

    public void createDatabase(String databaseName) {
        System.out.println("Creating database");
        CreateDatabaseRequest request = CreateDatabaseRequest.builder().databaseName(databaseName).build();
        try {
            timestreamWriteClient.createDatabase(request);
            System.out.println("Database [" + databaseName + "] created successfully");
        } catch (ConflictException e) {
            System.out.println("Database [" + databaseName + "] exists. Skipping database creation");
        }
    }

    public void createTable(String databaseName, String tableName) {
        System.out.println("Creating table");

        // S3 bucket creation to store rejected records for MagneticStoreWrite Upsert
        // Make the bucket name unique by appending 5 random characters at the end
        String s3ErrorReportBucketName =
                timestreamDependencyHelper.createS3Bucket(s3Client, ERROR_CONFIGURATION_S3_BUCKET_NAME_PREFIX +
                        RandomStringUtils.randomAlphanumeric(5).toLowerCase());

        // Enable MagneticStoreWrite Upsert
        final MagneticStoreRejectedDataLocation magneticStoreRejectedDataLocation = MagneticStoreRejectedDataLocation.builder()
                .s3Configuration(S3Configuration.builder().bucketName(s3ErrorReportBucketName)
                        .encryptionOption(S3EncryptionOption.SSE_S3)
                        .build()).build();
        final MagneticStoreWriteProperties magneticStoreWriteProperties =
                MagneticStoreWriteProperties.builder()
                        .enableMagneticStoreWrites(true)
                        .magneticStoreRejectedDataLocation(magneticStoreRejectedDataLocation)
                        .build();

        CreateTableRequest createTableRequest =
                CreateTableRequest.builder()
                        .databaseName(databaseName)
                        .tableName(tableName)
                        .retentionProperties(RetentionProperties.builder()
                                .memoryStoreRetentionPeriodInHours(HT_TTL_HOURS)
                                .magneticStoreRetentionPeriodInDays(CT_TTL_DAYS)
                                .build())
                        .magneticStoreWriteProperties(magneticStoreWriteProperties)
                        .build();
        try {
            timestreamWriteClient.createTable(createTableRequest);
            System.out.println("Table [" + tableName + "] successfully created.");
        } catch (ConflictException e) {
            System.out.println("Table [" + tableName + "] exists in database [" + databaseName + "] . Skipping table creation");
        }
    }

    public void deleteTable(String tableName, String databaseName) {
        System.out.println("Deleting table : " + tableName);
        final DeleteTableRequest deleteTableRequest =
                DeleteTableRequest.builder()
                        .databaseName(databaseName)
                        .tableName(tableName)
                        .build();
        try {
            DeleteTableResponse response =
                    timestreamWriteClient.deleteTable(deleteTableRequest);
            System.out.println("Delete table status: " + response.sdkHttpResponse().statusCode());
        } catch (final ResourceNotFoundException e) {
            System.out.println("Table " + tableName + " doesn't exist = " + e);
            //Do not throw exception here, because we want the following cleanups executes
        } catch (final Exception e) {
            System.out.println("Could not delete table " + tableName + " = " + e);
        }
    }

    public void deleteDatabase(String databaseName) {
        System.out.println("Deleting database : " + databaseName);
        final DeleteDatabaseRequest deleteDatabaseRequest = DeleteDatabaseRequest.builder()
                .databaseName(databaseName)
                .build();
        try {
            DeleteDatabaseResponse response =
                    timestreamWriteClient.deleteDatabase(deleteDatabaseRequest);
            System.out.println("Delete database status: " + response.sdkHttpResponse().statusCode());
        } catch (final ResourceNotFoundException e) {
            System.out.println("Database " + databaseName + " doesn't exist = " + e);
            //Do not throw exception here, because we want the following cleanup actions
        } catch (final Exception e) {
            System.out.println("Could not delete Database " + databaseName + " = " + e);
            //Do not throw exception here, because we want the following cleanup actions
        }
    }

    public void createDatabase() {
        createDatabase(DATABASE_NAME);
    }

    public void describeDatabase() {
        System.out.println("Describing database");
        final DescribeDatabaseRequest describeDatabaseRequest = DescribeDatabaseRequest.builder()
                .databaseName(DATABASE_NAME).build();
        try {
            DescribeDatabaseResponse response = timestreamWriteClient.describeDatabase(describeDatabaseRequest);
            final Database databaseRecord = response.database();
            final String databaseId = databaseRecord.arn();
            System.out.println("Database " + DATABASE_NAME + " has id " + databaseId);
        } catch (final Exception e) {
            System.out.println("Database doesn't exist = " + e);
            throw e;
        }
    }

    public void listDatabases() {
        System.out.println("Listing databases");
        ListDatabasesRequest request = ListDatabasesRequest.builder().maxResults(2).build();
        ListDatabasesIterable listDatabasesIterable = timestreamWriteClient.listDatabasesPaginator(request);
        for(ListDatabasesResponse listDatabasesResponse : listDatabasesIterable) {
            final List<Database> databases = listDatabasesResponse.databases();
            databases.forEach(database -> System.out.println(database.databaseName()));
        }
    }

    public void updateDatabase(String kmsKeyId) {

        if (kmsKeyId == null) {
            System.out.println("Skipping UpdateDatabase because KmsKeyId was not given");
            return;
        }

        System.out.println("Updating database");

        UpdateDatabaseRequest request = UpdateDatabaseRequest.builder()
                .databaseName(DATABASE_NAME)
                .kmsKeyId(kmsKeyId)
                .build();
        try {
            timestreamWriteClient.updateDatabase(request);
            System.out.println("Database [" + DATABASE_NAME + "] updated successfully with kmsKeyId " + kmsKeyId);
        } catch (ResourceNotFoundException e) {
            System.out.println("Database [" + DATABASE_NAME + "] does not exist. Skipping UpdateDatabase");
        } catch (Exception e) {
            System.out.println("UpdateDatabase failed: " + e);
        }
    }

    public void createTable() {
        createTable(DATABASE_NAME, TABLE_NAME);
    }

    public void updateTable() {
        System.out.println("Updating table");

        final RetentionProperties retentionProperties = RetentionProperties.builder()
                .memoryStoreRetentionPeriodInHours(HT_TTL_HOURS)
                .magneticStoreRetentionPeriodInDays(CT_TTL_DAYS).build();
        final UpdateTableRequest updateTableRequest = UpdateTableRequest.builder()
                .databaseName(DATABASE_NAME).tableName(TABLE_NAME).retentionProperties(retentionProperties).build();

        timestreamWriteClient.updateTable(updateTableRequest);
        System.out.println("Table updated");
    }

    public void describeTable() {
        System.out.println("Describing table");
        final DescribeTableRequest describeTableRequest = DescribeTableRequest.builder()
                .databaseName(DATABASE_NAME).tableName(TABLE_NAME).build();
        try {
            DescribeTableResponse response = timestreamWriteClient.describeTable(describeTableRequest);
            String tableId = response.table().arn();
            System.out.println("Table " + TABLE_NAME + " has id " + tableId);
        } catch (final Exception e) {
            System.out.println("Table " + TABLE_NAME + " doesn't exist = " + e);
            throw e;
        }
    }

    public void listTables() {
        System.out.println("Listing tables");
        ListTablesRequest request = ListTablesRequest.builder().databaseName(DATABASE_NAME).maxResults(2).build();
        ListTablesIterable listTablesIterable = timestreamWriteClient.listTablesPaginator(request);
        for(ListTablesResponse listTablesResponse : listTablesIterable) {
            final List<Table> tables = listTablesResponse.tables();
            tables.forEach(table -> System.out.println(table.tableName()));
        }
    }

    public void deleteTable() {
        deleteTable(TABLE_NAME, DATABASE_NAME);
    }

    public void deleteDatabase() {
        deleteDatabase(DATABASE_NAME);
    }

    private void printRejectedRecordsException(RejectedRecordsException e) {
        System.out.println("RejectedRecords: " + e);
        e.rejectedRecords().forEach(System.out::println);
    }
}
