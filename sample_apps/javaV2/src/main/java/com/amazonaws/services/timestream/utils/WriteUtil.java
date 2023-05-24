package com.amazonaws.services.timestream.utils;

import java.util.List;

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
import software.amazon.awssdk.services.timestreamwrite.model.ListDatabasesRequest;
import software.amazon.awssdk.services.timestreamwrite.model.ListDatabasesResponse;
import software.amazon.awssdk.services.timestreamwrite.model.ListTablesRequest;
import software.amazon.awssdk.services.timestreamwrite.model.ListTablesResponse;
import software.amazon.awssdk.services.timestreamwrite.model.Record;
import software.amazon.awssdk.services.timestreamwrite.model.ResourceNotFoundException;
import software.amazon.awssdk.services.timestreamwrite.model.RetentionProperties;
import software.amazon.awssdk.services.timestreamwrite.model.Table;
import software.amazon.awssdk.services.timestreamwrite.model.UpdateDatabaseRequest;
import software.amazon.awssdk.services.timestreamwrite.model.UpdateTableRequest;
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsRequest;
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsResponse;
import software.amazon.awssdk.services.timestreamwrite.paginators.ListDatabasesIterable;
import software.amazon.awssdk.services.timestreamwrite.paginators.ListTablesIterable;

import static com.amazonaws.services.timestream.utils.Constants.CT_TTL_DAYS;
import static com.amazonaws.services.timestream.utils.Constants.HT_TTL_HOURS;

public class WriteUtil {
    TimestreamWriteClient timestreamWriteClient;

    public WriteUtil(TimestreamWriteClient timestreamWriteClient) {
        this.timestreamWriteClient = timestreamWriteClient;
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

    public void describeDatabase(String databaseName) {
        System.out.println("Describing database");
        final DescribeDatabaseRequest describeDatabaseRequest = DescribeDatabaseRequest.builder()
                .databaseName(databaseName).build();
        try {
            DescribeDatabaseResponse response = timestreamWriteClient.describeDatabase(describeDatabaseRequest);
            final Database databaseRecord = response.database();
            final String databaseId = databaseRecord.arn();
            System.out.println("Database " + databaseName + " has id " + databaseId);
        } catch (final Exception e) {
            System.out.println("Database doesn't exist = " + e);
            throw e;
        }
    }

    public void listDatabases() {
        System.out.println("Listing databases");
        ListDatabasesRequest request = ListDatabasesRequest.builder().maxResults(2).build();
        ListDatabasesIterable listDatabasesIterable = timestreamWriteClient.listDatabasesPaginator(request);
        for (ListDatabasesResponse listDatabasesResponse : listDatabasesIterable) {
            final List<Database> databases = listDatabasesResponse.databases();
            databases.forEach(database -> System.out.println(database.databaseName()));
        }
    }

    public void updateDatabase(String databaseName, String kmsKeyId) {

        if (kmsKeyId == null) {
            System.out.println("Skipping UpdateDatabase because KmsKeyId was not given");
            return;
        }

        System.out.println("Updating database");

        UpdateDatabaseRequest request = UpdateDatabaseRequest.builder()
                .databaseName(databaseName)
                .kmsKeyId(kmsKeyId)
                .build();
        try {
            timestreamWriteClient.updateDatabase(request);
            System.out.println("Database [" + databaseName + "] updated successfully with kmsKeyId " + kmsKeyId);
        } catch (ResourceNotFoundException e) {
            System.out.println("Database [" + databaseName + "] does not exist. Skipping UpdateDatabase");
        } catch (Exception e) {
            System.out.println("UpdateDatabase failed: " + e);
        }
    }

    public void createTable(String databaseName, String tableName) {
        System.out.println("Creating table");

        final RetentionProperties retentionProperties = RetentionProperties.builder()
                .memoryStoreRetentionPeriodInHours(HT_TTL_HOURS)
                .magneticStoreRetentionPeriodInDays(CT_TTL_DAYS).build();
        final CreateTableRequest createTableRequest = CreateTableRequest.builder()
                .databaseName(databaseName).tableName(tableName).retentionProperties(retentionProperties).build();

        try {
            timestreamWriteClient.createTable(createTableRequest);
            System.out.println("Table [" + tableName + "] successfully created.");
        } catch (ConflictException e) {
            System.out.println("Table [" + tableName + "] exists on database [" + databaseName + "] . Skipping database creation");
        }
    }

    public void updateTable(String databaseName, String tableName) {
        System.out.println("Updating table");

        final RetentionProperties retentionProperties = RetentionProperties.builder()
                .memoryStoreRetentionPeriodInHours(HT_TTL_HOURS)
                .magneticStoreRetentionPeriodInDays(CT_TTL_DAYS).build();
        final UpdateTableRequest updateTableRequest = UpdateTableRequest.builder()
                .databaseName(databaseName).tableName(tableName).retentionProperties(retentionProperties).build();

        timestreamWriteClient.updateTable(updateTableRequest);
        System.out.println("Table updated");
    }

    public void describeTable(String databaseName, String tableName) {
        System.out.println("Describing table");
        final DescribeTableRequest describeTableRequest = DescribeTableRequest.builder()
                .databaseName(databaseName).tableName(tableName).build();
        try {
            DescribeTableResponse response = timestreamWriteClient.describeTable(describeTableRequest);
            String tableId = response.table().arn();
            System.out.println("Table " + tableName + " has id " + tableId);
        } catch (final Exception e) {
            System.out.println("Table " + tableName + " doesn't exist = " + e);
            throw e;
        }
    }

    public void listTables(String databaseName) {
        System.out.println("Listing tables");
        ListTablesRequest request = ListTablesRequest.builder().databaseName(databaseName).maxResults(2).build();
        ListTablesIterable listTablesIterable = timestreamWriteClient.listTablesPaginator(request);
        for (ListTablesResponse listTablesResponse : listTablesIterable) {
            final List<Table> tables = listTablesResponse.tables();
            tables.forEach(table -> System.out.println(table.tableName()));
        }
    }

    public void deleteTable(String databaseName, String tableName) {
        System.out.println("Deleting table");
        final DeleteTableRequest deleteTableRequest = DeleteTableRequest.builder()
                .databaseName(databaseName).tableName(tableName).build();
        try {
            DeleteTableResponse response =
                    timestreamWriteClient.deleteTable(deleteTableRequest);
            System.out.println("Delete table status: " + response.sdkHttpResponse().statusCode());
        } catch (final ResourceNotFoundException e) {
            System.out.println("Table " + tableName + " doesn't exist = " + e);
            throw e;
        } catch (final Exception e) {
            System.out.println("Could not delete table " + tableName + " = " + e);
            throw e;
        }
    }

    public void deleteDatabase(String databaseName) {
        System.out.println("Deleting database");
        final DeleteDatabaseRequest deleteDatabaseRequest = DeleteDatabaseRequest.builder().databaseName(databaseName).build();
        try {
            DeleteDatabaseResponse response =
                    timestreamWriteClient.deleteDatabase(deleteDatabaseRequest);
            System.out.println("Delete database status: " + response.sdkHttpResponse().statusCode());
        } catch (final ResourceNotFoundException e) {
            System.out.println("Database " + databaseName + " doesn't exist = " + e);
            throw e;
        } catch (final Exception e) {
            System.out.println("Could not delete Database " + databaseName + " = " + e);
            throw e;
        }
    }

    public void submitBatch(String databaseName, String tableName, List<Record> records, int counter) {
        WriteRecordsRequest writeRecordsRequest = WriteRecordsRequest.builder()
                .databaseName(databaseName).tableName(tableName).records(records).build();

        try {
            WriteRecordsResponse writeRecordsResponse = timestreamWriteClient.writeRecords(writeRecordsRequest);
            System.out.println("Processed " + counter + " records. WriteRecords Status: " +
                    writeRecordsResponse.sdkHttpResponse().statusCode());
        } catch (Exception e) {
            System.out.println("Error: " + e);
        }
    }
}
