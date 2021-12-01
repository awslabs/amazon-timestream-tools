package com.amazonaws.services.timestream;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.timestreamwrite.AmazonTimestreamWrite;
import com.amazonaws.services.timestreamwrite.model.ConflictException;
import com.amazonaws.services.timestreamwrite.model.CreateDatabaseRequest;
import com.amazonaws.services.timestreamwrite.model.CreateTableRequest;
import com.amazonaws.services.timestreamwrite.model.Database;
import com.amazonaws.services.timestreamwrite.model.DeleteDatabaseRequest;
import com.amazonaws.services.timestreamwrite.model.DeleteDatabaseResult;
import com.amazonaws.services.timestreamwrite.model.DeleteTableRequest;
import com.amazonaws.services.timestreamwrite.model.DeleteTableResult;
import com.amazonaws.services.timestreamwrite.model.DescribeDatabaseRequest;
import com.amazonaws.services.timestreamwrite.model.DescribeDatabaseResult;
import com.amazonaws.services.timestreamwrite.model.DescribeTableRequest;
import com.amazonaws.services.timestreamwrite.model.DescribeTableResult;
import com.amazonaws.services.timestreamwrite.model.Dimension;
import com.amazonaws.services.timestreamwrite.model.ListDatabasesRequest;
import com.amazonaws.services.timestreamwrite.model.ListDatabasesResult;
import com.amazonaws.services.timestreamwrite.model.ListTablesRequest;
import com.amazonaws.services.timestreamwrite.model.ListTablesResult;
import com.amazonaws.services.timestreamwrite.model.MeasureValueType;
import com.amazonaws.services.timestreamwrite.model.Record;
import com.amazonaws.services.timestreamwrite.model.RejectedRecordsException;
import com.amazonaws.services.timestreamwrite.model.ResourceNotFoundException;
import com.amazonaws.services.timestreamwrite.model.RetentionProperties;
import com.amazonaws.services.timestreamwrite.model.MagneticStoreRejectedDataLocation;
import com.amazonaws.services.timestreamwrite.model.MagneticStoreWriteProperties;
import com.amazonaws.services.timestreamwrite.model.S3Configuration;
import com.amazonaws.services.timestreamwrite.model.S3EncryptionOption;
import com.amazonaws.services.timestreamwrite.model.Table;
import com.amazonaws.services.timestreamwrite.model.UpdateDatabaseRequest;
import com.amazonaws.services.timestreamwrite.model.UpdateDatabaseResult;
import com.amazonaws.services.timestreamwrite.model.UpdateTableRequest;
import com.amazonaws.services.timestreamwrite.model.ValidationException;
import com.amazonaws.services.timestreamwrite.model.WriteRecordsRequest;
import com.amazonaws.services.timestreamwrite.model.WriteRecordsResult;

import static com.amazonaws.services.timestream.Main.DATABASE_NAME;
import static com.amazonaws.services.timestream.Main.TABLE_NAME;

public class CrudAndSimpleIngestionExample {
    public static final long HT_TTL_HOURS = 24L;
    public static final long CT_TTL_DAYS = 7L;

    AmazonTimestreamWrite amazonTimestreamWrite;
    TimestreamDependencyHelper timestreamDependencyHelper;

    public CrudAndSimpleIngestionExample(AmazonTimestreamWrite client, TimestreamDependencyHelper timestreamDependencyHelper) {
        this.amazonTimestreamWrite = client;
        this.timestreamDependencyHelper = timestreamDependencyHelper;
    }

    public void createDatabase(String databaseName) {
        System.out.println("Creating database");
        CreateDatabaseRequest request = new CreateDatabaseRequest();
        request.setDatabaseName(databaseName);
        try {
            amazonTimestreamWrite.createDatabase(request);
            System.out.println("Database [" + databaseName + "] created successfully");
        } catch (ConflictException e) {
            System.out.println("Database [" + databaseName + "] exists. Skipping database creation");
            //We do not throw exception here, we use the existed database instead
        }
    }

    public void describeDatabase() {
        System.out.println("Describing database");
        final DescribeDatabaseRequest describeDatabaseRequest = new DescribeDatabaseRequest();
        describeDatabaseRequest.setDatabaseName(DATABASE_NAME);
        try {
            DescribeDatabaseResult result = amazonTimestreamWrite.describeDatabase(describeDatabaseRequest);
            final Database databaseRecord = result.getDatabase();
            final String databaseId = databaseRecord.getArn();
            System.out.println("Database " + DATABASE_NAME + " has id " + databaseId);
        } catch (final Exception e) {
            System.out.println("Database doesn't exist = " + e);
            throw e;
        }
    }

    public void listDatabases() {
        System.out.println("Listing databases");
        ListDatabasesRequest request = new ListDatabasesRequest();
        ListDatabasesResult result = amazonTimestreamWrite.listDatabases(request);
        final List<Database> databases = result.getDatabases();
        printDatabases(databases);

        String nextToken = result.getNextToken();
        while (nextToken != null && !nextToken.isEmpty()) {
            request.setNextToken(nextToken);
            ListDatabasesResult nextResult = amazonTimestreamWrite.listDatabases(request);
            final List<Database> nextDatabases = nextResult.getDatabases();
            printDatabases(nextDatabases);
            nextToken = nextResult.getNextToken();
        }
    }

    private void printDatabases(List<Database> databases) {
        for (Database db : databases) {
            System.out.println(db.getDatabaseName());
        }
    }

    public void createTable(String databaseName, String tableName, String magneticStoreRejectedBucketName) {
        System.out.println("Creating table");
        CreateTableRequest createTableRequest = new CreateTableRequest();
        createTableRequest.setDatabaseName(databaseName);
        createTableRequest.setTableName(tableName);
        final RetentionProperties retentionProperties = new RetentionProperties()
                .withMemoryStoreRetentionPeriodInHours(HT_TTL_HOURS)
                .withMagneticStoreRetentionPeriodInDays(CT_TTL_DAYS);
        createTableRequest.setRetentionProperties(retentionProperties);
        //Set Magnetic Store Write Properties
        final MagneticStoreRejectedDataLocation magneticStoreRejectedDataLocation = new MagneticStoreRejectedDataLocation()
                .withS3Configuration(new S3Configuration().withBucketName(magneticStoreRejectedBucketName)
                        .withEncryptionOption(S3EncryptionOption.SSE_S3));
        final MagneticStoreWriteProperties magneticStoreWriteProperties = new MagneticStoreWriteProperties()
                .withEnableMagneticStoreWrites(true)
                .withMagneticStoreRejectedDataLocation(magneticStoreRejectedDataLocation);
        createTableRequest.setMagneticStoreWriteProperties(magneticStoreWriteProperties);
        try {
            amazonTimestreamWrite.createTable(createTableRequest);
            System.out.println("Table [" + tableName + "] successfully created.");
        } catch (ConflictException e) {
            System.out.println("Table [" + tableName + "] exists on database [" + databaseName + "] . Skipping table creation");
            //We do not throw exception here, we use the existed table instead
        }
    }

    public void updateTable() {
        System.out.println("Updating table");
        UpdateTableRequest updateTableRequest = new UpdateTableRequest();
        updateTableRequest.setDatabaseName(DATABASE_NAME);
        updateTableRequest.setTableName(TABLE_NAME);

        final RetentionProperties retentionProperties = new RetentionProperties()
                .withMemoryStoreRetentionPeriodInHours(HT_TTL_HOURS)
                .withMagneticStoreRetentionPeriodInDays(CT_TTL_DAYS);

        updateTableRequest.setRetentionProperties(retentionProperties);

        amazonTimestreamWrite.updateTable(updateTableRequest);
        System.out.println("Table updated");
    }

    public void describeTable() {
        System.out.println("Describing table");
        final DescribeTableRequest describeTableRequest = new DescribeTableRequest();
        describeTableRequest.setDatabaseName(DATABASE_NAME);
        describeTableRequest.setTableName(TABLE_NAME);
        try {
            DescribeTableResult result = amazonTimestreamWrite.describeTable(describeTableRequest);
            String tableId = result.getTable().getArn();
            System.out.println("Table " + TABLE_NAME + " has id " + tableId);
        } catch (final Exception e) {
            System.out.println("Table " + TABLE_NAME + " doesn't exist = " + e);
            throw e;
        }
    }

    public void listTables() {
        System.out.println("Listing tables");
        ListTablesRequest request = new ListTablesRequest();
        request.setDatabaseName(DATABASE_NAME);
        ListTablesResult result = amazonTimestreamWrite.listTables(request);
        printTables(result.getTables());

        String nextToken = result.getNextToken();
        while (nextToken != null && !nextToken.isEmpty()) {
            request.setNextToken(nextToken);
            ListTablesResult nextResult = amazonTimestreamWrite.listTables(request);

            printTables(nextResult.getTables());
            nextToken = nextResult.getNextToken();
        }
    }

    private void printTables(List<Table> tables) {
        for (Table table : tables) {
            System.out.println(table.getTableName());
        }
    }

    public void deleteTable(String tableName, String databaseName) {
        System.out.println("Deleting table : " + tableName);
        final DeleteTableRequest deleteTableRequest = new DeleteTableRequest();
        deleteTableRequest.setDatabaseName(databaseName);
        deleteTableRequest.setTableName(tableName);
        try {
            DeleteTableResult result =
                    amazonTimestreamWrite.deleteTable(deleteTableRequest);
            System.out.println("Delete table status: " + result.getSdkHttpMetadata().getHttpStatusCode());
        } catch (final ResourceNotFoundException e) {
            System.out.println("Table " + tableName + " doesn't exist = " + e);
            //Do not throw exception here, because we want the following cleanups executes
        } catch (final Exception e) {
            System.out.println("Could not delete table " + tableName + " = " + e);
            //Do not throw exception here, because we want the following cleanups executes
        }
    }

    public void deleteDatabase(String databaseName) {
        System.out.println("Deleting database : " + databaseName);
        final DeleteDatabaseRequest deleteDatabaseRequest = new DeleteDatabaseRequest();
        deleteDatabaseRequest.setDatabaseName(databaseName);
        try {
            DeleteDatabaseResult result =
                    amazonTimestreamWrite.deleteDatabase(deleteDatabaseRequest);
            System.out.println("Delete database status: " + result.getSdkHttpMetadata().getHttpStatusCode());
        } catch (final ResourceNotFoundException e) {
            System.out.println("Database " + databaseName + " doesn't exist = " + e);
            //Do not throw exception here, because we want the following cleanups executes
        } catch (final Exception e) {
            System.out.println("Could not delete Database " + databaseName + " = " + e);
            //Do not throw exception here, because we want the following cleanups executes
        }
    }

    public void updateDatabase(String kmsId) {
        if (kmsId == null) {
            System.out.println("Skipping UpdateDatabase because KmsKeyId was not given");
            return;
        }

        System.out.println("Updating database with kmsId " + kmsId);
        UpdateDatabaseRequest request = new UpdateDatabaseRequest();
        request.setDatabaseName(DATABASE_NAME);
        request.setKmsKeyId(kmsId);
        try {
            UpdateDatabaseResult result = amazonTimestreamWrite.updateDatabase(request);
            System.out.println("Update Database complete");
        } catch (final ValidationException e) {
            System.out.println("Update database failed:");
            e.printStackTrace();
        } catch (final ResourceNotFoundException e) {
            System.out.println("Database " + DATABASE_NAME + " doesn't exist = " + e);
        } catch (final Exception e) {
            System.out.println("Could not update Database " + DATABASE_NAME + " = " + e);
            throw e;
        }
    }

    private void printRejectedRecordsException(RejectedRecordsException e) {
        System.out.println("RejectedRecords: " + e);
        e.getRejectedRecords().forEach(System.out::println);
    }
}
