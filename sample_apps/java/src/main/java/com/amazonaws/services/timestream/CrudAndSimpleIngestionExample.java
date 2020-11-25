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
import com.amazonaws.services.timestreamwrite.model.RejectedRecord;
import com.amazonaws.services.timestreamwrite.model.RejectedRecordsException;
import com.amazonaws.services.timestreamwrite.model.ResourceNotFoundException;
import com.amazonaws.services.timestreamwrite.model.RetentionProperties;
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

    public CrudAndSimpleIngestionExample(AmazonTimestreamWrite client) {
        this.amazonTimestreamWrite = client;
    }

    public void createDatabase() {
        System.out.println("Creating database");
        CreateDatabaseRequest request = new CreateDatabaseRequest();
        request.setDatabaseName(DATABASE_NAME);
        try {
            amazonTimestreamWrite.createDatabase(request);
            System.out.println("Database [" + DATABASE_NAME + "] created successfully");
        } catch (ConflictException e) {
            System.out.println("Database [" + DATABASE_NAME + "] exists. Skipping database creation");
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

    public void createTable() {
        System.out.println("Creating table");
        CreateTableRequest createTableRequest = new CreateTableRequest();
        createTableRequest.setDatabaseName(DATABASE_NAME);
        createTableRequest.setTableName(TABLE_NAME);
        final RetentionProperties retentionProperties = new RetentionProperties()
                .withMemoryStoreRetentionPeriodInHours(HT_TTL_HOURS)
                .withMagneticStoreRetentionPeriodInDays(CT_TTL_DAYS);
        createTableRequest.setRetentionProperties(retentionProperties);

        try {
            amazonTimestreamWrite.createTable(createTableRequest);
            System.out.println("Table [" + TABLE_NAME + "] successfully created.");
        } catch (ConflictException e) {
            System.out.println("Table [" + TABLE_NAME + "] exists on database [" + DATABASE_NAME + "] . Skipping database creation");
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

    public void writeRecords() {
        System.out.println("Writing records");
        // Specify repeated values for all records
        List<Record> records = new ArrayList<>();
        final long time = System.currentTimeMillis();

        List<Dimension> dimensions = new ArrayList<>();
        final Dimension region = new Dimension().withName("region").withValue("us-east-1");
        final Dimension az = new Dimension().withName("az").withValue("az1");
        final Dimension hostname = new Dimension().withName("hostname").withValue("host1");

        dimensions.add(region);
        dimensions.add(az);
        dimensions.add(hostname);

        Record cpuUtilization = new Record()
                .withDimensions(dimensions)
                .withMeasureName("cpu_utilization")
                .withMeasureValue("13.5")
                .withMeasureValueType(MeasureValueType.DOUBLE)
                .withTime(String.valueOf(time));
        Record memoryUtilization = new Record()
                .withDimensions(dimensions)
                .withMeasureName("memory_utilization")
                .withMeasureValue("40")
                .withMeasureValueType(MeasureValueType.DOUBLE)
                .withTime(String.valueOf(time));

        records.add(cpuUtilization);
        records.add(memoryUtilization);

        WriteRecordsRequest writeRecordsRequest = new WriteRecordsRequest()
                .withDatabaseName(DATABASE_NAME)
                .withTableName(TABLE_NAME)
                .withRecords(records);

        try {
            WriteRecordsResult writeRecordsResult = amazonTimestreamWrite.writeRecords(writeRecordsRequest);
            System.out.println("WriteRecords Status: " + writeRecordsResult.getSdkHttpMetadata().getHttpStatusCode());
        } catch (RejectedRecordsException e) {
            printRejectedRecordsException(e);
        } catch (Exception e) {
            System.out.println("Error: " + e);
        }
    }

    public void writeRecordsWithCommonAttributes() {
        System.out.println("Writing records with extracting common attributes");
        // Specify repeated values for all records
        List<Record> records = new ArrayList<>();
        final long time = System.currentTimeMillis();

        List<Dimension> dimensions = new ArrayList<>();
        final Dimension region = new Dimension().withName("region").withValue("us-east-1");
        final Dimension az = new Dimension().withName("az").withValue("az1");
        final Dimension hostname = new Dimension().withName("hostname").withValue("host1");

        dimensions.add(region);
        dimensions.add(az);
        dimensions.add(hostname);

        Record commonAttributes = new Record()
                .withDimensions(dimensions)
                .withMeasureValueType(MeasureValueType.DOUBLE)
                .withTime(String.valueOf(time));

        Record cpuUtilization = new Record()
                .withMeasureName("cpu_utilization")
                .withMeasureValue("13.5");
        Record memoryUtilization = new Record()
                .withMeasureName("memory_utilization")
                .withMeasureValue("40");

        records.add(cpuUtilization);
        records.add(memoryUtilization);

        WriteRecordsRequest writeRecordsRequest = new WriteRecordsRequest()
                .withDatabaseName(DATABASE_NAME)
                .withTableName(TABLE_NAME)
                .withCommonAttributes(commonAttributes);
        writeRecordsRequest.setRecords(records);

        try {
            WriteRecordsResult writeRecordsResult = amazonTimestreamWrite.writeRecords(writeRecordsRequest);
            System.out.println("writeRecordsWithCommonAttributes Status: " + writeRecordsResult.getSdkHttpMetadata().getHttpStatusCode());
        } catch (RejectedRecordsException e) {
            printRejectedRecordsException(e);
        } catch (Exception e) {
            System.out.println("Error: " + e);
        }
    }

    public void writeRecordsWithUpsert() {
        System.out.println("Writing records with upsert");
        // Specify repeated values for all records
        List<Record> records = new ArrayList<>();
        final long time = System.currentTimeMillis();
        // To achieve upsert (last writer wins) semantic, one example is to use current time as the version if you are writing directly from the data source
        long version = System.currentTimeMillis();

        List<Dimension> dimensions = new ArrayList<>();
        final Dimension region = new Dimension().withName("region").withValue("us-east-1");
        final Dimension az = new Dimension().withName("az").withValue("az1");
        final Dimension hostname = new Dimension().withName("hostname").withValue("host1");

        dimensions.add(region);
        dimensions.add(az);
        dimensions.add(hostname);

        Record commonAttributes = new Record()
                .withDimensions(dimensions)
                .withMeasureValueType(MeasureValueType.DOUBLE)
                .withTime(String.valueOf(time))
                .withVersion(version);

        Record cpuUtilization = new Record()
                .withMeasureName("cpu_utilization")
                .withMeasureValue("13.5");
        Record memoryUtilization = new Record()
                .withMeasureName("memory_utilization")
                .withMeasureValue("40");

        records.add(cpuUtilization);
        records.add(memoryUtilization);

        WriteRecordsRequest writeRecordsRequest = new WriteRecordsRequest()
                .withDatabaseName(DATABASE_NAME)
                .withTableName(TABLE_NAME)
                .withCommonAttributes(commonAttributes);
        writeRecordsRequest.setRecords(records);

        // write records for first time
        try {
            WriteRecordsResult writeRecordsResult = amazonTimestreamWrite.writeRecords(writeRecordsRequest);
            System.out.println("WriteRecords Status for first time: " + writeRecordsResult.getSdkHttpMetadata().getHttpStatusCode());
        } catch (RejectedRecordsException e) {
            printRejectedRecordsException(e);
        } catch (Exception e) {
            System.out.println("Error: " + e);
        }

        // Successfully retry same writeRecordsRequest with same records and versions, because writeRecords API is idempotent.
        try {
            WriteRecordsResult writeRecordsResult = amazonTimestreamWrite.writeRecords(writeRecordsRequest);
            System.out.println("WriteRecords Status for retry: " + writeRecordsResult.getSdkHttpMetadata().getHttpStatusCode());
        } catch (RejectedRecordsException e) {
            printRejectedRecordsException(e);
        } catch (Exception e) {
            System.out.println("Error: " + e);
        }

        // upsert with lower version, this would fail because a higher version is required to update the measure value.
        version -= 1;
        commonAttributes.setVersion(version);

        cpuUtilization.setMeasureValue("14.5");
        memoryUtilization.setMeasureValue("50");

        List<Record> upsertedRecords = new ArrayList<>();
        upsertedRecords.add(cpuUtilization);
        upsertedRecords.add(memoryUtilization);

        WriteRecordsRequest writeRecordsUpsertRequest = new WriteRecordsRequest()
                .withDatabaseName(DATABASE_NAME)
                .withTableName(TABLE_NAME)
                .withCommonAttributes(commonAttributes);
        writeRecordsUpsertRequest.setRecords(upsertedRecords);

        try {
            WriteRecordsResult writeRecordsUpsertResult = amazonTimestreamWrite.writeRecords(writeRecordsUpsertRequest);
            System.out.println("WriteRecords Status for upsert with lower version: " + writeRecordsUpsertResult.getSdkHttpMetadata().getHttpStatusCode());
        } catch (RejectedRecordsException e) {
            System.out.println("WriteRecords Status for upsert with lower version: ");
            printRejectedRecordsException(e);
        } catch (Exception e) {
            System.out.println("Error: " + e);
        }

        // upsert with higher version as new data is generated
        version = System.currentTimeMillis();
        commonAttributes.setVersion(version);

        writeRecordsUpsertRequest = new WriteRecordsRequest()
                .withDatabaseName(DATABASE_NAME)
                .withTableName(TABLE_NAME)
                .withCommonAttributes(commonAttributes);
        writeRecordsUpsertRequest.setRecords(upsertedRecords);

        try {
            WriteRecordsResult writeRecordsUpsertResult = amazonTimestreamWrite.writeRecords(writeRecordsUpsertRequest);
            System.out.println("WriteRecords Status for upsert with higher version: " + writeRecordsUpsertResult.getSdkHttpMetadata().getHttpStatusCode());
        } catch (RejectedRecordsException e) {
            printRejectedRecordsException(e);
        } catch (Exception e) {
            System.out.println("Error: " + e);
        }
    }

    public void deleteTable() {
        System.out.println("Deleting table");
        final DeleteTableRequest deleteTableRequest = new DeleteTableRequest();
        deleteTableRequest.setDatabaseName(DATABASE_NAME);
        deleteTableRequest.setTableName(TABLE_NAME);
        try {
            DeleteTableResult result =
                    amazonTimestreamWrite.deleteTable(deleteTableRequest);
            System.out.println("Delete table status: " + result.getSdkHttpMetadata().getHttpStatusCode());
        } catch (final ResourceNotFoundException e) {
            System.out.println("Table " + TABLE_NAME + " doesn't exist = " + e);
            throw e;
        } catch (final Exception e) {
            System.out.println("Could not delete table " + TABLE_NAME + " = " + e);
            throw e;
        }
    }

    public void deleteDatabase() {
        System.out.println("Deleting database");
        final DeleteDatabaseRequest deleteDatabaseRequest = new DeleteDatabaseRequest();
        deleteDatabaseRequest.setDatabaseName(DATABASE_NAME);
        try {
            DeleteDatabaseResult result =
                    amazonTimestreamWrite.deleteDatabase(deleteDatabaseRequest);
            System.out.println("Delete database status: " + result.getSdkHttpMetadata().getHttpStatusCode());
        } catch (final ResourceNotFoundException e) {
            System.out.println("Database " + DATABASE_NAME + " doesn't exist = " + e);
            throw e;
        } catch (final Exception e) {
            System.out.println("Could not delete Database " + DATABASE_NAME + " = " + e);
            throw e;
        }
    }

    public void updateDatabase(String kmsId) {
        System.out.println("Updating kmsId to " + kmsId);
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
