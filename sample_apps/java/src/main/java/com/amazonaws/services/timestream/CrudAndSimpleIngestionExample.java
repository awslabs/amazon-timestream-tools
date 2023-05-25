package com.amazonaws.services.timestream;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.timestream.utils.WriteUtil;
import com.amazonaws.services.timestreamwrite.AmazonTimestreamWrite;
import com.amazonaws.services.timestreamwrite.model.Dimension;
import com.amazonaws.services.timestreamwrite.model.MeasureValueType;
import com.amazonaws.services.timestreamwrite.model.Record;
import com.amazonaws.services.timestreamwrite.model.RejectedRecordsException;
import com.amazonaws.services.timestreamwrite.model.WriteRecordsRequest;
import com.amazonaws.services.timestreamwrite.model.WriteRecordsResult;

import static com.amazonaws.services.timestream.utils.Constants.DATABASE_NAME;
import static com.amazonaws.services.timestream.utils.Constants.TABLE_NAME;

public class CrudAndSimpleIngestionExample {
    private final AmazonTimestreamWrite amazonTimestreamWrite;
    private final WriteUtil writeUtil;

    public CrudAndSimpleIngestionExample(final AmazonTimestreamWrite client, final WriteUtil writeUtil) {
        this.amazonTimestreamWrite = client;
        this.writeUtil = writeUtil;
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

    private void printRejectedRecordsException(RejectedRecordsException e) {
        System.out.println("RejectedRecords: " + e);
        e.getRejectedRecords().forEach(System.out::println);
    }

    public void run(InputArguments inputArguments) {
        writeUtil.createDatabase(DATABASE_NAME);
        writeUtil.describeDatabase(DATABASE_NAME);
        if (inputArguments.getKmsId() != null) {
            writeUtil.updateDatabase(DATABASE_NAME, inputArguments.getKmsId());
            writeUtil.describeDatabase(DATABASE_NAME);
        }
        writeUtil.listDatabases();


        writeUtil.createTable(DATABASE_NAME, TABLE_NAME);
        writeUtil.describeTable(DATABASE_NAME, TABLE_NAME);
        writeUtil.listTables(DATABASE_NAME);
        writeUtil.updateTable(DATABASE_NAME, TABLE_NAME);

        // simple record ingestion
        writeRecords();
        writeRecordsWithCommonAttributes();

        // upsert records
        writeRecordsWithUpsert();
    }
}
