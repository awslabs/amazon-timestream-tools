package com.amazonaws.services.timestream;

import java.util.ArrayList;
import java.util.List;

import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient;
import software.amazon.awssdk.services.timestreamwrite.model.Dimension;
import software.amazon.awssdk.services.timestreamwrite.model.MeasureValueType;
import software.amazon.awssdk.services.timestreamwrite.model.Record;
import software.amazon.awssdk.services.timestreamwrite.model.RejectedRecordsException;
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsRequest;
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsResponse;

import com.amazonaws.services.timestream.utils.WriteUtil;

import static com.amazonaws.services.timestream.utils.Constants.DATABASE_NAME;
import static com.amazonaws.services.timestream.utils.Constants.TABLE_NAME;

public class CrudAndSimpleIngestionExample {
    private final TimestreamWriteClient timestreamWriteClient;
    private final WriteUtil writeUtil;

    public CrudAndSimpleIngestionExample(TimestreamWriteClient client, WriteUtil writeUtil) {
        this.timestreamWriteClient = client;
        this.writeUtil = writeUtil;
    }

    public void writeRecords() {
        System.out.println("Writing records");
        // Specify repeated values for all records
        List<Record> records = new ArrayList<>();
        final long time = System.currentTimeMillis();

        List<Dimension> dimensions = new ArrayList<>();
        final Dimension region = Dimension.builder().name("region").value("us-east-1").build();
        final Dimension az = Dimension.builder().name("az").value("az1").build();
        final Dimension hostname = Dimension.builder().name("hostname").value("host1").build();

        dimensions.add(region);
        dimensions.add(az);
        dimensions.add(hostname);

        Record cpuUtilization = Record.builder()
                .dimensions(dimensions)
                .measureValueType(MeasureValueType.DOUBLE)
                .measureName("cpu_utilization")
                .measureValue("13.5")
                .time(String.valueOf(time)).build();

        Record memoryUtilization = Record.builder()
                .dimensions(dimensions)
                .measureValueType(MeasureValueType.DOUBLE)
                .measureName("memory_utilization")
                .measureValue("40")
                .time(String.valueOf(time)).build();

        records.add(cpuUtilization);
        records.add(memoryUtilization);

        WriteRecordsRequest writeRecordsRequest = WriteRecordsRequest.builder()
                .databaseName(DATABASE_NAME).tableName(TABLE_NAME).records(records).build();

        try {
            WriteRecordsResponse writeRecordsResponse = timestreamWriteClient.writeRecords(writeRecordsRequest);
            System.out.println("WriteRecords Status: " + writeRecordsResponse.sdkHttpResponse().statusCode());
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
        final Dimension region = Dimension.builder().name("region").value("us-east-1").build();
        final Dimension az = Dimension.builder().name("az").value("az1").build();
        final Dimension hostname = Dimension.builder().name("hostname").value("host1").build();

        dimensions.add(region);
        dimensions.add(az);
        dimensions.add(hostname);

        Record commonAttributes = Record.builder()
                .dimensions(dimensions)
                .measureValueType(MeasureValueType.DOUBLE)
                .time(String.valueOf(time)).build();

        Record cpuUtilization = Record.builder()
                .measureName("cpu_utilization")
                .measureValue("13.5").build();
        Record memoryUtilization = Record.builder()
                .measureName("memory_utilization")
                .measureValue("40").build();

        records.add(cpuUtilization);
        records.add(memoryUtilization);

        WriteRecordsRequest writeRecordsRequest = WriteRecordsRequest.builder()
                .databaseName(DATABASE_NAME)
                .tableName(TABLE_NAME)
                .commonAttributes(commonAttributes)
                .records(records).build();

        try {
            WriteRecordsResponse writeRecordsResponse = timestreamWriteClient.writeRecords(writeRecordsRequest);
            System.out.println("writeRecordsWithCommonAttributes Status: " + writeRecordsResponse.sdkHttpResponse().statusCode());
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
        final Dimension region = Dimension.builder().name("region").value("us-east-1").build();
        final Dimension az = Dimension.builder().name("az").value("az1").build();
        final Dimension hostname = Dimension.builder().name("hostname").value("host1").build();

        dimensions.add(region);
        dimensions.add(az);
        dimensions.add(hostname);

        Record commonAttributes = Record.builder()
                .dimensions(dimensions)
                .measureValueType(MeasureValueType.DOUBLE)
                .time(String.valueOf(time))
                .version(version)
                .build();

        Record cpuUtilization = Record.builder()
                .measureName("cpu_utilization")
                .measureValue("13.5").build();
        Record memoryUtilization = Record.builder()
                .measureName("memory_utilization")
                .measureValue("40").build();

        records.add(cpuUtilization);
        records.add(memoryUtilization);

        WriteRecordsRequest writeRecordsRequest = WriteRecordsRequest.builder()
                .databaseName(DATABASE_NAME)
                .tableName(TABLE_NAME)
                .commonAttributes(commonAttributes)
                .records(records).build();

        // write records for first time
        try {
            WriteRecordsResponse writeRecordsResponse = timestreamWriteClient.writeRecords(writeRecordsRequest);
            System.out.println("WriteRecords Status for first time: " + writeRecordsResponse.sdkHttpResponse().statusCode());
        } catch (RejectedRecordsException e) {
            printRejectedRecordsException(e);
        } catch (Exception e) {
            System.out.println("Error: " + e);
        }

        // Successfully retry same writeRecordsRequest with same records and versions, because writeRecords API is idempotent.
        try {
            WriteRecordsResponse writeRecordsResponse = timestreamWriteClient.writeRecords(writeRecordsRequest);
            System.out.println("WriteRecords Status for retry: " + writeRecordsResponse.sdkHttpResponse().statusCode());
        } catch (RejectedRecordsException e) {
            printRejectedRecordsException(e);
        } catch (Exception e) {
            System.out.println("Error: " + e);
        }

        // upsert with lower version, this would fail because a higher version is required to update the measure value.
        version -= 1;
        commonAttributes = Record.builder()
                .dimensions(dimensions)
                .measureValueType(MeasureValueType.DOUBLE)
                .time(String.valueOf(time))
                .version(version)
                .build();

        cpuUtilization = Record.builder()
                .measureName("cpu_utilization")
                .measureValue("14.5").build();
        memoryUtilization = Record.builder()
                .measureName("memory_utilization")
                .measureValue("50").build();

        List<Record> upsertedRecords = new ArrayList<>();
        upsertedRecords.add(cpuUtilization);
        upsertedRecords.add(memoryUtilization);

        WriteRecordsRequest writeRecordsUpsertRequest = WriteRecordsRequest.builder()
                .databaseName(DATABASE_NAME)
                .tableName(TABLE_NAME)
                .commonAttributes(commonAttributes)
                .records(upsertedRecords).build();

        try {
            WriteRecordsResponse writeRecordsResponse = timestreamWriteClient.writeRecords(writeRecordsUpsertRequest);
            System.out.println("WriteRecords Status for upsert with lower version: " + writeRecordsResponse.sdkHttpResponse().statusCode());
        } catch (RejectedRecordsException e) {
            System.out.println("WriteRecords Status for upsert with lower version: ");
            printRejectedRecordsException(e);
        } catch (Exception e) {
            System.out.println("Error: " + e);
        }

        // upsert with higher version as new data is generated
        version = System.currentTimeMillis();
        commonAttributes = Record.builder()
                .dimensions(dimensions)
                .measureValueType(MeasureValueType.DOUBLE)
                .time(String.valueOf(time))
                .version(version)
                .build();

        writeRecordsUpsertRequest = WriteRecordsRequest.builder()
                .databaseName(DATABASE_NAME)
                .tableName(TABLE_NAME)
                .commonAttributes(commonAttributes)
                .records(upsertedRecords).build();

        try {
            WriteRecordsResponse writeRecordsUpsertResponse = timestreamWriteClient.writeRecords(writeRecordsUpsertRequest);
            System.out.println("WriteRecords Status for upsert with higher version: " + writeRecordsUpsertResponse.sdkHttpResponse().statusCode());
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
