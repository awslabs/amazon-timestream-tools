package com.amazonaws.services.timestream;

import java.util.ArrayList;
import java.util.List;

import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient;
import software.amazon.awssdk.services.timestreamwrite.model.Dimension;
import software.amazon.awssdk.services.timestreamwrite.model.MeasureValue;
import software.amazon.awssdk.services.timestreamwrite.model.MeasureValueType;
import software.amazon.awssdk.services.timestreamwrite.model.Record;
import software.amazon.awssdk.services.timestreamwrite.model.RejectedRecordsException;
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsRequest;
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsResponse;

import static com.amazonaws.services.timestream.Main.DATABASE_NAME;
import static com.amazonaws.services.timestream.Main.TABLE_NAME;


public class MultiValueAttributeExample {

    TimestreamWriteClient timestreamWriteClient;

    public MultiValueAttributeExample(TimestreamWriteClient client) {
        this.timestreamWriteClient = client;
    }

    public void writeRecordsMultiMeasureValueSingleRecord() {
        System.out.println("Writing records with multi value attributes");

        List<Record> records = new ArrayList<>();
        final long time = System.currentTimeMillis();
        long version = System.currentTimeMillis();

        List<Dimension> dimensions = new ArrayList<>();
        final Dimension region =
                Dimension.builder().name("region").value("us-east-1").build();
        final Dimension az = Dimension.builder().name("az").value("az1").build();
        final Dimension hostname =
                Dimension.builder().name("hostname").value("host1").build();

        dimensions.add(region);
        dimensions.add(az);
        dimensions.add(hostname);

        Record commonAttributes = Record.builder()
                .dimensions(dimensions)
                .time(String.valueOf(time))
                .version(version)
                .build();

        MeasureValue cpuUtilization = MeasureValue.builder()
                .name("cpu_utilization")
                .type(MeasureValueType.DOUBLE)
                .value("13.5").build();
        MeasureValue memoryUtilization = MeasureValue.builder()
                .name("memory_utilization")
                .type(MeasureValueType.DOUBLE)
                .value("40").build();
        Record computationalResources = Record
                .builder()
                .measureName("cpu_memory")
                .measureValues(cpuUtilization, memoryUtilization)
                .measureValueType(MeasureValueType.MULTI)
                .build();

        records.add(computationalResources);

        WriteRecordsRequest writeRecordsRequest = WriteRecordsRequest.builder()
                .databaseName(DATABASE_NAME)
                .tableName(TABLE_NAME)
                .commonAttributes(commonAttributes)
                .records(records).build();

        // write records for first time
        try {
            WriteRecordsResponse writeRecordsResponse = timestreamWriteClient.writeRecords(writeRecordsRequest);
            System.out.println(
                    "WriteRecords Status for multi value attributes: " + writeRecordsResponse
                            .sdkHttpResponse()
                            .statusCode());
        } catch (RejectedRecordsException e) {
            printRejectedRecordsException(e);
        } catch (Exception e) {
            System.out.println("Error: " + e);
        }
    }

    public void writeRecordsMultiMeasureValueMultipleRecords() {
        System.out.println(
                "Writing records with multi value attributes mixture type");

        List<Record> records = new ArrayList<>();
        final long time = System.currentTimeMillis();
        long version = System.currentTimeMillis();

        List<Dimension> dimensions = new ArrayList<>();
        final Dimension region =
                Dimension.builder().name("region").value("us-east-1").build();
        final Dimension az = Dimension.builder().name("az").value("az1").build();
        final Dimension hostname =
                Dimension.builder().name("hostname").value("host1").build();

        dimensions.add(region);
        dimensions.add(az);
        dimensions.add(hostname);

        Record commonAttributes = Record.builder()
                .dimensions(dimensions)
                .time(String.valueOf(time))
                .version(version)
                .build();

        MeasureValue cpuUtilization = MeasureValue.builder()
                .name("cpu_utilization")
                .type(MeasureValueType.DOUBLE)
                .value("13.5").build();
        MeasureValue memoryUtilization = MeasureValue.builder()
                .name("memory_utilization")
                .type(MeasureValueType.DOUBLE)
                .value("40").build();
        MeasureValue activeCores = MeasureValue.builder()
                .name("active_cores")
                .type(MeasureValueType.BIGINT)
                .value("4").build();


        Record computationalResources = Record
                .builder()
                .measureName("computational_utilization")
                .measureValues(cpuUtilization, memoryUtilization, activeCores)
                .measureValueType(MeasureValueType.MULTI)
                .build();

        records.add(computationalResources);

        WriteRecordsRequest writeRecordsRequest = WriteRecordsRequest.builder()
                .databaseName(DATABASE_NAME)
                .tableName(TABLE_NAME)
                .commonAttributes(commonAttributes)
                .records(records).build();

        // write records for first time
        try {
            WriteRecordsResponse writeRecordsResponse = timestreamWriteClient.writeRecords(writeRecordsRequest);
            System.out.println(
                    "WriteRecords Status for multi value attributes: " + writeRecordsResponse
                            .sdkHttpResponse()
                            .statusCode());
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
}
