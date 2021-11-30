package com.amazonaws.services.timestream;

import static com.amazonaws.services.timestream.Main.DATABASE_NAME;
import static com.amazonaws.services.timestream.Main.REGION;
import static com.amazonaws.services.timestream.Main.TABLE_NAME;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.timestreamwrite.AmazonTimestreamWrite;
import com.amazonaws.services.timestreamwrite.model.Dimension;
import com.amazonaws.services.timestreamwrite.model.MeasureValue;
import com.amazonaws.services.timestreamwrite.model.MeasureValueType;
import com.amazonaws.services.timestreamwrite.model.Record;
import com.amazonaws.services.timestreamwrite.model.RejectedRecordsException;
import com.amazonaws.services.timestreamwrite.model.WriteRecordsRequest;
import com.amazonaws.services.timestreamwrite.model.WriteRecordsResult;


public class MultiValueAttributeExample {
    AmazonTimestreamWrite timestreamWriteClient;

    public MultiValueAttributeExample(AmazonTimestreamWrite client) {
        this.timestreamWriteClient = client;
    }

    public void writeRecordsMultiMeasureValueSingleRecord() {
        System.out.println("Writing records with multi value attributes");

        List<Record> records = new ArrayList<>();
        final long time = System.currentTimeMillis();
        long version = System.currentTimeMillis();

        List<Dimension> dimensions = new ArrayList<>();
        final Dimension region = new Dimension().withName("region").withValue(REGION);
        final Dimension az = new Dimension().withName("az").withValue("az1");
        final Dimension hostname = new Dimension().withName("hostname").withValue("host1");

        dimensions.add(region);
        dimensions.add(az);
        dimensions.add(hostname);

        Record commonAttributes = new Record()
                .withDimensions(dimensions)
                .withTime(String.valueOf(time))
                .withVersion(version);

        MeasureValue cpuUtilization = new MeasureValue()
                .withName("cpu_utilization")
                .withType(MeasureValueType.DOUBLE)
                .withValue("13.5");
        MeasureValue memoryUtilization = new MeasureValue()
                .withName("memory_utilization")
                .withType(MeasureValueType.DOUBLE)
                .withValue("40");
        Record computationalResources = new Record()
                .withMeasureName("cpu_memory")
                .withMeasureValues(cpuUtilization, memoryUtilization)
                .withMeasureValueType(MeasureValueType.MULTI);

        records.add(computationalResources);

        WriteRecordsRequest writeRecordsRequest = new WriteRecordsRequest()
                .withDatabaseName(DATABASE_NAME)
                .withTableName(TABLE_NAME)
                .withCommonAttributes(commonAttributes)
                .withRecords(records);

        // write records for first time
        try {
            WriteRecordsResult writeRecordResult = timestreamWriteClient.writeRecords(writeRecordsRequest);
            System.out.println(
                    "WriteRecords Status for multi value attributes: " + writeRecordResult
                            .getSdkHttpMetadata().getHttpStatusCode());
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
        final Dimension region = new Dimension().withName("region").withValue(REGION);
        final Dimension az = new Dimension().withName("az").withValue("az1");
        final Dimension hostname = new Dimension().withName("hostname").withValue("host1");

        dimensions.add(region);
        dimensions.add(az);
        dimensions.add(hostname);

        Record commonAttributes = new Record()
                .withDimensions(dimensions)
                .withTime(String.valueOf(time))
                .withVersion(version);

        MeasureValue cpuUtilization = new MeasureValue()
                .withName("cpu_utilization")
                .withType(MeasureValueType.DOUBLE)
                .withValue("13");
        MeasureValue memoryUtilization =new MeasureValue()
                .withName("memory_utilization")
                .withType(MeasureValueType.DOUBLE)
                .withValue("40");
        MeasureValue activeCores = new MeasureValue()
                .withName("active_cores")
                .withType(MeasureValueType.BIGINT)
                .withValue("4");


        Record computationalResources = new Record()
                .withMeasureName("computational_utilization")
                .withMeasureValues(cpuUtilization, memoryUtilization, activeCores)
                .withMeasureValueType(MeasureValueType.MULTI);

        records.add(computationalResources);

        WriteRecordsRequest writeRecordsRequest = new WriteRecordsRequest()
                .withDatabaseName(DATABASE_NAME)
                .withTableName(TABLE_NAME)
                .withCommonAttributes(commonAttributes)
                .withRecords(records);

        // write records for first time
        try {
            WriteRecordsResult writeRecordResult = timestreamWriteClient.writeRecords(writeRecordsRequest);
            System.out.println(
                    "WriteRecords Status for multi value attributes: " + writeRecordResult
                            .getSdkHttpMetadata().getHttpStatusCode());
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
}