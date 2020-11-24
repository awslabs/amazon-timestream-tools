package com.amazonaws.services.timestream;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient;
import software.amazon.awssdk.services.timestreamwrite.model.Dimension;
import software.amazon.awssdk.services.timestreamwrite.model.Record;
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsRequest;
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsResponse;

import static com.amazonaws.services.timestream.Main.DATABASE_NAME;
import static com.amazonaws.services.timestream.Main.TABLE_NAME;

public class CsvIngestionExample {

    private final TimestreamWriteClient timestreamWriteClient;

    public CsvIngestionExample(TimestreamWriteClient timestreamWriteClient) {
        this.timestreamWriteClient = timestreamWriteClient;
    }

    public void bulkWriteRecords(String csvFilePath) throws IOException {
        System.out.println("Uploading CSV file data:");
        BufferedReader reader = new BufferedReader(new FileReader(csvFilePath));
        try {

            List<Record> records = new ArrayList<>();
            long currentTime = System.currentTimeMillis();
            int counter = 0;

            while (true) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }
                String[] columns = line.split(",");

                List<Dimension> dimensions = new ArrayList<>();
                dimensions.add(Dimension.builder().name(columns[0]).value(columns[1]).build());
                dimensions.add(Dimension.builder().name(columns[2]).value(columns[3]).build());
                dimensions.add(Dimension.builder().name(columns[4]).value(columns[5]).build());

                // override the value on the file to get an ingestion that is around current time
                final long recordTime = currentTime - counter * 50;
                Record record = Record.builder()
                        .dimensions(dimensions)
                        .measureName(columns[6])
                        .measureValue(columns[7])
                        .measureValueType(columns[8])
                        .time(String.valueOf(recordTime))
                        .build();

                records.add(record);
                counter++;
                // when the batch hits the max size, submit the batch
                if (records.size() == 100) {
                    submitBatch(records, counter);
                    records.clear();
                }
            }
            if (records.size() != 0) {
                submitBatch(records, counter);
            }
            System.out.println("ingested " + counter + "records");
        } finally {
            reader.close();
        }
    }

    private void submitBatch(List<Record> records, int counter) {
        WriteRecordsRequest writeRecordsRequest = WriteRecordsRequest.builder()
                .databaseName(DATABASE_NAME).tableName(TABLE_NAME).records(records).build();

        try {
            WriteRecordsResponse writeRecordsResponse = timestreamWriteClient.writeRecords(writeRecordsRequest);
            System.out.println("Processed " + counter + " records. WriteRecords Status: " +
                    writeRecordsResponse.sdkHttpResponse().statusCode());
        } catch (Exception e) {
            System.out.println("Error: " + e);
        }
    }
}
