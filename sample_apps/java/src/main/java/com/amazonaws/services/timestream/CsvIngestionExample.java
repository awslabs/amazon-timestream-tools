package com.amazonaws.services.timestream;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.timestreamwrite.AmazonTimestreamWrite;
import com.amazonaws.services.timestreamwrite.model.Dimension;
import com.amazonaws.services.timestreamwrite.model.Record;
import com.amazonaws.services.timestreamwrite.model.WriteRecordsRequest;
import com.amazonaws.services.timestreamwrite.model.WriteRecordsResult;

import static com.amazonaws.services.timestream.Main.DATABASE_NAME;
import static com.amazonaws.services.timestream.Main.TABLE_NAME;

public class CsvIngestionExample {

    private final AmazonTimestreamWrite amazonTimestreamWrite;

    public CsvIngestionExample(AmazonTimestreamWrite amazonTimestreamWrite) {
        this.amazonTimestreamWrite = amazonTimestreamWrite;
    }

    public void bulkWriteRecords(String csvFilePath) throws IOException {
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
                dimensions.add(new Dimension().withName(columns[0]).withValue(columns[1]));
                dimensions.add(new Dimension().withName(columns[2]).withValue(columns[3]));
                dimensions.add(new Dimension().withName(columns[4]).withValue(columns[5]));

                // override the value on the file to get an ingestion that is around current time
                final long recordTime = currentTime - counter * 50;
                Record record = new Record()
                        .withDimensions(dimensions)
                        .withMeasureName(columns[6])
                        .withMeasureValue(columns[7])
                        .withMeasureValueType(columns[8])
                        .withTime(String.valueOf(recordTime));

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
        WriteRecordsRequest writeRecordsRequest = new WriteRecordsRequest()
                .withDatabaseName(DATABASE_NAME)
                .withTableName(TABLE_NAME);
        writeRecordsRequest.setRecords(records);

        try {
            WriteRecordsResult writeRecordsResult = amazonTimestreamWrite.writeRecords(writeRecordsRequest);
            System.out.println("Processed " + counter + " records. WriteRecords Status: " +
                    writeRecordsResult.getSdkHttpMetadata().getHttpStatusCode());
        } catch (Exception e) {
            System.out.println("Error: " + e);
        }
    }
}
