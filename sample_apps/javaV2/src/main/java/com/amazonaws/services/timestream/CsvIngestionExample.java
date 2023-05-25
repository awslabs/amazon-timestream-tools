package com.amazonaws.services.timestream;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient;
import software.amazon.awssdk.services.timestreamwrite.model.Dimension;
import software.amazon.awssdk.services.timestreamwrite.model.Record;

import com.amazonaws.services.timestream.utils.WriteUtil;

import static com.amazonaws.services.timestream.utils.Constants.DATABASE_NAME;
import static com.amazonaws.services.timestream.utils.Constants.TABLE_NAME;

public class CsvIngestionExample {

    private final TimestreamWriteClient timestreamWriteClient;
    private final WriteUtil writeUtil;

    public CsvIngestionExample(TimestreamWriteClient timestreamWriteClient) {
        this.timestreamWriteClient = timestreamWriteClient;
        this.writeUtil = new WriteUtil(timestreamWriteClient);
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
                    writeUtil.submitBatch(DATABASE_NAME, TABLE_NAME, records, counter);
                    records.clear();
                }
            }
            if (records.size() != 0) {
                writeUtil.submitBatch(DATABASE_NAME, TABLE_NAME, records, counter);
            }
            System.out.println("ingested " + counter + "records");
        } finally {
            reader.close();
        }
    }
}
