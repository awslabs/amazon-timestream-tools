package com.amazonaws.services.timestream;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.timestream.utils.WriteUtil;
import com.amazonaws.services.timestreamwrite.AmazonTimestreamWrite;
import com.amazonaws.services.timestreamwrite.model.Dimension;
import com.amazonaws.services.timestreamwrite.model.Record;

import static com.amazonaws.services.timestream.utils.Constants.DATABASE_NAME;
import static com.amazonaws.services.timestream.utils.Constants.TABLE_NAME;

public class CsvIngestionExample {

    private final AmazonTimestreamWrite amazonTimestreamWrite;
    private final WriteUtil writeUtil;

    public CsvIngestionExample(AmazonTimestreamWrite amazonTimestreamWrite) {
        this.amazonTimestreamWrite = amazonTimestreamWrite;
        this.writeUtil = new WriteUtil(amazonTimestreamWrite);
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
