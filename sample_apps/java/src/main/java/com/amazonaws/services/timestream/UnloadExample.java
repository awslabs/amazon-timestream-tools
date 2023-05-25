package com.amazonaws.services.timestream;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;

import com.amazonaws.services.timestream.utils.Constants;
import com.amazonaws.services.timestream.utils.TimestreamDependencyHelper;
import com.amazonaws.services.timestream.utils.UnloadUtil;
import com.amazonaws.services.timestream.utils.WriteUtil;
import com.amazonaws.services.timestreamquery.AmazonTimestreamQuery;
import com.amazonaws.services.timestreamwrite.AmazonTimestreamWrite;
import com.amazonaws.services.timestreamwrite.model.Dimension;
import com.amazonaws.services.timestreamwrite.model.MeasureValue;
import com.amazonaws.services.timestreamwrite.model.MeasureValueType;
import com.amazonaws.services.timestreamwrite.model.Record;

import static com.amazonaws.services.timestream.utils.Constants.DATABASE_NAME;
import static com.amazonaws.services.timestream.utils.Constants.UNLOAD_TABLE_NAME;

public class UnloadExample {
    private final InputArguments inputArguments;
    private final AmazonTimestreamWrite writeClient;
    private final AmazonTimestreamQuery queryClient;
    private final WriteUtil writeUtil;
    private final UnloadUtil unloadUtil;
    private final TimestreamDependencyHelper timestreamDependencyHelper;
    private final String exportedBucketName;

    public UnloadExample(InputArguments inputArguments, final AmazonTimestreamWrite writeClient,
            final AmazonTimestreamQuery queryClient) {
        this.inputArguments = inputArguments;
        this.writeClient = writeClient;
        this.queryClient = queryClient;
        this.writeUtil = new WriteUtil(writeClient);
        this.timestreamDependencyHelper = new TimestreamDependencyHelper(inputArguments.getRegion());
        StringJoiner joiner = new StringJoiner("-");
        joiner.add(Constants.S3_BUCKET_PREFIX);
        joiner.add(inputArguments.getRegion());
        joiner.add(timestreamDependencyHelper.getAccount());
        this.exportedBucketName = joiner.toString();
        this.unloadUtil = new UnloadUtil(queryClient, timestreamDependencyHelper, exportedBucketName);

    }

    private void bulkWriteShoppingRecords() throws IOException {
        System.out.printf("Ingestion to %s.%s%n", DATABASE_NAME, UNLOAD_TABLE_NAME);
        try (CSVParser reader = new CSVParser(new BufferedReader(new FileReader(inputArguments.getInputFile())),
                CSVFormat.Builder.create().setHeader().build())) {

            // 0 - 'channel',
            // 1 - 'ip_address',
            // 2 - 'session_id',
            // 3 - 'user_id',
            // 4 - 'event',
            // 5 - 'user_group',
            // 6 - 'current_time',
            // 7 - 'query',  - measure
            // 8 - 'product_id',  - measure
            // 9 - 'product',   -measure
            // 10 - 'quantity'  - measure

            //['Referral',
            // '63.181.186.170',
            // 'c8e5d3a8b61ba4963245eaf5e9b66e3cf0d53103d868b35b6be8955e576135f0',
            // '316223',
            // 'Search',
            // 'grp-060',
            // '1677613824427',
            // 'Blade A5 2020 2/32GB Black',
            // '',
            // '',
            // ''

            int rowCounter = 0;
            List<Record> records = new ArrayList<>();

            while (reader.iterator().hasNext()) {
                CSVRecord csvRecord = reader.iterator().next();

                // First [0-5] records are dimensions
                List<Dimension> dimensions = new ArrayList<>();
                for (int i = 0; i < 6; i++) {
                    dimensions.add(new Dimension().withName(reader.getHeaderNames().get(i))
                            .withValue(csvRecord.get(i)));
                }

                // 6th is time
                long currentTime = System.currentTimeMillis();
                // override the value on the file to get an ingestion that is around current time
                final long recordTime = currentTime - rowCounter * 50L;

                // 7-9 records are measures with varchar type
                List<MeasureValue> measureValues = new ArrayList<>();
                for (int i = 7; i < 10; i++) {
                    if (StringUtils.isNotBlank(csvRecord.get(i))) {
                        measureValues.add(new MeasureValue()
                                .withName(reader.getHeaderNames().get(i))
                                .withType(MeasureValueType.VARCHAR)
                                .withValue(csvRecord.get(i)));
                    }
                }

                if (StringUtils.isNotBlank(csvRecord.get(10))) {
                    // 10th is measure with type double
                    measureValues.add(new MeasureValue()
                            .withName(reader.getHeaderNames().get(10))
                            .withType(MeasureValueType.DOUBLE)
                            .withValue(csvRecord.get(10)));
                }

                Record record = new Record()
                        .withDimensions(dimensions)
                        .withTime(String.valueOf(recordTime))
                        .withMeasureName("metrics")
                        .withMeasureValueType(MeasureValueType.MULTI)
                        .withMeasureValues(measureValues);

                records.add(record);
                rowCounter++;
                if (records.size() % 100 == 0) {
                    writeUtil.submitBatch(DATABASE_NAME, UNLOAD_TABLE_NAME, records, rowCounter);
                    records = new ArrayList<>();
                }
            }

            // Ingest remaining ones
            if (!records.isEmpty()) {
                writeUtil.submitBatch(DATABASE_NAME, UNLOAD_TABLE_NAME, records, rowCounter);
            }

            System.out.printf("Ingested %d records%n", rowCounter);
        }
    }

    public void run() throws IOException {
        if (inputArguments.getInputFile() == null) {
            System.err.println("Input file is required");
        }
        try {
            writeUtil.createDatabase(DATABASE_NAME);
            if (inputArguments.getKmsId() != null) {
                writeUtil.updateDatabase(DATABASE_NAME, inputArguments.getKmsId());
            }
            writeUtil.createTable(DATABASE_NAME, UNLOAD_TABLE_NAME);
            timestreamDependencyHelper.createS3Bucket(this.exportedBucketName);

            bulkWriteShoppingRecords();
            unloadUtil.runAllQueries();

        } finally {
            if (!inputArguments.isSkipDeletion()) {
                writeUtil.deleteTable(DATABASE_NAME, UNLOAD_TABLE_NAME);
                writeUtil.deleteDatabase(DATABASE_NAME);
                timestreamDependencyHelper.deleteS3Bucket(exportedBucketName);
            }
        }
    }
}
