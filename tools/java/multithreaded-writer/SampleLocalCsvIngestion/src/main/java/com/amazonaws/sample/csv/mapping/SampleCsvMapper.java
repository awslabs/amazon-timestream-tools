package com.amazonaws.sample.csv.mapping;

import com.google.common.collect.Lists;
import lombok.SneakyThrows;
import software.amazon.awssdk.services.timestreamwrite.model.Dimension;
import software.amazon.awssdk.services.timestreamwrite.model.Record;
import software.amazon.awssdk.services.timestreamwrite.model.TimeUnit;
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsRequest;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

public class SampleCsvMapper {
    private static final long MAX_TIMESTAMP_FROM_SAMPLE_FILE = 1584502929599L;

    private static final int MAX_RECORDS_IN_BATCH = 100;
    private static final DateFormat m_ISO8601Local = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final String database;
    private final String table;

    public SampleCsvMapper(final String database, final String table) {
        this.database = database;
        this.table = table;
        m_ISO8601Local.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    @SneakyThrows
    public List<WriteRecordsRequest> mapRecords(final List<SampleCsvRow> records) {
        final List<WriteRecordsRequest> result = new ArrayList<>();
        long currentTime = System.currentTimeMillis();
        final List<Record> timestreamRecords = new ArrayList<>();

        for (final SampleCsvRow csvRow: records) {
            List<Dimension> dimensions = new ArrayList<>();
            addDimensionIfNotEmpty(dimensions, "region", csvRow.getRegion());
            addDimensionIfNotEmpty(dimensions, "az", csvRow.getAz());
            addDimensionIfNotEmpty(dimensions, "hostname", csvRow.getHostname());

            long csvRecordTime = m_ISO8601Local.parse(csvRow.getTimestamp()).getTime();
            // Shifting the timestamps from the sample file.
            // The sample file contains records from 2020-03-18 01:28:29 to 2020-03-18 03:42:09.
            // The line below will map them to now() + (csvRecordTime - 2020-03-18 03:42:09), so effectively to
            // now() - 0...2h 14min, so they can be ingested to Memory Tier in Timestream.
            long recordTime = currentTime + (csvRecordTime - MAX_TIMESTAMP_FROM_SAMPLE_FILE);

            timestreamRecords.add(Record.builder()
                    .dimensions(dimensions)
                    .measureName(csvRow.getMeasure_name())
                    .measureValue(csvRow.getMeasure_value())
                    .measureValueType("DOUBLE")
                    .time(String.valueOf(recordTime))
                    .timeUnit(TimeUnit.MILLISECONDS)
                    .build()
            );
        }

        // TODO: group records by some common attributes, to decrease ingestion cost and bytes transmitted over network
        // TODO: then, extract common attributes from each WriteRecordsRequest
        final List<List<Record>> recordBatches = Lists.partition(timestreamRecords, MAX_RECORDS_IN_BATCH);
        for (final List<Record> batch: recordBatches) {
            result.add(WriteRecordsRequest.builder()
                    .databaseName(database)
                    .tableName(table)
                    .records(batch)
                    .build()
            );
        }
        return result;
    }

    private void addDimensionIfNotEmpty(List<Dimension> dimensions, String dimName, String dimValue) {
        if (dimValue != null) {
            dimensions.add(Dimension.builder().name(dimName).value(dimValue).build());
        }
    }
}
