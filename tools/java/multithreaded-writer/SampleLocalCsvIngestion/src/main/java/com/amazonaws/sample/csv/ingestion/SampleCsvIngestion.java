package com.amazonaws.sample.csv.ingestion;

import com.amazonaws.sample.csv.mapping.SampleCsvMapper;
import com.amazonaws.sample.csv.mapping.SampleCsvRow;
import com.amazonaws.sample.timestream.multithreaded.TimestreamWriter;
import lombok.NonNull;
import org.simpleflatmapper.csv.CsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsRequest;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SampleCsvIngestion {
    private static final Logger LOG = LoggerFactory.getLogger(SampleCsvIngestion.class);

    private final TimestreamWriter timestreamWriter;

    private static final int RECORDS_TO_PROCESS_AT_ONCE = 10000;
    private final SampleCsvMapper mapper;

    public SampleCsvIngestion(final @NonNull TimestreamWriter writer,
                              final @NonNull String database,
                              final @NonNull String table) {
        this.timestreamWriter = writer;
        this.mapper = new SampleCsvMapper(database, table);
    }

    public void bulkWriteRecords(final InputStream inputStream) throws IOException {
        List<SampleCsvRow> records = new ArrayList<>(RECORDS_TO_PROCESS_AT_ONCE);
        try (final BufferedReader reader = new BufferedReader(
                new InputStreamReader(
                        inputStream,
                        StandardCharsets.UTF_8))
        ) {
            final Iterator<SampleCsvRow> it = CsvParser.mapTo(SampleCsvRow.class).iterator(reader);
            while (it.hasNext()) {
                records.add(it.next());
                if (records.size() >= RECORDS_TO_PROCESS_AT_ONCE) {
                    final List<WriteRecordsRequest> writeRecordsRequests = mapper.mapRecords(records);
                    submitRequests(writeRecordsRequests);
                    records.clear();
                }
            }

            if (records.size() > 0) {
                final List<WriteRecordsRequest> writeRecordsRequests = mapper.mapRecords(records);
                submitRequests(writeRecordsRequests);
            }
        }
    }

    private void submitRequests(final List<WriteRecordsRequest> requests) {
        for (final WriteRecordsRequest r : requests) {
            try {
                while (!timestreamWriter.putWriteRecordRequest(r)) {
                    LOG.info("The writer queue is full, retrying writing the records in 1 sec...");
                    Thread.sleep(1000);
                }
            } catch (Exception e) {
                LOG.error("Error: ", e);
            }
        }
    }
}
