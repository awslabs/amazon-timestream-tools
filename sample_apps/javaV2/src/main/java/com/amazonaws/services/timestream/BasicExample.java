package com.amazonaws.services.timestream;

import java.io.IOException;

import software.amazon.awssdk.services.timestreamquery.TimestreamQueryClient;
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient;

import com.amazonaws.services.timestream.utils.WriteUtil;

import static com.amazonaws.services.timestream.utils.Constants.DATABASE_NAME;
import static com.amazonaws.services.timestream.utils.Constants.TABLE_NAME;

public class BasicExample {
    private final InputArguments inputArguments;
    private final TimestreamWriteClient writeClient;
    private final TimestreamQueryClient queryClient;
    private final CrudAndSimpleIngestionExample crudAndSimpleIngestionExample;
    private final CsvIngestionExample csvIngestionExample;
    private final QueryExample queryExample;
    private final WriteUtil writeUtil;

    public BasicExample(InputArguments inputArguments, final TimestreamWriteClient writeClient, final TimestreamQueryClient queryClient) {
        this.inputArguments = inputArguments;
        this.writeClient = writeClient;
        this.queryClient = queryClient;
        this.writeUtil = new WriteUtil(writeClient);
        this.crudAndSimpleIngestionExample = new CrudAndSimpleIngestionExample(writeClient, writeUtil);
        this.csvIngestionExample = new CsvIngestionExample(writeClient);
        this.queryExample = new QueryExample(queryClient);
    }

    public void run() throws IOException {
        this.crudAndSimpleIngestionExample.run(inputArguments);
        if (inputArguments.getInputFile() != null) {
            // Bulk record ingestion for bootstrapping a table with fresh data
            csvIngestionExample.bulkWriteRecords(inputArguments.getInputFile());
        }
        try {
            // Query samples
            queryExample.runAllQueries();

            // Try cancelling a query
            queryExample.cancelQuery();

            // Run a query with Multiple pages
            queryExample.runQueryWithMultiplePages(20000);
        } catch (Exception e) {
            // Some queries might fail with 500 if the result of a sequence function has more than 10000 entries
            e.printStackTrace();
        } finally {
            if (!inputArguments.isSkipDeletion()) {
                writeUtil.deleteTable(DATABASE_NAME, TABLE_NAME);
                writeUtil.deleteDatabase(DATABASE_NAME);
            }
        }
    }
}
