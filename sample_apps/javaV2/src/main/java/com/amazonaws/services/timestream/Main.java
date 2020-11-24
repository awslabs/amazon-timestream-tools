package com.amazonaws.services.timestream;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.timestreamquery.TimestreamQueryClient;
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import java.net.URI;

import java.io.IOException;
import java.time.Duration;

public class Main {
    public static final String DATABASE_NAME = "testJavaV2DB";
    public static final String TABLE_NAME = "testJavaV2Table";

    public static void main(String[] args) throws IOException {
        TimestreamWriteClient writeClient = buildWriteClient();
        TimestreamQueryClient queryClient = buildQueryClient();

        InputArguments inputArguments = parseArguments(args);
        System.out.println("INPUT ARGS: " + inputArguments);

        CrudAndSimpleIngestionExample crudAndSimpleIngestionExample = new CrudAndSimpleIngestionExample(writeClient);
        CsvIngestionExample csvIngestionExample = new CsvIngestionExample(writeClient);
        QueryExample queryExample = new QueryExample(queryClient);

        crudAndSimpleIngestionExample.createDatabase();
        crudAndSimpleIngestionExample.describeDatabase();
        crudAndSimpleIngestionExample.listDatabases();
        crudAndSimpleIngestionExample.updateDatabase(inputArguments.kmsId);

        crudAndSimpleIngestionExample.createTable();
        crudAndSimpleIngestionExample.describeTable();
        crudAndSimpleIngestionExample.listTables();
        crudAndSimpleIngestionExample.updateTable();

        // simple record ingestion
        crudAndSimpleIngestionExample.writeRecords();
        crudAndSimpleIngestionExample.writeRecordsWithCommonAttributes();

        // update records
        crudAndSimpleIngestionExample.writeRecordsWithUpsert();

        if(inputArguments.inputFile != null) {
            // Bulk record ingestion for bootstrapping a table with fresh data
            csvIngestionExample.bulkWriteRecords(inputArguments.inputFile);
        }

        // Query samples
        queryExample.runAllQueries();

        // Try cancelling a query
        queryExample.cancelQuery();

        // Run a query with Multiple pages
        queryExample.runQueryWithMultiplePages(20000);

        // Commenting out cleanup. Enable as per your usecase.
        // crudAndSimpleIngestionExample.deleteTable();
        // crudAndSimpleIngestionExample.deleteDatabase();

        System.exit(0);
    }

    private static InputArguments parseArguments(String[] args) {
        InputArguments inputArguments = new InputArguments();
        final CmdLineParser parser = new CmdLineParser(inputArguments);

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            System.exit(1);
        }

        return inputArguments;
    }

    /**
     * Recommended Timestream write client SDK configuration:
     *  - Set SDK retry count to 10.
     *  - Use SDK DEFAULT_BACKOFF_STRATEGY
     *  - Set RequestTimeout to 20 seconds .
     *  - Set max connections to 5000 or higher.
     */
    private static TimestreamWriteClient buildWriteClient() {
        ApacheHttpClient.Builder httpClientBuilder =
                ApacheHttpClient.builder();
        httpClientBuilder.maxConnections(5000);

        RetryPolicy.Builder retryPolicy =
                RetryPolicy.builder();
        retryPolicy.numRetries(10);

        ClientOverrideConfiguration.Builder overrideConfig =
                ClientOverrideConfiguration.builder();
        overrideConfig.apiCallAttemptTimeout(Duration.ofSeconds(20));
        overrideConfig.retryPolicy(retryPolicy.build());

        return TimestreamWriteClient.builder()
                .httpClientBuilder(httpClientBuilder)
                .overrideConfiguration(overrideConfig.build())
                .region(Region.US_EAST_1)
                .build();
    }

    private static TimestreamQueryClient buildQueryClient() {
        return TimestreamQueryClient.builder()
                .region(Region.US_EAST_1)
                .build();
    }
}
