package com.amazonaws.services.timestream;

import java.io.IOException;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.timestream.utils.WriteUtil;
import com.amazonaws.services.timestreamquery.AmazonTimestreamQuery;
import com.amazonaws.services.timestreamquery.AmazonTimestreamQueryClient;
import com.amazonaws.services.timestreamwrite.AmazonTimestreamWrite;
import com.amazonaws.services.timestreamwrite.AmazonTimestreamWriteClientBuilder;

import static com.amazonaws.services.timestream.utils.Constants.DATABASE_NAME;
import static com.amazonaws.services.timestream.utils.Constants.TABLE_NAME;
import static com.amazonaws.services.timestream.utils.Constants.UNLOAD_TABLE_NAME;

public class Main {

    public static void main(String[] args) throws IOException {
        InputArguments inputArguments = parseArguments(args);
        AmazonTimestreamWrite writeClient = buildWriteClient(inputArguments.getRegion());
        final AmazonTimestreamQuery queryClient = buildQueryClient(inputArguments.getRegion());
        switch (inputArguments.getAppType()) {
            case BASIC:
                new BasicExample(inputArguments, writeClient, queryClient).run();
                break;
            case UNLOAD:
                new UnloadExample(inputArguments, writeClient, queryClient).run();
                break;
            case CLEANUP:
                WriteUtil writeUtil = new WriteUtil(writeClient);
                writeUtil.deleteTable(DATABASE_NAME, TABLE_NAME);
                writeUtil.deleteTable(DATABASE_NAME, UNLOAD_TABLE_NAME);
                writeUtil.deleteDatabase(DATABASE_NAME);
                break;
            case COMPOSITE_PARTITION_KEY:
                new CompositePartitionKeyExample(inputArguments, writeClient, queryClient).run();
                break;
            default:
                throw new UnsupportedOperationException("App Type not supported: " + inputArguments.getAppType());
        }
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
     * - Set SDK retry count to 10.
     * - Use SDK DEFAULT_BACKOFF_STRATEGY
     * - Set RequestTimeout to 20 seconds .
     * - Set max connections to 5000 or higher.
     */
    private static AmazonTimestreamWrite buildWriteClient(String region) {
        final ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withMaxConnections(5000)
                .withRequestTimeout(20 * 1000)
                .withMaxErrorRetry(10);
        return AmazonTimestreamWriteClientBuilder
                .standard()
                .withRegion(region)
                .withClientConfiguration(clientConfiguration)
                .build();
    }

    private static AmazonTimestreamQuery buildQueryClient(String region) {
        return AmazonTimestreamQueryClient.builder().withRegion(region).build();
    }

}

