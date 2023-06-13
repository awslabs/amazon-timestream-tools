package com.amazonaws.services.timestream;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.timestreamquery.TimestreamQueryClient;
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient;

import com.amazonaws.services.timestream.utils.WriteUtil;

import static com.amazonaws.services.timestream.utils.Constants.DATABASE_NAME;
import static com.amazonaws.services.timestream.utils.Constants.TABLE_NAME;
import static com.amazonaws.services.timestream.utils.Constants.UNLOAD_TABLE_NAME;

public class Main {
    public static void main(String[] args) throws IOException, URISyntaxException {
        InputArguments inputArguments = parseArguments(args);
        System.out.println("INPUT ARGS: " + inputArguments);

        TimestreamWriteClient writeClient = buildWriteClient(inputArguments.getRegion());
        TimestreamQueryClient queryClient = buildQueryClient(inputArguments.getRegion());

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
    private static TimestreamWriteClient buildWriteClient(Region region) {
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
                .region(region)
                .build();
    }

    private static TimestreamQueryClient buildQueryClient(Region region) {
        return TimestreamQueryClient.builder()
                .region(region)
                .build();
    }
}
