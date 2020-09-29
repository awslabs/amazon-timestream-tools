package com.amazonaws.services.timestream;

import com.amazonaws.services.timestreamquery.AmazonTimestreamQuery;
import com.amazonaws.services.timestreamquery.AmazonTimestreamQueryClient;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import java.io.IOException;

public class Main {

    public static void main(final String[] args) throws IOException {
        final InputArguments inputArguments = parseArguments(args);

        final QueryExample queryExample = new QueryExample(
                inputArguments.database, inputArguments.host);

        // Query samples using the JDBC driver.
        queryExample.runAllQueriesWithTimestreamConnection();

        System.exit(0);
    }

    private static InputArguments parseArguments(final String[] args) {
        final InputArguments inputArguments = new InputArguments();
        final CmdLineParser parser = new CmdLineParser(inputArguments);
        try {
            parser.parseArgument(args);
        } catch (final CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            System.exit(1);
        }
        return inputArguments;
    }
}

