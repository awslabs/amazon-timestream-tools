package com.amazonaws.services.timestream;

import org.kohsuke.args4j.Option;

public class InputArguments {

    @Option(name = "--database", aliases = "-d", usage = "database with DevOps table", required = true)
    String database;

    @Option(name = "--host", aliases = "-h", usage = "DevOps table host to query", required = true)
    String host;
}
