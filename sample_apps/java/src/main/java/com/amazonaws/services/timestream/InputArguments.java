package com.amazonaws.services.timestream;

import lombok.Getter;
import org.kohsuke.args4j.Option;

@Getter
public class InputArguments {

    @Option(name = "--type", aliases = "-t", usage = "choose type of workload to run", required = true)
    private AppType appType = AppType.BASIC;

    @Option(name = "--inputFile", aliases = "-i", usage = "input to the csv file path for ingestion")
    private String inputFile;

    @Option(name = "--kmsId", aliases = "-k", usage = "kmsId for update")
    private String kmsId;

    @Option(name = "--region", aliases = "-r", usage = "AWS region", required = true)
    private String region;

    @Option(name = "--skip_deletion", aliases = "-sd", usage = "Skip deletion of the timestream resources created")
    private boolean skipDeletion;

    public enum AppType {
        BASIC,
        UNLOAD,
        CLEANUP,
        COMPOSITE_PARTITION_KEY
    }

}
