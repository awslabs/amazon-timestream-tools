package com.amazonaws.sample.lambda;

import lombok.Getter;
import lombok.Setter;
import lombok.var;

public class EnvVariablesHelper {
    @Getter
    @Setter
    private static int TimestreamWriterThreadPoolSize = getIntEnvVariable(
            "TIMESTREAM_WRITER_THREAD_POOL_SIZE", 1024);
    @Getter
    @Setter
    private static int TimestreamWriterQueueSize = getIntEnvVariable(
            "TIMESTREAM_WRITER_QUEUE_SIZE", 5 * TimestreamWriterThreadPoolSize);

    @Getter
    @Setter
    private static String S3Region = System.getenv("S3_REGION");
    @Getter
    @Setter
    private static String TimestreamRegion = System.getenv("TIMESTREAM_REGION");

    @Getter
    @Setter
    private static String TargetTimestreamDatabase = System.getenv("TARGET_TIMESTREAM_DATABASE");
    @Getter
    @Setter
    private static String TargetTimestreamTable = System.getenv("TARGET_TIMESTREAM_TABLE");

    @Getter
    @Setter
    private static int MaxTimestreamRecordInsertionRetryDurationMs = getIntEnvVariable(
            "MAX_TIMESTREAM_RECORD_INSERTION_RETRY_DURATION_S", 300) * 1000;

    private static int getIntEnvVariable(final String name, final int defaultValue) {
        var fromEnv = System.getenv(name);
        if (fromEnv == null) {
            return defaultValue;
        } else {
            return Integer.parseInt(fromEnv);
        }
    }
}
