package com.amazonaws.sample.csv.ingestion;

import com.amazonaws.sample.timestream.multithreaded.TimestreamWriter;
import com.amazonaws.sample.timestream.multithreaded.TimestreamWriterConfig;
import com.amazonaws.sample.timestream.multithreaded.TimestreamWriterImpl;
import com.amazonaws.sample.timestream.multithreaded.util.LogMetricsPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import software.amazon.awssdk.services.timestreamwrite.model.RetentionProperties;

import java.io.FileInputStream;
import java.io.InputStream;
import java.time.Duration;

@Command(name = "com.amazonaws.sample.csv.ingestion.Main", mixinStandardHelpOptions = true)
class Main implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    @Option(names = {"-f", "--filePath"},
            description = "File Path",
            required = true)
    private String filePath;

    @Option(names = {"--database"},
            description = "Target Timestream database name to ingest records to.",
            required = true)
    private String database;

    @Option(names = {"--table"},
            description = "Target Timestream table name to ingest records to.",
            required = true)
    private String table;

    @Option(names = {"--region"},
            description = "Timestream region",
            required = true)
    private String region;

    @Option(names = {"-t", "--threadpool"},
            description = "TheadPool size to use when inserting records to Timestream",
            required = true)
    private Integer threadPoolSize;

    @Option(names = {"-q", "--queuesize"},
            description = "The size of the queue buffer for Timestream WriteRequests to be consumed by multiple writers",
            required = true)
    private Integer queueSize;


    public static void main(String[] args) {
        int exitCode = new CommandLine(new Main()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public void run() {
        try {
            LOG.info("Starting ingesting records from: {}", filePath);

            final TimestreamWriterConfig writerConfig = TimestreamWriterConfig.builder()
                    .queueSize(queueSize)
                    .threadPoolSize(threadPoolSize)
                    .writeClient(TimestreamWriterConfig.defaultRecommendedWriteClient(region))
                    .maxRetryDurationMs(Duration.ofMinutes(5).toMillis())
                    .createTableIfNotExists(
                            new TimestreamWriterConfig.TimestreamResourceCreationConfig(
                                    RetentionProperties.builder()
                                            .memoryStoreRetentionPeriodInHours(5L)
                                            .magneticStoreRetentionPeriodInDays(7L)
                                    .build()
                            )
                    )
                    .build();

            final TimestreamWriter writer = new TimestreamWriterImpl(writerConfig);
            final LogMetricsPublisher logMetricsPublisher = new LogMetricsPublisher(writer, Duration.ofSeconds(5));
            try {
                final SampleCsvIngestion csvIngestion = new SampleCsvIngestion(writer, database, table);
                try (final InputStream inputStream = new FileInputStream(filePath)) {
                    csvIngestion.bulkWriteRecords(inputStream);
                }
                LOG.info("Finished adding records to the multithreaded Timestream Writer.");
            } finally {
                // Ensure all tasks are completed and then shut down the threads
                writer.shutDownGracefully();
                LOG.info("Last metrics: {}", logMetricsPublisher.getNow());
                LOG.info("Total metrics: {}", logMetricsPublisher.getTotalMetrics());
            }
        } catch (Exception e) {
            LOG.error("Exception occurred:", e);
        }
    }
}
