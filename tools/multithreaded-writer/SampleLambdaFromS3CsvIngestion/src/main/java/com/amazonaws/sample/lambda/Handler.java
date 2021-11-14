package com.amazonaws.sample.lambda;

import com.amazonaws.sample.csv.ingestion.SampleCsvIngestion;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.FileInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class Handler implements RequestHandler<Map<String, Object>, String> {
    private static final Logger LOG = LoggerFactory.getLogger(Handler.class);
    private HandlerState state;

    public Handler() {
        LOG.info("Creating lambda instance!");
        try {
            state = new HandlerState();
        } catch (Exception e) {
            LOG.error("COULDN'T INITIALIZE LAMBDA: ", e);
            System.exit(1);
        }
    }

    @Override
    public String handleRequest(Map<String, Object> event, Context context) {
        try {
            LOG.debug("WHOLE OBJ: {}", event);
            Instant start = Instant.now();

            final EventInfo inputFile = EventInfo.getInputFile(event);
            LOG.info("Processing file: {}", inputFile);

            InputStream inputStream;
            if (inputFile.getLocalPath() != null) {
                inputStream = new FileInputStream(inputFile.getLocalPath());
            } else {
                final ResponseInputStream<GetObjectResponse> s3Object = state.getS3()
                        .getObject(GetObjectRequest.builder()
                                .bucket(inputFile.getBucketName())
                                .key(inputFile.getBucketKey())
                                .build());
                inputStream = s3Object;
            }

            final SampleCsvIngestion sampleCsvIngestion = new SampleCsvIngestion(
                    state.writer,
                    EnvVariablesHelper.getTargetTimestreamDatabase(),
                    EnvVariablesHelper.getTargetTimestreamTable());
            sampleCsvIngestion.bulkWriteRecords(inputStream);

            waitForWritesCompletion();
            LOG.info("Finished ingesting records from: {} in {}", inputFile, Duration.between(start, Instant.now()));
            LOG.info("Last metrics: {}", state.logMetricsPublisher.getNow());
            LOG.info("Total metrics: {}", state.logMetricsPublisher.getTotalMetrics());
            state.logMetricsPublisher.clearTotalMetrics();

            return "OK";
        } catch (final Exception e) {
            LOG.error("Unknown error: ", e);
            System.exit(1);
        }
        return "OK";
    }

    private void waitForWritesCompletion() throws InterruptedException {
        while (true) {
            if (checkWritesCompleted()) {
                return;
            } else {
                LOG.info("Writing is not completed. (current queue size: {}, writes in flight: {}). " +
                                "Waiting 1s and checking again.",
                        state.writer.getQueueSize(), state.writer.getWritesInFlight());
                Thread.sleep(1000);
            }
        }
    }

    private boolean checkWritesCompleted() throws InterruptedException {
        // check 10 times if isWriteApproximatelyComplete returns true - if yes, the assume the writes are completed
        for (int i = 0; i < 10; i++) {
            Thread.sleep(10);
            if (!state.writer.isWriteApproximatelyComplete())
                return false;
        }
        return true;
    }
}