package com.amazonaws.sample.timestream.multithreaded;

import com.amazonaws.sample.timestream.multithreaded.metrics.TimestreamInsertionMetrics;
import com.amazonaws.sample.timestream.multithreaded.util.TimestreamInitializer;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient;
import software.amazon.awssdk.services.timestreamwrite.model.*;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TimestreamWriterWorker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(TimestreamWriterWorker.class);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(1000);

    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private final BlockingQueue<WriteRecordsRequest> queue;
    private final TimestreamWriteClient writeClient;
    private final AtomicInteger writesInFlight;
    private final TimestreamInsertionMetrics insertionMetrics;
    private final TimestreamInitializer timestreamInitializer;
    private final long maxRetryDurationMs;

    public TimestreamWriterWorker(@NonNull final BlockingQueue<WriteRecordsRequest> queue,
                                  @NonNull final TimestreamWriterConfig writerConfig,
                                  @NonNull final AtomicInteger writesInFlight,
                                  @NonNull final TimestreamInsertionMetrics insertionMetrics,
                                  @NonNull final TimestreamInitializer timestreamInitializer) {
        this.queue = queue;
        this.writeClient = writerConfig.getWriteClient();
        this.maxRetryDurationMs = writerConfig.getMaxRetryDurationMs();
        this.writesInFlight = writesInFlight;
        this.insertionMetrics = insertionMetrics;
        this.timestreamInitializer = timestreamInitializer;
    }

    @Override
    public void run() {
        try {
            while (isRunning.get()) {
                final WriteRecordsRequest writeRecord = queue.poll(POLL_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                if (writeRecord == null) // Empty poll? Try again.
                    continue;
                // If isWriteApproximatelyComplete checks happens here, it will return true,
                // where it should return false. This is ok if we need approximate check.
                writesInFlight.incrementAndGet();
                try {
                    safelyProcessWriteRecord(writeRecord);
                } finally {
                    writesInFlight.decrementAndGet();
                }
            }
        } catch (final InterruptedException e) {
            LOG.info("Current thread was interrupted. Exiting.");
            Thread.currentThread().interrupt();
        }
    }

    private void safelyProcessWriteRecord(@NonNull final WriteRecordsRequest writeRecord) {
        try {
            final long recordPollTime = System.currentTimeMillis();
            do {
                final TimestreamInsertionMetrics currentWriteMetrics = insertToTimestream(writeRecord);
                // if we won't retry inserting this record, capture write latency
                if (currentWriteMetrics.getNonSDKReties().get() == 0) {
                    currentWriteMetrics.recordLatencyMs(System.currentTimeMillis() - recordPollTime);
                    insertionMetrics.accumulate(currentWriteMetrics);
                    break;
                }

                // In case of retryable errors we want to retry inserting the record to Timestream,
                // in addition to SDK retry up to MAX_RETRY_DURATION_MS.
                final long recordMillisAge = System.currentTimeMillis() - recordPollTime;
                if (recordMillisAge > maxRetryDurationMs) {
                    LOG.error("WriteRecordsRequest age exceeded {} - was {}.",
                            Duration.ofMillis(maxRetryDurationMs), Duration.ofMillis(recordMillisAge));
                    dropWrite(currentWriteMetrics, writeRecord);
                    currentWriteMetrics.recordLatencyMs(System.currentTimeMillis() - recordPollTime);
                    insertionMetrics.accumulate(currentWriteMetrics);
                    break;
                }

                //accumulate without latency, for retry
                insertionMetrics.accumulate(currentWriteMetrics);
            } while (true);
        } catch (final Exception e) {
            LOG.error("Unexpected exception occurred while processing WriteRecordsRequest. This shouldn't happen: ", e);
        }
    }

    private void dropWrite(@NonNull final TimestreamInsertionMetrics metrics,
                           @NonNull final WriteRecordsRequest writeRecord) {
        LOG.error("Dropping WriteRecordsRequest: {}", writeRecord);
        metrics.getRecordsDrop().set(writeRecord.records().size());
        metrics.getWritesDrop().set(1);
    }

    // Tries writing WriteRecordsRequest to Timestream. Returns metrics from single write call.
    private TimestreamInsertionMetrics insertToTimestream(@NonNull final WriteRecordsRequest writeRecord) {
        final TimestreamInsertionMetrics metrics = new TimestreamInsertionMetrics();
        int batchSize = writeRecord.records().size();
        try {
            final WriteRecordsResponse writeRecordsResult = writeClient.writeRecords(writeRecord);
            LOG.debug("WriteRecords Status: {}, Batch Size: {}",
                    writeRecordsResult.sdkHttpResponse().statusCode(), batchSize);
            metrics.getWritesSuccess().set(1);
            metrics.getRecordsSuccess().set(batchSize);
        } catch (final InternalServerException e) {
            LOG.error("InternalServerException occurred while inserting to Timestream. Details: " +
                    "Status Code: {}, Request ID: {}", e.statusCode(), e.requestId());
            metrics.getNonSDKReties().set(1);
            metrics.getWritesErrorAll().set(1);
            metrics.getWritesErrorInternalServer().set(1);
        } catch (final ThrottlingException e) {
            LOG.error("ThrottlingException occurred while inserting to Timestream. Details: " +
                    "Status Code: {}, Request ID: {}", e.statusCode(), e.requestId());
            metrics.getNonSDKReties().set(1);
            metrics.getWritesErrorAll().set(1);
            metrics.getWritesErrorThrottling().set(1);
        } catch (final ResourceNotFoundException e) {
            metrics.getNonSDKReties().set(1);
            metrics.getWritesErrorAll().set(1);
            metrics.getWritesResourceNotFound().set(1);
            timestreamInitializer.initialize(writeRecord.databaseName(), writeRecord.tableName());
        } catch (final ValidationException e) {
            // NonSDKReties is set to 0 - do not retry record
            metrics.getRecordsRejectAll().set(batchSize);
            metrics.getWritesErrorAll().set(1);
            LOG.warn("Got exception from Timestream: ", e);
            dropWrite(metrics, writeRecord);
        } catch (final RejectedRecordsException e) {
            final List<RejectedRecord> rejectedRecords = e.rejectedRecords();
            final int numberOfRejectedRecords = rejectedRecords.size();

            metrics.getRecordsSuccess().set(batchSize - numberOfRejectedRecords);
            metrics.getRecordsRejectAll().set(numberOfRejectedRecords);
            metrics.getWritesSuccess().set(1);

            LOG.warn("Timestream rejected {} records.", rejectedRecords.size());
            LOG.debug("\tDiscarded Record Common Attributes Data: -> {}", writeRecord.commonAttributes());
            for (RejectedRecord rejectedRecord : rejectedRecords) {
                final int index = rejectedRecord.recordIndex();

                LOG.warn("\tDiscarded Record: -> {}", rejectedRecord);
                LOG.debug("\tDiscarded Record Data: -> {}", writeRecord.records().get(index));
                // Unfortunately, for now (08/2021) this is the best way to distinguish different rejection reasons.
                if (rejectedRecord.reason().contains("A higher version is required to update the measure value") ||
                        rejectedRecord.reason().contains("A higher record version must be specified in order to update the measure value")) {
                    metrics.getRecordsRejectInvalidVersion().incrementAndGet();
                }
            }
        } catch (final Exception e) {
            LOG.error("Unknown error occurred while inserting to Timestream. Error: ", e);
            metrics.getNonSDKReties().set(1);
            metrics.getWritesErrorAll().set(1);
        }
        return metrics;
    }

    public void stop() {
        isRunning.set(false);
    }
}
