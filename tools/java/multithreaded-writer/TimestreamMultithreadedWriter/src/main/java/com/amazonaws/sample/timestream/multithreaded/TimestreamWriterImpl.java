package com.amazonaws.sample.timestream.multithreaded;

import com.amazonaws.sample.timestream.multithreaded.metrics.TimestreamInsertionMetrics;
import com.amazonaws.sample.timestream.multithreaded.metrics.TimestreamWriterMetrics;
import com.amazonaws.sample.timestream.multithreaded.util.TimestreamInitializer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient;
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsRequest;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;


public class TimestreamWriterImpl implements TimestreamWriter {
    private static final Logger LOG = LoggerFactory.getLogger(TimestreamWriterImpl.class);
    private static final int WORKER_STATE_STARTED = 1;
    private static final int WORKER_STATE_SHUTDOWN = 2;

    private final TimestreamWriterWorker[] workers;
    private final Thread[] workerThreads;
    private final AtomicInteger workerState = new AtomicInteger(WORKER_STATE_STARTED);
    private final AtomicInteger writesInFlight = new AtomicInteger();
    private final BlockingQueue<WriteRecordsRequest> writeQueue;
    private final TimestreamInsertionMetrics insertionMetrics;

    public TimestreamWriterImpl(final @NonNull TimestreamWriterConfig writerConfig) {
        writerConfig.validate();

        insertionMetrics = new TimestreamInsertionMetrics();
        writeQueue = new ArrayBlockingQueue<>(writerConfig.getQueueSize());
        final TimestreamWriteClient writeClient = writerConfig.getWriteClient();
        final TimestreamInitializer timestreamInitializer = new TimestreamInitializer(
                writerConfig.getCreateTableIfNotExists(), writeClient);

        final int threadPoolSize = writerConfig.getThreadPoolSize();
        LOG.info("Starting {} writer threads...", threadPoolSize);
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setDaemon(true)
                .setUncaughtExceptionHandler((t, e) ->
                        LOG.error("Uncaught Exception occurred in writer thread {}:", t.getName(), e))
                .build();

        workers = new TimestreamWriterWorker[threadPoolSize];
        workerThreads = new Thread[threadPoolSize];
        for (int i = 0; i < threadPoolSize; ++i) {
            workers[i] = new TimestreamWriterWorker(
                    writeQueue, writerConfig, writesInFlight, insertionMetrics, timestreamInitializer);
            workerThreads[i] = threadFactory.newThread(workers[i]);
            workerThreads[i].setName(Thread.currentThread().getName() + "-TS-Writer-Worker-" + i);
            workerThreads[i].start();
        }
        LOG.info("Writer threads started. {} threads started.", threadPoolSize);
    }

    @Override
    public boolean putWriteRecordRequest(@NonNull final WriteRecordsRequest writeRequest) throws InterruptedException {
        if (workerState.get() != WORKER_STATE_STARTED) {
            throw new RuntimeException("Writer only accepts writes when it is in started state!");
        }
        if (writeQueue.remainingCapacity() < 1) {
            return false;
        } else {
            writeQueue.put(writeRequest);
            return true;
        }
    }

    @Override
    public void shutDownGracefully() {
        try {
            LOG.info("Shutting down writer...");
            workerState.compareAndSet(WORKER_STATE_STARTED, WORKER_STATE_SHUTDOWN);
            while (!isWriteApproximatelyComplete()) {
                LOG.info("Writing is not completed. (current queue size: {}, writes in flight: {}). " +
                        "Waiting 1s and checking again.", getQueueSize(), writesInFlight.get());
                Thread.sleep(1000);
            }
            LOG.info("Writing is completed. No records in queue or writes in flight.");
            shutdownWorkers();
            LOG.info("Writer shutdown complete.");
        } catch (InterruptedException e) {
            LOG.error("Shutdown was interrupted: ", e);
        }
    }

    private void shutdownWorkers() throws InterruptedException {
        for (final TimestreamWriterWorker worker : workers) {
            worker.stop();
        }
        for (final Thread workerThread : workerThreads) {
            workerThread.join();
        }
    }

    @Override
    public boolean isWriteApproximatelyComplete() {
        return writeQueue.size() == 0 && writesInFlight.get() == 0;
    }

    @Override
    public int getQueueSize() {
        return writeQueue.size();
    }

    @Override
    public int getWritesInFlight() {
        return writesInFlight.get();
    }

    @Override
    public TimestreamWriterMetrics getAndClearMetrics() {
        return new TimestreamWriterMetrics(insertionMetrics.getAndClear(), writeQueue.size(), writesInFlight.get());
    }
}
