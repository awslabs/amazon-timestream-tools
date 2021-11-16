package com.amazonaws.sample.timestream.multithreaded.util;

import com.amazonaws.sample.timestream.multithreaded.metrics.TimestreamInsertionMetrics;
import com.amazonaws.sample.timestream.multithreaded.metrics.TimestreamWriterMetrics;
import com.amazonaws.sample.timestream.multithreaded.TimestreamWriter;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class LogMetricsPublisher {
    private static final Logger LOG = LoggerFactory.getLogger(LogMetricsPublisher.class);

    private final TimestreamWriter timestreamWriter;
    private TimestreamInsertionMetrics allTimeMetrics = new TimestreamInsertionMetrics();
    private final Duration publishingInterval;

    public LogMetricsPublisher(@NonNull final TimestreamWriter timestreamWriter,
                               @NonNull final Duration publishingInterval) {
        this.timestreamWriter = timestreamWriter;
        this.publishingInterval = publishingInterval;

        final Runnable task = () -> {
            while(true) {
                try {
                    Thread.sleep(publishingInterval.toMillis());
                } catch (InterruptedException e) {
                    LOG.info("Sleep was interrupted.");
                    break;
                }
                print();
            }
        };
        final Thread daemonThread = new Thread(task);
        daemonThread.setDaemon(true);
        daemonThread.start();
    }

    private TimestreamWriterMetrics getAndAggregate() {
        final TimestreamWriterMetrics newMetrics = timestreamWriter.getAndClearMetrics();
        allTimeMetrics.accumulate(newMetrics.getInsertionMetrics());
        return newMetrics;
    }

    public String getNow() {
        return getAndAggregate().toString();
    }

    public String getTotalMetrics() {
        return allTimeMetrics.toString();
    }

    private void print() {
        LOG.info("Metrics over last {}: {}",
                publishingInterval,
                getAndAggregate().toString()
        );
        LOG.info("Total metrics: {}", allTimeMetrics.toString());
    }

    public void clearTotalMetrics() {
        allTimeMetrics = new TimestreamInsertionMetrics();
    }
}
