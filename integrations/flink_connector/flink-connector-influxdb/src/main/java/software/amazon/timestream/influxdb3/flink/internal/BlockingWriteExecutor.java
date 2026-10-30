/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.internal;

import software.amazon.timestream.influxdb3.flink.InfluxDB3SinkException;

import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** Executes blocking writes with bounded concurrency and independently enforced deadlines. */
final class BlockingWriteExecutor {
    private static final long MIN_CANCELLATION_CONFIRMATION_MILLIS = 1L;
    private static final long MAX_CANCELLATION_CONFIRMATION_MILLIS = 1_000L;
    private static final AtomicInteger POOL_SEQUENCE = new AtomicInteger();

    private final InfluxDB3SinkConfig config;
    private final InfluxDBClientManager clientManager;
    private final InfluxDBSinkMetrics metrics;
    private final ThreadPoolExecutor writeExecutor;
    private final ScheduledThreadPoolExecutor timeoutScheduler;
    private final ScheduledThreadPoolExecutor retryScheduler;
    private final long cancellationConfirmationMillis;

    BlockingWriteExecutor(
            InfluxDB3SinkConfig config,
            InfluxDBClientManager clientManager,
            InfluxDBSinkMetrics metrics) {
        this.config = config;
        this.clientManager = clientManager;
        this.metrics = metrics;
        this.cancellationConfirmationMillis = cancellationConfirmationMillis(config);

        int poolId = POOL_SEQUENCE.incrementAndGet();
        this.writeExecutor =
                new ThreadPoolExecutor(
                        config.getMaxInFlightRequests(),
                        config.getMaxInFlightRequests(),
                        0L,
                        TimeUnit.MILLISECONDS,
                        new ArrayBlockingQueue<>(config.getMaxInFlightRequests()),
                        namedDaemonThreadFactory("influxdb3-write-" + poolId + "-"),
                        new ThreadPoolExecutor.AbortPolicy());
        this.timeoutScheduler =
                new ScheduledThreadPoolExecutor(
                        config.getMaxInFlightRequests(),
                        namedDaemonThreadFactory("influxdb3-timeout-" + poolId + "-"));
        configureScheduler(timeoutScheduler);
        this.retryScheduler =
                new ScheduledThreadPoolExecutor(
                        1, namedDaemonThreadFactory("influxdb3-retry-" + poolId + "-"));
        configureScheduler(retryScheduler);
    }

    void submit(
            List<InfluxDB3WriteRequest> requestEntries,
            List<String> records,
            SingleCompletionResultHandler<InfluxDB3WriteRequest> resultHandler,
            WriteFailureCallback failureCallback) {
        long payloadSizeInBytes = metrics.batchSubmitted(records);

        final InfluxDBClientManager.ClientLease client;
        try {
            client = clientManager.acquire();
        } catch (RuntimeException error) {
            resultHandler.completeExceptionally(
                    new InfluxDB3SinkException(
                            "Unable to acquire the active InfluxDB client", error));
            return;
        }

        WriteAttempt attempt =
                new WriteAttempt(
                        client,
                        requestEntries,
                        records,
                        resultHandler,
                        failureCallback,
                        payloadSizeInBytes,
                        System.nanoTime());
        metrics.writeRequestStarted();
        try {
            ScheduledFuture<?> timeoutFuture =
                    timeoutScheduler.schedule(
                            attempt::timeout,
                            config.getWriteTimeout().toMillis(),
                            TimeUnit.MILLISECONDS);
            attempt.setTimeoutFuture(timeoutFuture);
        } catch (RejectedExecutionException error) {
            attempt.abort(
                    new InfluxDB3SinkException(
                            "InfluxDB write timeout could not be scheduled", error));
            return;
        }
        try {
            writeExecutor.execute(attempt.task);
        } catch (RejectedExecutionException error) {
            attempt.abort(
                    new InfluxDB3SinkException(
                            "InfluxDB write executor rejected a request", error));
        }
    }

    void scheduleRetry(
            List<InfluxDB3WriteRequest> retry,
            Duration delay,
            SingleCompletionResultHandler<InfluxDB3WriteRequest> resultHandler) {
        retryScheduler.schedule(
                () -> resultHandler.retryForEntries(retry),
                delay.toMillis(),
                TimeUnit.MILLISECONDS);
    }

    /**
     * Shuts down all executors against one shared requestTimeout deadline. Each executor may
     * additionally wait up to the cancellation confirmation period after forced shutdown.
     */
    Exception close() {
        long closeDeadlineNanos =
                System.nanoTime()
                        + TimeUnit.MILLISECONDS.toNanos(config.getRequestTimeout().toMillis());
        writeExecutor.shutdown();
        boolean terminated = false;
        Exception failure = null;
        try {
            terminated =
                    writeExecutor.awaitTermination(
                            remainingNanos(closeDeadlineNanos), TimeUnit.NANOSECONDS);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            failure = error;
        }
        if (!terminated) {
            writeExecutor.shutdownNow();
            if (failure == null) {
                try {
                    terminated =
                            writeExecutor.awaitTermination(
                                    cancellationConfirmationMillis, TimeUnit.MILLISECONDS);
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    failure = error;
                }
            }
            if (!terminated && failure == null) {
                failure =
                        new InfluxDB3SinkException(
                                "InfluxDB write executor did not terminate after cancellation");
            }
        }

        failure =
                closeScheduler(
                        timeoutScheduler,
                        closeDeadlineNanos,
                        cancellationConfirmationMillis,
                        "timeout",
                        failure);
        failure =
                closeScheduler(
                        retryScheduler,
                        closeDeadlineNanos,
                        cancellationConfirmationMillis,
                        "retry",
                        failure);
        return failure;
    }

    void shutdownImmediately() {
        writeExecutor.shutdownNow();
        timeoutScheduler.shutdownNow();
        retryScheduler.shutdownNow();
    }

    @FunctionalInterface
    interface WriteFailureCallback {
        void handle(
                InfluxDBClientManager.ClientGeneration client,
                List<InfluxDB3WriteRequest> requests,
                Exception error,
                SingleCompletionResultHandler<InfluxDB3WriteRequest> resultHandler);
    }

    private enum WriteAttemptState {
        PENDING,
        COMPLETED,
        TIMED_OUT,
        ABORTED
    }

    private final class WriteAttempt {
        private final InfluxDBClientManager.ClientLease client;
        private final List<InfluxDB3WriteRequest> requestEntries;
        private final List<String> records;
        private final SingleCompletionResultHandler<InfluxDB3WriteRequest> resultHandler;
        private final WriteFailureCallback failureCallback;
        private final long payloadSizeInBytes;
        private final long startNanos;
        private final TrackedWriteTask task;
        private final AtomicReference<WriteAttemptState> state =
                new AtomicReference<>(WriteAttemptState.PENDING);
        private final AtomicBoolean timeoutOutcomeResolved = new AtomicBoolean();
        private final AtomicBoolean resourcesReleased = new AtomicBoolean();
        private final AtomicReference<ScheduledFuture<?>> timeoutFuture = new AtomicReference<>();
        private final AtomicReference<ScheduledFuture<?>> cancellationCheckFuture =
                new AtomicReference<>();

        private WriteAttempt(
                InfluxDBClientManager.ClientLease client,
                List<InfluxDB3WriteRequest> requestEntries,
                List<String> records,
                SingleCompletionResultHandler<InfluxDB3WriteRequest> resultHandler,
                WriteFailureCallback failureCallback,
                long payloadSizeInBytes,
                long startNanos) {
            this.client = client;
            this.requestEntries = requestEntries;
            this.records = records;
            this.resultHandler = resultHandler;
            this.failureCallback = failureCallback;
            this.payloadSizeInBytes = payloadSizeInBytes;
            this.startNanos = startNanos;
            this.task = new TrackedWriteTask(this::execute);
        }

        private void setTimeoutFuture(ScheduledFuture<?> scheduledTimeout) {
            timeoutFuture.set(scheduledTimeout);
            if (state.get() != WriteAttemptState.PENDING) {
                scheduledTimeout.cancel(false);
            }
        }

        private void execute() {
            Exception failure = null;
            Throwable fatalFailure = null;
            try {
                client.client().writeRecords(records);
            } catch (Exception error) {
                failure = error;
            } catch (Throwable error) {
                fatalFailure = error;
            }

            if (!state.compareAndSet(
                    WriteAttemptState.PENDING, WriteAttemptState.COMPLETED)) {
                return;
            }
            cancelTimeout();
            metrics.writeFinished(startNanos);
            if (fatalFailure != null) {
                metrics.recordsSendErrors(requestEntries.size());
                resultHandler.completeExceptionally(
                        new InfluxDB3SinkException(
                                "Unexpected error from the InfluxDB write client", fatalFailure));
            } else if (failure == null) {
                metrics.recordsWritten(requestEntries.size());
                metrics.bytesWritten(payloadSizeInBytes);
                resultHandler.complete();
            } else {
                metrics.writeAttemptFailed(failure);
                failureCallback.handle(
                        client.generation(), requestEntries, failure, resultHandler);
            }
        }

        private void timeout() {
            if (!state.compareAndSet(
                    WriteAttemptState.PENDING, WriteAttemptState.TIMED_OUT)) {
                return;
            }
            metrics.writeTimeout();
            metrics.writeFinished(startNanos);
            boolean cancellationAccepted = task.cancel(true);
            if (cancellationAccepted && !task.hasRunEntered()) {
                writeExecutor.remove(task);
                releaseResources();
                completeConfirmedTimeout();
                return;
            }
            if (task.hasRunExited()) {
                completeConfirmedTimeout();
                return;
            }
            scheduleCancellationCheck();
        }

        private void abort(Exception error) {
            if (!state.compareAndSet(
                    WriteAttemptState.PENDING, WriteAttemptState.ABORTED)) {
                return;
            }
            cancelTimeout();
            cancelCancellationCheck();
            boolean cancellationAccepted = task.cancel(true);
            if (cancellationAccepted && !task.hasRunEntered()) {
                writeExecutor.remove(task);
                releaseResources();
            } else if (task.hasRunExited()) {
                releaseResources();
            }
            metrics.writeFinished(startNanos);
            resultHandler.completeExceptionally(error);
        }

        private void scheduleCancellationCheck() {
            try {
                ScheduledFuture<?> scheduledCheck =
                        timeoutScheduler.schedule(
                                this::verifyCancellation,
                                cancellationConfirmationMillis,
                                TimeUnit.MILLISECONDS);
                cancellationCheckFuture.set(scheduledCheck);
                if (timeoutOutcomeResolved.get()) {
                    scheduledCheck.cancel(false);
                }
            } catch (RejectedExecutionException error) {
                if (task.hasRunExited()) {
                    completeConfirmedTimeout();
                } else {
                    completeUnconfirmedTimeout(error);
                }
            }
        }

        private void verifyCancellation() {
            if (task.hasRunExited()) {
                completeConfirmedTimeout();
            } else {
                completeUnconfirmedTimeout(null);
            }
        }

        private void onTaskExited() {
            releaseResources();
            if (state.get() == WriteAttemptState.TIMED_OUT) {
                completeConfirmedTimeout();
            }
        }

        private void completeConfirmedTimeout() {
            if (!timeoutOutcomeResolved.compareAndSet(false, true)) {
                return;
            }
            cancelCancellationCheck();
            failureCallback.handle(
                    client.generation(),
                    requestEntries,
                    writeTimeoutException(),
                    resultHandler);
        }

        private void completeUnconfirmedTimeout(Exception verificationError) {
            if (task.hasRunExited()) {
                completeConfirmedTimeout();
                return;
            }
            if (!timeoutOutcomeResolved.compareAndSet(false, true)) {
                return;
            }
            metrics.recordsSendErrors(requestEntries.size());
            InfluxDB3SinkException failure =
                    new InfluxDB3SinkException(
                            "InfluxDB write exceeded "
                                    + config.getWriteTimeout()
                                    + " and did not terminate within "
                                    + cancellationConfirmationMillis
                                    + " ms after cancellation",
                            writeTimeoutException());
            if (verificationError != null) {
                failure.addSuppressed(verificationError);
            }
            resultHandler.completeExceptionally(failure);
        }

        private HttpTimeoutException writeTimeoutException() {
            return new HttpTimeoutException(
                    "InfluxDB write exceeded " + config.getWriteTimeout());
        }

        private void cancelTimeout() {
            ScheduledFuture<?> scheduledTimeout = timeoutFuture.get();
            if (scheduledTimeout != null) {
                scheduledTimeout.cancel(false);
            }
        }

        private void cancelCancellationCheck() {
            ScheduledFuture<?> scheduledCheck = cancellationCheckFuture.get();
            if (scheduledCheck != null) {
                scheduledCheck.cancel(false);
            }
        }

        private void releaseResources() {
            if (!resourcesReleased.compareAndSet(false, true)) {
                return;
            }
            metrics.writeRequestFinished();
            client.release();
        }

        private final class TrackedWriteTask extends FutureTask<Void> {
            private final AtomicBoolean runEntered = new AtomicBoolean();
            private final AtomicBoolean runExited = new AtomicBoolean();

            private TrackedWriteTask(Runnable command) {
                super(command, null);
            }

            @Override
            public void run() {
                runEntered.set(true);
                try {
                    super.run();
                } finally {
                    runExited.set(true);
                    onTaskExited();
                }
            }

            private boolean hasRunEntered() {
                return runEntered.get();
            }

            private boolean hasRunExited() {
                return runExited.get();
            }
        }
    }

    // Flink's requestTimeout must cover writeTimeout + cancellation confirmation + retry backoff.
    // The confirmation wait gets strictly less than half of the remaining headroom; the rest is
    // slack for scheduler and thread jitter so Flink's deadline never fires before ours resolves.
    // The builder guarantees at least 2 ms of headroom, so the 1 ms minimum still fits.
    private static long cancellationConfirmationMillis(InfluxDB3SinkConfig config) {
        long headroomMillis =
                config.getRequestTimeout().toMillis()
                        - config.getWriteTimeout().toMillis()
                        - config.getMaxRetryBackoff().toMillis();
        long confirmationMillis =
                Math.max(
                        MIN_CANCELLATION_CONFIRMATION_MILLIS,
                        (headroomMillis - 1L) / 2L);
        return Math.min(MAX_CANCELLATION_CONFIRMATION_MILLIS, confirmationMillis);
    }

    private static void configureScheduler(ScheduledThreadPoolExecutor scheduler) {
        scheduler.setRemoveOnCancelPolicy(true);
        scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(true);
    }

    private static Exception closeScheduler(
            ScheduledThreadPoolExecutor scheduler,
            long closeDeadlineNanos,
            long confirmationMillis,
            String schedulerName,
            Exception failure) {
        scheduler.shutdown();
        boolean terminated = scheduler.isTerminated();
        if (!terminated && !Thread.currentThread().isInterrupted()) {
            try {
                terminated =
                        scheduler.awaitTermination(
                                remainingNanos(closeDeadlineNanos), TimeUnit.NANOSECONDS);
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                failure = appendFailure(failure, error);
            }
        }
        if (!terminated) {
            // Forced shutdown only counts as a failure if it dropped queued work or a running
            // callback did not stop; an idle scheduler may still be retiring its threads.
            boolean droppedWork = !scheduler.shutdownNow().isEmpty();
            if (!droppedWork && !Thread.currentThread().isInterrupted()) {
                try {
                    terminated =
                            scheduler.awaitTermination(
                                    confirmationMillis, TimeUnit.MILLISECONDS);
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    failure = appendFailure(failure, error);
                }
            }
            if (!terminated && !Thread.currentThread().isInterrupted()) {
                failure =
                        appendFailure(
                                failure,
                                new InfluxDB3SinkException(
                                        "InfluxDB "
                                                + schedulerName
                                                + " scheduler did not terminate "
                                                + "before its close timeout"));
            }
        }
        return failure;
    }

    private static long remainingNanos(long deadlineNanos) {
        return Math.max(0L, deadlineNanos - System.nanoTime());
    }

    private static Exception appendFailure(Exception failure, Exception additionalFailure) {
        if (failure == null) {
            return additionalFailure;
        }
        failure.addSuppressed(additionalFailure);
        return failure;
    }

    private static ThreadFactory namedDaemonThreadFactory(String prefix) {
        AtomicInteger sequence = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable, prefix + sequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }
}
