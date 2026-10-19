/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.internal;

import org.apache.flink.connector.base.sink.writer.ResultHandler;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** Guards a Flink result handler against duplicate terminal callbacks. */
final class SingleCompletionResultHandler<T> implements ResultHandler<T> {
    enum Outcome {
        SUCCESS,
        COMPLETED_WITH_DROPS,
        RETRIED,
        FAILED
    }

    private final ResultHandler<T> delegate;
    private final Consumer<Outcome> onOutcome;
    private final AtomicBoolean completed = new AtomicBoolean();
    private final AtomicBoolean recordsDropped = new AtomicBoolean();

    SingleCompletionResultHandler(ResultHandler<T> delegate, Consumer<Outcome> onOutcome) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.onOutcome = Objects.requireNonNull(onOutcome, "onOutcome");
    }

    void markRecordsDropped() {
        recordsDropped.set(true);
    }

    @Override
    public void complete() {
        completeOnce(Outcome.SUCCESS, delegate::complete);
    }

    @Override
    public void completeExceptionally(Exception exception) {
        completeOnce(
                Outcome.FAILED, () -> delegate.completeExceptionally(exception));
    }

    @Override
    public void retryForEntries(List<T> requests) {
        completeOnce(Outcome.RETRIED, () -> delegate.retryForEntries(requests));
    }

    private void completeOnce(Outcome outcome, Runnable completion) {
        if (completed.compareAndSet(false, true)) {
            try {
                onOutcome.accept(
                        outcome == Outcome.SUCCESS && recordsDropped.get()
                                ? Outcome.COMPLETED_WITH_DROPS
                                : outcome);
            } finally {
                completion.run();
            }
        }
    }
}
