/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.internal;

import org.apache.flink.connector.base.sink.writer.ResultHandler;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SingleCompletionResultHandlerTest {

    @Test
    void completesDelegateWhenOutcomeCallbackThrows() {
        AtomicInteger completions = new AtomicInteger();
        ResultHandler<String> delegate =
                new ResultHandler<>() {
                    @Override
                    public void complete() {
                        completions.incrementAndGet();
                    }

                    @Override
                    public void completeExceptionally(Exception exception) {
                        throw new AssertionError("unexpected exceptional completion", exception);
                    }

                    @Override
                    public void retryForEntries(List<String> requests) {
                        throw new AssertionError("unexpected retry");
                    }
                };
        SingleCompletionResultHandler<String> handler =
                new SingleCompletionResultHandler<>(
                        delegate,
                        ignored -> {
                            throw new IllegalStateException("metrics callback failed");
                        });

        assertThatThrownBy(handler::complete)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("metrics callback failed");
        assertThat(completions).hasValue(1);

        handler.complete();

        assertThat(completions).hasValue(1);
    }
}
