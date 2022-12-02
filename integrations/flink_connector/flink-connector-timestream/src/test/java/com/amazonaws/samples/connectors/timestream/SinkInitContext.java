package com.amazonaws.samples.connectors.timestream;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink.InitContext;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorImpl;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;
import org.apache.flink.util.UserCodeClassLoader;
import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.mockito.stubbing.Answer;

import java.util.OptionalLong;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SinkInitContext implements InitContext {
    private static final TestProcessingTimeService processingTimeService = new TestProcessingTimeService();

    private SinkWriterMetricGroup sinkMetricGroup;

    @Override
    public UserCodeClassLoader getUserCodeClassLoader() {
        return null;
    }

    @Override
    public ProcessingTimeService getProcessingTimeService() {
        return new ProcessingTimeService() {
            @Override
            public long getCurrentProcessingTime() {
                return processingTimeService.getCurrentProcessingTime();
            }

            @Override
            public ScheduledFuture<?> registerTimer(long time, ProcessingTimeCallback processingTimerCallback) {
                return processingTimeService.registerTimer(time, processingTimerCallback);
            }
        };
    }

    @Override
    public int getSubtaskId() {
        return 0;
    }

    @Override
    public int getNumberOfParallelSubtasks() {
        return 0;
    }

    /**
     * @return The metric group this writer belongs to.
     */
    @Override
    public SinkWriterMetricGroup metricGroup() {
        SinkWriterMetricGroup sinkMetricGroup = mock(SinkWriterMetricGroup.class);
        when(sinkMetricGroup.counter(anyString()))
                .thenAnswer(((Answer<SimpleCounter>) invocation -> new SimpleCounter()));

        SinkWriterMetricGroup mg = mock(SinkWriterMetricGroup.class, RETURNS_DEEP_STUBS);
        when(mg.addGroup(anyString())).thenReturn(sinkMetricGroup);
        return mg;
    }

    @Override
    public OptionalLong getRestoredCheckpointId() {
        return null;
    }

    @Override
    public SerializationSchema.InitializationContext asSerializationSchemaInitializationContext() {
        return null;
    }

    @Override
    public MailboxExecutor getMailboxExecutor() {
        StreamTaskActionExecutor streamTaskActionExecutor = new StreamTaskActionExecutor() {
            @Override
            public void run(RunnableWithException e) throws Exception {
                e.run();
            }

            @Override
            public <E extends Throwable> void runThrowing(ThrowingRunnable<E> throwingRunnable)
                    throws E {
                throwingRunnable.run();
            }

            @Override
            public <R> R call(Callable<R> callable) throws Exception {
                return callable.call();
            }
        };

        return new MailboxExecutorImpl(
                new TaskMailboxImpl(),
                Integer.MAX_VALUE,
                streamTaskActionExecutor);
    }

    public TestProcessingTimeService getTestProcessingTimeService() {
        return processingTimeService;
    }

    public void setSinkMetricGroup(SinkWriterMetricGroup sinkMetricGroup) {
        this.sinkMetricGroup = sinkMetricGroup;
    }

    public SinkWriterMetricGroup getSinkMetricGroup() {
        return this.sinkMetricGroup;
    }
}