package com.amazonaws.samples.connectors.timestream;

import org.apache.flink.annotation.PublicEvolving;
import software.amazon.awssdk.services.timestreamwrite.model.Record;
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsRequest;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

@PublicEvolving
public interface WriteRequestFailureHandler extends Serializable {

    void open(final Consumer<Exception> fatalExceptionConsumer,
              final TimestreamSinkConfig.FailureHandlerConfig failureHandlerConfig);

    /**
     * Handle a failed {@link WriteRecordsRequest}. Use {@code fatalExceptionConsumer} to cause sink failure.
     *
     * @param requestEntries                   - original entries which were transformed to WriteRecordsRequest
     * @param writeRecordsRequest              - WriteRecordsRequest which caused the failure
     * @param exception                        - the cause of failure - exception from Timestream
     * @param retryOrSuccessCompletionConsumer - consumer of entries which should be put back to buffer and retried. Note: this consumer must be invoked at least with empty list.
     * @param dropCompletionConsumer           - consumer of entries which will be dropped
     */
    void onWriteError(final List<Record> requestEntries,
                      final WriteRecordsRequest writeRecordsRequest,
                      final Exception exception,
                      final Consumer<List<Record>> retryOrSuccessCompletionConsumer,
                      final Consumer<List<Record>> dropCompletionConsumer);
}
