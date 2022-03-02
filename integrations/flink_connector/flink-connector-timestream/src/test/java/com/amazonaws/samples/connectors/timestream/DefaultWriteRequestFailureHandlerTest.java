package com.amazonaws.samples.connectors.timestream;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import software.amazon.awssdk.services.timestreamwrite.model.InternalServerException;
import software.amazon.awssdk.services.timestreamwrite.model.Record;
import software.amazon.awssdk.services.timestreamwrite.model.RejectedRecordsException;
import software.amazon.awssdk.services.timestreamwrite.model.ServiceQuotaExceededException;
import software.amazon.awssdk.services.timestreamwrite.model.ThrottlingException;
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsRequest;

public class DefaultWriteRequestFailureHandlerTest {
    private long countSuccess;
    private long countDrop;
    private long countFail;
    private final Consumer<List<Record>> incSuccess = (records) -> {countSuccess++;};
    private final Consumer<List<Record>> incDrop = (records) -> {countDrop++;};
    private final Consumer<Exception> incFail = (records) -> {countFail++;};
    private final List<Record> fakeRecords = List.of(Record.builder().build());
    private final WriteRecordsRequest fakeRequest =
            WriteRecordsRequest.builder().records(fakeRecords).build();

    private final Exception testException = new RuntimeException("TestException");
    private final ServiceQuotaExceededException overLimitException = ServiceQuotaExceededException.builder().build();
    private final InternalServerException internalServerException = InternalServerException.builder().build();
    private final ThrottlingException throttlingException = ThrottlingException.builder().build();
    private final RejectedRecordsException rejectedRecordsException = RejectedRecordsException.builder().build();
    private final InterruptedException interruptedException = new InterruptedException();

    @Test
    public void testNoFailureSettings() {
        DefaultWriteRequestFailureHandler handler = new DefaultWriteRequestFailureHandler();
        handler.open(incFail, TimestreamSinkConfig.FailureHandlerConfig.builder()
                        .failProcessingOnErrorDefault(false)
                        .failProcessingOnRejectedRecordsException(false).build());

        // retry on internal server error
        handler.handleRetryableException(fakeRecords, fakeRequest, internalServerException, incSuccess, incDrop);
        assertCount(1, 0, 0);

        // retry on throttle
        handler.handleRetryableException(fakeRecords, fakeRequest, throttlingException, incSuccess, incDrop);
        assertCount(2, 0, 0);

        // retry on rejected, but also record drop
        handler.handleRejectedRecordsException(fakeRecords, fakeRequest, rejectedRecordsException, incSuccess, incDrop);
        assertCount(3, 1, 0);

        // for all default exceptions, we retry and also record drop
        handler.handleDefaultException(fakeRecords, fakeRequest, testException, incSuccess, incDrop);
        assertCount(4, 2, 0);
        handler.handleDefaultException(fakeRecords, fakeRequest, overLimitException, incSuccess, incDrop);
        assertCount(5, 3, 0);

        // retry on interrupted
        handler.handleRetryableException(fakeRecords, fakeRequest, interruptedException, incSuccess, incDrop);
        assertCount(6, 3, 0);
    }

    @Test
    public void testAllFailure() {
        DefaultWriteRequestFailureHandler handler = new DefaultWriteRequestFailureHandler();
        handler.open(incFail, TimestreamSinkConfig.FailureHandlerConfig.builder()
                .failProcessingOnErrorDefault(true)
                .failProcessingOnRejectedRecordsException(true).build());

        // retry on internal server error
        handler.handleRetryableException(fakeRecords, fakeRequest, internalServerException, incSuccess, incDrop);
        assertCount(1, 0, 0);

        // retry on throttle
        handler.handleRetryableException(fakeRecords, fakeRequest, throttlingException, incSuccess, incDrop);
        assertCount(2, 0, 0);

        // reject on rejectedRecord
        handler.handleRejectedRecordsException(fakeRecords, fakeRequest, rejectedRecordsException, incSuccess, incDrop);
        assertCount(2, 0, 1);

        // reject on all default exceptions
        handler.handleDefaultException(fakeRecords, fakeRequest, testException, incSuccess, incDrop);
        assertCount(2, 0, 2);
        handler.handleDefaultException(fakeRecords, fakeRequest, overLimitException, incSuccess, incDrop);
        assertCount(2, 0, 3);

        // retry on interrupted
        handler.handleRetryableException(fakeRecords, fakeRequest, interruptedException, incSuccess, incDrop);
        assertCount(3, 0, 3);
    }

    private void assertCount(long countSuccess, long countDrop, long countFail) {
        Assertions.assertEquals(countSuccess, this.countSuccess, "countSuccess check failed");
        Assertions.assertEquals(countDrop, this.countDrop, "countDrop check failed");
        Assertions.assertEquals(countFail, this.countFail, "countFail check failed");
    }
}
