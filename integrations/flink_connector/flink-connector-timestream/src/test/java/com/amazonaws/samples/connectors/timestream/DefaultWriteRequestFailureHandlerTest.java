package com.amazonaws.samples.connectors.timestream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.endpointdiscovery.EndpointDiscoveryFailedException;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkInterruptedException;
import software.amazon.awssdk.crt.http.HttpException;
import software.amazon.awssdk.services.timestreamwrite.model.AccessDeniedException;
import software.amazon.awssdk.services.timestreamwrite.model.InternalServerException;
import software.amazon.awssdk.services.timestreamwrite.model.Record;
import software.amazon.awssdk.services.timestreamwrite.model.RejectedRecordsException;
import software.amazon.awssdk.services.timestreamwrite.model.ResourceNotFoundException;
import software.amazon.awssdk.services.timestreamwrite.model.ServiceQuotaExceededException;
import software.amazon.awssdk.services.timestreamwrite.model.ThrottlingException;
import software.amazon.awssdk.services.timestreamwrite.model.TimestreamWriteException;
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsRequest;

import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.function.Consumer;

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

    private final List<Exception> retryableExceptionExamples = List.of(
            // typical exceptions
            internalServerException,
            throttlingException,
            new IOException(),

            // SdkClientExceptions caused by timeouts
            SdkClientException.builder().cause(ApiCallTimeoutException.builder().build()).build(),
            SdkClientException.builder().cause(ApiCallAttemptTimeoutException.builder().build()).build(),
            SdkClientException.builder().cause(new SdkInterruptedException()).build(),
            SdkClientException.builder().cause(new HttpException(400)).build(),
            SdkClientException.builder().cause(new HttpException(404)).build(),
            SdkClientException.builder().cause(new HttpException(500)).build(),
            SdkClientException.builder().cause(new SocketTimeoutException()).build(),
            SdkClientException.builder().cause(new SocketException()).build(),

            // EndpointDiscoveryFailedException caused by timeouts
            EndpointDiscoveryFailedException.builder().cause(ApiCallTimeoutException.builder().build()).build(),
            EndpointDiscoveryFailedException.builder().cause(ApiCallAttemptTimeoutException.builder().build()).build(),
            EndpointDiscoveryFailedException.builder().cause(new SdkInterruptedException()).build(),
            EndpointDiscoveryFailedException.builder().cause(new HttpException(400)).build(),
            EndpointDiscoveryFailedException.builder().cause(new HttpException(404)).build(),
            EndpointDiscoveryFailedException.builder().cause(new HttpException(500)).build(),
            EndpointDiscoveryFailedException.builder().cause(new SocketTimeoutException()).build(),
            EndpointDiscoveryFailedException.builder().cause(new SocketException()).build()
    );

    private final List<Exception> nonRetryableExceptionExamples = List.of(
            // typical exceptions
            AccessDeniedException.builder().build(),

            // invalid security token
            TimestreamWriteException.builder()
                    .statusCode(403)
                    .awsErrorDetails(AwsErrorDetails.builder()
                            .errorCode("UnrecognizedClientException")
                            .errorMessage("The security token included in the request is invalid.")
                            .build())
                    .build(),

            // SdkClientExceptions caused by non-retryable exceptions
            SdkClientException.builder().cause(AccessDeniedException.builder().build()).build(),
            SdkClientException.builder().cause(ResourceNotFoundException.builder().build()).build(),

            // EndpointDiscoveryFailedException caused by non-retryable exceptions
            EndpointDiscoveryFailedException.builder().cause(AccessDeniedException.builder().build()).build(),
            EndpointDiscoveryFailedException.builder().cause(ResourceNotFoundException.builder().build()).build()
    );

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

    @Test
    public void testIsRetryableExceptionCheck() {
        for (final Exception exc : retryableExceptionExamples) {
            Assertions.assertTrue(DefaultWriteRequestFailureHandler.checkIsRetryableException(exc),
                    "The following exception should have been evaluated as retryable: "
                            + getExceptionMessageWithCause(exc)
            );
        }

        for (final Exception exc : nonRetryableExceptionExamples) {
            Assertions.assertFalse(DefaultWriteRequestFailureHandler.checkIsRetryableException(exc),
                    "The following exception should have been evaluated as non-retryable: "
                            + getExceptionMessageWithCause(exc)
            );
        }
    }

    private static String getExceptionMessageWithCause(Throwable e) {
        StringBuilder sb = new StringBuilder();
        sb.append(e);
        Throwable t = e.getCause();
        while (t != null) {
            sb.append("\nCaused by: ").append(t);
            t = t.getCause();
        }
        return sb.toString();
    }

    private void assertCount(long countSuccess, long countDrop, long countFail) {
        Assertions.assertEquals(countSuccess, this.countSuccess, "countSuccess check failed");
        Assertions.assertEquals(countDrop, this.countDrop, "countDrop check failed");
        Assertions.assertEquals(countFail, this.countFail, "countFail check failed");
    }
}
