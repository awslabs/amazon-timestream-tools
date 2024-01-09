package com.amazonaws.samples.connectors.timestream;

import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.awscore.internal.AwsErrorCode;
import software.amazon.awssdk.core.endpointdiscovery.EndpointDiscoveryFailedException;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.core.exception.RetryableException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.exception.SdkInterruptedException;
import software.amazon.awssdk.crt.http.HttpException;
import software.amazon.awssdk.services.timestreamwrite.model.AccessDeniedException;
import software.amazon.awssdk.services.timestreamwrite.model.ConflictException;
import software.amazon.awssdk.services.timestreamwrite.model.InternalServerException;
import software.amazon.awssdk.services.timestreamwrite.model.InvalidEndpointException;
import software.amazon.awssdk.services.timestreamwrite.model.Record;
import software.amazon.awssdk.services.timestreamwrite.model.RejectedRecord;
import software.amazon.awssdk.services.timestreamwrite.model.RejectedRecordsException;
import software.amazon.awssdk.services.timestreamwrite.model.ResourceNotFoundException;
import software.amazon.awssdk.services.timestreamwrite.model.ServiceQuotaExceededException;
import software.amazon.awssdk.services.timestreamwrite.model.ThrottlingException;
import software.amazon.awssdk.services.timestreamwrite.model.ValidationException;
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsRequest;

import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

public class DefaultWriteRequestFailureHandler implements WriteRequestFailureHandler {
    private static final long serialVersionUID = -818223387498623035L;

    private static final Set<String> RETRYABLE_ERROR_CODES = Set.of("InternalFailure", "ServiceUnavailable");
    private static final Set<Integer> RETRYABLE_HTTP_STATUS_CODES = Set.of(500, 502, 503, 509);

    private static final Logger LOG = LoggerFactory.getLogger(DefaultWriteRequestFailureHandler.class);
    private boolean printFailedRequests;
    private boolean failProcessingOnErrorDefault;
    private boolean failProcessingOnValidationException;
    private boolean failProcessingOnRejectedRecordsException;

    @FunctionalInterface
    public interface ExceptionConsumer {
        void accept(
                List<Record> requestEntries,
                    WriteRecordsRequest writeRecordsRequest,
                    Exception exception,
                    Consumer<List<Record>> retryOrSuccessCompletionConsumer,
                    Consumer<List<Record>> dropCompletionConsumer);
    }

    Consumer<Exception> fatalExceptionConsumer;
    final transient HashMap<Class<? extends Throwable>, ExceptionConsumer>
            exceptionTypeToExceptionHandleMethod;

    public DefaultWriteRequestFailureHandler() {
        exceptionTypeToExceptionHandleMethod = new HashMap<>();
        // For retryable cases check @{code checkIsRetryableException} method.
        // TimestreamWriteException subclasses, special cases:
        exceptionTypeToExceptionHandleMethod.put(RejectedRecordsException.class, this::handleRejectedRecordsException);
        exceptionTypeToExceptionHandleMethod.put(ValidationException.class, this::handleValidationException);


        // TimestreamWriteException subclasses, default behavior (based on configuration - fail or drop):
        exceptionTypeToExceptionHandleMethod.put(AccessDeniedException.class, this::handleDefaultException);
        exceptionTypeToExceptionHandleMethod.put(ConflictException.class, this::handleDefaultException);
        exceptionTypeToExceptionHandleMethod.put(InvalidEndpointException.class, this::handleDefaultException);
        exceptionTypeToExceptionHandleMethod.put(ResourceNotFoundException.class, this::handleDefaultException);
        exceptionTypeToExceptionHandleMethod.put(ServiceQuotaExceededException.class, this::handleDefaultException);
        // see also handleDefaultException
    }

    private void handleValidationException(final List<Record> records,
                                           final WriteRecordsRequest writeRecordsRequest,
                                           final Exception e,
                                           final Consumer<List<Record>> retryOrSuccessCompletionConsumer,
                                           final Consumer<List<Record>> dropCompletionConsumer) {
        LOG.error("Validation of WriteRecordsRequest failed.", e);
        if (printFailedRequests) {
            LOG.info("\tWriteRecordsRequest Data: -> {}", writeRecordsRequest);
        }

        if (failProcessingOnValidationException) {
            LOG.info("'failProcessingOnValidationException' is {}, therefore failing without retry.", failProcessingOnValidationException);
            fatalExceptionConsumer.accept(e); // fail, no retry
        } else {
            LOG.info("'failProcessingOnValidationException' is {}, therefore dropping records..", failProcessingOnValidationException);
            dropCompletionConsumer.accept(records); // mark every record as dropped - unfortunately we can't selectively retry records without validation errors
            retryOrSuccessCompletionConsumer.accept(Collections.emptyList()); // notify request completion
        }
    }

    void handleRetryableException(final List<Record> records,
                                       final WriteRecordsRequest writeRecordsRequest,
                                       final Exception e,
                                       final Consumer<List<Record>> retryOrSuccessCompletionConsumer,
                                       final Consumer<List<Record>> dropCompletionConsumer) {
        if (e instanceof AwsServiceException) {
            // if it's AwsServiceException, log it along StatusCode and RequestId
            AwsServiceException se = (AwsServiceException) e;
            // no reason to log stack trace - there won't be anything what can help
            LOG.error(String.format("Retryable '%s' occurred while inserting to Timestream. Records insertion will be retried. " +
                    "Details: Status Code: %s, Request ID: %s", e.getClass().getSimpleName(), se.statusCode(), se.requestId()), e);
        } else {
            LOG.error(String.format("Retryable '%s' occurred while inserting to Timestream. Records insertion will be retried.",
                    e.getClass().getSimpleName()), e);
        }
        retryOrSuccessCompletionConsumer.accept(records); // retry all records
    }

    void handleRejectedRecordsException(final List<Record> records,
                                        final WriteRecordsRequest writeRecordsRequest,
                                        final Exception e,
                                        final Consumer<List<Record>> retryOrSuccessCompletionConsumer,
                                        final Consumer<List<Record>> dropCompletionConsumer) {
        RejectedRecordsException rre = (RejectedRecordsException) e;
        final List<RejectedRecord> rejectedRecords = rre.rejectedRecords();

        LOG.warn("Timestream rejected {} records.", rejectedRecords.size());
        if (printFailedRequests) {
            LOG.info("\tRejected Record Common Attributes Data: -> {}", writeRecordsRequest.commonAttributes());
        }

        final ArrayList<Record> rejectedOriginalRecords = new ArrayList<>(rejectedRecords.size());
        for (RejectedRecord rejectedRecord : rejectedRecords) {
            final int index = rejectedRecord.recordIndex();
            LOG.warn("\tRejected Record: -> {}", rejectedRecord);

            final Record originalRecord = writeRecordsRequest.records().get(index);
            rejectedOriginalRecords.add(originalRecord);

            if (printFailedRequests) {
                LOG.info("\tRejected Record Data: -> {}", originalRecord);
            }
            LOG.warn("\tRejected Record Reason: -> {}", rejectedRecord.reason());
        }

        if (failProcessingOnRejectedRecordsException) {
            LOG.info("'failProcessingOnRejectedRecordsException' is {}, therefore failing without retry.", failProcessingOnRejectedRecordsException);
            fatalExceptionConsumer.accept(e); // fail, no retry
        } else {
            LOG.info("'failProcessingOnRejectedRecordsException' is {}, therefore dropping records..", failProcessingOnRejectedRecordsException);
            dropCompletionConsumer.accept(rejectedOriginalRecords); // mark rejected records as dropped
            retryOrSuccessCompletionConsumer.accept(Collections.emptyList()); // notify request completion
        }
    }

    void handleDefaultException(final List<Record> records,
                                final WriteRecordsRequest writeRecordsRequest,
                                final Exception e,
                                final Consumer<List<Record>> retryOrSuccessCompletionConsumer,
                                final Consumer<List<Record>> dropCompletionConsumer) {
        final Class<? extends Exception> exceptionClass = e.getClass();
        if (exceptionTypeToExceptionHandleMethod.containsKey(exceptionClass)) {
            // if it's known exception - no need to log the exception stack trace
            LOG.error("Error occurred while inserting to Timestream: {}: {}", exceptionClass.getSimpleName(), e.getMessage());
        } else {
            LOG.error("Unknown error occurred while inserting to Timestream. Error: ", e);
        }
        if (printFailedRequests) {
            LOG.info("\tData causing failure: -> {}", writeRecordsRequest);
        }
        if (failProcessingOnErrorDefault) {
            LOG.info("'failProcessingOnErrorDefault' is {}, therefore failing without retry.", failProcessingOnErrorDefault);
            fatalExceptionConsumer.accept(e); // fail, no retry
        } else {
            LOG.info("'failProcessingOnErrorDefault' is {}, therefore dropping records..", failProcessingOnErrorDefault);
            dropCompletionConsumer.accept(records); // mark everything as dropped
            retryOrSuccessCompletionConsumer.accept(Collections.emptyList()); // notify request completion
        }
    }

    @Override
    public void open(final Consumer<Exception> fatalExceptionConsumer,
                     final TimestreamSinkConfig.FailureHandlerConfig failureHandlerConfig) {
        this.fatalExceptionConsumer = fatalExceptionConsumer;
        this.printFailedRequests = failureHandlerConfig.isPrintFailedRequests();
        this.failProcessingOnErrorDefault = failureHandlerConfig.isFailProcessingOnErrorDefault();
        this.failProcessingOnValidationException = failureHandlerConfig.isFailProcessingOnValidationException();
        this.failProcessingOnRejectedRecordsException = failureHandlerConfig.isFailProcessingOnRejectedRecordsException();
    }

    @Override
    public void onWriteError(final List<Record> requestEntries,
                             final WriteRecordsRequest writeRecordsRequest,
                             final Exception exception,
                             final Consumer<List<Record>> retryOrSuccessCompletionConsumer,
                             final Consumer<List<Record>> dropCompletionConsumer) {
        final Class<? extends Exception> exceptionClass = exception.getClass();
        LOG.debug("Sending WriteRecordsRequest failed. Starting handling exception: {}", exceptionClass.getName());
        if (exceptionTypeToExceptionHandleMethod.containsKey(exceptionClass)) {
            LOG.debug("Found designated exception handler method.");
            exceptionTypeToExceptionHandleMethod
                    .get(exceptionClass)
                    .accept(requestEntries, writeRecordsRequest, exception,
                            retryOrSuccessCompletionConsumer, dropCompletionConsumer);
        } else if (checkIsRetryableException(exception)) {
            handleRetryableException(requestEntries, writeRecordsRequest, exception,
                    retryOrSuccessCompletionConsumer, dropCompletionConsumer);
        } else {
            LOG.debug("No designated exception handler method found. Launching the default handler.");
            handleDefaultException(requestEntries, writeRecordsRequest, exception,
                    retryOrSuccessCompletionConsumer, dropCompletionConsumer);
        }
    }

    protected static boolean checkIsRetryableException(final @NonNull Exception e) {
        // Most common, TimestreamWriteException subclasses exceptions:
        if (e instanceof InternalServerException || e instanceof ThrottlingException) {
            return true;
        }

        // Other generic AwsServiceExceptions:
        if (e instanceof AwsServiceException) {
            AwsServiceException awsServiceException = (AwsServiceException) e;
            if (awsServiceException.awsErrorDetails() != null) {
                if (AwsErrorCode.isRetryableErrorCode(awsServiceException.awsErrorDetails().errorCode()) ||
                        AwsErrorCode.isThrottlingErrorCode(awsServiceException.awsErrorDetails().errorCode()) ||
                        RETRYABLE_ERROR_CODES.contains(awsServiceException.awsErrorDetails().errorCode()) ||
                        (awsServiceException.awsErrorDetails().sdkHttpResponse() != null &&
                                RETRYABLE_HTTP_STATUS_CODES.contains(awsServiceException.awsErrorDetails().sdkHttpResponse().statusCode()))
                ) {
                    return true;
                }
            }
        }

        // Nested exceptions:
        return isRetryableException(e) ||
                (e instanceof SdkException && ((SdkException)e).retryable());
    }


    private static boolean isRetryableException(final Throwable t) {
        if (t instanceof SdkClientException && t.getCause() != null) {
            return isRetryableException(t.getCause());
        } else if (t instanceof EndpointDiscoveryFailedException && t.getCause() != null) {
            return isRetryableException(t.getCause());
        } else {
            return t instanceof IOException ||
                    // Retry on exceptions caused by wrapped TimeoutException
                    (t.getCause() != null && t.getCause() instanceof TimeoutException) ||
                    t instanceof HttpException || // AWS CRT HTTP
                    t instanceof ApiCallTimeoutException || t instanceof ApiCallAttemptTimeoutException ||
                    t instanceof RetryableException ||
                    t instanceof SdkInterruptedException ||
                    t instanceof SocketTimeoutException || t instanceof SocketException;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DefaultWriteRequestFailureHandler that = (DefaultWriteRequestFailureHandler) o;
        return printFailedRequests == that.printFailedRequests &&
                failProcessingOnErrorDefault == that.failProcessingOnErrorDefault &&
                failProcessingOnValidationException == that.failProcessingOnValidationException &&
                failProcessingOnRejectedRecordsException == that.failProcessingOnRejectedRecordsException &&
                Objects.equals(fatalExceptionConsumer, that.fatalExceptionConsumer) &&
                Objects.equals(exceptionTypeToExceptionHandleMethod, that.exceptionTypeToExceptionHandleMethod);
    }

    @Override
    public int hashCode() {
        return Objects.hash(printFailedRequests,
                failProcessingOnErrorDefault,
                failProcessingOnValidationException,
                failProcessingOnRejectedRecordsException,
                fatalExceptionConsumer,
                exceptionTypeToExceptionHandleMethod);
    }
}
