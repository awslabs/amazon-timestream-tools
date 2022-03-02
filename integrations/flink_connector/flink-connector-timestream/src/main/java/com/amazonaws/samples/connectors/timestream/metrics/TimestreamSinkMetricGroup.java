package com.amazonaws.samples.connectors.timestream.metrics;

import lombok.Getter;
import lombok.Setter;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import software.amazon.awssdk.services.timestreamwrite.model.AccessDeniedException;
import software.amazon.awssdk.services.timestreamwrite.model.ConflictException;
import software.amazon.awssdk.services.timestreamwrite.model.InternalServerException;
import software.amazon.awssdk.services.timestreamwrite.model.InvalidEndpointException;
import software.amazon.awssdk.services.timestreamwrite.model.RejectedRecordsException;
import software.amazon.awssdk.services.timestreamwrite.model.ResourceNotFoundException;
import software.amazon.awssdk.services.timestreamwrite.model.ServiceQuotaExceededException;
import software.amazon.awssdk.services.timestreamwrite.model.ThrottlingException;
import software.amazon.awssdk.services.timestreamwrite.model.ValidationException;

import java.util.HashMap;

@Internal
public class TimestreamSinkMetricGroup {
    private static final String UNKNOWN_EXCEPTION_NAME = "UNKNOWNEXCEPTION";
    private static final String EXCEPTION_COUNTER_PATTERN = "numOf%s";

    private final MetricGroup sinkGroup;

    @VisibleForTesting
    static final String SINK_METRIC_GROUP = "timestreamSink";

    @Getter
    // records ingested successfully
    private final Counter numRecordsSuccess;

    @Getter
    // all successful writes. Includes partially successful writes (with rejected records).
    private final Counter numWritesSuccess;

    @Getter
    // dropped records
    private final Counter numRecordsDrop;

    @Getter
    // retries handled by sink, despite AWS SDK retries
    private final Counter numWritesNonSDKRetries;

    @Setter
    private int numOfRecordsPerWriteRecordRequest;

    @Setter
    private int numOfMeasuresPerWriteRecordRequest;

    @Setter
    private int numOfCommonAttributesDimensionsPerWriteRecordRequest;

    // exception counters - any exception - retried/ignored/etc will increase respective counter
    final HashMap<String, Counter> exceptionTypeToCounter;


    public TimestreamSinkMetricGroup(MetricGroup metricGroup) {
        sinkGroup = metricGroup.addGroup(SINK_METRIC_GROUP);
        numRecordsSuccess = sinkGroup.counter("numRecordsSuccess");
        numWritesSuccess = sinkGroup.counter("numWritesSuccess");
        numWritesNonSDKRetries = sinkGroup.counter("numWritesNonSDKRetries");
        numRecordsDrop = sinkGroup.counter("numRecordsDrop");

        exceptionTypeToCounter = new HashMap<>();
        // TimestreamWriteException subclasses:
        addCounter(exceptionTypeToCounter, AccessDeniedException.class, sinkGroup);
        addCounter(exceptionTypeToCounter, ConflictException.class, sinkGroup);
        addCounter(exceptionTypeToCounter, InternalServerException.class, sinkGroup);
        addCounter(exceptionTypeToCounter, InvalidEndpointException.class, sinkGroup);
        addCounter(exceptionTypeToCounter, RejectedRecordsException.class, sinkGroup);
        addCounter(exceptionTypeToCounter, ResourceNotFoundException.class, sinkGroup);
        addCounter(exceptionTypeToCounter, ServiceQuotaExceededException.class, sinkGroup);
        addCounter(exceptionTypeToCounter, ThrottlingException.class, sinkGroup);
        addCounter(exceptionTypeToCounter, ValidationException.class, sinkGroup);

        // AWS SDK Exceptions:
        // Not adding ApiCallAttemptTimeoutException, SdkInterruptedException, ApiCallTimeoutException
        // and RetryableException as they shouldn't be common exceptions.
        // They will be reported under @{see UNKNOWN_EXCEPTION_NAME}

        exceptionTypeToCounter.put(UNKNOWN_EXCEPTION_NAME,
                sinkGroup.counter(String.format(EXCEPTION_COUNTER_PATTERN, UNKNOWN_EXCEPTION_NAME)));

        sinkGroup.gauge("numOfRecordsPerWriteRecordRequest", () -> numOfRecordsPerWriteRecordRequest);
        sinkGroup.gauge("numOfMeasuresPerWriteRecordRequest", () -> numOfMeasuresPerWriteRecordRequest);
        sinkGroup.gauge("numOfCommonAttributesDimensionsPerWriteRecordRequest", () -> numOfCommonAttributesDimensionsPerWriteRecordRequest);
    }

    private void addCounter(final HashMap<String, Counter> exceptionTypeToCounter,
                            final Class<?> exceptionClass,
                            final MetricGroup sinkGroup) {
        final String exceptionName = exceptionClass.getSimpleName();
        exceptionTypeToCounter.put(exceptionName,
                sinkGroup.counter(String.format(EXCEPTION_COUNTER_PATTERN, exceptionName)));
    }

    public void incrementExceptionCounter(String simpleExceptionName) {
        if (exceptionTypeToCounter.containsKey(simpleExceptionName)) {
            exceptionTypeToCounter.get(simpleExceptionName).inc();
        } else {
            exceptionTypeToCounter.get(UNKNOWN_EXCEPTION_NAME).inc();
        }
    }
}
