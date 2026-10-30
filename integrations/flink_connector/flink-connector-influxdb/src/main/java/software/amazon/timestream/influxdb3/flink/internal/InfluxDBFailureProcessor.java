/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.internal;

import org.slf4j.Logger;

import com.influxdb.v3.client.InfluxDBApiHttpException;
import com.influxdb.v3.client.InfluxDBPartialWriteException;
import com.influxdb.v3.client.InfluxDBPartialWriteException.LineError;
import software.amazon.timestream.influxdb3.flink.InfluxDB3SinkException;
import software.amazon.timestream.influxdb3.flink.failure.FailureAction;
import software.amazon.timestream.influxdb3.flink.failure.WriteFailure;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** Applies authentication, partial-write, retry, drop, and terminal failure policy. */
final class InfluxDBFailureProcessor {
    private static final String UNKNOWN_PARTIAL_WRITE_ERROR = "unknown partial-write error";

    private final InfluxDB3SinkConfig config;
    private final InfluxDBClientManager clientManager;
    private final RetryScheduler retryScheduler;
    private final InfluxDBSinkMetrics metrics;
    private final Logger log;
    private final Object failureHandlerLock = new Object();

    InfluxDBFailureProcessor(
            InfluxDB3SinkConfig config,
            InfluxDBClientManager clientManager,
            RetryScheduler retryScheduler,
            InfluxDBSinkMetrics metrics,
            Logger log) {
        this.config = Objects.requireNonNull(config, "config");
        this.clientManager = Objects.requireNonNull(clientManager, "clientManager");
        this.retryScheduler = Objects.requireNonNull(retryScheduler, "retryScheduler");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.log = Objects.requireNonNull(log, "log");
    }

    void handleWriteFailure(
            InfluxDBClientManager.ClientGeneration client,
            List<InfluxDB3WriteRequest> requests,
            Exception error,
            SingleCompletionResultHandler<InfluxDB3WriteRequest> resultHandler) {
        InfluxDBApiHttpException httpError = findHttpError(error);
        Integer statusCode = httpError == null ? null : httpError.statusCode();
        if (statusCode != null && (statusCode == 401 || statusCode == 403)) {
            handleAuthenticationFailure(
                    client, requests, error, statusCode, resultHandler);
            return;
        }
        if (statusCode != null && statusCode == 413) {
            metrics.recordsSendErrors(requests.size());
            logFailedRequests(requests, statusCode, "InfluxDB rejected an oversized batch");
            resultHandler.completeExceptionally(
                    new InfluxDB3SinkException(
                            "InfluxDB rejected a write batch with HTTP 413", error));
            return;
        }
        if (statusCode != null && statusCode == 429) {
            metrics.throttledRequest();
        }
        if (config.isAcceptPartial() && error instanceof InfluxDBPartialWriteException) {
            handlePartialWrite(
                    requests,
                    (InfluxDBPartialWriteException) error,
                    resultHandler);
            return;
        }
        metrics.recordsSendErrors(requests.size());
        applyFailureDecisions(
                requests,
                error,
                httpError,
                false,
                Collections.emptyMap(),
                resultHandler);
    }

    private void handleAuthenticationFailure(
            InfluxDBClientManager.ClientGeneration failedClient,
            List<InfluxDB3WriteRequest> requests,
            Exception error,
            Integer statusCode,
            SingleCompletionResultHandler<InfluxDB3WriteRequest> resultHandler) {
        metrics.recordsSendErrors(requests.size());
        if (requests.stream().anyMatch(request -> request.getAuthenticationRefreshes() >= 1)) {
            logFailedRequests(
                    requests, statusCode, "Authentication remained rejected after token refresh");
            resultHandler.completeExceptionally(
                    new InfluxDB3SinkException(
                            "InfluxDB authentication failed after one token refresh", error));
            return;
        }

        try {
            if (clientManager.refresh(failedClient)) {
                metrics.authenticationRefresh();
            }
            List<InfluxDB3WriteRequest> retry =
                    requests.stream()
                            .map(InfluxDB3WriteRequest::afterAuthenticationRefresh)
                            .collect(Collectors.toList());
            metrics.retries(retry.size());
            resultHandler.retryForEntries(retry);
        } catch (Exception refreshError) {
            logFailedRequests(
                    requests,
                    null,
                    "Unable to refresh the InfluxDB authentication token");
            resultHandler.completeExceptionally(
                    new InfluxDB3SinkException(
                            "Unable to refresh the InfluxDB authentication token",
                            refreshError));
        }
    }

    private void handlePartialWrite(
            List<InfluxDB3WriteRequest> requests,
            InfluxDBPartialWriteException error,
            SingleCompletionResultHandler<InfluxDB3WriteRequest> resultHandler) {
        final Map<Integer, String> failedLines;
        try {
            failedLines = mapPartialFailures(requests, error.lineErrors());
        } catch (RuntimeException mappingError) {
            metrics.recordsSendErrors(requests.size());
            resultHandler.completeExceptionally(
                    mappingError instanceof InfluxDB3SinkException
                            ? (InfluxDB3SinkException) mappingError
                            : new InfluxDB3SinkException(
                                    "Unable to process InfluxDB partial-write details",
                                    mappingError));
            return;
        }

        metrics.recordsWritten(requests.size() - failedLines.size());
        metrics.bytesWritten(acceptedPayloadSizeInBytes(requests, failedLines));
        metrics.recordsSendErrors(failedLines.size());
        metrics.partialWriteFailures(failedLines.size());

        List<InfluxDB3WriteRequest> failedRequests = new ArrayList<>(failedLines.size());
        Map<Integer, String> messagesByFailedPosition = new LinkedHashMap<>();
        int failedPosition = 0;
        for (Map.Entry<Integer, String> entry : failedLines.entrySet()) {
            failedRequests.add(requests.get(entry.getKey()));
            messagesByFailedPosition.put(failedPosition++, entry.getValue());
        }
        applyFailureDecisions(
                failedRequests,
                error,
                error,
                true,
                messagesByFailedPosition,
                resultHandler);
    }

    private void applyFailureDecisions(
            List<InfluxDB3WriteRequest> failedRequests,
            Exception error,
            InfluxDBApiHttpException httpError,
            boolean partialWrite,
            Map<Integer, String> messages,
            SingleCompletionResultHandler<InfluxDB3WriteRequest> resultHandler) {
        Integer statusCode = httpError == null ? null : httpError.statusCode();
        List<InfluxDB3WriteRequest> retry = new ArrayList<>();
        int dropped = 0;

        for (int index = 0; index < failedRequests.size(); index++) {
            InfluxDB3WriteRequest request = failedRequests.get(index);
            String serverMessage = messages.getOrDefault(index, error.getMessage());
            final FailureAction action;
            try {
                if (!partialWrite && isValidationStatus(statusCode)) {
                    action =
                            config.isFailOnValidation()
                                    ? FailureAction.FAIL
                                    : FailureAction.DROP;
                } else {
                    action =
                            classifyFailure(
                                    new WriteFailure(
                                            request.getLineProtocol(),
                                            error,
                                            request.getRetryAttempt(),
                                            statusCode,
                                            partialWrite,
                                            serverMessage));
                }
            } catch (RuntimeException handlerError) {
                logFailedRequest("FAIL", request, statusCode, serverMessage);
                resultHandler.completeExceptionally(
                        new InfluxDB3SinkException(
                                "WriteFailureHandler failed while classifying an error",
                                handlerError));
                return;
            }

            if (action == FailureAction.FAIL) {
                logFailedRequest("FAIL", request, statusCode, serverMessage);
                resultHandler.completeExceptionally(
                        new InfluxDB3SinkException(
                                "InfluxDB write failed with a non-retryable error", error));
                return;
            }
            if (action == FailureAction.DROP) {
                dropped++;
                logFailedRequest("DROP", request, statusCode, serverMessage);
                continue;
            }
            if (request.getRetryAttempt() >= config.getMaxRetries()) {
                if (config.isFailOnRetryExhaustion()) {
                    logFailedRequest("FAIL", request, statusCode, serverMessage);
                    resultHandler.completeExceptionally(
                            new InfluxDB3SinkException(
                                    "InfluxDB write exhausted "
                                            + config.getMaxRetries()
                                            + " retries",
                                    error));
                    return;
                }
                dropped++;
                logFailedRequest("DROP", request, statusCode, serverMessage);
                continue;
            }
            retry.add(request.nextRetry());
        }

        if (dropped > 0) {
            metrics.recordsDropped(dropped);
            resultHandler.markRecordsDropped();
        }
        if (retry.isEmpty()) {
            resultHandler.complete();
            return;
        }

        int retryAttempt =
                retry.stream()
                        .mapToInt(InfluxDB3WriteRequest::getRetryAttempt)
                        .max()
                        .orElse(1);
        try {
            Duration configuredDelay =
                    RetryDelayCalculator.calculate(
                            retryAttempt,
                            config.getInitialRetryBackoff(),
                            config.getMaxRetryBackoff(),
                            config.getRetryJitter());
            Duration serverDelay = min(retryAfter(httpError), config.getMaxRetryBackoff());
            Duration delay =
                    configuredDelay.compareTo(serverDelay) >= 0
                            ? configuredDelay
                            : serverDelay;
            retryScheduler.schedule(retry, delay, resultHandler);
            metrics.retries(retry.size());
            metrics.retryScheduled(delay.toMillis(), retryAfter(httpError).toMillis());
        } catch (RuntimeException retryError) {
            resultHandler.completeExceptionally(
                    new InfluxDB3SinkException(
                            "Unable to schedule an InfluxDB write retry", retryError));
        }
    }

    private FailureAction classifyFailure(WriteFailure failure) {
        synchronized (failureHandlerLock) {
            return Objects.requireNonNull(
                    config.getFailureHandler().onFailure(failure),
                    "WriteFailureHandler returned null");
        }
    }

    private void logFailedRequest(
            String action,
            InfluxDB3WriteRequest request,
            Integer statusCode,
            String message) {
        if (!config.isPrintFailedRequests()) {
            return;
        }
        log.warn(
                "InfluxDB request action={} status={} message={} lineProtocol={}",
                action,
                statusCode,
                FailureLogUtils.escapeAndTruncateForLog(message),
                FailureLogUtils.escapeAndTruncateForLog(request.getLineProtocol()));
    }

    private void logFailedRequests(
            List<InfluxDB3WriteRequest> requests,
            Integer statusCode,
            String message) {
        requests.forEach(
                request -> logFailedRequest("FAIL", request, statusCode, message));
    }

    private static boolean isValidationStatus(Integer statusCode) {
        return statusCode != null && (statusCode == 400 || statusCode == 422);
    }

    private static Map<Integer, String> mapPartialFailures(
            List<InfluxDB3WriteRequest> requests, List<LineError> lineErrors) {
        if (lineErrors.isEmpty()) {
            throw new InfluxDB3SinkException(
                    "InfluxDB reported a partial write without line-level error details");
        }

        Map<Integer, String> failures = new LinkedHashMap<>();
        for (LineError lineError : lineErrors) {
            Integer index = resolveLineIndex(requests, lineError, failures);
            if (index == null) {
                throw new InfluxDB3SinkException(
                        "Unable to map an InfluxDB partial-write error to its input line");
            }
            String message =
                    Objects.requireNonNullElse(
                            lineError.errorMessage(), UNKNOWN_PARTIAL_WRITE_ERROR);
            failures.merge(
                    index,
                    message,
                    (first, second) -> first + "; " + second);
        }
        return failures;
    }

    private static Integer resolveLineIndex(
            List<InfluxDB3WriteRequest> requests,
            LineError lineError,
            Map<Integer, String> alreadyMapped) {
        Integer lineNumber = lineError.lineNumber();
        String original = lineError.originalLine();
        Integer lineNumberCandidate = null;
        if (lineNumber != null) {
            int candidate = lineNumber == 0 ? 0 : lineNumber - 1;
            if (candidate >= 0 && candidate < requests.size()) {
                lineNumberCandidate = candidate;
                if (original == null) {
                    if (!alreadyMapped.containsKey(candidate)) {
                        return candidate;
                    }
                } else if (original.equals(requests.get(candidate).getLineProtocol())) {
                    return candidate;
                }
            }
        }

        boolean originalMatchesMappedRequest = false;
        if (original != null) {
            for (int index = 0; index < requests.size(); index++) {
                if (original.equals(requests.get(index).getLineProtocol())) {
                    if (!alreadyMapped.containsKey(index)) {
                        return index;
                    }
                    originalMatchesMappedRequest = true;
                }
            }
        }
        // InfluxDB can normalize original_line in partial-write responses, for
        // example by removing quotes from string field values. Prefer an exact
        // match when one exists, but otherwise trust a valid server line number.
        return lineNumberCandidate != null
                        && !alreadyMapped.containsKey(lineNumberCandidate)
                        && !originalMatchesMappedRequest
                ? lineNumberCandidate
                : null;
    }

    private static Duration retryAfter(InfluxDBApiHttpException httpError) {
        if (httpError == null || httpError.headers() == null) {
            return Duration.ZERO;
        }
        return httpError.headers()
                .firstValue("Retry-After")
                .map(InfluxDBFailureProcessor::parseRetryAfter)
                .orElse(Duration.ZERO);
    }

    private static long acceptedPayloadSizeInBytes(
            List<InfluxDB3WriteRequest> requests, Map<Integer, String> failedLines) {
        List<String> accepted = new ArrayList<>(requests.size() - failedLines.size());
        for (int index = 0; index < requests.size(); index++) {
            if (!failedLines.containsKey(index)) {
                accepted.add(requests.get(index).getLineProtocol());
            }
        }
        return InfluxDBSinkMetrics.lineProtocolPayloadSizeInBytes(accepted);
    }

    static InfluxDBApiHttpException findHttpError(Throwable error) {
        Throwable current = error;
        while (current != null) {
            if (current instanceof InfluxDBApiHttpException) {
                return (InfluxDBApiHttpException) current;
            }
            Throwable cause = current.getCause();
            if (cause == current) {
                break;
            }
            current = cause;
        }
        return null;
    }

    private static Duration parseRetryAfter(String value) {
        try {
            long seconds = Long.parseLong(value);
            return seconds <= 0 ? Duration.ZERO : Duration.ofSeconds(seconds);
        } catch (NumberFormatException ignored) {
            try {
                Duration delay =
                        Duration.between(
                                ZonedDateTime.now(),
                                ZonedDateTime.parse(
                                        value,
                                        DateTimeFormatter.RFC_1123_DATE_TIME));
                return delay.isNegative() ? Duration.ZERO : delay;
            } catch (DateTimeParseException invalidDate) {
                return Duration.ZERO;
            }
        }
    }

    private static Duration min(Duration first, Duration second) {
        return first.compareTo(second) <= 0 ? first : second;
    }

    @FunctionalInterface
    interface RetryScheduler {
        void schedule(
                List<InfluxDB3WriteRequest> requests,
                Duration delay,
                SingleCompletionResultHandler<InfluxDB3WriteRequest> resultHandler);
    }
}
