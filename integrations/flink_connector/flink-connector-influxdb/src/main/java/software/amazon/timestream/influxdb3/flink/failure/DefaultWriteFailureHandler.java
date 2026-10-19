/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.failure;

import org.apache.flink.annotation.PublicEvolving;

import java.io.EOFException;
import java.net.ProtocolException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.net.http.HttpTimeoutException;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLPeerUnverifiedException;

/**
 * Default failure classification for the InfluxDB v3 write endpoint.
 *
 * <p>The built-in policy retries explicitly transient HTTP responses and recognized network
 * failures. TLS handshake, peer verification, HTTP protocol, and unclassified I/O failures use
 * the configured terminal-error policy.
 */
@PublicEvolving
public final class DefaultWriteFailureHandler implements WriteFailureHandler {
    private static final long serialVersionUID = 1L;

    /** Shared stateless handler instance. */
    public static final DefaultWriteFailureHandler INSTANCE =
            new DefaultWriteFailureHandler(false, true);

    private final boolean failOnRejected;
    private final boolean failOnError;

    /**
     * Creates the built-in failure policy.
     *
     * @param failOnRejected whether partial-write line rejections fail the sink
     * @param failOnError whether other terminal errors fail the sink
     */
    public DefaultWriteFailureHandler(boolean failOnRejected, boolean failOnError) {
        this.failOnRejected = failOnRejected;
        this.failOnError = failOnError;
    }

    @Override
    public FailureAction onFailure(WriteFailure failure) {
        if (failure.isPartialWrite()) {
            return failOnRejected ? FailureAction.FAIL : FailureAction.DROP;
        }

        if (failure.getStatusCode().isPresent()) {
            int status = failure.getStatusCode().getAsInt();
            if (isRetryableHttpStatus(status)) {
                return FailureAction.RETRY;
            }
            if (status == 401 || status == 403 || status == 413) {
                return FailureAction.FAIL;
            }
            return failOnError ? FailureAction.FAIL : FailureAction.DROP;
        }

        if (isRetryableTransportFailure(failure.getCause())) {
            return FailureAction.RETRY;
        }
        return failOnError ? FailureAction.FAIL : FailureAction.DROP;
    }

    private static boolean isRetryableHttpStatus(int status) {
        return status == 408
                || status == 425
                || status == 429
                || status == 500
                || status == 502
                || status == 503
                || status == 504;
    }

    private static boolean isRetryableTransportFailure(Throwable error) {
        boolean retryable = false;
        Throwable current = error;
        while (current != null) {
            if (current instanceof SSLHandshakeException
                    || current instanceof SSLPeerUnverifiedException
                    || current instanceof ProtocolException) {
                return false;
            }
            if (current instanceof SocketException
                    || current instanceof SocketTimeoutException
                    || current instanceof HttpTimeoutException
                    || current instanceof UnknownHostException
                    || current instanceof EOFException) {
                retryable = true;
            }
            Throwable cause = current.getCause();
            if (cause == null || cause == current) {
                break;
            }
            current = cause;
        }
        return retryable;
    }
}
