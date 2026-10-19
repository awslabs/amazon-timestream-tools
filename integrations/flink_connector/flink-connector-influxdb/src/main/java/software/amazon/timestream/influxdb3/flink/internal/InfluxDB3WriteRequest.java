/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.internal;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;
import java.util.Objects;

/** Buffered request state for one line protocol row. */
@Internal
public final class InfluxDB3WriteRequest implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String lineProtocol;
    private final int retryAttempt;
    private final int authenticationRefreshes;
    private final String validationFailureMessage;

    public InfluxDB3WriteRequest(
            String lineProtocol, int retryAttempt, int authenticationRefreshes) {
        this(lineProtocol, retryAttempt, authenticationRefreshes, null);
    }

    private InfluxDB3WriteRequest(
            String lineProtocol,
            int retryAttempt,
            int authenticationRefreshes,
            String validationFailureMessage) {
        if (validationFailureMessage == null) {
            this.lineProtocol = Objects.requireNonNull(lineProtocol, "lineProtocol");
            if (lineProtocol.isBlank()
                    || lineProtocol.indexOf('\n') >= 0
                    || lineProtocol.indexOf('\r') >= 0) {
                throw new IllegalArgumentException(
                        "lineProtocol must contain exactly one non-blank line");
            }
        } else {
            this.lineProtocol = null;
        }
        if (retryAttempt < 0) {
            throw new IllegalArgumentException("retryAttempt must not be negative");
        }
        if (authenticationRefreshes < 0) {
            throw new IllegalArgumentException("authenticationRefreshes must not be negative");
        }
        this.retryAttempt = retryAttempt;
        this.authenticationRefreshes = authenticationRefreshes;
        this.validationFailureMessage = validationFailureMessage;
    }

    public static InfluxDB3WriteRequest initial(String lineProtocol) {
        return new InfluxDB3WriteRequest(lineProtocol, 0, 0);
    }

    static InfluxDB3WriteRequest validationFailure(String message) {
        return new InfluxDB3WriteRequest(
                null, 0, 0, Objects.requireNonNull(message, "message"));
    }

    public String getLineProtocol() {
        return lineProtocol;
    }

    public int getRetryAttempt() {
        return retryAttempt;
    }

    public int getAuthenticationRefreshes() {
        return authenticationRefreshes;
    }

    public boolean isValidationFailure() {
        return validationFailureMessage != null;
    }

    public String getValidationFailureMessage() {
        return validationFailureMessage;
    }

    public InfluxDB3WriteRequest nextRetry() {
        requireWritable();
        return new InfluxDB3WriteRequest(
                lineProtocol, Math.addExact(retryAttempt, 1), authenticationRefreshes);
    }

    public InfluxDB3WriteRequest afterAuthenticationRefresh() {
        requireWritable();
        return new InfluxDB3WriteRequest(
                lineProtocol,
                retryAttempt,
                Math.addExact(authenticationRefreshes, 1));
    }

    private void requireWritable() {
        if (isValidationFailure()) {
            throw new IllegalStateException("A validation failure cannot be written or retried");
        }
    }
}
