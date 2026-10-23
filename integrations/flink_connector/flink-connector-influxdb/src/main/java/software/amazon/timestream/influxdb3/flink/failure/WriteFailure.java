/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.failure;

import org.apache.flink.annotation.PublicEvolving;

import java.util.OptionalInt;

/** Information available to a {@link WriteFailureHandler}. */
@PublicEvolving
public final class WriteFailure {
    private final String lineProtocol;
    private final Throwable cause;
    private final int attempt;
    private final Integer statusCode;
    private final boolean partialWrite;
    private final String serverMessage;

    /**
     * Creates immutable failure details.
     *
     * @param lineProtocol rejected line protocol row
     * @param cause client or server failure
     * @param attempt zero-based retry attempt
     * @param statusCode HTTP status code, or {@code null} for a transport failure
     * @param partialWrite whether valid rows in the same batch were accepted
     * @param serverMessage line-level server message when available
     */
    public WriteFailure(
            String lineProtocol,
            Throwable cause,
            int attempt,
            Integer statusCode,
            boolean partialWrite,
            String serverMessage) {
        this.lineProtocol = lineProtocol;
        this.cause = cause;
        this.attempt = attempt;
        this.statusCode = statusCode;
        this.partialWrite = partialWrite;
        this.serverMessage = serverMessage;
    }

    /** @return rejected line protocol row */
    public String getLineProtocol() {
        return lineProtocol;
    }

    /** @return client or server failure */
    public Throwable getCause() {
        return cause;
    }

    /** @return zero-based retry attempt for this record */
    public int getAttempt() {
        return attempt;
    }

    /** @return HTTP status code when the failure came from an HTTP response */
    public OptionalInt getStatusCode() {
        return statusCode == null ? OptionalInt.empty() : OptionalInt.of(statusCode);
    }

    /** @return whether valid rows in the same batch were accepted */
    public boolean isPartialWrite() {
        return partialWrite;
    }

    /** @return line-level server message when available */
    public String getServerMessage() {
        return serverMessage;
    }
}
