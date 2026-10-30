/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink;

import org.apache.flink.annotation.PublicEvolving;

/** Fatal connector exception that causes the Flink job to fail. */
@PublicEvolving
public class InfluxDB3SinkException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    /**
     * Creates a fatal connector exception.
     *
     * @param message failure description
     */
    public InfluxDB3SinkException(String message) {
        super(message);
    }

    /**
     * Creates a fatal connector exception with its cause.
     *
     * @param message failure description
     * @param cause underlying failure
     */
    public InfluxDB3SinkException(String message, Throwable cause) {
        super(message, cause);
    }
}
