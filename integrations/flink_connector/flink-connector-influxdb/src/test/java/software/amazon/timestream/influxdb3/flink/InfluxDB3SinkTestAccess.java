/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink;

import software.amazon.timestream.influxdb3.flink.internal.InfluxDB3SinkConfig;

/** Test-only access to package-private sink implementation details. */
public final class InfluxDB3SinkTestAccess {

    private InfluxDB3SinkTestAccess() {}

    /** Returns the internal configuration used by the supplied sink. */
    public static InfluxDB3SinkConfig configurationOf(InfluxDB3Sink<?> sink) {
        return sink.getConfiguration();
    }
}
