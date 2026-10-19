/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.internal;

import org.apache.flink.annotation.Internal;

import java.util.List;

/** Minimal write-only client surface used by the sink writer. */
@Internal
public interface InfluxDBWriteClient extends AutoCloseable {

    void writeRecords(List<String> records);

    @Override
    void close() throws Exception;
}

