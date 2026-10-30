/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.internal;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;

/** Creates write clients inside sink subtasks. */
@Internal
@FunctionalInterface
public interface InfluxDBClientFactory extends Serializable {

    InfluxDBWriteClient create(InfluxDB3SinkConfig config, char[] token);
}
