/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

/**
 * Converts an input record into one InfluxDB line protocol row.
 *
 * @param <InputT> type of record to convert
 */
@FunctionalInterface
@PublicEvolving
public interface LineProtocolConverter<InputT> extends Serializable {

    /**
     * Converts an input record into line protocol.
     *
     * <p>The returned value must contain exactly one non-empty line.
     *
     * @param element input record
     * @return one non-empty line protocol row
     */
    String toLineProtocol(InputT element);
}
