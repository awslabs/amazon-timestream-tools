/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import software.amazon.timestream.influxdb3.flink.internal.InfluxDBClientFactory;
import software.amazon.timestream.influxdb3.flink.internal.InfluxDB3SinkConfig;
import software.amazon.timestream.influxdb3.flink.internal.InfluxDB3SinkWriter;
import software.amazon.timestream.influxdb3.flink.internal.InfluxDB3WriteRequest;
import software.amazon.timestream.influxdb3.flink.internal.InfluxDB3WriterStateSerializer;
import software.amazon.timestream.influxdb3.flink.internal.LineProtocolElementConverter;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * An at-least-once Flink DataStream sink for Amazon Timestream for InfluxDB 3.
 *
 * @param <InputT> type of records consumed by the sink
 */
@PublicEvolving
public final class InfluxDB3Sink<InputT>
        extends AsyncSinkBase<InputT, InfluxDB3WriteRequest> {
    private static final long serialVersionUID = 1L;

    private final InfluxDB3SinkConfig config;
    private final InfluxDBClientFactory clientFactory;

    InfluxDB3Sink(
            LineProtocolConverter<InputT> converter,
            InfluxDB3SinkConfig config,
            InfluxDBClientFactory clientFactory) {
        super(
                new LineProtocolElementConverter<>(
                        converter,
                        config.isFailOnValidation(),
                        config.isPrintFailedRequests(),
                        config.getMaxRecordSizeInBytes()),
                config.getMaxBatchSize(),
                config.getMaxInFlightRequests(),
                config.getMaxBufferedRequests(),
                config.getMaxBatchSizeInBytes(),
                config.getMaxTimeInBufferMillis(),
                config.getMaxRecordSizeInBytes(),
                config.getRequestTimeout().toMillis(),
                config.isFailOnRequestTimeout());
        this.config = config;
        this.clientFactory = clientFactory;
    }

    /**
     * Creates a sink builder.
     *
     * @param <InputT> type of records consumed by the sink
     * @return a builder with the documented defaults
     */
    public static <InputT> InfluxDB3SinkBuilder<InputT> builder() {
        return new InfluxDB3SinkBuilder<>();
    }

    InfluxDB3SinkConfig getConfiguration() {
        return config;
    }

    @Override
    public StatefulSinkWriter<InputT, BufferedRequestState<InfluxDB3WriteRequest>> createWriter(
            WriterInitContext context) throws IOException {
        return restoreWriter(context, Collections.emptyList());
    }

    @Override
    public StatefulSinkWriter<InputT, BufferedRequestState<InfluxDB3WriteRequest>> restoreWriter(
            WriterInitContext context,
            Collection<BufferedRequestState<InfluxDB3WriteRequest>> recoveredState)
            throws IOException {
        return new InfluxDB3SinkWriter<>(
                getElementConverter(), context, config, clientFactory, recoveredState);
    }

    @Override
    public SimpleVersionedSerializer<BufferedRequestState<InfluxDB3WriteRequest>>
            getWriterStateSerializer() {
        return new InfluxDB3WriterStateSerializer(config.getMaxRecordSizeInBytes());
    }
}
