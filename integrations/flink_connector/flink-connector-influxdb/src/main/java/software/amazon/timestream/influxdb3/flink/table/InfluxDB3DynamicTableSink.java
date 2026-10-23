/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;

import software.amazon.timestream.influxdb3.flink.InfluxDB3Sink;

import java.util.Objects;

/** Insert-only table sink backed by {@link InfluxDB3Sink}. */
@Internal
final class InfluxDB3DynamicTableSink implements DynamicTableSink {

    private final InfluxDB3Sink<RowData> sink;
    private final Integer parallelism;

    InfluxDB3DynamicTableSink(InfluxDB3Sink<RowData> sink, Integer parallelism) {
        this.sink = Objects.requireNonNull(sink, "sink");
        this.parallelism = parallelism;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return SinkV2Provider.of(sink, parallelism);
    }

    @Override
    public DynamicTableSink copy() {
        return new InfluxDB3DynamicTableSink(sink, parallelism);
    }

    @Override
    public String asSummaryString() {
        return "TimestreamInfluxDB3";
    }

    InfluxDB3Sink<RowData> getSink() {
        return sink;
    }

    Integer getParallelism() {
        return parallelism;
    }
}
