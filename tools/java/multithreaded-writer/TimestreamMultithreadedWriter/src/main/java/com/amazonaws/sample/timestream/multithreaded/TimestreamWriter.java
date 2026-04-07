// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.sample.timestream.multithreaded;

import com.amazonaws.sample.timestream.multithreaded.metrics.TimestreamWriterMetrics;
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsRequest;

// See README.md for interface description.
public interface TimestreamWriter {
    boolean putWriteRecordRequest(WriteRecordsRequest writeRequest) throws InterruptedException;

    void shutDownGracefully();

    boolean isWriteApproximatelyComplete();

    int getQueueSize();

    int getWritesInFlight();

    TimestreamWriterMetrics getAndClearMetrics();
}
