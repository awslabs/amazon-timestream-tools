/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.internal;

import org.apache.flink.connector.base.sink.writer.Batch;
import org.apache.flink.connector.base.sink.writer.BatchCreator;
import org.apache.flink.connector.base.sink.writer.RequestBuffer;
import org.apache.flink.connector.base.sink.writer.RequestEntryWrapper;
import org.apache.flink.connector.base.sink.writer.strategy.RequestInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * Creates batches whose byte size matches the newline-joined line protocol HTTP payload.
 *
 * <p>Validation failure sentinels are retained for writer-side handling but do not contribute to
 * the payload size.
 */
final class LineProtocolBatchCreator implements BatchCreator<InfluxDB3WriteRequest> {
    private static final long NEWLINE_SIZE_IN_BYTES = 1L;

    private final long maxBatchSizeInBytes;

    LineProtocolBatchCreator(long maxBatchSizeInBytes) {
        if (maxBatchSizeInBytes <= 0) {
            throw new IllegalArgumentException("maxBatchSizeInBytes must be positive");
        }
        this.maxBatchSizeInBytes = maxBatchSizeInBytes;
    }

    @Override
    public Batch<InfluxDB3WriteRequest> createNextBatch(
            RequestInfo requestInfo, RequestBuffer<InfluxDB3WriteRequest> requestBuffer) {
        List<InfluxDB3WriteRequest> batchEntries =
                new ArrayList<>(requestInfo.getBatchSize());
        long batchSizeInBytes = 0L;
        boolean containsWritableEntry = false;

        while (batchEntries.size() < requestInfo.getBatchSize() && !requestBuffer.isEmpty()) {
            RequestEntryWrapper<InfluxDB3WriteRequest> next = requestBuffer.peek();
            InfluxDB3WriteRequest request = next.getRequestEntry();
            boolean writable = !request.isValidationFailure();
            long separatorSize =
                    writable && containsWritableEntry ? NEWLINE_SIZE_IN_BYTES : 0L;

            if (writable
                    && !fitsInBatch(batchSizeInBytes, separatorSize, next.getSize())) {
                if (batchEntries.isEmpty()) {
                    throw new IllegalStateException(
                            "Buffered request entry size "
                                    + next.getSize()
                                    + " exceeds maxBatchSizeInBytes "
                                    + maxBatchSizeInBytes);
                }
                break;
            }

            requestBuffer.poll();
            batchEntries.add(request);
            if (writable) {
                batchSizeInBytes =
                        Math.addExact(
                                batchSizeInBytes,
                                Math.addExact(separatorSize, next.getSize()));
                containsWritableEntry = true;
            }
        }

        return new Batch<>(batchEntries, batchSizeInBytes);
    }

    private boolean fitsInBatch(long currentSize, long separatorSize, long entrySize) {
        if (entrySize < 0) {
            throw new IllegalArgumentException("request entry size must not be negative");
        }
        long remaining = maxBatchSizeInBytes - currentSize;
        return separatorSize <= remaining && entrySize <= remaining - separatorSize;
    }
}
