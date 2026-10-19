/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.internal;

import org.apache.flink.connector.base.sink.writer.RequestBuffer;
import org.apache.flink.connector.base.sink.writer.RequestEntryWrapper;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicLong;

/** Request buffer that exposes its current payload size safely to metric reporters. */
final class PayloadSizeTrackingRequestBuffer<RequestEntryT extends Serializable>
        implements RequestBuffer<RequestEntryT> {
    private final Deque<RequestEntryWrapper<RequestEntryT>> buffer = new ArrayDeque<>();
    private final AtomicLong totalSizeInBytes = new AtomicLong();

    @Override
    public void add(RequestEntryWrapper<RequestEntryT> entry, boolean addToFront) {
        if (addToFront) {
            buffer.addFirst(entry);
        } else {
            buffer.addLast(entry);
        }
        totalSizeInBytes.updateAndGet(current -> Math.addExact(current, entry.getSize()));
    }

    @Override
    public RequestEntryWrapper<RequestEntryT> poll() {
        RequestEntryWrapper<RequestEntryT> entry = buffer.pollFirst();
        if (entry != null) {
            totalSizeInBytes.updateAndGet(
                    current -> Math.max(0L, Math.subtractExact(current, entry.getSize())));
        }
        return entry;
    }

    @Override
    public RequestEntryWrapper<RequestEntryT> peek() {
        return buffer.peekFirst();
    }

    @Override
    public boolean isEmpty() {
        return buffer.isEmpty();
    }

    @Override
    public int size() {
        return buffer.size();
    }

    @Override
    public Collection<RequestEntryWrapper<RequestEntryT>> getBufferedState() {
        return new ArrayList<>(buffer);
    }

    @Override
    public long totalSizeInBytes() {
        return totalSizeInBytes.get();
    }
}
