package com.amazonaws.samples.connectors.timestream;

import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import software.amazon.awssdk.services.timestreamwrite.model.Record;

import java.io.IOException;

/**
 * A {@link SimpleVersionedSerializer} that does not expect to do any serialising.
 * This class will throw an exception if any records are passed to it.
 */
public class NoOpSimpleVersionedSerializer implements SimpleVersionedSerializer<BufferedRequestState<Record>> {
    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(BufferedRequestState<Record> records) throws IOException {
        if (records.getBufferedRequestEntries().isEmpty()) {
            return new byte[] {};
        }

        throw new IllegalStateException("This sink does not participate in checkpointing, " +
                "we should have flushed the buffer.");
    }

    @Override
    public BufferedRequestState<Record> deserialize(int i, byte[] bytes) throws IOException {
        if (bytes.length == 0) {
            return BufferedRequestState.emptyState();
        }

        throw new IllegalStateException("This sink does not participate in checkpointing, " +
                "we should have flushed the buffer.");
    }
}
