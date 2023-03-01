package com.amazonaws.samples.connectors.timestream;

import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.RequestEntryWrapper;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.timestreamwrite.model.Record;

import java.io.IOException;

import static java.util.Collections.singletonList;
import static org.apache.flink.connector.base.sink.writer.BufferedRequestState.emptyState;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class NoOpSimpleVersionedSerializerTest {

    private final NoOpSimpleVersionedSerializer serializer = new NoOpSimpleVersionedSerializer();

    @Test
    void testGetVersion() {
        assertThat(serializer.getVersion()).isEqualTo(1);
    }

    @Test
    void testSerializeEmptyState() throws IOException {
        assertThat(serializer.serialize(emptyState())).isEmpty();
    }

    @Test
    void testSerializeUnexpected() {
        BufferedRequestState<Record> state = new BufferedRequestState<>(
                singletonList(new RequestEntryWrapper<>(Record.builder().build(), 5)));

        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(() -> serializer.serialize(state))
                .withMessageContaining("This sink does not participate in checkpointing, " +
                        "we should have flushed the buffer.");
    }

    @Test
    void testDeserializeEmptyState() throws IOException {
        assertThat(serializer.deserialize(1, new byte[] {}).getBufferedRequestEntries()).isEmpty();
    }

    @Test
    void testDeserializeUnexpected() {
        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(() -> serializer.deserialize(1, new byte[] {0}))
                .withMessageContaining("This sink does not participate in checkpointing, " +
                        "we should have flushed the buffer.");
    }

}