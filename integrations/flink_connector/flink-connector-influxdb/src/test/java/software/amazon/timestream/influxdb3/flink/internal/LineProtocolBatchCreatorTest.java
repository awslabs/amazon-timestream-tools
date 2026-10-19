/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.internal;

import org.apache.flink.connector.base.sink.writer.Batch;
import org.apache.flink.connector.base.sink.writer.RequestEntryWrapper;
import org.apache.flink.connector.base.sink.writer.strategy.BasicRequestInfo;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LineProtocolBatchCreatorTest {

    @Test
    void accountsForUtf8BytesAndOnlyInterRecordNewlines() {
        PayloadSizeTrackingRequestBuffer<InfluxDB3WriteRequest> buffer =
                new PayloadSizeTrackingRequestBuffer<>();
        add(buffer, "a");
        add(buffer, "\u00e9");
        add(buffer, "b");
        LineProtocolBatchCreator creator = new LineProtocolBatchCreator(4);

        Batch<InfluxDB3WriteRequest> batch =
                creator.createNextBatch(new BasicRequestInfo(3), buffer);

        assertThat(batch.getBatchEntries())
                .extracting(InfluxDB3WriteRequest::getLineProtocol)
                .containsExactly("a", "\u00e9");
        assertThat(batch.getSizeInBytes()).isEqualTo(4);
        assertThat(buffer.getBufferedState()).singleElement()
                .satisfies(
                        entry ->
                                assertThat(entry.getRequestEntry().getLineProtocol())
                                        .isEqualTo("b"));
    }

    @Test
    void validationFailuresDoNotConsumePayloadBytesOrSeparators() {
        PayloadSizeTrackingRequestBuffer<InfluxDB3WriteRequest> buffer =
                new PayloadSizeTrackingRequestBuffer<>();
        buffer.add(
                new RequestEntryWrapper<>(
                        InfluxDB3WriteRequest.validationFailure("invalid first line"), 0),
                false);
        add(buffer, "abc");
        buffer.add(
                new RequestEntryWrapper<>(
                        InfluxDB3WriteRequest.validationFailure("invalid second line"), 0),
                false);
        add(buffer, "def");
        LineProtocolBatchCreator creator = new LineProtocolBatchCreator(7);

        Batch<InfluxDB3WriteRequest> batch =
                creator.createNextBatch(new BasicRequestInfo(4), buffer);

        assertThat(batch.getBatchEntries()).hasSize(4);
        assertThat(batch.getSizeInBytes()).isEqualTo(7);
        assertThat(buffer).matches(PayloadSizeTrackingRequestBuffer::isEmpty);
    }

    @Test
    void respectsTheRequestedRecordCount() {
        PayloadSizeTrackingRequestBuffer<InfluxDB3WriteRequest> buffer =
                new PayloadSizeTrackingRequestBuffer<>();
        add(buffer, "a");
        add(buffer, "b");
        LineProtocolBatchCreator creator = new LineProtocolBatchCreator(100);

        Batch<InfluxDB3WriteRequest> batch =
                creator.createNextBatch(new BasicRequestInfo(1), buffer);

        assertThat(batch.getBatchEntries()).singleElement()
                .satisfies(
                        request -> assertThat(request.getLineProtocol()).isEqualTo("a"));
        assertThat(batch.getSizeInBytes()).isEqualTo(1);
        assertThat(buffer.size()).isEqualTo(1);
    }

    @Test
    void rejectsAnOversizedHeadEntryInsteadOfReturningAnEmptyBatch() {
        PayloadSizeTrackingRequestBuffer<InfluxDB3WriteRequest> buffer =
                new PayloadSizeTrackingRequestBuffer<>();
        add(buffer, "abcde");
        LineProtocolBatchCreator creator = new LineProtocolBatchCreator(4);

        assertThatThrownBy(
                        () ->
                                creator.createNextBatch(
                                        new BasicRequestInfo(1), buffer))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("entry size 5")
                .hasMessageContaining("maxBatchSizeInBytes 4");
        assertThat(buffer.size()).isEqualTo(1);
    }

    @Test
    void rejectsInvalidLimitsAndEntrySizes() {
        assertThatThrownBy(() -> new LineProtocolBatchCreator(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("positive");

        PayloadSizeTrackingRequestBuffer<InfluxDB3WriteRequest> buffer =
                new PayloadSizeTrackingRequestBuffer<>();
        buffer.add(
                new RequestEntryWrapper<>(
                        InfluxDB3WriteRequest.initial("cpu value=1i"), -1),
                false);

        assertThatThrownBy(
                        () ->
                                new LineProtocolBatchCreator(100)
                                        .createNextBatch(new BasicRequestInfo(1), buffer))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("negative");
    }

    private static void add(
            PayloadSizeTrackingRequestBuffer<InfluxDB3WriteRequest> buffer,
            String lineProtocol) {
        buffer.add(
                new RequestEntryWrapper<>(
                        InfluxDB3WriteRequest.initial(lineProtocol),
                        lineProtocol.getBytes(StandardCharsets.UTF_8).length),
                false);
    }
}
