/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.internal;

import org.apache.flink.connector.base.sink.writer.RequestEntryWrapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PayloadSizeTrackingRequestBufferTest {

    @Test
    void tracksPayloadSizeAndRetryOrdering() {
        PayloadSizeTrackingRequestBuffer<InfluxDB3WriteRequest> buffer =
                new PayloadSizeTrackingRequestBuffer<>();
        RequestEntryWrapper<InfluxDB3WriteRequest> first =
                entry("cpu value=1i", 13);
        RequestEntryWrapper<InfluxDB3WriteRequest> retry =
                entry("cpu value=2i", 17);

        buffer.add(first, false);
        buffer.add(retry, true);

        assertThat(buffer.size()).isEqualTo(2);
        assertThat(buffer.totalSizeInBytes()).isEqualTo(30);
        assertThat(buffer.peek()).isSameAs(retry);
        assertThat(buffer.getBufferedState()).containsExactly(retry, first);

        assertThat(buffer.poll()).isSameAs(retry);
        assertThat(buffer.totalSizeInBytes()).isEqualTo(13);
        assertThat(buffer.poll()).isSameAs(first);
        assertThat(buffer.poll()).isNull();
        assertThat(buffer.isEmpty()).isTrue();
        assertThat(buffer.totalSizeInBytes()).isZero();
    }

    private static RequestEntryWrapper<InfluxDB3WriteRequest> entry(
            String lineProtocol, long size) {
        return new RequestEntryWrapper<>(InfluxDB3WriteRequest.initial(lineProtocol), size);
    }
}
