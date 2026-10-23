/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.internal;

import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.RequestEntryWrapper;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InfluxDB3WriterStateSerializerTest {

    private static final long MAX_RECORD_SIZE_IN_BYTES = 1_024;

    private final InfluxDB3WriterStateSerializer serializer =
            new InfluxDB3WriterStateSerializer(MAX_RECORD_SIZE_IN_BYTES);

    @Test
    void usesSingleVersionOneRequestFormat() throws Exception {
        assertThat(serializer.getVersion()).isEqualTo(1);
        assertThat(
                        HexFormat.of()
                                .formatHex(
                                        serializeRequest(
                                                new InfluxDB3WriteRequest(
                                                        "m f=1i", 2, 1))))
                .isEqualTo("00000000066d20663d31690000000200000001");
        assertThat(
                        HexFormat.of()
                                .formatHex(
                                        serializeRequest(
                                                InfluxDB3WriteRequest.validationFailure("bad"))))
                .isEqualTo("0100000003626164");
    }

    @Test
    void roundTripsBufferedRequestsAndRetryMetadata() throws Exception {
        String firstLine = "cpu value=1i";
        String secondLine = "weather,city=Z\u00fcrich temperature=20.5";
        BufferedRequestState<InfluxDB3WriteRequest> state =
                new BufferedRequestState<>(
                        List.of(
                                new RequestEntryWrapper<>(
                                        new InfluxDB3WriteRequest(firstLine, 2, 1),
                                        utf8Size(firstLine)),
                                new RequestEntryWrapper<>(
                                        new InfluxDB3WriteRequest(secondLine, 0, 0),
                                        utf8Size(secondLine))));

        byte[] bytes = serializer.serialize(state);
        BufferedRequestState<InfluxDB3WriteRequest> restored =
                serializer.deserialize(serializer.getVersion(), bytes);

        assertThat(restored.getBufferedRequestEntries()).hasSize(2);
        assertThat(restored.getBufferedRequestEntries().get(0).getSize())
                .isEqualTo(utf8Size(firstLine));
        assertThat(restored.getBufferedRequestEntries().get(0).getRequestEntry())
                .satisfies(
                        request -> {
                            assertThat(request.getLineProtocol()).isEqualTo("cpu value=1i");
                            assertThat(request.getRetryAttempt()).isEqualTo(2);
                            assertThat(request.getAuthenticationRefreshes()).isEqualTo(1);
                        });
        assertThat(restored.getBufferedRequestEntries().get(1).getRequestEntry().getLineProtocol())
                .isEqualTo(secondLine);
    }

    @Test
    void roundTripsValidationFailuresAndEmptyState() throws Exception {
        BufferedRequestState<InfluxDB3WriteRequest> validationState =
                new BufferedRequestState<>(
                        List.of(
                                new RequestEntryWrapper<>(
                                        InfluxDB3WriteRequest.validationFailure(
                                                "converter returned multiple lines"),
                                        0)));

        BufferedRequestState<InfluxDB3WriteRequest> restoredValidation =
                serializer.deserialize(
                        serializer.getVersion(), serializer.serialize(validationState));
        BufferedRequestState<InfluxDB3WriteRequest> restoredEmpty =
                serializer.deserialize(
                        serializer.getVersion(),
                        serializer.serialize(BufferedRequestState.emptyState()));

        assertThat(restoredValidation.getBufferedRequestEntries()).singleElement()
                .satisfies(
                        entry ->
                                assertThat(entry.getRequestEntry())
                                        .satisfies(
                                                request -> {
                                                    assertThat(request.isValidationFailure())
                                                            .isTrue();
                                                    assertThat(request.getLineProtocol()).isNull();
                                                    assertThat(
                                                                    request
                                                                            .getValidationFailureMessage())
                                                            .isEqualTo(
                                                                    "converter returned multiple lines");
                                                }));
        assertThat(restoredEmpty.getBufferedRequestEntries()).isEmpty();
    }

    @Test
    void checkpointStateDoesNotContainARejectedPayload() throws Exception {
        String marker = "sensitive-invalid-payload-marker";
        String invalidPayload = marker.repeat(100_000) + "\nsecond value=2i";
        InfluxDB3WriteRequest request =
                new LineProtocolElementConverter<String>(
                                ignored -> invalidPayload,
                                false,
                                false,
                                MAX_RECORD_SIZE_IN_BYTES)
                        .apply("input", null);
        BufferedRequestState<InfluxDB3WriteRequest> state =
                new BufferedRequestState<>(
                        List.of(new RequestEntryWrapper<>(request, 0)));

        byte[] serialized = serializer.serialize(state);

        assertThat(serialized).hasSizeLessThan(512);
        assertThat(new String(serialized, StandardCharsets.ISO_8859_1))
                .doesNotContain(marker);
    }

    @Test
    void rejectsUnknownVersions() throws Exception {
        byte[] bytes = serializer.serialize(BufferedRequestState.emptyState());

        assertThatThrownBy(() -> serializer.deserialize(serializer.getVersion() + 1, bytes))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unsupported writer state version");
    }

    @Test
    void rejectsNegativeRequestCount() throws Exception {
        byte[] bytes = serializer.serialize(BufferedRequestState.emptyState());
        ByteBuffer.wrap(bytes).putInt(Long.BYTES, -1);

        assertThatThrownBy(() -> serializer.deserialize(serializer.getVersion(), bytes))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("negative request count");
    }

    @Test
    void rejectsNonZeroValidationFailureRequestSize() throws Exception {
        BufferedRequestState<InfluxDB3WriteRequest> state =
                new BufferedRequestState<>(
                        List.of(
                                new RequestEntryWrapper<>(
                                        InfluxDB3WriteRequest.validationFailure("invalid"), 0)));
        byte[] bytes = serializer.serialize(state);
        ByteBuffer.wrap(bytes).putLong(Long.BYTES + Integer.BYTES, 1L);

        assertThatThrownBy(() -> serializer.deserialize(serializer.getVersion(), bytes))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("validation failure request size must be zero");
    }

    @Test
    void rejectsTruncatedState() throws Exception {
        String lineProtocol = "cpu value=1i";
        BufferedRequestState<InfluxDB3WriteRequest> state =
                new BufferedRequestState<>(
                        List.of(
                                new RequestEntryWrapper<>(
                                        InfluxDB3WriteRequest.initial(lineProtocol),
                                        utf8Size(lineProtocol))));
        byte[] bytes = serializer.serialize(state);

        assertThatThrownBy(
                        () ->
                                serializer.deserialize(
                                        serializer.getVersion(),
                                        Arrays.copyOf(bytes, bytes.length - 1)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("truncated");
    }

    @Test
    void rejectsLengthThatWouldConsumeTheFollowingEntry() throws Exception {
        BufferedRequestState<InfluxDB3WriteRequest> state =
                new BufferedRequestState<>(
                        List.of(
                                new RequestEntryWrapper<>(
                                        InfluxDB3WriteRequest.initial("a"), 1),
                                new RequestEntryWrapper<>(
                                        InfluxDB3WriteRequest.initial("b"), 1)));
        byte[] bytes = serializer.serialize(state);
        int firstStringLengthOffset =
                Long.BYTES + Integer.BYTES + Long.BYTES + Byte.BYTES;
        ByteBuffer.wrap(bytes).putInt(firstStringLengthOffset, 9);

        assertThatThrownBy(() -> serializer.deserialize(serializer.getVersion(), bytes))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("does not match request size");
    }

    @Test
    void rejectsOversizedValidationFailureMessages() throws Exception {
        InfluxDB3WriteRequest request =
                InfluxDB3WriteRequest.validationFailure("x".repeat(1_025));

        assertThatThrownBy(() -> serializeRequest(request))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("exceeds the maximum encoded length");

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeByte(1);
            output.writeInt(1_025);
        }
        assertThatThrownBy(() -> deserializeRequest(bytes.toByteArray(), 0))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("length exceeds");
    }

    @Test
    void rejectsOversizedLineProtocolDuringSerialization() {
        InfluxDB3WriteRequest request =
                InfluxDB3WriteRequest.initial("x".repeat((int) MAX_RECORD_SIZE_IN_BYTES + 1));

        assertThatThrownBy(() -> serializeRequest(request))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("exceeds the maximum encoded length");
    }

    @Test
    void rejectsOversizedLineProtocolBeforeAllocating() throws Exception {
        BufferedRequestState<InfluxDB3WriteRequest> state =
                new BufferedRequestState<>(
                        List.of(
                                new RequestEntryWrapper<>(
                                        InfluxDB3WriteRequest.initial("x"), 1)));
        byte[] bytes = serializer.serialize(state);
        int requestSizeOffset = Long.BYTES + Integer.BYTES;
        int lineLengthOffset = requestSizeOffset + Long.BYTES + Byte.BYTES;
        ByteBuffer.wrap(bytes)
                .putLong(requestSizeOffset, MAX_RECORD_SIZE_IN_BYTES + 1)
                .putInt(lineLengthOffset, (int) MAX_RECORD_SIZE_IN_BYTES + 1);

        assertThatThrownBy(
                        () ->
                                serializer.deserialize(
                                        serializer.getVersion(),
                                        bytes))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("line protocol length exceeds");
    }

    @Test
    void rejectsNegativeLineLength() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeByte(0);
            output.writeInt(-2);
        }

        assertThatThrownBy(() -> deserializeRequest(bytes.toByteArray(), 0))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("negative line protocol length");
    }

    @Test
    void rejectsNegativeRetryMetadata() throws Exception {
        assertThatThrownBy(() -> deserializeRequest(requestBytes("cpu value=1i", -1, 0), 12))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("negative retry metadata");
        assertThatThrownBy(() -> deserializeRequest(requestBytes("cpu value=1i", 0, -1), 12))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("negative retry metadata");
    }

    @Test
    void rejectsInvalidLineProtocolInState() throws Exception {
        assertThatThrownBy(() -> deserializeRequest(requestBytes("", 0, 0), 0))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("invalid write request")
                .hasCauseInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsUnknownRequestType() {
        assertThatThrownBy(() -> deserializeRequest(new byte[] {2}, 0))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("unknown request type");
    }

    private InfluxDB3WriteRequest deserializeRequest(byte[] bytes, long requestSize)
            throws IOException {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes))) {
            return serializer.deserializeRequestFromStream(requestSize, input);
        }
    }

    private byte[] serializeRequest(InfluxDB3WriteRequest request) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            serializer.serializeRequestToStream(request, output);
        }
        return bytes.toByteArray();
    }

    private static byte[] requestBytes(
            String lineProtocol, int retryAttempt, int authenticationRefreshes)
            throws IOException {
        byte[] line = lineProtocol.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeByte(0);
            output.writeInt(line.length);
            output.write(line);
            output.writeInt(retryAttempt);
            output.writeInt(authenticationRefreshes);
        }
        return bytes.toByteArray();
    }

    private static int utf8Size(String value) {
        return value.getBytes(StandardCharsets.UTF_8).length;
    }
}
