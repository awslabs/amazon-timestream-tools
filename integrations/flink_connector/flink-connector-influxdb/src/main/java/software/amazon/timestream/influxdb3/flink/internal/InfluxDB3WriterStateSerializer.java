/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriterStateSerializer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/** Versioned checkpoint serializer for buffered InfluxDB line protocol requests. */
@Internal
public final class InfluxDB3WriterStateSerializer
        extends AsyncSinkWriterStateSerializer<InfluxDB3WriteRequest> {
    private static final int VERSION = 1;
    private static final int WRITE_REQUEST = 0;
    private static final int VALIDATION_FAILURE = 1;
    // Validation messages are connector-generated; this also bounds allocations from corrupt state.
    private static final int MAX_VALIDATION_FAILURE_MESSAGE_SIZE_BYTES = 1_024;

    private final long maxRecordSizeInBytes;

    /** Creates a serializer that rejects line-protocol state above the configured record limit. */
    public InfluxDB3WriterStateSerializer(long maxRecordSizeInBytes) {
        if (maxRecordSizeInBytes <= 0) {
            throw new IllegalArgumentException("maxRecordSizeInBytes must be positive");
        }
        this.maxRecordSizeInBytes = maxRecordSizeInBytes;
    }

    @Override
    protected void serializeRequestToStream(
            InfluxDB3WriteRequest request, DataOutputStream out) throws IOException {
        if (request.isValidationFailure()) {
            out.writeByte(VALIDATION_FAILURE);
            writeBoundedString(
                    request.getValidationFailureMessage(),
                    "validation failure message",
                    MAX_VALIDATION_FAILURE_MESSAGE_SIZE_BYTES,
                    out);
        } else {
            out.writeByte(WRITE_REQUEST);
            writeBoundedString(
                    request.getLineProtocol(),
                    "line protocol",
                    maxRecordSizeInBytes,
                    out);
            out.writeInt(request.getRetryAttempt());
            out.writeInt(request.getAuthenticationRefreshes());
        }
    }

    @Override
    protected InfluxDB3WriteRequest deserializeRequestFromStream(
            long requestSize, DataInputStream in) throws IOException {
        try {
            int requestType = in.readUnsignedByte();
            if (requestType == VALIDATION_FAILURE) {
                if (requestSize != 0) {
                    throw new IOException(
                            "Corrupt state: validation failure request size must be zero");
                }
                return InfluxDB3WriteRequest.validationFailure(
                        readBoundedString(
                                in,
                                "validation failure message",
                                MAX_VALIDATION_FAILURE_MESSAGE_SIZE_BYTES));
            }
            if (requestType == WRITE_REQUEST) {
                return createWriteRequest(
                        readLineProtocol(in, requestSize), in.readInt(), in.readInt());
            }
            throw new IOException("Corrupt state: unknown request type " + requestType);
        } catch (EOFException error) {
            EOFException truncated = new EOFException("Corrupt state: truncated write request");
            truncated.initCause(error);
            throw truncated;
        }
    }

    private static InfluxDB3WriteRequest createWriteRequest(
            String lineProtocol, int retryAttempt, int authenticationRefreshes)
            throws IOException {
        if (retryAttempt < 0 || authenticationRefreshes < 0) {
            throw new IOException("Corrupt state: negative retry metadata");
        }
        try {
            return new InfluxDB3WriteRequest(
                    lineProtocol, retryAttempt, authenticationRefreshes);
        } catch (IllegalArgumentException e) {
            throw new IOException("Corrupt state: invalid write request", e);
        }
    }

    private static void writeBoundedString(
            String value, String fieldName, long maxLength, DataOutputStream out)
            throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > maxLength) {
            throw new IOException(
                    fieldName + " exceeds the maximum encoded length of " + maxLength + " bytes");
        }
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private String readLineProtocol(DataInputStream in, long requestSize) throws IOException {
        int length = readLength(in, "line protocol");
        if (requestSize < 0 || length != requestSize) {
            throw new IOException(
                    "Corrupt state: line protocol length "
                            + length
                            + " does not match request size "
                            + requestSize);
        }
        if (length > maxRecordSizeInBytes) {
            throw new IOException(
                    "Corrupt state: line protocol length exceeds "
                            + maxRecordSizeInBytes
                            + " bytes");
        }
        return readStringBytes(in, "line protocol", length);
    }

    private static String readBoundedString(
            DataInputStream in, String fieldName, int maxLength) throws IOException {
        int length = readLength(in, fieldName);
        if (length > maxLength) {
            throw new IOException(
                    "Corrupt state: "
                            + fieldName
                            + " length exceeds "
                            + maxLength
                            + " bytes");
        }
        return readStringBytes(in, fieldName, length);
    }

    private static int readLength(DataInputStream in, String fieldName) throws IOException {
        int length = in.readInt();
        if (length < 0) {
            throw new IOException("Corrupt state: negative " + fieldName + " length");
        }
        return length;
    }

    private static String readStringBytes(
            DataInputStream in, String fieldName, int length) throws IOException {
        byte[] bytes = new byte[length];
        try {
            in.readFully(bytes);
        } catch (EOFException error) {
            EOFException truncated =
                    new EOFException("Corrupt state: truncated " + fieldName + " value");
            truncated.initCause(error);
            throw truncated;
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public BufferedRequestState<InfluxDB3WriteRequest> deserialize(
            int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unsupported writer state version: " + version);
        }
        if (serialized.length >= Long.BYTES + Integer.BYTES
                && ByteBuffer.wrap(serialized).getInt(Long.BYTES) < 0) {
            throw new IOException("Corrupt state: negative request count");
        }
        return super.deserialize(version, serialized);
    }

    @Override
    public int getVersion() {
        return VERSION;
    }
}
