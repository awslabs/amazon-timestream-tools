/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.timestream.influxdb3.flink.LineProtocolConverter;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Adapts the public line protocol converter to Flink's buffered request representation. */
@Internal
public final class LineProtocolElementConverter<InputT>
        implements ElementConverter<InputT, InfluxDB3WriteRequest> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(LineProtocolElementConverter.class);

    private final LineProtocolConverter<InputT> converter;
    private final boolean failOnValidation;
    private final boolean printFailedRequests;
    private final long maxRecordSizeInBytes;

    public LineProtocolElementConverter(
            LineProtocolConverter<InputT> converter,
            boolean failOnValidation,
            boolean printFailedRequests,
            long maxRecordSizeInBytes) {
        this.converter = Objects.requireNonNull(converter, "converter");
        this.failOnValidation = failOnValidation;
        this.printFailedRequests = printFailedRequests;
        if (maxRecordSizeInBytes <= 0) {
            throw new IllegalArgumentException("maxRecordSizeInBytes must be positive");
        }
        this.maxRecordSizeInBytes = maxRecordSizeInBytes;
    }

    @Override
    public InfluxDB3WriteRequest apply(InputT element, SinkWriter.Context context) {
        final String line;
        try {
            line = converter.toLineProtocol(element);
        } catch (RuntimeException error) {
            return validationFailure(
                    null, "LineProtocolConverter threw an exception", error);
        }
        if (line == null || line.isBlank()) {
            return validationFailure(
                    line, "LineProtocolConverter returned a blank line", null);
        }
        if (line.indexOf('\n') >= 0 || line.indexOf('\r') >= 0) {
            return validationFailure(
                    line,
                    "LineProtocolConverter must return exactly one line protocol row",
                    null);
        }
        if (line.stripLeading().startsWith("#")) {
            // Line protocol treats a line starting with '#' as a comment, which would be dropped.
            return validationFailure(
                    line,
                    "LineProtocolConverter returned a comment line starting with '#'",
                    null);
        }
        if (line.getBytes(StandardCharsets.UTF_8).length > maxRecordSizeInBytes) {
            return validationFailure(
                    line,
                    "LineProtocolConverter output exceeds the maximum encoded record size of "
                            + maxRecordSizeInBytes
                            + " bytes",
                    null);
        }
        return InfluxDB3WriteRequest.initial(line);
    }

    private InfluxDB3WriteRequest validationFailure(
            String lineProtocol, String message, RuntimeException cause) {
        if (printFailedRequests) {
            LOG.warn(
                    "Line protocol validation action={} message={} lineProtocol={}",
                    failOnValidation ? "FAIL" : "DROP",
                    FailureLogUtils.escapeAndTruncateForLog(message),
                    FailureLogUtils.escapeAndTruncateForLog(lineProtocol));
        }
        if (failOnValidation) {
            throw new IllegalArgumentException(message, cause);
        }
        return InfluxDB3WriteRequest.validationFailure(message);
    }
}
