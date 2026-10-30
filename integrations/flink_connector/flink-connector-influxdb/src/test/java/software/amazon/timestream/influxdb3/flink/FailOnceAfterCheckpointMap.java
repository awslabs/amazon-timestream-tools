/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.CheckpointListener;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Maps generated record numbers and fails the job once, after the first completed checkpoint.
 *
 * @param <OutT> type of mapped records
 */
public final class FailOnceAfterCheckpointMap<OutT>
        implements MapFunction<Long, OutT>, CheckpointListener {
    private static final long serialVersionUID = 1L;

    private final String failureMarkerPath;
    private final RecordFormatter<OutT> formatter;

    public FailOnceAfterCheckpointMap(
            String failureMarkerPath,
            RecordFormatter<OutT> formatter) {
        this.failureMarkerPath = failureMarkerPath;
        this.formatter = formatter;
    }

    @Override
    public OutT map(Long record) {
        return formatter.format(record);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        Path failureMarker = Path.of(failureMarkerPath);
        if (!Files.exists(failureMarker)) {
            // Fail in the callback so a bounded source cannot outrun the checkpoint notification.
            Files.writeString(
                    failureMarker,
                    "failed after checkpoint " + checkpointId,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE_NEW);
            throw new IllegalStateException("Intentional failure after a completed checkpoint");
        }
    }

    /**
     * Formats one generated record number.
     *
     * @param <OutT> type of mapped records
     */
    @FunctionalInterface
    public interface RecordFormatter<OutT> extends Serializable {
        OutT format(long record);
    }
}
