package com.amazonaws.sample.timestream.multithreaded.metrics;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class TimestreamWriterMetrics {
    @Getter
    private final TimestreamInsertionMetrics insertionMetrics;
    private final int queueSize;
    private final int writesInFlight;

    @Override
    public String toString() {
        return "TimestreamWriterMetrics{" +
                "insertionMetrics=" + insertionMetrics +
                ", queueSize=" + queueSize +
                ", writesInFlight=" + writesInFlight +
                '}';
    }
}
