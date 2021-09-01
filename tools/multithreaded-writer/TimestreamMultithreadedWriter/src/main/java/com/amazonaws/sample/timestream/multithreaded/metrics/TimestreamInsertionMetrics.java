package com.amazonaws.sample.timestream.multithreaded.metrics;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@Getter
public class TimestreamInsertionMetrics {
    // records ingested successfully
    private final transient AtomicLong recordsSuccess = new AtomicLong();
    // all records rejected (due to being malformed/having invalid version/etc)
    private final transient AtomicLong recordsRejectAll = new AtomicLong();
    // records rejected - with invalid version
    private final transient AtomicLong recordsRejectInvalidVersion = new AtomicLong();
    // records rejected - not passed Timestream Validation
    private final transient AtomicLong recordsRejectValidation = new AtomicLong();

    // all successful writes. Includes partially successful writes (with rejected records).
    private final transient AtomicLong writesSuccess = new AtomicLong();

    // records dropped (despite retries), excluding rejected records
    private final transient AtomicLong recordsDrop = new AtomicLong();
    // writes dropped (despite retries)
    private final transient AtomicLong writesDrop = new AtomicLong();

    // all write errors, including throttles/internal server errors/etc.
    // Note: RejectedRecordsException is threaded as success, as part of the records is being inserted.
    private final transient AtomicLong writesErrorAll = new AtomicLong();
    // specific counter for throttled writes
    private final transient AtomicLong writesErrorThrottling = new AtomicLong();
    // specific counter for writes which failed due to missing resource in Timestream
    private final transient AtomicLong writesResourceNotFound = new AtomicLong();
    // specific counter for throttled writes
    private final transient AtomicLong writesErrorInternalServer = new AtomicLong();

    // retries not handled by SDK
    private final transient AtomicLong nonSDKReties = new AtomicLong();

    @Getter(AccessLevel.NONE)
    private final transient AtomicLong writeLatencyMsSum = new AtomicLong(0);
    @Getter(AccessLevel.NONE)
    private final transient AtomicLong writeLatencyMsCount = new AtomicLong(0);

    private final LinkedHashMap<String, AtomicLong> allMetrics = new LinkedHashMap<String, AtomicLong>() {{
        put("recordsSuccess", recordsSuccess);
        put("recordsRejectAll", recordsRejectAll);
        put("recordsRejectInvalidVersion", recordsRejectInvalidVersion);
        put("recordsRejectValidation", recordsRejectValidation);
        put("writesSuccess", writesSuccess);
        put("recordsDrop", recordsDrop);
        put("writesDrop", writesDrop);
        put("writesErrorAll", writesErrorAll);
        put("writesErrorThrottling", writesErrorThrottling);
        put("writesResourceNotFound", writesResourceNotFound);
        put("writesErrorInternalServer", writesErrorInternalServer);
        put("writeLatencyMsSum", writeLatencyMsSum);
        put("writeLatencyMsCount", writeLatencyMsCount);
        put("nonSDKReties", nonSDKReties);
    }};

    public void recordLatencyMs(long latency) {
        writeLatencyMsSum.addAndGet(latency);
        writeLatencyMsCount.incrementAndGet();
    }

    public long getAverageLatencyMs() {
        if (writeLatencyMsCount.get() == 0) {
            return 0;
        }
        return writeLatencyMsSum.get() / writeLatencyMsCount.get();
    }

    public void accumulate(@NonNull final TimestreamInsertionMetrics other) {
        allMetrics.forEach((name, currentMetricValue) -> {
            final AtomicLong otherMetric = other.allMetrics.get(name);
            currentMetricValue.addAndGet(otherMetric.get());
        });
    }

    public TimestreamInsertionMetrics getAndClear() {
        final TimestreamInsertionMetrics result = new TimestreamInsertionMetrics();
        allMetrics.forEach((name, currentMetricValue) ->
                result.allMetrics.get(name).addAndGet(currentMetricValue.getAndSet(0)));
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("TimestreamInsertionMetrics{\n");
        boolean first = true;
        for (final Map.Entry<String, AtomicLong> metric : allMetrics.entrySet()) {
            if (first) {
                appendField(metric.getKey(), metric.getValue().get(), sb);
                first = false;
            } else {
                sb.append(",\n");
                appendField(metric.getKey(), metric.getValue().get(), sb);
            }
        }
        sb.append(",\n");
        appendField("writeLatencyMsAvg", getAverageLatencyMs(), sb);
        sb.append("\n}");
        return sb.toString();
    }

    private void appendField(final String fieldName, final Long fieldValue, final StringBuilder sb) {
        sb.append("\t");
        sb.append(fieldName);
        sb.append("=");
        sb.append(fieldValue);
    }
}
