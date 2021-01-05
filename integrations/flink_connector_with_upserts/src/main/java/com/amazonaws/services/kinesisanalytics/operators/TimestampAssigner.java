package com.amazonaws.services.kinesisanalytics.operators;

import com.amazonaws.services.timestream.TimestreamPoint;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimestampAssigner implements AssignerWithPeriodicWatermarks<TimestreamPoint> {
    private static final Logger LOG = LoggerFactory.getLogger(TimestampAssigner.class);

    //to accommodate expected slight latency, the window function will fire at timeWindow + maxOutOfOrderness (e.g. 120 seconds + 3.5 seconds)
    private final long maxOutOfOrderness = 3500; // 3.5 seconds

    private long currentMaxTimestamp;

    @Override
    public long extractTimestamp(TimestreamPoint element, long previousElementTimestamp) {
        long timestamp = element.getTime() * 1000L;
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        return timestamp;
    }

    @Override
    public Watermark getCurrentWatermark() {
        // return the watermark as current highest timestamp minus the out-of-orderness bound
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }
}