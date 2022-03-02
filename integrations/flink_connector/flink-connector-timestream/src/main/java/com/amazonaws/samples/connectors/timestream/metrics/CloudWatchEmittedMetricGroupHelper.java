package com.amazonaws.samples.connectors.timestream.metrics;

import org.apache.flink.metrics.MetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloudWatchEmittedMetricGroupHelper {
    private static final Logger LOG = LoggerFactory.getLogger(CloudWatchEmittedMetricGroupHelper.class);

    // https://docs.aws.amazon.com/kinesisanalytics/latest/java/monitoring-metrics-custom.html
    private static String MAGIC_KDA_EXPORT_METRICS_GROUP = "kinesisanalytics";

    public static volatile boolean StaticEmitSinkMetricsToCloudWatch = false;

    public static MetricGroup extendMetricGroup(final MetricGroup in) {
        if (StaticEmitSinkMetricsToCloudWatch) {
            LOG.debug("EmitSinkMetricsToCloudWatch is set to true - adding '{}' to MetricGroup",
                    MAGIC_KDA_EXPORT_METRICS_GROUP);
            return in.addGroup(MAGIC_KDA_EXPORT_METRICS_GROUP);
        }
        return in;
    }
}
