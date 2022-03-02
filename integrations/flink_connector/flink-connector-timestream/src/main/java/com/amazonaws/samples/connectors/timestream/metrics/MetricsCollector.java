package com.amazonaws.samples.connectors.timestream.metrics;

import com.amazonaws.samples.connectors.timestream.TimestreamModelUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.timestreamwrite.model.Record;
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsRequest;

import java.util.Collection;

public class MetricsCollector {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsCollector.class);

    private final TimestreamSinkMetricGroup metricGroup;

    public MetricsCollector(TimestreamSinkMetricGroup metricGroup) {
        this.metricGroup = metricGroup;
    }

    public void collectSuccessMetrics(WriteRecordsRequest request) {
        final int noRecords = TimestreamModelUtils.countRecordsInWriteRecordsRequest(request);
        LOG.debug("Ingested successfully {} records...", noRecords);
        metricGroup.getNumWritesSuccess().inc();
        metricGroup.getNumRecordsSuccess().inc(noRecords);
    }

    public void collectExceptionMetrics(Exception exception) {
        final String simpleExceptionName = exception.getClass().getSimpleName(); // like "ArrayIndexOutOfBoundsException"
        metricGroup.incrementExceptionCounter(simpleExceptionName);
    }

    public void collectRetries(Collection<Record> records) {
        // every collectRetries invocation = 1 WriteRecordsRequest = 1 retry
        metricGroup.getNumWritesNonSDKRetries().inc();
        if (records.size() == 0) {
            LOG.debug("No records to be retried.");
        } else {
            LOG.info("Non AWS SDK retry on {} records", records.size());
        }
    }

    public void collectDropped(Collection<Record> droppedRecords, WriteRecordsRequest origRequest) {
        metricGroup.getNumRecordsDrop().inc(droppedRecords.size());

        // Best effort to calculate which records succeeded.
        // This is non-straightforward as customer {@code BatchConverter} can change request structure.
        // Therefore, assuming minimum increase of 0.
        final int succRecordsCount = Math.max(0,
                TimestreamModelUtils.countRecordsInWriteRecordsRequest(origRequest) - droppedRecords.size());
        metricGroup.getNumRecordsSuccess().inc(succRecordsCount);
        // increase successful writes counter for all partially successful writes
        if (succRecordsCount > 0) {
            metricGroup.getNumWritesSuccess().inc();
        }
        LOG.info("Successfully ingested {} records & dropped {} records.", succRecordsCount, droppedRecords.size());
    }

    public void collectPreWriteMetrics(WriteRecordsRequest writeRecordsRequest) {
        metricGroup.setNumOfRecordsPerWriteRecordRequest(
                TimestreamModelUtils.countRecordsInWriteRecordsRequest(writeRecordsRequest));
        metricGroup.setNumOfMeasuresPerWriteRecordRequest(
                TimestreamModelUtils.countMeasuresInWriteRecordsRequest(writeRecordsRequest));
        metricGroup.setNumOfCommonAttributesDimensionsPerWriteRecordRequest(
                TimestreamModelUtils.countCommonAttributesDimensionsPerWriteRecordRequest(writeRecordsRequest));
    }
}
