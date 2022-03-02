package com.amazonaws.samples.connectors.timestream.metrics;

import java.util.List;

import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.metrics.groups.MetricGroupTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.timestreamwrite.model.Record;
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsRequest;

public class MetricCollectorTest {
    private TimestreamSinkMetricGroup metricGroup;

    private MetricsCollector metricsCollector;

    private final Record record1 = Record.builder().build();
    private final Record record2 = Record.builder().build();

    private final List<Record> recordsSuccess = List.of(record1);

    private final WriteRecordsRequest writeRequestSuccess = WriteRecordsRequest.builder().records(recordsSuccess).build();
    private final WriteRecordsRequest writeRequestDropped = WriteRecordsRequest.builder().records(List.of(record1, record2)).build();

    @BeforeEach
    public void init() {
        metricGroup = new TimestreamSinkMetricGroup(new MetricGroupTest.DummyAbstractMetricGroup(
                NoOpMetricRegistry.INSTANCE));
        metricsCollector = new MetricsCollector(metricGroup);
    }

    @Test
    public void testCountSingleRecord() {
        metricsCollector.collectPreWriteMetrics(writeRequestSuccess);

        metricsCollector.collectRetries(recordsSuccess);
        Assertions.assertEquals(0, metricGroup.getNumWritesSuccess().getCount());
        Assertions.assertEquals(0, metricGroup.getNumRecordsSuccess().getCount());
        Assertions.assertEquals(0, metricGroup.getNumRecordsDrop().getCount());
        Assertions.assertEquals(1, metricGroup.getNumWritesNonSDKRetries().getCount());
        Assertions.assertEquals(0, countTotalExceptions());

        metricsCollector.collectSuccessMetrics(writeRequestSuccess);
        Assertions.assertEquals(1, metricGroup.getNumWritesSuccess().getCount());
        Assertions.assertEquals(1, metricGroup.getNumRecordsSuccess().getCount());
        Assertions.assertEquals(0, metricGroup.getNumRecordsDrop().getCount());
        Assertions.assertEquals(1, metricGroup.getNumWritesNonSDKRetries().getCount());
        Assertions.assertEquals(0, countTotalExceptions());

        metricsCollector.collectExceptionMetrics(new RuntimeException(""));
        Assertions.assertEquals(1, metricGroup.getNumWritesSuccess().getCount());
        Assertions.assertEquals(1, metricGroup.getNumRecordsSuccess().getCount());
        Assertions.assertEquals(0, metricGroup.getNumRecordsDrop().getCount());
        Assertions.assertEquals(1, metricGroup.getNumWritesNonSDKRetries().getCount());
        Assertions.assertEquals(1, countTotalExceptions());

        // test dropping one record out of two
        metricsCollector.collectDropped(recordsSuccess, writeRequestDropped);
        Assertions.assertEquals(2, metricGroup.getNumWritesSuccess().getCount());
        Assertions.assertEquals(2, metricGroup.getNumRecordsSuccess().getCount());
        Assertions.assertEquals(1, metricGroup.getNumRecordsDrop().getCount());
        Assertions.assertEquals(1, metricGroup.getNumWritesNonSDKRetries().getCount());
        Assertions.assertEquals(1, countTotalExceptions());

        // test dropping everything
        metricsCollector.collectDropped(recordsSuccess, writeRequestSuccess);
        Assertions.assertEquals(2, metricGroup.getNumWritesSuccess().getCount());
        Assertions.assertEquals(2, metricGroup.getNumRecordsSuccess().getCount());
        Assertions.assertEquals(2, metricGroup.getNumRecordsDrop().getCount());
        Assertions.assertEquals(1, metricGroup.getNumWritesNonSDKRetries().getCount());
        Assertions.assertEquals(1, countTotalExceptions());
    }

    private long countTotalExceptions() {
        return metricGroup.exceptionTypeToCounter.values().stream().map(Counter::getCount).reduce(
                Long::sum).orElse(0L);
    }
}
