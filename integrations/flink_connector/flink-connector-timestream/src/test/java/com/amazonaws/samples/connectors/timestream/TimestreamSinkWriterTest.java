package com.amazonaws.samples.connectors.timestream;

import org.apache.flink.api.connector.sink2.Sink.InitContext;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;

import com.amazonaws.samples.connectors.timestream.metrics.MetricsCollector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteAsyncClient;
import software.amazon.awssdk.services.timestreamwrite.model.MeasureValue;
import software.amazon.awssdk.services.timestreamwrite.model.MeasureValueType;
import software.amazon.awssdk.services.timestreamwrite.model.Record;
import software.amazon.awssdk.services.timestreamwrite.model.ThrottlingException;
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsRequest;
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class TimestreamSinkWriterTest {
    private TimestreamSinkWriter<Record> sinkWriter;
    private final TimestreamWriteAsyncClient asyncClient = mock(TimestreamWriteAsyncClient.class);
    private final MetricsCollector metricsCollector = mock(MetricsCollector.class);
    private SinkInitContext sinkInitContext;
    private final Exception failedException = new RuntimeException("Failed");

    private TimestreamSinkConfig getTimestreamSinkConfig(int maxBatchSize, int maxInFlightRequests, int maxBufferedRequest, int maxTimeInBuffer) {
        return TimestreamSinkConfig
                .builder()
                .maxBatchSize(maxBatchSize)
                .maxInFlightRequests(maxInFlightRequests)
                .maxBufferedRequests(maxBufferedRequest)
                .writeClientConfig(TimestreamSinkConfig.WriteClientConfig
                        .builder()
                        .region("us-east-1")
                        .build())
                .maxTimeInBufferMS(maxTimeInBuffer)
                .build();
    }

    private final ElementConverter<Record, Record> elementConverter =
            (element, context) -> element;

    private final BatchConverter batchConverter =
            element -> WriteRecordsRequest.builder()
                    .tableName("table")
                    .databaseName("database")
                    .records(element)
                    .build();

    private Record getRecordFromSeed(int seed) {
        return Record.builder()
            .measureValues(
                    MeasureValue.builder()
                            .name("singleMeasure1")
                            .value(String.valueOf(seed))
                            .type(MeasureValueType.DOUBLE)
                            .build(),
                    MeasureValue.builder()
                            .name("singleMeasure2")
                            .value(String.valueOf(seed))
                            .type(MeasureValueType.DOUBLE)
                            .build()
            )
            .measureName("multiMeasure")
            .measureValueType(MeasureValueType.MULTI)
            .time(String.valueOf(System.currentTimeMillis()))
            .build();
    }

    @BeforeEach
    public void init() {
        sinkInitContext = new SinkInitContext();
    }

    @AfterEach
    public void verifyNoMore() {
        Mockito.verifyNoMoreInteractions(asyncClient);
        Mockito.verifyNoMoreInteractions(metricsCollector);
    }

    private void mockNormalClient() {
        Mockito.when(asyncClient.writeRecords(Mockito.any(WriteRecordsRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(
                        WriteRecordsResponse.builder().build()));
    }

    private void mockFailureClient() {
        Mockito.when(asyncClient.writeRecords(Mockito.any(WriteRecordsRequest.class)))
                .thenReturn(CompletableFuture.failedFuture(failedException));
    }

    private void mockThrottleOnceClient() {
        Mockito.when(asyncClient.writeRecords(Mockito.any(WriteRecordsRequest.class)))
                .thenReturn(CompletableFuture.failedFuture(ThrottlingException
                        .builder().build()))
                .thenReturn(CompletableFuture.completedFuture(
                        WriteRecordsResponse.builder().build()));
    }

    @Test
    public void testSingleWrite() throws Exception {
        mockNormalClient();
        sinkWriter = new TimestreamTestSinkWriter<>(
            elementConverter, batchConverter, sinkInitContext, getTimestreamSinkConfig(80, 1, 160, 15000)
        );
        List<Record> recordsIngested = new ArrayList<>();
        for (int i = 0; i < 80; i++) {
            Record currentRecord = getRecordFromSeed(i);
            sinkWriter.write(currentRecord, null);
            recordsIngested.add(currentRecord);
        }
        WriteRecordsRequest request = batchConverter.apply(recordsIngested);
        verify(asyncClient).writeRecords(request);
        verify(metricsCollector).collectPreWriteMetrics(request);
        verify(metricsCollector).collectSuccessMetrics(request);
    }

    @Test
    public void testMultipleBatchWrites() throws Exception {
        mockNormalClient();
        sinkWriter = new TimestreamTestSinkWriter<>(
                elementConverter, batchConverter, sinkInitContext, getTimestreamSinkConfig(10, 1, 160, 15000)
        );
        List<List<Record>> batchRecordsIngested = new ArrayList<>(8);
        for (int i = 0; i < 8; i++) {
            batchRecordsIngested.add(new ArrayList<>());
        }
        for (int i = 0; i < 80; i++) {
            Record currentRecord = getRecordFromSeed(i);
            sinkWriter.write(currentRecord, null);
            batchRecordsIngested.get(i/10).add(currentRecord);
        }

        sinkWriter.flush(true);

        for (List<Record> currBatch : batchRecordsIngested) {
            WriteRecordsRequest request = batchConverter.apply(currBatch);
            verify(asyncClient).writeRecords(request);
            verify(metricsCollector).collectPreWriteMetrics(request);
            verify(metricsCollector).collectSuccessMetrics(request);
        }
    }

    @Test
    public void testNormalFailure() throws Exception {
        mockFailureClient();
        sinkWriter = new TimestreamTestSinkWriter<>(
                elementConverter, batchConverter, sinkInitContext, getTimestreamSinkConfig(1, 1, 160, 15000)
        );
        Record record = getRecordFromSeed(0);
        WriteRecordsRequest request = batchConverter.apply(Collections.singletonList(record));

        sinkWriter.write(record, null);
        Exception e = Assertions.assertThrows(
                RuntimeException.class,
                () -> sinkWriter.flush(true)
        );
        Assertions.assertEquals(failedException.getMessage(), e.getMessage());
        verify(asyncClient).writeRecords(request);
        verify(metricsCollector).collectPreWriteMetrics(request);
        verify(metricsCollector).collectExceptionMetrics(failedException);
    }

    @Test
    public void testMaxTimeInBuffer() throws Exception{
        mockNormalClient();
        sinkWriter = new TimestreamTestSinkWriter<>(
                elementConverter, batchConverter, sinkInitContext, getTimestreamSinkConfig(2, 1, 160, 200)
        );
        TestProcessingTimeService tpts = sinkInitContext.getTestProcessingTimeService();

        Record record = getRecordFromSeed(0);
        WriteRecordsRequest request = batchConverter.apply(List.of(record));

        tpts.setCurrentTime(0);
        sinkWriter.write(record, null);
        tpts.setCurrentTime(199);
        Mockito.verifyNoMoreInteractions(asyncClient);
        tpts.setCurrentTime(200);

        verify(asyncClient).writeRecords(request);
        verify(metricsCollector).collectPreWriteMetrics(request);
        verify(metricsCollector).collectSuccessMetrics(request);
    }

    @Test
    public void testThrottledRecord() throws Exception {
        sinkWriter = new TimestreamTestSinkWriter<>(
                elementConverter, batchConverter, sinkInitContext, getTimestreamSinkConfig(10, 1, 160, 15000)
        );
        List<Record> recordsIngested = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            recordsIngested.add(getRecordFromSeed(i));
        }
        WriteRecordsRequest origRequest = batchConverter.apply(recordsIngested);
        mockThrottleOnceClient();
        for (int i = 0; i < 3; i++) {
            sinkWriter.write(recordsIngested.get(i), null);
        }
        sinkWriter.flush(true);

        // 1st write. Prewrite, Exception, Retry
        // 2nd write. Prewrite, Success
        verify(asyncClient, Mockito.times(2)).writeRecords(origRequest);
        verify(metricsCollector, Mockito.times(2)).collectPreWriteMetrics(origRequest);
        verify(metricsCollector).collectSuccessMetrics(origRequest);
        verify(metricsCollector).collectRetries(origRequest.records());
        verify(metricsCollector).collectExceptionMetrics(Mockito.any(ThrottlingException.class));
    }

    @Test
    public void testFlushOnSnapshot() throws Exception {
        mockNormalClient();
        sinkWriter = new TimestreamTestSinkWriter<>(
                elementConverter, batchConverter, sinkInitContext, getTimestreamSinkConfig(80, 1, 160, 15000)
        );
        List<Record> recordsIngested = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            Record currentRecord = getRecordFromSeed(i);
            sinkWriter.write(currentRecord, null);
            recordsIngested.add(currentRecord);
        }
        WriteRecordsRequest request = batchConverter.apply(recordsIngested);
        verify(asyncClient, never()).writeRecords(any(WriteRecordsRequest.class));

        sinkWriter.snapshotState(1);

        verify(asyncClient).writeRecords(request);
        verify(metricsCollector).collectPreWriteMetrics(request);
        verify(metricsCollector).collectSuccessMetrics(request);
    }

    private class TimestreamTestSinkWriter<InputT> extends TimestreamSinkWriter<InputT> {
        public TimestreamTestSinkWriter(
               ElementConverter<InputT, Record> elementConverter,
               BatchConverter batchConverter,
               InitContext context,
               TimestreamSinkConfig timestreamSinkConfig) {
           super(elementConverter, batchConverter, context, timestreamSinkConfig);
        }

        @Override
        protected TimestreamWriteAsyncClient openAsyncClient(TimestreamSinkConfig timestreamSinkConfig) {
            return asyncClient;
        }

        @Override
        protected MetricsCollector openMetricCollector(InitContext context) {
            return metricsCollector;
        }
    }
}
