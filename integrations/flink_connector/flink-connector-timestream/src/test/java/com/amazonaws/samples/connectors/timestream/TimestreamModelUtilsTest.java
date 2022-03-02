package com.amazonaws.samples.connectors.timestream;

import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.timestreamwrite.model.Dimension;
import software.amazon.awssdk.services.timestreamwrite.model.MeasureValue;
import software.amazon.awssdk.services.timestreamwrite.model.MeasureValueType;
import software.amazon.awssdk.services.timestreamwrite.model.Record;
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsRequest;

public class TimestreamModelUtilsTest {
    private final Record defaultSingleMeasure = Record.builder()
            .measureValue("1.0")
            .measureName("singleMeasure")
            .measureValueType(MeasureValueType.DOUBLE)
            .time(String.valueOf(System.currentTimeMillis()))
            .build();

    private final Record defaultMultiMeasure = Record.builder()
            .measureValues(
                    MeasureValue.builder()
                            .name("singleMeasure1")
                            .value("1.0")
                            .type(MeasureValueType.DOUBLE)
                            .build(),
                    MeasureValue.builder()
                            .name("singleMeasure2")
                            .value("2.0")
                            .type(MeasureValueType.DOUBLE)
                            .build()
            )
            .measureName("multiMeasure")
            .measureValueType(MeasureValueType.MULTI)
            .time(String.valueOf(System.currentTimeMillis()))
            .build();


    @Test
    public void testCountRecordsNoRecord() {
        WriteRecordsRequest request = WriteRecordsRequest.builder().build();
        Assertions.assertEquals(0, TimestreamModelUtils.countRecordsInWriteRecordsRequest(request));
    }

    @Test
    public void testCountRecordsMultipleRecord() {
        WriteRecordsRequest request = WriteRecordsRequest.builder()
                .records(Record.builder().build(), Record.builder().build())
                .build();
        Assertions.assertEquals(2, TimestreamModelUtils.countRecordsInWriteRecordsRequest(request));
    }

    @Test
    public void testCountMeasureInWriteRequestSingle() {
        WriteRecordsRequest request = WriteRecordsRequest.builder()
                .records(defaultSingleMeasure, defaultSingleMeasure, defaultSingleMeasure)
                .build();
        Assertions.assertEquals(3, TimestreamModelUtils.countMeasuresInWriteRecordsRequest(request));
    }

    @Test
    public void testCountMeasureInWriteRequestMulti() {
        WriteRecordsRequest request = WriteRecordsRequest.builder()
                .records(defaultMultiMeasure, defaultMultiMeasure)
                .build();
        Assertions.assertEquals(4, TimestreamModelUtils.countMeasuresInWriteRecordsRequest(request));
    }

    @Test
    public void testGetRecordSizeInBytesSingle() {
        // 8 (measureValue) + 8 (time) + 13 (measureName)
        Assertions.assertEquals(29, TimestreamModelUtils.getRecordSizeInBytes(defaultSingleMeasure));
    }

    @Test
    public void testGetRecordSizeInBytesMulti() {
        // (8+14)*2 (measureValues) + 8 (time) + 12 (measureName)
        Assertions.assertEquals(64, TimestreamModelUtils.getRecordSizeInBytes(defaultMultiMeasure));
    }

    @Test
    public void countCommonAttributesDimensionsRecordRequestDefault() {
        WriteRecordsRequest request = WriteRecordsRequest.builder()
                .records(defaultMultiMeasure)
                .build();
        Assertions.assertEquals(0, TimestreamModelUtils.countCommonAttributesDimensionsPerWriteRecordRequest(request));
    }

    @Test
    public void countCommonAttributesDimensionsRecordRequestCommonDimensions() {
        List<Dimension> dimensionList = List.of(
                Dimension.builder().name("host_name").value("host1").build(),
                Dimension.builder().name("az").value("az1").build()
        );
        WriteRecordsRequest request = WriteRecordsRequest.builder()
                .records(defaultMultiMeasure)
                .commonAttributes(
                        Record.builder()
                                .dimensions(dimensionList)
                                .build()
                )
                .build();
        Assertions.assertEquals(2, TimestreamModelUtils.countCommonAttributesDimensionsPerWriteRecordRequest(request));
    }
}
