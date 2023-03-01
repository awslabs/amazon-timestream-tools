package com.amazonaws.samples.connectors.timestream;

import org.apache.flink.shaded.guava30.com.google.common.base.Utf8;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.timestreamwrite.model.Dimension;
import software.amazon.awssdk.services.timestreamwrite.model.MeasureValue;
import software.amazon.awssdk.services.timestreamwrite.model.MeasureValueType;
import software.amazon.awssdk.services.timestreamwrite.model.Record;
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsRequest;

public class TimestreamModelUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TimestreamModelUtils.class);

    private static final int LONG_BYTE_SIZE = 8;
    private final static int BOOLEAN_BYTE_SIZE = 1;
    private final static int DOUBLE_BYTE_SIZE = 8;
    private static final long DATAPOINT_INITIAL_VERSION = 1;

    public static int countRecordsInWriteRecordsRequest(WriteRecordsRequest wrr) {
        if (wrr.records() != null) {
            return wrr.records().size();
        }
        LOG.error("Unexpected argument: Request is null for countRecordsInWriteRecordsRequest");
        return 0;
    }

    public static int countMeasuresInWriteRecordsRequest(WriteRecordsRequest wrr) {
        // measures per WriteRecordsRequest = (common attributes measures) * (measures in individual records)
        int commonAttributesMeasures = (wrr.commonAttributes() == null) ? 1 : countMeasuresInRecord(wrr.commonAttributes());
        int allRecordMeasures = 0;
        for (Record r : wrr.records()) {
            allRecordMeasures += countMeasuresInRecord(r);
        }
        return commonAttributesMeasures * allRecordMeasures;
    }

    private static int countMeasuresInRecord(Record record) {
        if (record != null) {
            if (MeasureValueType.MULTI.equals(record.measureValueType())) {
                return record.measureValues().size();
            } else {
                return 1;
            }
        }
        LOG.error("Unexpected argument: Record is null for countMeasuresInRecord");
        return 0;
    }

    public static long getRecordSizeInBytes(Record record) {
        long bytesRecord = 0;

        if (record.measureName() != null) {
            bytesRecord += sizeOfString(record.measureName());
        }

        if (record.hasDimensions()) {
            for (final Dimension dimension : record.dimensions()) {
                if (dimension.name() != null) {
                    bytesRecord += dimension.name().length();
                }
                if (dimension.value() != null) {
                    bytesRecord += dimension.value().length();
                }
            }
        }

        if (record.time() != null) {
            bytesRecord += LONG_BYTE_SIZE;
        }

        if (record.measureValueType() != null && record.measureValue() != null) {
            bytesRecord += getMeasureValueSizeForScalarMeasureType(record.measureValueType() , record.measureValue());
        } else if (record.measureValues() != null && MeasureValueType.MULTI.equals(record.measureValueType())) {
            for (MeasureValue measureValue : record.measureValues()) {
                bytesRecord += sizeOfString(measureValue.name());
                bytesRecord += getMeasureValueSizeForScalarMeasureType(measureValue.type(), measureValue.value());
            }
        }


        return bytesRecord;
    }

    private static int getMeasureValueSizeForScalarMeasureType(MeasureValueType measureValueType, String measureValue) {
        switch (measureValueType) {
            case DOUBLE:
                return DOUBLE_BYTE_SIZE;
            case TIMESTAMP:
            case BIGINT:
                return LONG_BYTE_SIZE;
            case VARCHAR:
                return sizeOfString(measureValue);
            case BOOLEAN:
                return BOOLEAN_BYTE_SIZE;
            default:
                LOG.error("Unsupported Measure type: {}. Calculated record size, and emitted metrics will be incorrect", measureValueType);
                return 0;
        }
    }

    private static int sizeOfString(final String stringToSize) {
        return Utf8.encodedLength(stringToSize);
    }


    public static int countCommonAttributesDimensionsPerWriteRecordRequest(WriteRecordsRequest wrr) {
        if (wrr.commonAttributes() != null) {
            if (wrr.commonAttributes().dimensions() != null) {
                return wrr.commonAttributes().dimensions().size();
            }
        }
        return 0;
    }
}
