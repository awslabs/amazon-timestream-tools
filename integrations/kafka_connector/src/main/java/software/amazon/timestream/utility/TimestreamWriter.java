package software.amazon.timestream.utility;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.timestreamwrite.model.Record;
import software.amazon.awssdk.services.timestreamwrite.model.*;
import software.amazon.timestream.TimestreamSinkConnectorConfig;
import software.amazon.timestream.TimestreamSinkConstants;
import software.amazon.timestream.exception.TimestreamSinkConnectorError;
import software.amazon.timestream.exception.TimestreamSinkConnectorException;
import software.amazon.timestream.exception.TimestreamSinkErrorCodes;
import software.amazon.timestream.schema.RejectedRecord;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Class that receives the non-empty Kafka messages as {@link SinkRecord}
 * objects and writes to Timestream table as records
 */
public class TimestreamWriter {

    /**
     * Logger object
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(TimestreamWriter.class);
    /**
     * Timestream database name
     */
    private final String databaseName;
    /**
     * Timestream table name
     */
    private final String tableName;
    /**
     * Version will be added to record if enabled
     */
    private final boolean enableVersioning;
    /**
     * Empty dimension value is skipped if enabled
     */
    private final boolean skipDimension;
    /**
     * Empty measure value is skipped if enabled
     */
    private final boolean skipMeasure;
    /**
     * Offset tracker per partition
     */
    private final Map<String, Set<Long>> recordOffsets = new ConcurrentHashMap<>();
    /**
     * Offset tracker set
     */
    private final Set<Long> recordOffsetsList = new HashSet<>();
    /**
     * Timestream table schema definition
     */
    private final DataModel schemaDefinition;

    /**
     *
     * @param schemaDefinition table schema
     * @param config connector config
     */
    public TimestreamWriter(final DataModel schemaDefinition, final TimestreamSinkConnectorConfig config) {

        this.databaseName = config.getDatabaseName();
        this.tableName = config.getTableName();
        this.enableVersioning = config.isTimestreamRecordAutoVersioning();
        this.skipDimension = config.isSkipEmptyDimensions();
        this.skipMeasure = config.isSkipEmptyMeasures();
        this.schemaDefinition = schemaDefinition;
    }

    /**
     * Writes incoming sinkRecords as Records in Timestream table
     *
     * @param sinkRecords List of incoming records from the source Kafka topic
     */
    public List<RejectedRecord> writeRecords(final AWSServiceClientFactory clientFactory, final Collection<SinkRecord> sinkRecords) {

        LOGGER.trace("Begin::TimeStreamSimpleWriter::writeRecords");
        final List<RejectedRecord> rejectedRecords = new ArrayList<>();
        final List<Record> records = getTimestreamRecordsFromSinkRecords(sinkRecords, rejectedRecords);
        try {
            if (!records.isEmpty()) {
                final WriteRecordsRequest writeRequest = WriteRecordsRequest.builder()
                        .databaseName(databaseName)
                        .tableName(tableName)
                        .records(records)
                        .build();
                final WriteRecordsResponse writeResponse = clientFactory.getTimestreamClient().writeRecords(writeRequest);
                LOGGER.debug("DEBUG::TimeStreamSimpleWriter::writeRecords:size {}, status {} ", records.size(), writeResponse.sdkHttpResponse().statusCode());
            }
        } catch (RejectedRecordsException e) {
            LOGGER.error("ERROR::TimestreamSimpleWriter::writeRecords: Few records have been rejected, due to [{}]", e.getLocalizedMessage());
            if (e.hasRejectedRecords()) {
                rejectedRecords.addAll(getRejectedTimestreamRecords(e.rejectedRecords(), records));
            }
        } catch (SdkException e) {
            LOGGER.error("ERROR::TimestreamSimpleWriter::writeRecords", e);
        }
        return rejectedRecords;
    }

    /**
     * @param sinkRecord Incoming record from the source Kafka topic
     * @param columnName Timestream column name to be validated
     * @return true if the value in context is empty otherwise false
     */
    private boolean isSinkRecordValueEmpty(final SinkRecord sinkRecord, final String columnName) {
        final Map record = (Map) sinkRecord.value();
        final Object recordValue = record.get(columnName);
        return recordValue == null || recordValue.equals("") || recordValue.equals("\"\"");
    }

    /**
     * Get the formatted value for the given target data type
     * @return formatted string value
     */
    private String getFormattedValue(final String targetDatatype, final String sinkRecordValue) {
        String formatted;
        switch (MeasureValueType.valueOf(targetDatatype)) {
            case TIMESTAMP:
                final Date timestamp = Date.from(Instant.parse(sinkRecordValue));
                formatted = String.valueOf(timestamp.getTime());
                break;
            case DOUBLE:
                formatted = String.format("%.10f", Double.parseDouble(sinkRecordValue));
                break;
            default:
                formatted = sinkRecordValue;
                break;
        }
        return formatted;
    }

    /**
     * Method to get the list of Timestream MeasureValues
     * whole record would be skipped if skipEmptyMeasures is set to false
     *
     * @param sinkRecord Incoming record from the source Kafka topic
     * @return List of MeasureValues
     */
    private List<MeasureValue> getMultiMeasureValuesAsList(final SinkRecord sinkRecord) {

        final List<MeasureValue> measureValueList = new ArrayList<>();
        final Map record = (Map) sinkRecord.value();
        for (final MultiMeasureAttributeMapping mapping : schemaDefinition.multiMeasureMappings().multiMeasureAttributeMappings()) {
            if (isSinkRecordValueEmpty(sinkRecord, mapping.sourceColumn())) {
                if (skipMeasure) {
                    LOGGER.debug("DEBUG::getMultiMeasureValuesAsList: Empty valued multi-measure [{}] is allowed to be skipped for the record [{}] ",
                            mapping.sourceColumn(),
                            sinkRecord);
                    continue;
                } else {
                    final TimestreamSinkConnectorError error = new TimestreamSinkConnectorError(TimestreamSinkErrorCodes.INVALID_MEASURE_VALUE, mapping.sourceColumn());
                    throw new TimestreamSinkConnectorException(error);
                }
            }
            measureValueList.add(getMeasureValue(record, mapping));
        }
        return measureValueList;
    }

    /**
     * Method to get the measure value from the sink record for the given mapping
     * @param record - sink record as map
     * @param mapping - multi-measure mapping definition
     * @return
     */
    private MeasureValue getMeasureValue(final Map record, final MultiMeasureAttributeMapping mapping){
        final String sinkRecordValue = String.valueOf(record.get(mapping.sourceColumn())).replace("\"", "");
        return MeasureValue.builder().name(mapping.targetMultiMeasureAttributeName())
                .value(getFormattedValue(mapping.measureValueTypeAsString(), sinkRecordValue)).type(mapping.measureValueTypeAsString()).build();
    }

    /**
     * Gets list of Timestream Dimensions from the source record
     * Note: If the source record does not contain a value for a dimension
     * @param sinkRecord Incoming record from the source Kafka topic
     * @return List of Dimensions to be included in Timestream table record
     */
    private List<Dimension> getDimensionsAsList(final SinkRecord sinkRecord) {
        final List<Dimension> dimensions = new ArrayList<>();
        final Map record = (Map) sinkRecord.value();
        for (final DimensionMapping dimensionMapping : schemaDefinition.dimensionMappings()) {
            if (isSinkRecordValueEmpty(sinkRecord, dimensionMapping.sourceColumn())) {
                if (skipDimension) {
                    LOGGER.debug("DEBUG::getDimensionsAsList: Empty valued dimension [{}] is allowed to be skipped for the record [{}] ",
                            dimensionMapping.sourceColumn(), sinkRecord);
                    continue;
                } else {
                    final TimestreamSinkConnectorError error = new TimestreamSinkConnectorError(TimestreamSinkErrorCodes.INVALID_DIMENSION_VALUE,
                            dimensionMapping.sourceColumn());
                    throw new TimestreamSinkConnectorException(error);
                }
            }
            final String dimensionValue = String.valueOf(record.get(dimensionMapping.sourceColumn()));
            dimensions.add(Dimension.builder().name(dimensionMapping.destinationColumn()).value(dimensionValue).build());
        }
        return dimensions;
    }

    /**
     * Gets TimestreamRecord for given list of dimensions and measureValueList
     *
     * @param sinkRecord    Incoming record from the source Kafka topic
     * @param dimensions    Dimensions to be included in a Timestream record
     * @param measureValues MeasureValues to be included in a Timestream record
     * @return Record to be ingested to a Timestream table
     */
    private Record getTimestreamRecord(final SinkRecord sinkRecord, final List<Dimension> dimensions, final List<MeasureValue> measureValues) {

        final Map record = (Map) sinkRecord.value();
        final String timeVal = String.valueOf(record.get(schemaDefinition.timeColumn()));

        // Handle timestamps which are not in EPOCH Format.
        final String timeUnit = TimestreamSinkConstants.TIMEUNIT_DATETIME.equals(schemaDefinition.timeUnitAsString()) ? TimeUnit.MILLISECONDS.name() : schemaDefinition.timeUnitAsString();
        final String timeValue = TimestreamSinkConstants.TIMEUNIT_DATETIME.equals(schemaDefinition.timeUnitAsString()) ?
                String.valueOf(Date.from(Instant.parse(timeVal.replace("\"", ""))).getTime()) : timeVal;
        final String measureColumnName = getMeasureName(record);
        final Record.Builder recordBuilder = Record
                .builder()
                .measureName(measureColumnName)
                .dimensions(dimensions)
                .time(timeValue)
                .timeUnit(timeUnit)
                .measureValues(measureValues)
                .measureValueType(MeasureValueType.MULTI);
        if (enableVersioning) {
            recordBuilder.version(System.currentTimeMillis());
        }
        return recordBuilder.build();
    }

    /**
     * @param record sink record as Map
     * @return the measure name
     */
    private String getMeasureName (final Map record) {
        final boolean isMeasureGiven = schemaDefinition.measureNameColumn() != null && !schemaDefinition.measureNameColumn().isEmpty();
        return isMeasureGiven ?
                schemaDefinition.measureNameColumn().charAt(0) == '$'
                    ? String.valueOf(record.get(schemaDefinition.measureNameColumn().substring(1)))
                    : schemaDefinition.measureNameColumn() : TimestreamSinkConstants.DEFAULT_MEASURE;
    }

    /**
     * Method to convert valid SinkRecord to Timestream Record
     *
     * @param sinkRecords List of SinkRecords from the Kafka topic
     * @return list of converted Timestream table records
     * @see SinkRecord
     * @see Record
     */
    private List<Record> getTimestreamRecordsFromSinkRecords(final Collection<SinkRecord> sinkRecords, final List<RejectedRecord> failedRecords) {

        LOGGER.trace("Begin::TimeStreamSimpleWriter::getTimestreamRecordsFromSinkRecords");
        final List<Record> records = new ArrayList<>();

        for (final SinkRecord sinkRecord : sinkRecords) {
            LOGGER.trace("TimeStreamSimpleWriter::getTimestreamRecordsFromSinkRecords {} , {}", sinkRecord.value().getClass().getName(), sinkRecord.value());
            if (!TimestreamSinkConfigurationValidator.isSinkRecordValidType(sinkRecord)) {
                final TimestreamSinkConnectorError error = new TimestreamSinkConnectorError(TimestreamSinkErrorCodes.INVALID_SINK_RECORD,
                        sinkRecord);
                failedRecords.add(new RejectedRecord(sinkRecord,error.getErrorCode()));
                continue;
            }
            recordOffsetDetails(sinkRecord);
            final List<Dimension> dimensions;
            try {
                dimensions = getDimensionsAsList(sinkRecord);
            } catch(TimestreamSinkConnectorException te) {
                LOGGER.error("ERROR::getDimensionsAsList: {}", te.getMessage());
                failedRecords.add(new RejectedRecord(sinkRecord,te.getMessage()));
                continue;
            }

            final List<MeasureValue> measureValueList;
            try {
                measureValueList = getMultiMeasureValuesAsList(sinkRecord);
            } catch(TimestreamSinkConnectorException te) {
                LOGGER.error("ERROR::getMultiMeasureValuesAsList: {}", te.getMessage());
                failedRecords.add(new RejectedRecord(sinkRecord,te.getMessage()));
                continue;
            }
            records.add(getTimestreamRecord(sinkRecord, dimensions, measureValueList));
        }
        LOGGER.trace("Complete::TimeStreamSimpleWriter::getTimestreamRecordsFromSinkRecords: BEFORE INGESTION: " +
                "Record's size: {}, FailedRecords Size: {}", records.size(), failedRecords.size());
        return records;
    }

    /**
     * Method to get the rejected records while ingesting into Timestream table
     *
     * @param rejectedRecords records that are rejected while ingestion
     * @param allRecords all records that are attempted for ingestion
     * @return List of {@link RejectedRecord}
     */
    private List<RejectedRecord> getRejectedTimestreamRecords(final List<software.amazon.awssdk.services.timestreamwrite.model.RejectedRecord> rejectedRecords, final List<Record> allRecords) {
        final List<RejectedRecord> rejectedTSRecords = new ArrayList<>(rejectedRecords.size());
        for (final software.amazon.awssdk.services.timestreamwrite.model.RejectedRecord rejectedRecord : rejectedRecords) {
            final Record record = allRecords.get(rejectedRecord.recordIndex());
            LOGGER.error("ERROR::TimestreamSimpleWriter::getRejectedTimestreamRecords: Rejected record Index: [{}], reason: [{}] and the record is [{}]",
                    rejectedRecord.recordIndex(), rejectedRecord.reason(), record);
            rejectedTSRecords.add(new RejectedRecord(record, rejectedRecord.reason()));
        }
        return rejectedTSRecords;
    }


    /**
     * Method to record the offset per Kafka topic partition
     *
     * @param sinkRecord Incoming record from the source Kafka topic
     */
    private void recordOffsetDetails(final SinkRecord sinkRecord) {

        final String topicName = sinkRecord.topic();
        final long offset = sinkRecord.kafkaOffset();
        final String partition = topicName + "-" + sinkRecord.kafkaPartition();
        if (recordOffsets.containsKey(partition)) {
            final Set<Long> recordOffset = recordOffsets.get(partition);
            recordOffset.add(offset);
            recordOffsets.put(partition, recordOffset);
        } else {
            recordOffsetsList.add(offset);
            recordOffsets.put(partition, recordOffsetsList);
        }
    }
}
