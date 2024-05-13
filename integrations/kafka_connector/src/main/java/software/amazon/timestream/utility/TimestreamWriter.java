package software.amazon.timestream.utility;

import com.influxdb.client.WriteApiBlocking;
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

// Influx:
import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.influxdb.query.FluxTable;

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

    //// INFLUXDB
    private final Boolean liveAnalyticsEnabled;
    private final Boolean influxDBEnabled;
    private final String influxDBBucket;
    private final String influxDBUrl;
    private final String influxDBToken;
    private final String influxDBOrg;
    private static InfluxDBClient influxDBClient = null;
    private static WriteApiBlocking influxWriteApi = null;

    ////////////////////////

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

        // InfluxDB
        this.liveAnalyticsEnabled = config.isLiveAnalyticsEnabled();
        this.influxDBEnabled = config.isInfluxDBEnabled();
        this.influxDBBucket = config.getInfluxDBBucket();
        this.influxDBUrl = config.getInfluxDBUrl();
        this.influxDBToken = config.getInfluxDBToken();
        this.influxDBOrg = config.getInfluxDBOrg();

        if (this.influxDBEnabled) {
            this.influxDBClient = getInfluxDBClient(
                    this.influxDBUrl, this.influxDBToken, this.influxDBBucket, this.influxDBOrg
            );
            if (this.influxDBClient != null) {
                LOGGER.error("INFO::TimeStreamWriter:: influxDB client successfull connected: [{}] [{}] [{}]",
                        this.influxDBUrl, this.influxDBBucket, influxDBOrg
                );
                this.influxWriteApi = getInfluxDBWriteApi(this.influxDBClient);
                if (this.influxWriteApi != null) {
                    LOGGER.error("INFO::TimeStreamWriter:: influxDB writer API successfull connected: [{}] [{}] [{}]",
                            this.influxDBUrl, this.influxDBBucket, influxDBOrg
                    );
                }
            }
            else {
                LOGGER.error("ERROR::TimeStreamWriter:: getInfluxDBClient failed on : [{}]", this.influxDBUrl);
            }
        }

        if (!this.influxDBEnabled && !this.liveAnalyticsEnabled) {
            LOGGER.error("ERROR::TimeStreamWriter:: initialization failed on : [{}]", TimestreamSinkErrorCodes.NO_INGESTION_TARGET);

        }

        /////////////////////////
    }

    /**
     * Writes incoming sinkRecords as Records in Timestream table in batches
     * @link TimestreamSinkConstants.DEFAULT_BATCHSIZE
     * @param sinkRecords List of incoming records from the source Kafka topic
     */
    public List<RejectedRecord> writeRecords(final AWSServiceClientFactory clientFactory, final Collection<SinkRecord> sinkRecords) {
        Boolean writeToLiveAnalytics = true;
        Boolean writeToInfluxDB = true;

        LOGGER.trace("Begin::TimeStreamWriter::writeRecords");
        final List<RejectedRecord> rejectedRecords = new ArrayList<>();
        final List<Record> records = getTimestreamRecordsFromSinkRecords(sinkRecords, rejectedRecords);
        if (!records.isEmpty()) {
            final int batchSize = records.size() / TimestreamSinkConstants.DEFAULT_BATCHSIZE + 1;
            List<Record> batchRecords = null;
            for (int currentBatch = 0; currentBatch < batchSize; currentBatch ++) {
                if (!writeToInfluxDB && !writeToLiveAnalytics) {
                    // no target specified, cannot write, send records to DLQ
                    batchRecords = getBatchRecords(records, currentBatch);
                    for (Record record : batchRecords) {
                        RejectedRecord rejectedRecord = new RejectedRecord(record,TimestreamSinkErrorCodes.NO_INGESTION_TARGET);
                        rejectedRecords.add(rejectedRecord);
                    }
                    LOGGER.error("ERROR::TimeStreamWriter::writeRecords: Records have been rejected in the batch [{}] , due to [{}]", currentBatch, TimestreamSinkErrorCodes.NO_INGESTION_TARGET);
                }
                else {
                    try {
                        batchRecords = getBatchRecords(records, currentBatch);
                        if (batchRecords != null && !batchRecords.isEmpty()) {
                            if (writeToLiveAnalytics) {

                                final WriteRecordsRequest writeRequest = WriteRecordsRequest.builder()
                                        .databaseName(databaseName)
                                        .tableName(tableName)
                                        .records(batchRecords)
                                        .build();
                                final WriteRecordsResponse writeResponse = clientFactory.getTimestreamClient().writeRecords(writeRequest);
                                LOGGER.debug("DEBUG::TimeStreamWriter::writeRecords: batch size [{}], status [{}] ", batchRecords.size(), writeResponse.sdkHttpResponse().statusCode());
                            } else {
                                LOGGER.debug("DEBUG::TimeStreamWriter::writeRecords: LiveAnalytics disabled");
                            }
                            if (writeToInfluxDB && influxWriteApi != null) {
                                LOGGER.info("INFO::TimeStreamWriter::writeRecords: InfluxDB writing {} records", batchRecords.size());

                                final ArrayList<Point> pointList = convertLiveAnalyticsRecord(batchRecords);
                                // enhance here
                                influxWriteApi.writePoints(pointList);

                                /* // writing one record at time only
                                convertAndWriteLiveAnalyticsRecord(batchRecords);
                                */
                            } else {
                                LOGGER.debug("DEBUG::TimeStreamWriter::writeRecords: InfluxDB disabled");
                            }
                        } else {
                            LOGGER.debug("DEBUG::TimeStreamWriter::writeRecords: Batch ingestion is complete for the records of size [{}] ", records.size());
                        }
                    } catch (RejectedRecordsException e) {
                        LOGGER.error("ERROR::TimeStreamWriter::writeRecords: Few records have been rejected in the batch [{}] , due to [{}]", currentBatch, e.getLocalizedMessage());
                        if (e.hasRejectedRecords()) {
                            rejectedRecords.addAll(getRejectedTimestreamRecords(e.rejectedRecords(), batchRecords));
                        }
                    } catch (SdkException e) {
                        LOGGER.error("ERROR::TimeStreamWriter::writeRecords", e);
                    }
                }
            }
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

        LOGGER.trace("Begin::TimeStreamWriter::getTimestreamRecordsFromSinkRecords");
        final List<Record> records = new ArrayList<>();

        for (final SinkRecord sinkRecord : sinkRecords) {
            LOGGER.trace("TimeStreamWriter::getTimestreamRecordsFromSinkRecords {} , {}", sinkRecord.value().getClass().getName(), sinkRecord.value());
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
        LOGGER.trace("Complete::TimeStreamWriter::getTimestreamRecordsFromSinkRecords: BEFORE INGESTION: " +
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
            LOGGER.error("ERROR::TimeStreamWriter::getRejectedTimestreamRecords: Rejected record Index: [{}], reason: [{}] and the record is [{}]",
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

    /**
     * Method to get records for the given batch number
     * @see TimestreamSinkConstants
     *
     * @param allRecords - all the records to be ingested to Timestream table
     * @param currentBatch - current batch number
     * @return batch records
     */
    private List<Record> getBatchRecords(final List<Record> allRecords, final int currentBatch) {

        List<Record> batch = null;
        if (allRecords != null) {
            final int total = allRecords.size();
            LOGGER.trace("Begin::TimeStreamWriter::getBatchRecords: allRecords Size [{}]", total);
            final int fromIndex = currentBatch * TimestreamSinkConstants.DEFAULT_BATCHSIZE;
            int toIndex = fromIndex + TimestreamSinkConstants.DEFAULT_BATCHSIZE;
            if (fromIndex <= total) {
                if (toIndex > total) {
                    toIndex = total;
                }
                try {
                    batch = allRecords.subList(fromIndex, toIndex);
                    LOGGER.trace("Complete::TimeStreamWriter::getBatchRecords: batch Size [{}]", batch.size());
                } catch (IndexOutOfBoundsException ie) {
                    LOGGER.error("ERROR::TimeStreamWriter::getBatchRecords: all records Size: [{}], from: [{}], to: [{}]", allRecords.size(), fromIndex, toIndex);
                }
            }
        }
        return batch;
    }

    // InfluxDB enhancements

    public static InfluxDBClient getInfluxDBClient(String url, String token, String bucket, String org) {
        if (influxDBClient != null) return influxDBClient;

        InfluxDBClient client = InfluxDBClientFactory.create(
                url,
                token.toCharArray(), org, bucket);

        influxDBClient = client;

        return influxDBClient;
    }

    public static WriteApiBlocking getInfluxDBWriteApi(InfluxDBClient client) {
        if (influxWriteApi==null) {
            WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
            influxWriteApi = writeApi;
        }
        return influxWriteApi;
    }

    private ArrayList<Point> convertLiveAnalyticsRecord(List<Record> recordList){
        ArrayList<Point> pointList = new ArrayList<Point>();
        for (final Record record : recordList) {
            // LOGGER.trace("Sink Record: {} ", record);

            List<Dimension> dimensions = record.dimensions();
            List<MeasureValue> measureValues = record.measureValues();
            String measureName = record.measureName();
            Long time = Long.parseLong(record.time());

            Point point = Point
                    .measurement(measureName)
                            .time(time, WritePrecision.MS);

            for (Dimension dimension : dimensions) {
                String key = dimension.name();
                String value = dimension.value();
                point = point.addTag(key, value);
            }

            for (MeasureValue measureValue : measureValues) {
                String key = measureValue.name();
                String value = measureValue.value();
                MeasureValueType type = measureValue.type();

                switch (type) {
                    case TIMESTAMP: // not existent in InfluxDB, use Long for now
                    case BIGINT:
                        Long value_l = Long.parseLong(value);
                        point = point.addField(key, value_l);
                        break;
                    case DOUBLE:
                        Double value_d = Double.parseDouble(value);
                        point = point.addField(key,value_d);
                        break;
                    case BOOLEAN:
                        Boolean valued_b = Boolean.parseBoolean(value);
                        point = point.addField(key, valued_b);
                    case VARCHAR:
                        point = point.addField(key, value);
                        break;
                }
            }

            /* // STATIC from InfluxDB example code
            Point point = Point
                    .measurement("mem")
                    .addTag("host", "host1")
                    .addField("used_percent", 23.43234543)
                    .time(1714180752000L, WritePrecision.MS);
            // replace above with data from SinkRecord
            */

            LOGGER.info("Complete::TimeStreamWriter::convertLiveAnalyticsRecord: line protocol [{}]", point.toLineProtocol());

            pointList.add(point);
        }
        return pointList;
    }

    private void convertAndWriteLiveAnalyticsRecord(List<Record> recordList){
        for (final Record record : recordList) {
            // LOGGER.trace("Sink Record: {} ", record);
            Point point = Point
                    .measurement("mem")
                    .addTag("host", "host1")
                    .addField("used_percent", 23.43234543)
                    .time(1714180752000L, WritePrecision.MS);
            // replace above with data from SinkRecord
            WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
            writeApi.writePoint(influxDBBucket, influxDBOrg, point);
        }
    }

    //////////////////

}