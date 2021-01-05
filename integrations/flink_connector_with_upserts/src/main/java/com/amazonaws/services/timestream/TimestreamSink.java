package com.amazonaws.services.timestream;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.timestreamwrite.AmazonTimestreamWrite;
import com.amazonaws.services.timestreamwrite.AmazonTimestreamWriteClientBuilder;
import com.amazonaws.services.timestreamwrite.model.*;
import com.amazonaws.services.timestreamwrite.model.Record;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// references:
// 1. https://stackoverflow.com/questions/58742213/buffering-transformed-messagesexample-1000-count-using-apache-flink-stream-pr
// 2. https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/state/state.html
/**
 * Sink function for Flink to ingest data to Timestream
 */
public class TimestreamSink extends RichSinkFunction<TimestreamPoint> implements CheckpointedFunction {
    private static final Logger LOG = LoggerFactory.getLogger(TimestreamSink.class);

    private static final long RECORDS_FLUSH_INTERVAL_MILLISECONDS = 60L * 1000L; // One minute

    private transient ListState<Record> checkpointedState;

    private transient AmazonTimestreamWrite writeClient;

    private final String region;
    private final String db;
    private final String table;
    private final Integer batchSize;

    private List<Record> bufferedRecords;
    private long emptyListTimetamp;

    public TimestreamSink(String region, String databaseName, String tableName, int batchSize) {
        this.region = region;
        this.db = databaseName;
        this.table = tableName;
        this.batchSize = batchSize;
        this.bufferedRecords = new ArrayList<>();
        this.emptyListTimetamp = System.currentTimeMillis();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        final ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withMaxConnections(5000)
                .withRequestTimeout(20 * 1000)
                .withMaxErrorRetry(10);

        this.writeClient = AmazonTimestreamWriteClientBuilder
                .standard()
                .withRegion(this.region)
                .withClientConfiguration(clientConfiguration)
                .build();
    }

    @Override
    public void invoke(TimestreamPoint value, Context context) throws Exception {
        List<Dimension> dimensions = new ArrayList<>();

        for(Map.Entry<String, String> entry : value.getDimensions().entrySet()) {
            Dimension dim = new Dimension().withName(entry.getKey()).withValue(entry.getValue());
            dimensions.add(dim);
        }

        Record measure = new Record()
                .withDimensions(dimensions)
                .withMeasureName(value.getMeasureName())
                .withMeasureValueType(value.getMeasureValueType())
                .withMeasureValue(value.getMeasureValue())
                .withTimeUnit(value.getTimeUnit())
                .withTime(String.valueOf(value.getTime()));

        bufferedRecords.add(measure);

        if(shouldPublish()) {
            WriteRecordsRequest writeRecordsRequest = new WriteRecordsRequest()
                    .withDatabaseName(this.db)
                    .withTableName(this.table)
                    .withRecords(bufferedRecords);

            try {
                WriteRecordsResult writeRecordsResult = this.writeClient.writeRecords(writeRecordsRequest);
                LOG.debug("writeRecords Status: " + writeRecordsResult.getSdkHttpMetadata().getHttpStatusCode());
                bufferedRecords.clear();
                emptyListTimetamp = System.currentTimeMillis();

            }   catch (RejectedRecordsException e){
                List<RejectedRecord> rejectedRecords = e.getRejectedRecords();
                LOG.warn("Rejected Records -> " + rejectedRecords.size());

                for (int i = rejectedRecords.size()-1 ; i >= 0 ; i-- ) {

                    LOG.warn("Discarding Malformed Record ->" + rejectedRecords.get(i).toString());
                    LOG.warn("Rejected Record Reason ->" + 	rejectedRecords.get(i).getReason());
                    bufferedRecords.remove(rejectedRecords.get(i).getRecordIndex());

                }
            }   catch (Exception e) {
                LOG.error("Error: " + e);
            }
        }
    }

    // Method to validate if record batch should be published.
    // This method would return true if the accumulated records has reached the batch size.
    // Or if records have been accumulated for last RECORDS_FLUSH_INTERVAL_MILLISECONDS time interval.
    private boolean shouldPublish() {
        if (bufferedRecords.size() == batchSize) {
            LOG.debug("Batch of size " + bufferedRecords.size() + " should get published");
            return true;
        } else if(System.currentTimeMillis() - emptyListTimetamp >= RECORDS_FLUSH_INTERVAL_MILLISECONDS) {
            LOG.debug("Records after flush interval should get published");
            return true;
        }
        return false;
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        checkpointedState.clear();
        for (Record element : bufferedRecords) {
            checkpointedState.add(element);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        ListStateDescriptor<Record> descriptor =
                new ListStateDescriptor<>("recordList",
                        Record.class);

        checkpointedState = functionInitializationContext.getOperatorStateStore().getListState(descriptor);

        if (functionInitializationContext.isRestored()) {
            for (Record element : checkpointedState.get()) {
                bufferedRecords.add(element);
            }
        }
    }
}