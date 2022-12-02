package com.amazonaws.samples.connectors.timestream;

import com.amazonaws.samples.connectors.timestream.metrics.CloudWatchEmittedMetricGroupHelper;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.timestreamwrite.model.Record;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

@PublicEvolving
public class TimestreamSink<InputT> extends AsyncSinkBase<InputT, Record> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(TimestreamSink.class);

    // Timestream does not limit record/WriteRecordsRequest size in bytes - although there are limits
    // on number of records/measures/dimensions outside of sink control.
    // See: https://docs.aws.amazon.com/timestream/latest/developerguide/ts-limits.html
    protected static final int MAX_BATCH_SIZE_IN_BYTES = Integer.MAX_VALUE;
    protected static final int MAX_RECORD_SIZE_IN_BYTES = Integer.MAX_VALUE;

    private final BatchConverter batchConverter;
    private final TimestreamSinkConfig timestreamSinkConfig;

    public TimestreamSink(
            ElementConverter<InputT, Record> elementConverter,
            BatchConverter batchConverter,
            TimestreamSinkConfig timestreamSinkConfig) {
        super(elementConverter,
                timestreamSinkConfig.getMaxBatchSize(),
                timestreamSinkConfig.getMaxInFlightRequests(),
                timestreamSinkConfig.getMaxBufferedRequests(),
                MAX_BATCH_SIZE_IN_BYTES,
                timestreamSinkConfig.getMaxTimeInBufferMS(),
                MAX_RECORD_SIZE_IN_BYTES);
        this.batchConverter = batchConverter;
        this.timestreamSinkConfig = timestreamSinkConfig;
        LOG.debug("Initialized TimestreamSink class");
    }

    @Override
    public StatefulSinkWriter<InputT, BufferedRequestState<Record>> createWriter(InitContext initContext) throws IOException {
        return restoreWriter(initContext, Collections.emptyList());
    }

    @Override
    public StatefulSinkWriter<InputT, BufferedRequestState<Record>> restoreWriter(InitContext context, Collection<BufferedRequestState<Record>> collection) throws IOException {
        LOG.debug("Creating a new TimestreamSinkWriter...");
        CloudWatchEmittedMetricGroupHelper.StaticEmitSinkMetricsToCloudWatch = timestreamSinkConfig.isEmitSinkMetricsToCloudWatch();
        return new TimestreamSinkWriter<>(
                getElementConverter(),
                batchConverter,
                context,
                timestreamSinkConfig
        );
    }

    @Override
    public SimpleVersionedSerializer<BufferedRequestState<Record>> getWriterStateSerializer() {
        return new NoOpSimpleVersionedSerializer();
    }
}
