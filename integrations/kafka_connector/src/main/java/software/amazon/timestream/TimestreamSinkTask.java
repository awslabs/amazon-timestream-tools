package software.amazon.timestream;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.timestream.exception.TimestreamSinkConnectorException;
import software.amazon.timestream.utility.reader.TimestreamS3SchemaReader;
import software.amazon.timestream.utility.reader.TimestreamSchemaReader;
import software.amazon.timestream.utility.AWSServiceClientFactory;
import software.amazon.timestream.utility.TimestreamSinkConfigurationValidator;
import software.amazon.timestream.utility.TimestreamWriter;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Class that writes to Timestream using @{@link TimestreamWriter}
 */
public class TimestreamSinkTask extends SinkTask {

    /**
     * Logger object
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(TimestreamSinkTask.class);
    /**
     * Timestream table writer
     */
    private TimestreamWriter timeStreamWriter;
    /**
     * AWSServiceClientFactory object
     */
    private AWSServiceClientFactory clientFactory;
    /**
     * Offset tracker per partition
     */
    private final ConcurrentHashMap<String, HashSet<Long>> recordOffsets = new ConcurrentHashMap<>();

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(final Map<String, String> map) {
        try {
            LOGGER.info("Begin::TimestreamSinkTask::start");
            TimestreamSinkConfigurationValidator.validateInitialConfig(map);
            final TimestreamSinkConnectorConfig sinkConfig = new TimestreamSinkConnectorConfig(map);
            clientFactory = new AWSServiceClientFactory(sinkConfig);
            TimestreamSinkConfigurationValidator.validateTimestreamSinkConnectorConfig(clientFactory, sinkConfig);
            final TimestreamSchemaReader schemaReader = new TimestreamS3SchemaReader(clientFactory, sinkConfig);
            timeStreamWriter = new TimestreamWriter(schemaReader.getSchemaDefinition(), sinkConfig);
            LOGGER.info("Complete::TimestreamSinkTask::start");
        } catch (ConnectException e) {
            LOGGER.error("Error::TimestreamSinkTask::start", e);
            throw new TimestreamSinkConnectorException(e);
        }
    }

    @Override
    public void put(final Collection<SinkRecord> collection) {

        if (!collection.isEmpty()) {
            LOGGER.trace("TimestreamSinkTask::put Collection size {}", collection.size());
            final ArrayList<SinkRecord> recordList = new ArrayList<>(collection);
            for (final SinkRecord record : recordList) {
                LOGGER.trace("Sink Record: {} ", record);
            }
            timeStreamWriter.writeRecords(clientFactory, collection);
        }
    }

    @Override
    public void flush(final Map<TopicPartition, OffsetAndMetadata> map) {
        LOGGER.info("Complete::TimestreamSinkTask::flush");
    }

    @Override
    public void stop() {
        LOGGER.info("Complete::TimestreamSinkTask::stop");
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(final Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        LOGGER.trace("Begin::TimestreamSinkTask::preCommit");
        final Map<TopicPartition, OffsetAndMetadata> offsetsChange = new ConcurrentHashMap<>();
        for (final Map.Entry<TopicPartition, OffsetAndMetadata> entry : currentOffsets.entrySet()) {
            final TopicPartition topicPartition = entry.getKey();
            if (recordOffsets.isEmpty()) {
                offsetsChange.put(topicPartition, entry.getValue());
            } else {
                final HashSet<Long> offset = recordOffsets.get(String.valueOf(entry.getKey()));
                final OffsetAndMetadata offsetMax = new OffsetAndMetadata(Collections.max(offset) + 1);
                offsetsChange.put(topicPartition, offsetMax);
                LOGGER.debug("DEBUG:TimestreamSinkTask::PreCommit:: Current Offset [{}] for partition [{}] ", entry.getValue(), topicPartition);
                LOGGER.debug("DEBUG:TimestreamSinkTask::PreCommit:: Write record Offsets [{}] {} for partition [{}] ",
                        offsetMax, Arrays.toString(offset.toArray()), topicPartition);
            }
        }
        return offsetsChange;
    }
}
