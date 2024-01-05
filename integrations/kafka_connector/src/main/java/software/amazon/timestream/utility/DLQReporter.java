package software.amazon.timestream.utility;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.Getter;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.timestream.TimestreamSinkConnectorConfig;
import software.amazon.timestream.TimestreamSinkConstants;
import software.amazon.timestream.exception.TimestreamSinkConnectorException;
import software.amazon.timestream.schema.RejectedRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Class that publishes the rejected records to the configured dead letter queue.
 */
@Getter
public class DLQReporter {
    /**
     * Logger object
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(DLQReporter.class);
    /**
     * JSON serializer/de-serializer
     */
    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();
    /**
     * configured dead letter topic name
     */
    private final String dlqTopicName;

    /**
     * Error reporter
     */
    private final ErrantRecordReporter errantRecordReporter;

    /**
     * Kafka client: DLQ Publisher
     */
    private KafkaProducer<String, String> dlqPublisher;

    /**
     *
     * @param config Timestream Sink connector Configuration
     */
    public DLQReporter(final TimestreamSinkConnectorConfig config, final ErrantRecordReporter reporter) {
        this.dlqTopicName = config.getString(TimestreamSinkConstants.DLQ_TOPIC_NAME);
        this.errantRecordReporter = reporter;
        try {
            this.dlqPublisher = new KafkaProducer<>(initiateDLQConfiguration(config));
            LOGGER.info("DLQReporter: bootstrapServer: [{}], dlqTopicName: [{}]", config.getString(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG), dlqTopicName);
        } catch (KafkaException ke) {
            LOGGER.error("ERROR: while configuring DLQReporter kafka producer client", ke);
        }
    }

    /**
     * Method to initiate DLQ Publisher configuration
     * @param config TimestreamSinkConnectorConfig
     * @return DLQ Publisher configuration
     */
    private Properties initiateDLQConfiguration(final TimestreamSinkConnectorConfig config) {
        final Properties dlqPubConfig = new Properties();
        dlqPubConfig.put(CommonClientConfigs.CLIENT_ID_CONFIG, TimestreamSinkConstants.CONST_DLQ_CLIENT_ID + "-" + Math.random());
        dlqPubConfig.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG));
        dlqPubConfig.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, config.getString(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        dlqPubConfig.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, config.getString(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        //Security: IAM Authentication based
        dlqPubConfig.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_SSL.name);
        dlqPubConfig.put(SaslConfigs.SASL_MECHANISM, TimestreamSinkConstants.DEFAULT_SASL_MECHANISM );
        dlqPubConfig.put(SaslConfigs.SASL_JAAS_CONFIG, TimestreamSinkConstants.DEFAULT_SASL_JAAS_CONFIG);
        dlqPubConfig.put(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS, TimestreamSinkConstants.DEFAULT_SASL_CLIENT_CALLBACK_HANDLER);
        LOGGER.info("DLQPublisher: initiateDLQPublisherProperties: [{}]", dlqPubConfig);
        return dlqPubConfig;
    }
    /**
     * Method to publish the rejected Timestream record to DLQ for further processing
     *
     * @param rejectedRecords: Rejected Timestream records
     */
    public void reportRejectedRecords(final List<RejectedRecord> rejectedRecords) {
        LOGGER.trace("Begin::DLQPublisher::reportRejectedRecords");
        for (final RejectedRecord record : rejectedRecords) {
            try {
                final String rejRecordTxt = GSON.toJson(record.toString());
                LOGGER.trace("DLQPublisher: reportRejectedRecords: Record: [{}]", rejRecordTxt);
                if (record.getRejectedRecord() != null && dlqPublisher != null) {
                    final List<Header> headers = new ArrayList<>();
                    headers.add(new RecordHeader("rejection-reason", record.getReason().getBytes()));
                    dlqPublisher.send(new ProducerRecord<>(this.dlqTopicName, null, record.getRejectedRecord().time(), rejRecordTxt, headers));
                } else if (record.getRejectedSinkRecord() != null) {
                    errantRecordReporter.report(record.getRejectedSinkRecord(), new TimestreamSinkConnectorException(record.getReason()));
                }
            } catch (RuntimeException re) {
                LOGGER.error("DLQPublisher::reportRejectedRecords: Error while publishing the rejected record: [{}] to DLQ: [{}]",
                        record, this.dlqTopicName, re);
            }
        }
    }
}
