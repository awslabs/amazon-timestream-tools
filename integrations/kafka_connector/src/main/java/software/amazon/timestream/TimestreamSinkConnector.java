package software.amazon.timestream;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that implements {@link SinkConnector} - meant for starting the connector,
 * loading configurations and stopping the connector
 */
public class TimestreamSinkConnector extends SinkConnector {
    /**
     * Logger object
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(TimestreamSinkConnector.class);
    /**
     * Timestream Sink Connector Config values
     */
    private TimestreamSinkConnectorConfig connectorConfig;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(final Map<String, String> props) {
        LOGGER.info("Begin::TimestreamSinkConnector::start");
        connectorConfig = new TimestreamSinkConnectorConfig(props);
        LOGGER.info("Complete::TimestreamSinkConnector::start");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return TimestreamSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(final int maxTasks) {
        LOGGER.trace("Begin::TimestreamSinkConnector::taskConfigs");
        if (maxTasks == 0) {
            LOGGER.warn("No Connector tasks have been configured.");
        }
        final List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(new ConcurrentHashMap<>(connectorConfig.originalsStrings()));
        }
        LOGGER.trace("Complete::TimestreamSinkConnector::taskConfigs");
        return configs;
    }

    @Override
    public void stop() {
        LOGGER.info("Begin::TimestreamSinkConnector::stop");
    }

    @Override
    public ConfigDef config() {
        LOGGER.trace("Begin::TimestreamSinkConnector::config");
        final ConfigDef conf = TimestreamSinkConnectorConfig.conf();
        LOGGER.trace("Complete::TimestreamSinkConnector::config");
        return conf;
    }
}
