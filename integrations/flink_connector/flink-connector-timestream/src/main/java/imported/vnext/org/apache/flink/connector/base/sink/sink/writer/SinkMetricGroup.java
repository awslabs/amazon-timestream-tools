package imported.vnext.org.apache.flink.connector.base.sink.sink.writer;

import lombok.Getter;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

@Internal
public class SinkMetricGroup {
    @VisibleForTesting
    static final String SINK_METRIC_GROUP = "asyncSink";

    @VisibleForTesting
    static final String CURRENT_SEND_TIME_GAUGE = "currentSendTime";

    /* Counter for number of buffered input elements this sink has attempted to send to the destination. */
    @VisibleForTesting
    static final String NUM_RECORDS_OUT_COUNTER = "numRecordsOut";

    /* Counter for number of bytes this sink has attempted to send to the destination. */
    @VisibleForTesting
    static final String NUM_BYTES_OUT_COUNTER = "numBytesOut";

    @VisibleForTesting
    static final String IN_FLIGHT_REQUESTS_GAUGE = "inFlightRequests";

    @VisibleForTesting
    static final String BUFFERED_RECORDS_GAUGE = "bufferedRecords";

    @VisibleForTesting
    static final String MILLIS_TIME_BETWEEN_FLUSHES_GAUGE = "millisTimeBetweenFlushes";

    private final MetricGroup sinkGroup;

    @Getter
    private final Counter numRecordsOutCounter;

    @Getter
    private final Counter numBytesOutCounter;

    @Getter
    private Gauge<Long> currentSendTimeGauge;

    @Getter
    private Gauge<Long> inFlightRequestsGauge;

    @Getter
    private Gauge<Long> bufferedRecordsGauge;

    @Getter
    private Gauge<Long> millisTimeBetweenFlushesGauge;

    public SinkMetricGroup(MetricGroup metricGroup) {
        sinkGroup = metricGroup.addGroup(SINK_METRIC_GROUP);
        numRecordsOutCounter = sinkGroup.counter(NUM_RECORDS_OUT_COUNTER);
        numBytesOutCounter = sinkGroup.counter(NUM_BYTES_OUT_COUNTER);
    }

    public void setCurrentSendTimeGauge(Gauge<Long> currentSendTimeGauge) {
        sinkGroup.gauge(CURRENT_SEND_TIME_GAUGE, currentSendTimeGauge);
        this.currentSendTimeGauge = currentSendTimeGauge;
    }

    public void setInFlightRequestsGauge(Gauge<Long> inFlightRequestsGauge) {
        sinkGroup.gauge(IN_FLIGHT_REQUESTS_GAUGE, inFlightRequestsGauge);
        this.inFlightRequestsGauge = inFlightRequestsGauge;
    }

    public void setBufferedRecordsGauge(Gauge<Long> bufferedRecordsGauge) {
        sinkGroup.gauge(BUFFERED_RECORDS_GAUGE, bufferedRecordsGauge);
        this.bufferedRecordsGauge = bufferedRecordsGauge;
    }

    public void setMillisTimeBetweenFlushesGauge(Gauge<Long> millisTimeBetweenFlushesGauge) {
        sinkGroup.gauge(MILLIS_TIME_BETWEEN_FLUSHES_GAUGE, millisTimeBetweenFlushesGauge);
        this.millisTimeBetweenFlushesGauge = millisTimeBetweenFlushesGauge;
    }
}