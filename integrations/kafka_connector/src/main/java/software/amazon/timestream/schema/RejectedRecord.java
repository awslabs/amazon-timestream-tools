package software.amazon.timestream.schema;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.kafka.connect.sink.SinkRecord;
import software.amazon.awssdk.services.timestreamwrite.model.Record;



/**
 * Class that encapsulates the record that gets rejected while ingesting to
 * Timestream table along with the reason for rejection
 */
@Getter
@Setter
@ToString
public class RejectedRecord {

    /**
     * Record that is rejected while ingesting to Timestream table
     */
    private Record rejectedRecord;

    /**
     * Record that is rejected while ingesting to Timestream table
     */
    private SinkRecord rejectedSinkRecord;
    /**
     * rejection reason
     */
    private String reason;
    /**
     * @param rejectedRecord Record - rejected while ingesting to Timestream table
     * @param reason rejection reason
     */
    public RejectedRecord(final Record rejectedRecord, final String reason) {
        this.rejectedRecord = rejectedRecord;
        this.reason = reason;
    }
    /**
     * @param rejectedRecord Sink Record rejected while ingesting to Timestream table
     * @param reason rejection reason
     */
    public RejectedRecord(final SinkRecord rejectedRecord, final String reason) {
        this.rejectedSinkRecord = rejectedRecord;
        this.reason = reason;
    }
}
