package com.amazonaws.samples.connectors.timestream;

import org.apache.flink.annotation.PublicEvolving;
import software.amazon.awssdk.services.timestreamwrite.model.Record;
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsRequest;

import java.io.Serializable;
import java.util.List;

@PublicEvolving
public interface BatchConverter extends Serializable {
    /**
     * This method will convert multiple buffered records to single WriteRecordRequest to Timestream.
     */
    WriteRecordsRequest apply(List<Record> bufferedRecords);
}
