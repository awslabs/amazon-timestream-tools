package software.amazon.timestream.utility.reader;

import software.amazon.awssdk.services.timestreamwrite.model.DataModel;

/**
 * Timestream table schema definition reader
 */
public interface TimestreamSchemaReader {

    /**
     * gets schema definition
     */
    DataModel getSchemaDefinition();
}
