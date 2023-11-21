package software.amazon.timestream.utility.reader;

import software.amazon.timestream.table.schema.SchemaDefinition;

/**
 * Timestream table schema definition reader
 */
public interface TimestreamSchemaReader {

    /**
     * gets schema definition
     */
    SchemaDefinition getSchemaDefinition();
}
