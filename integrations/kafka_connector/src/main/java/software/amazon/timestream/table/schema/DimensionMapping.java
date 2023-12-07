package software.amazon.timestream.table.schema;

import com.google.gson.annotations.SerializedName;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Timestream Table schema definition - Dimension Mapping
 */
@Getter
@EqualsAndHashCode
public class DimensionMapping {

    /**
     *  Source column name
     */
    @SerializedName("SourceColumn")
    private String sourceColumn;

    /**
     * Timestream column name
     */
    @SerializedName("DestinationColumn")
    private String destinationColumn;
}
