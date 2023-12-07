package software.amazon.timestream.table.schema;

import java.util.List;
import com.google.gson.annotations.SerializedName;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Timestream Table schema definition root class
 */
@Getter
@EqualsAndHashCode
public class SchemaDefinition {
    /**
     * Timestream Time unit
     */
    @SerializedName("TimeUnit")
    private String timeUnit;
    /**
     * Column that represents time
     */
    @SerializedName("TimeColumn")
    private String timeColumn;
    /**
     * Timestream Dimension Mapping
     */
    @SerializedName("DimensionMappings")
    private List<DimensionMapping> dimensionMappings;
    /**
     * Timestream Multi Measure Mapping
     */
    @SerializedName("MultiMeasureMappings")
    private MultiMeasureMappings multiMappings;
    /**
     * Column that represents the measure
     */
    @SerializedName("MeasureNameColumn")
    private String measureNameColumn;
}
