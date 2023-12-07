package software.amazon.timestream.table.schema;

import java.util.List;

import com.google.gson.annotations.SerializedName;
import lombok.EqualsAndHashCode;
import lombok.Getter;
/**
 * Timestream Table schema definition - List of MultiMeasure Attribute Mapping
 */
@Getter
@EqualsAndHashCode
public class MultiMeasureMappings {
    /**
     * Timestream Multi Measure Attribute Mapping
     */
    @SerializedName("MultiMeasureAttributeMappings")
    private List<MultiMeasureAttributeMapping> attributeMappings;
}
