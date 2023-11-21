package software.amazon.timestream.table.schema;

import com.google.gson.annotations.SerializedName;
import lombok.EqualsAndHashCode;
import lombok.Getter;
/**
 * Timestream Table schema definition - MultiMeasure Attribute Mapping
 */
@Getter
@EqualsAndHashCode
public class MultiMeasureAttributeMapping {
    /**
     *  Source column name
     */
    @SerializedName("SourceColumn")
    private String sourceColumn;
    /**
     *  Timestream Multi-measure attribute name
     */
    @SerializedName("TargetMultiMeasureAttributeName")
    private String targetAttribute;
    /**
     *  Timestream Multi-measure value type
     */
    @SerializedName("MeasureValueType")
    private String measureValueType;
}
