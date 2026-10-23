/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import com.influxdb.v3.client.write.WritePrecision;
import software.amazon.timestream.influxdb3.flink.LineProtocolConverter;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Converts Flink SQL rows into line protocol.
 *
 * <p>Tags are written in key order, null or empty tags and null or non-finite fields are omitted,
 * and rows that cannot be represented throw {@link IllegalArgumentException} so that the sink's
 * validation policy applies.
 */
@Internal
final class RowDataLineProtocolConverter implements LineProtocolConverter<RowData> {
    private static final long serialVersionUID = 1L;

    /** Describes the measurement, which is the sink table name, in validation errors. */
    private static final String MEASUREMENT_DESCRIPTION = "Measurement (sink table name)";

    /** Column name reserved by InfluxDB 3 for the point timestamp. */
    static final String RESERVED_TIME_COLUMN = "time";

    /** Name prefixes reserved by InfluxDB 3 for system and internal use. */
    static final List<String> RESERVED_PREFIXES = List.of("_", "iox_");

    private static final Set<LogicalTypeRoot> TAG_TYPES =
            EnumSet.of(LogicalTypeRoot.CHAR, LogicalTypeRoot.VARCHAR);
    private static final Set<LogicalTypeRoot> TIMESTAMP_TYPES =
            EnumSet.of(
                    LogicalTypeRoot.BIGINT,
                    LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
                    LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    private static final Set<LogicalTypeRoot> FIELD_TYPES =
            EnumSet.of(
                    LogicalTypeRoot.BOOLEAN,
                    LogicalTypeRoot.TINYINT,
                    LogicalTypeRoot.SMALLINT,
                    LogicalTypeRoot.INTEGER,
                    LogicalTypeRoot.BIGINT,
                    LogicalTypeRoot.FLOAT,
                    LogicalTypeRoot.DOUBLE,
                    LogicalTypeRoot.DECIMAL,
                    LogicalTypeRoot.CHAR,
                    LogicalTypeRoot.VARCHAR);

    private final String escapedMeasurement;
    private final List<Column> tags;
    private final List<Column> fields;
    private final Column timestamp;
    private final WritePrecision precision;

    private RowDataLineProtocolConverter(
            String escapedMeasurement,
            List<Column> tags,
            List<Column> fields,
            Column timestamp,
            WritePrecision precision) {
        this.escapedMeasurement = escapedMeasurement;
        this.tags = tags;
        this.fields = fields;
        this.timestamp = timestamp;
        this.precision = precision;
    }

    /**
     * Validates the table schema and creates a converter.
     *
     * @param rowType physical row type of the sink table
     * @param measurement measurement written by every row; the sink table name
     * @param tagColumns columns written as tags
     * @param timestampColumn column providing each row's timestamp
     * @param precision precision of written timestamps
     * @return a converter for rows of {@code rowType}
     * @throws ValidationException if the schema cannot be written as line protocol
     */
    static RowDataLineProtocolConverter create(
            RowType rowType,
            String measurement,
            List<String> tagColumns,
            String timestampColumn,
            WritePrecision precision) {
        Objects.requireNonNull(precision, "precision");
        if (measurement == null || measurement.isBlank()) {
            throw new ValidationException(MEASUREMENT_DESCRIPTION + " must not be blank.");
        }
        requireNoLineBreak(measurement, MEASUREMENT_DESCRIPTION);
        if (measurement.startsWith("#")) {
            // Line protocol treats a line starting with '#' as a comment, which would be dropped.
            throw new ValidationException(MEASUREMENT_DESCRIPTION + " must not start with '#'.");
        }
        String reservedPrefix = reservedPrefixOf(measurement);
        if (reservedPrefix != null) {
            throw new ValidationException(
                    MEASUREMENT_DESCRIPTION
                            + " must not start with '"
                            + reservedPrefix
                            + "', which InfluxDB 3 reserves.");
        }
        List<String> names = rowType.getFieldNames();
        for (String name : names) {
            requireNoLineBreak(name, "Column name '" + name + "'");
        }

        int timestampIndex = names.indexOf(timestampColumn);
        if (timestampIndex < 0) {
            throw new ValidationException(
                    "Timestamp column '" + timestampColumn + "' does not exist in the table.");
        }
        LogicalType timestampType = rowType.getTypeAt(timestampIndex);
        if (!TIMESTAMP_TYPES.contains(timestampType.getTypeRoot())) {
            throw new ValidationException(
                    "Timestamp column '"
                            + timestampColumn
                            + "' has unsupported type "
                            + timestampType.asSummaryString()
                            + ". Supported types are TIMESTAMP, TIMESTAMP_LTZ, and BIGINT.");
        }
        Column timestamp = new Column(timestampColumn, null, timestampIndex, timestampType);

        Set<String> uniqueTags = new LinkedHashSet<>();
        List<Column> tags = new ArrayList<>();
        for (String tag : tagColumns) {
            if (!uniqueTags.add(tag)) {
                throw new ValidationException("Tag column '" + tag + "' is listed more than once.");
            }
            if (tag.equals(timestampColumn)) {
                throw new ValidationException(
                        "Column '" + tag + "' cannot be both a tag and the timestamp column.");
            }
            int index = names.indexOf(tag);
            if (index < 0) {
                throw new ValidationException(
                        "Tag column '" + tag + "' does not exist in the table.");
            }
            requireNotReserved(tag, "Tag");
            LogicalType type = rowType.getTypeAt(index);
            if (!TAG_TYPES.contains(type.getTypeRoot())) {
                throw new ValidationException(
                        "Tag column '"
                                + tag
                                + "' has unsupported type "
                                + type.asSummaryString()
                                + ". Tag columns must be STRING, VARCHAR, or CHAR; cast other "
                                + "types in the query.");
            }
            tags.add(new Column(tag, escapeKey(tag), index, type));
        }
        tags.sort(Comparator.comparing(Column::name));

        List<Column> fields = new ArrayList<>();
        for (int index = 0; index < names.size(); index++) {
            String name = names.get(index);
            if (index == timestampIndex || uniqueTags.contains(name)) {
                continue;
            }
            requireNotReserved(name, "Field");
            LogicalType type = rowType.getTypeAt(index);
            if (!FIELD_TYPES.contains(type.getTypeRoot())) {
                throw new ValidationException(
                        "Field column '"
                                + name
                                + "' has unsupported type "
                                + type.asSummaryString()
                                + ". Supported field types are BOOLEAN, TINYINT, SMALLINT, INT, "
                                + "BIGINT, FLOAT, DOUBLE, DECIMAL, and string types.");
            }
            fields.add(new Column(name, escapeKey(name), index, type));
        }
        if (fields.isEmpty()) {
            throw new ValidationException(
                    "The table must have at least one field column besides the tag and "
                            + "timestamp columns.");
        }

        return new RowDataLineProtocolConverter(
                escapeMeasurement(measurement),
                List.copyOf(tags),
                List.copyOf(fields),
                timestamp,
                precision);
    }

    @Override
    public String toLineProtocol(RowData row) {
        long timestampValue = timestampOf(row);
        StringBuilder line = new StringBuilder(escapedMeasurement);
        for (Column tag : tags) {
            if (row.isNullAt(tag.position())) {
                continue;
            }
            String value = row.getString(tag.position()).toString();
            if (value.isEmpty()) {
                continue;
            }
            requireNoLineBreakInValue(value, tag);
            line.append(',').append(tag.escapedKey()).append('=').append(escapeKey(value));
        }

        line.append(' ');
        boolean hasField = false;
        for (Column field : fields) {
            if (row.isNullAt(field.position())) {
                continue;
            }
            String value = fieldValue(row, field);
            if (value == null) {
                continue;
            }
            if (hasField) {
                line.append(',');
            }
            line.append(field.escapedKey()).append('=').append(value);
            hasField = true;
        }
        if (!hasField) {
            throw new IllegalArgumentException(
                    "Row has no non-null field values; line protocol requires at least one field");
        }
        return line.append(' ').append(timestampValue).toString();
    }

    private long timestampOf(RowData row) {
        int position = timestamp.position();
        if (row.isNullAt(position)) {
            throw new IllegalArgumentException(
                    "Timestamp column '" + timestamp.name() + "' is null");
        }
        LogicalType type = timestamp.type();
        if (type.getTypeRoot() == LogicalTypeRoot.BIGINT) {
            return row.getLong(position);
        }
        TimestampData value = row.getTimestamp(position, LogicalTypeChecks.getPrecision(type));
        try {
            return toEpoch(value, precision);
        } catch (ArithmeticException error) {
            throw new IllegalArgumentException(
                    "Timestamp column '"
                            + timestamp.name()
                            + "' value "
                            + value
                            + " is out of range for precision "
                            + precision,
                    error);
        }
    }

    static long toEpoch(TimestampData value, WritePrecision precision) {
        long millis = value.getMillisecond();
        int nanosOfMilli = value.getNanoOfMillisecond();
        switch (precision) {
            case NS:
                return Math.addExact(Math.multiplyExact(millis, 1_000_000L), nanosOfMilli);
            case US:
                return Math.addExact(Math.multiplyExact(millis, 1_000L), nanosOfMilli / 1_000);
            case MS:
                return millis;
            case S:
                return Math.floorDiv(millis, 1_000L);
            default:
                throw new IllegalStateException("Unsupported precision " + precision);
        }
    }

    /** Returns the encoded field value, or {@code null} when the value must be omitted. */
    private static String fieldValue(RowData row, Column field) {
        int position = field.position();
        LogicalType type = field.type();
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return row.getBoolean(position) ? "true" : "false";
            case TINYINT:
                return row.getByte(position) + "i";
            case SMALLINT:
                return row.getShort(position) + "i";
            case INTEGER:
                return row.getInt(position) + "i";
            case BIGINT:
                return row.getLong(position) + "i";
            case FLOAT:
                float floatValue = row.getFloat(position);
                return Float.isFinite(floatValue) ? plainDecimal(Float.toString(floatValue)) : null;
            case DOUBLE:
                double doubleValue = row.getDouble(position);
                return Double.isFinite(doubleValue)
                        ? plainDecimal(Double.toString(doubleValue))
                        : null;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                double decimalValue =
                        row.getDecimal(position, decimalType.getPrecision(), decimalType.getScale())
                                .toBigDecimal()
                                .doubleValue();
                return plainDecimal(Double.toString(decimalValue));
            case CHAR:
            case VARCHAR:
                String value = row.getString(position).toString();
                requireNoLineBreakInValue(value, field);
                return '"' + escapeStringValue(value) + '"';
            default:
                throw new IllegalStateException("Unsupported field type " + type);
        }
    }

    /** Formats the shortest round-trip representation of a float without an exponent. */
    private static String plainDecimal(String shortestRepresentation) {
        return new BigDecimal(shortestRepresentation).toPlainString();
    }

    static String escapeMeasurement(String value) {
        return escape(value, ", \\");
    }

    static String escapeKey(String value) {
        return escape(value, ",= \\");
    }

    static String escapeStringValue(String value) {
        return escape(value, "\"\\");
    }

    private static String escape(String value, String specialCharacters) {
        StringBuilder escaped = null;
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (specialCharacters.indexOf(character) >= 0) {
                if (escaped == null) {
                    escaped = new StringBuilder(value.length() + 8);
                    escaped.append(value, 0, index);
                }
                escaped.append('\\');
            }
            if (escaped != null) {
                escaped.append(character);
            }
        }
        return escaped == null ? value : escaped.toString();
    }

    private static void requireNotReserved(String column, String role) {
        if (RESERVED_TIME_COLUMN.equals(column)) {
            throw new ValidationException(
                    role
                            + " column '"
                            + column
                            + "' uses a name reserved by InfluxDB 3. Rename the column; only the "
                            + "timestamp column may be named 'time'.");
        }
        String reservedPrefix = reservedPrefixOf(column);
        if (reservedPrefix != null) {
            throw new ValidationException(
                    role
                            + " column '"
                            + column
                            + "' starts with '"
                            + reservedPrefix
                            + "', which InfluxDB 3 reserves. Rename the column in the sink "
                            + "table.");
        }
    }

    private static String reservedPrefixOf(String name) {
        for (String prefix : RESERVED_PREFIXES) {
            if (name.startsWith(prefix)) {
                return prefix;
            }
        }
        return null;
    }

    private static void requireNoLineBreak(String value, String description) {
        if (containsLineBreak(value)) {
            throw new ValidationException(description + " must not contain line breaks.");
        }
    }

    private static void requireNoLineBreakInValue(String value, Column column) {
        if (containsLineBreak(value)) {
            throw new IllegalArgumentException(
                    "Column '"
                            + column.name()
                            + "' contains a line break, which line protocol cannot represent");
        }
    }

    private static boolean containsLineBreak(String value) {
        return value.indexOf('\n') >= 0 || value.indexOf('\r') >= 0;
    }

    private record Column(String name, String escapedKey, int position, LogicalType type)
            implements Serializable {}
}
