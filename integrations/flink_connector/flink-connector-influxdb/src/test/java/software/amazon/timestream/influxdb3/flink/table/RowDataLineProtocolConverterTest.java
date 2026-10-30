/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.table;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.influxdb.v3.client.write.WritePrecision;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RowDataLineProtocolConverterTest {

    private static final LogicalType STRING = VarCharType.STRING_TYPE;
    private static final long EPOCH_MILLIS = 1_735_689_600_123L;
    private static final TimestampData TIMESTAMP =
            TimestampData.fromEpochMillis(EPOCH_MILLIS, 456_789);

    @Test
    void writesEverySupportedFieldType() {
        RowType rowType =
                rowType(
                        new String[] {
                            "host", "ok", "tiny", "small", "count", "big", "ratio", "value",
                            "amount", "label", "code", "ts"
                        },
                        STRING,
                        new BooleanType(),
                        new TinyIntType(),
                        new SmallIntType(),
                        new IntType(),
                        new BigIntType(),
                        new FloatType(),
                        new DoubleType(),
                        new DecimalType(10, 3),
                        STRING,
                        new CharType(3),
                        new TimestampType(9));
        RowDataLineProtocolConverter converter =
                RowDataLineProtocolConverter.create(
                        rowType, "cpu", List.of("host"), "ts", WritePrecision.NS);

        String line =
                converter.toLineProtocol(
                        GenericRowData.of(
                                StringData.fromString("a"),
                                true,
                                (byte) -1,
                                (short) 2,
                                3,
                                4_000_000_000L,
                                0.1f,
                                1.0e20,
                                DecimalData.fromBigDecimal(new BigDecimal("12.345"), 10, 3),
                                StringData.fromString("hello"),
                                StringData.fromString("abc"),
                                TIMESTAMP));

        assertThat(line)
                .isEqualTo(
                        "cpu,host=a ok=true,tiny=-1i,small=2i,count=3i,big=4000000000i,"
                                + "ratio=0.1,value=100000000000000000000,amount=12.345,"
                                + "label=\"hello\",code=\"abc\" 1735689600123456789");
    }

    @Test
    void writesFalseBooleansAndPlainSmallFloats() {
        RowDataLineProtocolConverter converter =
                converter(
                        rowType(
                                new String[] {"ok", "value", "ts"},
                                new BooleanType(),
                                new DoubleType(),
                                new BigIntType()),
                        List.of(),
                        "ts");

        assertThat(converter.toLineProtocol(GenericRowData.of(false, 1.5e-7, 7L)))
                .isEqualTo("m ok=false,value=0.00000015 7");
    }

    @Test
    void sortsTagsByKeyAndOmitsNullAndEmptyTags() {
        RowDataLineProtocolConverter converter =
                converter(
                        rowType(
                                new String[] {"zone", "host", "region", "value", "ts"},
                                STRING,
                                STRING,
                                STRING,
                                new IntType(),
                                new BigIntType()),
                        List.of("zone", "region", "host"),
                        "ts");

        assertThat(
                        converter.toLineProtocol(
                                GenericRowData.of(
                                        StringData.fromString("z1"),
                                        null,
                                        StringData.fromString(""),
                                        1,
                                        10L)))
                .isEqualTo("m,zone=z1 value=1i 10");
        assertThat(
                        converter.toLineProtocol(
                                GenericRowData.of(
                                        StringData.fromString("z1"),
                                        StringData.fromString("h1"),
                                        StringData.fromString("r1"),
                                        1,
                                        10L)))
                .isEqualTo("m,host=h1,region=r1,zone=z1 value=1i 10");
    }

    @Test
    void omitsNullAndNonFiniteFields() {
        RowDataLineProtocolConverter converter =
                converter(
                        rowType(
                                new String[] {"a", "b", "c", "d", "e", "ts"},
                                new IntType(),
                                new DoubleType(),
                                new DoubleType(),
                                new FloatType(),
                                new DoubleType(),
                                new BigIntType()),
                        List.of(),
                        "ts");

        assertThat(
                        converter.toLineProtocol(
                                GenericRowData.of(
                                        null,
                                        Double.NaN,
                                        Double.POSITIVE_INFINITY,
                                        Float.NEGATIVE_INFINITY,
                                        2.5,
                                        1L)))
                .isEqualTo("m e=2.5 1");
    }

    @Test
    void rejectsRowsWithoutAnyWritableField() {
        RowDataLineProtocolConverter converter =
                converter(
                        rowType(
                                new String[] {"a", "b", "ts"},
                                new IntType(),
                                new DoubleType(),
                                new BigIntType()),
                        List.of(),
                        "ts");

        assertThatThrownBy(() -> converter.toLineProtocol(GenericRowData.of(null, Double.NaN, 1L)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no non-null field values");
    }

    @Test
    void rejectsNullTimestamps() {
        RowDataLineProtocolConverter converter =
                converter(
                        rowType(new String[] {"a", "ts"}, new IntType(), new TimestampType(3)),
                        List.of(),
                        "ts");

        assertThatThrownBy(() -> converter.toLineProtocol(GenericRowData.of(1, null)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Timestamp column 'ts' is null");
    }

    @Test
    void rejectsLineBreaksInTagAndStringFieldValues() {
        RowDataLineProtocolConverter converter =
                converter(
                        rowType(
                                new String[] {"host", "label", "ts"},
                                STRING,
                                STRING,
                                new BigIntType()),
                        List.of("host"),
                        "ts");

        assertThatThrownBy(
                        () ->
                                converter.toLineProtocol(
                                        GenericRowData.of(
                                                StringData.fromString("a\nb"),
                                                StringData.fromString("ok"),
                                                1L)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Column 'host' contains a line break");
        assertThatThrownBy(
                        () ->
                                converter.toLineProtocol(
                                        GenericRowData.of(
                                                StringData.fromString("a"),
                                                StringData.fromString("x\ry"),
                                                1L)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Column 'label' contains a line break");
    }

    @Test
    void escapesSpecialCharactersInNamesAndValues() {
        RowType rowType =
                rowType(
                        new String[] {"host name", "a=b,c", "ts"},
                        STRING,
                        STRING,
                        new BigIntType());
        RowDataLineProtocolConverter converter =
                RowDataLineProtocolConverter.create(
                        rowType, "my measurement,x=y", List.of("host name"), "ts",
                        WritePrecision.NS);

        assertThat(
                        converter.toLineProtocol(
                                GenericRowData.of(
                                        StringData.fromString("web 1,a=b\\"),
                                        StringData.fromString("say \"hi\" \\ bye"),
                                        5L)))
                .isEqualTo(
                        "my\\ measurement\\,x=y,host\\ name=web\\ 1\\,a\\=b\\\\ "
                                + "a\\=b\\,c=\"say \\\"hi\\\" \\\\ bye\" 5");
    }

    @ParameterizedTest
    @CsvSource({"NS,1735689600123456789", "US,1735689600123456", "MS,1735689600123", "S,1735689600"})
    void convertsTimestampsToTheConfiguredPrecision(WritePrecision precision, long expected) {
        RowDataLineProtocolConverter converter =
                RowDataLineProtocolConverter.create(
                        rowType(new String[] {"v", "ts"}, new IntType(), new TimestampType(9)),
                        "m",
                        List.of(),
                        "ts",
                        precision);

        assertThat(converter.toLineProtocol(GenericRowData.of(1, TIMESTAMP)))
                .isEqualTo("m v=1i " + expected);
    }

    @Test
    void truncatesPreEpochTimestampsTowardNegativeInfinity() {
        TimestampData beforeEpoch = TimestampData.fromEpochMillis(-1_500L, 0);

        assertThat(RowDataLineProtocolConverter.toEpoch(beforeEpoch, WritePrecision.S))
                .isEqualTo(-2L);
        assertThat(RowDataLineProtocolConverter.toEpoch(beforeEpoch, WritePrecision.NS))
                .isEqualTo(-1_500_000_000L);
    }

    @Test
    void interpretsTimestampWithoutTimeZoneAsUtcAndSupportsLocalZonedTimestamps() {
        TimestampData localDateTime =
                TimestampData.fromLocalDateTime(LocalDateTime.of(2025, 1, 1, 0, 0, 0, 5_000_000));
        RowDataLineProtocolConverter withoutTimeZone =
                converter(
                        rowType(new String[] {"v", "ts"}, new IntType(), new TimestampType(3)),
                        List.of(),
                        "ts");
        RowDataLineProtocolConverter localZoned =
                converter(
                        rowType(
                                new String[] {"v", "ts"},
                                new IntType(),
                                new LocalZonedTimestampType(3)),
                        List.of(),
                        "ts");

        assertThat(withoutTimeZone.toLineProtocol(GenericRowData.of(1, localDateTime)))
                .isEqualTo("m v=1i 1735689600005000000");
        assertThat(
                        localZoned.toLineProtocol(
                                GenericRowData.of(1, TimestampData.fromEpochMillis(5L))))
                .isEqualTo("m v=1i 5000000");
    }

    @Test
    void rejectsTimestampsOutsideTheNanosecondRange() {
        RowDataLineProtocolConverter converter =
                converter(
                        rowType(new String[] {"v", "ts"}, new IntType(), new TimestampType(3)),
                        List.of(),
                        "ts");
        TimestampData farFuture =
                TimestampData.fromLocalDateTime(LocalDateTime.of(9999, 1, 1, 0, 0));

        assertThatThrownBy(() -> converter.toLineProtocol(GenericRowData.of(1, farFuture)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("out of range for precision NS")
                .hasCauseInstanceOf(ArithmeticException.class);
    }

    @Test
    void survivesJavaSerialization() throws Exception {
        RowDataLineProtocolConverter converter =
                converter(
                        rowType(
                                new String[] {"host", "v", "ts"},
                                STRING,
                                new DecimalType(5, 2),
                                new BigIntType()),
                        List.of("host"),
                        "ts");
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(converter);
        }
        RowDataLineProtocolConverter restored;
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (RowDataLineProtocolConverter) input.readObject();
        }

        assertThat(
                        restored.toLineProtocol(
                                GenericRowData.of(
                                        StringData.fromString("a"),
                                        DecimalData.fromBigDecimal(new BigDecimal("1.50"), 5, 2),
                                        3L)))
                .isEqualTo("m,host=a v=1.5 3");
    }

    @Test
    void rejectsInvalidSchemas() {
        RowType valid =
                rowType(
                        new String[] {"host", "count", "ts"},
                        STRING,
                        new IntType(),
                        new BigIntType());

        assertInvalid(
                valid, " ", List.of(), "ts", "Measurement (sink table name) must not be blank.");
        assertInvalid(
                valid, "a\nb", List.of(), "ts",
                "Measurement (sink table name) must not contain line breaks.");
        assertInvalid(
                valid, "#m", List.of(), "ts",
                "Measurement (sink table name) must not start with '#'.");
        assertInvalid(
                valid, "m", List.of(), "missing",
                "Timestamp column 'missing' does not exist in the table.");
        assertInvalid(
                valid, "m", List.of(), "host",
                "Timestamp column 'host' has unsupported type STRING.");
        assertInvalid(
                valid, "m", List.of("host", "host"), "ts",
                "Tag column 'host' is listed more than once.");
        assertInvalid(
                valid, "m", List.of("ts"), "ts",
                "Column 'ts' cannot be both a tag and the timestamp column.");
        assertInvalid(
                valid, "m", List.of("missing"), "ts",
                "Tag column 'missing' does not exist in the table.");
        assertInvalid(
                valid, "m", List.of("count"), "ts",
                "Tag column 'count' has unsupported type INT.");
        assertInvalid(
                rowType(new String[] {"host", "ts"}, STRING, new BigIntType()),
                "m", List.of("host"), "ts",
                "The table must have at least one field column");
        assertInvalid(
                rowType(new String[] {"time", "v", "ts"}, STRING, new IntType(), new BigIntType()),
                "m", List.of("time"), "ts",
                "Tag column 'time' uses a name reserved by InfluxDB 3.");
        assertInvalid(
                rowType(new String[] {"time", "ts"}, new IntType(), new BigIntType()),
                "m", List.of(), "ts",
                "Field column 'time' uses a name reserved by InfluxDB 3.");
        assertInvalid(
                rowType(new String[] {"a\nb", "ts"}, new IntType(), new BigIntType()),
                "m", List.of(), "ts",
                "must not contain line breaks.");
        for (LogicalType unsupported :
                List.of(new DateType(), new VarBinaryType(4), new ArrayType(new IntType()))) {
            assertInvalid(
                    rowType(new String[] {"v", "ts"}, unsupported, new BigIntType()),
                    "m", List.of(), "ts",
                    "Field column 'v' has unsupported type " + unsupported.asSummaryString());
        }
    }

    @Test
    void rejectsNamesWithPrefixesReservedByInfluxDB3() {
        RowType valid = rowType(new String[] {"v", "ts"}, new IntType(), new BigIntType());

        assertInvalid(
                valid, "_m", List.of(), "ts",
                "Measurement (sink table name) must not start with '_', which InfluxDB 3 "
                        + "reserves.");
        assertInvalid(
                valid, "iox_m", List.of(), "ts",
                "Measurement (sink table name) must not start with 'iox_', which InfluxDB 3 "
                        + "reserves.");
        assertInvalid(
                rowType(new String[] {"_host", "v", "ts"}, STRING, new IntType(), new BigIntType()),
                "m", List.of("_host"), "ts",
                "Tag column '_host' starts with '_', which InfluxDB 3 reserves.");
        assertInvalid(
                rowType(new String[] {"_v", "ts"}, new IntType(), new BigIntType()),
                "m", List.of(), "ts",
                "Field column '_v' starts with '_', which InfluxDB 3 reserves.");
        assertInvalid(
                rowType(new String[] {"iox_v", "ts"}, new IntType(), new BigIntType()),
                "m", List.of(), "ts",
                "Field column 'iox_v' starts with 'iox_', which InfluxDB 3 reserves.");
    }

    @Test
    void allowsReservedPrefixesInTheTimestampColumnAndInsideNames() {
        RowDataLineProtocolConverter converter =
                RowDataLineProtocolConverter.create(
                        rowType(
                                new String[] {"host_iox_", "cpu_v", "_ts"},
                                STRING,
                                new IntType(),
                                new BigIntType()),
                        "m_iox_",
                        List.of("host_iox_"),
                        "_ts",
                        WritePrecision.NS);

        assertThat(
                        converter.toLineProtocol(
                                GenericRowData.of(StringData.fromString("a"), 1, 2L)))
                .isEqualTo("m_iox_,host_iox_=a cpu_v=1i 2");
    }

    @Test
    void allowsTheTimestampColumnToBeNamedTime() {
        RowDataLineProtocolConverter converter =
                converter(
                        rowType(new String[] {"v", "time"}, new IntType(), new BigIntType()),
                        List.of(),
                        "time");

        assertThat(converter.toLineProtocol(GenericRowData.of(1, 2L))).isEqualTo("m v=1i 2");
    }

    private static void assertInvalid(
            RowType rowType,
            String measurement,
            List<String> tags,
            String timestampColumn,
            String message) {
        assertThatThrownBy(
                        () ->
                                RowDataLineProtocolConverter.create(
                                        rowType,
                                        measurement,
                                        tags,
                                        timestampColumn,
                                        WritePrecision.NS))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(message);
    }

    private static RowDataLineProtocolConverter converter(
            RowType rowType, List<String> tags, String timestampColumn) {
        return RowDataLineProtocolConverter.create(
                rowType, "m", tags, timestampColumn, WritePrecision.NS);
    }

    private static RowType rowType(String[] names, LogicalType... types) {
        return RowType.of(types, names);
    }
}
