package com.amazonaws.samples.kinesis2timestream.model;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import software.amazon.awssdk.services.timestreamwrite.model.Dimension;
import software.amazon.awssdk.services.timestreamwrite.model.MeasureValue;
import software.amazon.awssdk.services.timestreamwrite.model.MeasureValueType;
import software.amazon.awssdk.services.timestreamwrite.model.Record;
import software.amazon.awssdk.services.timestreamwrite.model.TimeUnit;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TimestreamRecordDeserializerTest {
    private TimestreamRecordDeserializer deserializer;

    @BeforeAll
    public void init() {
        deserializer = new TimestreamRecordDeserializer();
    }

    @Test
    public void testMetricsDeserialize() {
        String jsonString = getDefaultJsonMetrics().toString();
        Record record = bytesToRecord(jsonString.getBytes(
                StandardCharsets.UTF_8));
        Assertions.assertTrue(record.hasDimensions());
        Assertions.assertTrue(record.hasMeasureValues());
        Assertions.assertEquals(MeasureValueType.MULTI,
                record.measureValueType());
        Assertions.assertEquals("metrics_record", record.measureName());
        Assertions.assertEquals("1642191519", record.time());
        Assertions.assertEquals(TimeUnit.SECONDS, record.timeUnit());

        // check dimensions
        Assertions.assertEquals(8, record.dimensions().size());
        assertDimensionExists(record, "region", "eu-west-1");
        assertDimensionExists(record, "cell", "eu-west-1-cell-10");
        assertDimensionExists(record, "silo", "eu-west-1-cell-10-silo-2");
        assertDimensionExists(record, "availability_zone", "eu-west-1-3");
        assertDimensionExists(record, "microservice_name", "apollo");
        assertDimensionExists(record, "instance_type", "r5.4x");
        assertDimensionExists(record, "os_version", "AL2");
        assertDimensionExists(record, "instance_name", "i-zaZswmJk-apollo-0002.amazonaws.com");

        // check measure values
        Assertions.assertEquals(20, record.measureValues().size());
        assertMeasureValueExists(record, "cpu_user", "8.420000");
        assertMeasureValueExists(record, "cpu_system", "0.65");
        assertMeasureValueExists(record, "cpu_steal", "0.35");
        assertMeasureValueExists(record, "cpu_iowait", "0.28");
        assertMeasureValueExists(record, "cpu_nice", "0.71");
        assertMeasureValueExists(record, "cpu_hi", "0.64");
        assertMeasureValueExists(record, "cpu_si", "0.81");
        assertMeasureValueExists(record, "cpu_idle", "88.14");
        assertMeasureValueExists(record, "memory_free", "22.26");
        assertMeasureValueExists(record, "memory_used", "51.39");
        assertMeasureValueExists(record, "memory_cached", "78.72");
        assertMeasureValueExists(record, "disk_io_reads", "36.84");
        assertMeasureValueExists(record, "disk_io_writes", "50.57");
        assertMeasureValueExists(record, "latency_per_read", "92.53");
        assertMeasureValueExists(record, "latency_per_write", "90.11");
        assertMeasureValueExists(record, "network_bytes_in", "57.49");
        assertMeasureValueExists(record, "network_bytes_out", "7.74");
        assertMeasureValueExists(record, "disk_used", "75.18");
        assertMeasureValueExists(record, "disk_free", "13.09");
        assertMeasureValueExists(record, "file_descriptors_in_use", "61.30");
    }

    @Test
    public void testEventsDeserialize() {
        String jsonString = getDefaultJsonEvents().toString();
        Record record = bytesToRecord(jsonString.getBytes(
                StandardCharsets.UTF_8));
        Assertions.assertTrue(record.hasDimensions());
        Assertions.assertTrue(record.hasMeasureValues());
        Assertions.assertEquals(MeasureValueType.MULTI,
                record.measureValueType());
        Assertions.assertEquals("events_record", record.measureName());
        Assertions.assertEquals("1642205551", record.time());
        Assertions.assertEquals(TimeUnit.SECONDS, record.timeUnit());

        // check dimensions
        Assertions.assertEquals(8, record.dimensions().size());
        assertDimensionExists(record, "region", "us_east_1");
        assertDimensionExists(record, "cell", "us_east_1-cell-1");
        assertDimensionExists(record, "silo", "us_east_1-cell-1-silo-1");
        assertDimensionExists(record, "availability_zone", "us_east_1-1");
        assertDimensionExists(record, "microservice_name", "hercules");
        assertDimensionExists(record, "instance_name", "i-zaZswmJk-hercules-0000.amazonaws.com");
        assertDimensionExists(record, "process_name", "server");
        assertDimensionExists(record, "jdk_version", "JDK_8");

        // check measure values
        Assertions.assertEquals(5, record.measureValues().size());
        assertMeasureValueExists(record, "task_completed", "373");
        assertMeasureValueExists(record, "task_end_state", "SUCCESS_WITH_RESULT");
        assertMeasureValueExists(record, "gc_reclaimed", "34.86");
        assertMeasureValueExists(record, "gc_pause", "33.16");
        assertMeasureValueExists(record, "memory_free", "19.1");
    }

    @Test
    public void testDeserializeDoubleNegativeZero() {
        final JSONObject defaultJsonMetrics = getDefaultJsonMetrics();
        defaultJsonMetrics.remove("disk_free");
        defaultJsonMetrics.put("disk_free", 333); //unfortunately even if you put -0.0 here, it gets serialized as -0, which is different from negative zero
        String jsonString = defaultJsonMetrics.toString();
        jsonString = jsonString.replace("\"disk_free\":333", "\"disk_free\":-0.0");
        Record record = bytesToRecord(jsonString.getBytes(StandardCharsets.UTF_8));
        assertMeasureValueExists(record, "disk_free", "0.0");
    }

    @Test
    public void testEventAsMetric_exception() {
        String jsonString = getDefaultJsonEvents().put("@type", "metrics").toString();
        Assertions.assertThrows(RuntimeException.class, () -> bytesToRecord(jsonString.getBytes(
                StandardCharsets.UTF_8)));
    }

    @Test
    public void testMetricAsEvent_exception() {
        String jsonString = getDefaultJsonMetrics().put("@type", "events").toString();
        Assertions.assertThrows(RuntimeException.class, () -> bytesToRecord(jsonString.getBytes(
                StandardCharsets.UTF_8)));
    }

    @Test
    public void testUnknownObjectType_exception() {
        String jsonString = getDefaultJsonMetrics().put("@type", "null").toString();
        Assertions.assertThrows(RuntimeException.class, () -> bytesToRecord(jsonString.getBytes(
                StandardCharsets.UTF_8)));
    }

    private JSONObject getDefaultJsonMetrics() {
        JSONObject inputJson = new JSONObject();
        inputJson.put("region", "eu-west-1");
        inputJson.put("cell", "eu-west-1-cell-10");
        inputJson.put("silo", "eu-west-1-cell-10-silo-2");
        inputJson.put("availability_zone", "eu-west-1-3");
        inputJson.put("microservice_name", "apollo");
        inputJson.put("instance_type", "r5.4x");
        inputJson.put("os_version", "AL2");
        inputJson.put("instance_name", "i-zaZswmJk-apollo-0002.amazonaws.com");
        inputJson.put("cpu_user", 8.42);
        inputJson.put("cpu_system", 0.65);
        inputJson.put("cpu_steal", 0.35);
        inputJson.put("cpu_iowait", 0.28);
        inputJson.put("cpu_nice", 0.71);
        inputJson.put("cpu_hi", 0.64);
        inputJson.put("cpu_si", 0.81);
        inputJson.put("cpu_idle", 88.14);
        inputJson.put("memory_free", 22.26);
        inputJson.put("memory_used", 51.39);
        inputJson.put("memory_cached", 78.72);
        inputJson.put("disk_io_reads", 36.84);
        inputJson.put("disk_io_writes", 50.57);
        inputJson.put("latency_per_read", 92.53);
        inputJson.put("latency_per_write", 90.11);
        inputJson.put("network_bytes_in", 57.49);
        inputJson.put("network_bytes_out", 7.74);
        inputJson.put("disk_used", 75.18);
        inputJson.put("disk_free", 13.09);
        inputJson.put("file_descriptors_in_use", 61.3);
        inputJson.put("time", 1642191519);
        inputJson.put("@type", "metrics");
        return inputJson;
    }

    private JSONObject getDefaultJsonEvents() {
        JSONObject inputJson = new JSONObject();
        inputJson.put("region", "us_east_1");
        inputJson.put("cell", "us_east_1-cell-1");
        inputJson.put("silo", "us_east_1-cell-1-silo-1");
        inputJson.put("availability_zone", "us_east_1-1");
        inputJson.put("microservice_name", "hercules");
        inputJson.put("instance_name", "i-zaZswmJk-hercules-0000.amazonaws.com");
        inputJson.put("process_name", "server");
        inputJson.put("jdk_version", "JDK_8");
        inputJson.put("task_completed", 373);
        inputJson.put("task_end_state", "SUCCESS_WITH_RESULT");
        inputJson.put("gc_reclaimed", 34.86);
        inputJson.put("gc_pause", 33.16);
        inputJson.put("memory_free", 19.1);
        inputJson.put("time", 1642205551);
        inputJson.put("@type", "events");
        return inputJson;
    }

    private Record bytesToRecord(byte[] bytes) {
        return TimestreamRecordConverter.convert(deserializer.deserialize(bytes));
    }

    // Inefficient, optimize to set if needed
    private void assertMeasureValueExists(Record record, String key, String val) {
        final Optional<MeasureValue> measureValue = record.measureValues()
                .stream().filter(mv -> mv.name().equals(key)).findFirst();
        Assertions.assertTrue(measureValue.isPresent());
        assertValueEquals(measureValue.get().type(), val, measureValue.get().value());
    }

    // Inefficient, optimize to set if needed
    private void assertDimensionExists(Record record, String key, String val) {
        final Optional<Dimension> dimension = record.dimensions()
                .stream().filter(mv -> mv.name().equals(key)).findFirst();
        Assertions.assertTrue(dimension.isPresent());
        Assertions.assertEquals(val, dimension.get().value());
    }

    private void assertValueEquals(MeasureValueType type, String val1, String val2) {
        if (type.equals(MeasureValueType.DOUBLE)) {
            double double1 = Double.parseDouble(val1);
            double double2 = Double.parseDouble(val2);
            Assertions.assertEquals(double1, double2, 0.0000000001);
        } else {
            Assertions.assertEquals(val1, val2);
        }
    }
}
