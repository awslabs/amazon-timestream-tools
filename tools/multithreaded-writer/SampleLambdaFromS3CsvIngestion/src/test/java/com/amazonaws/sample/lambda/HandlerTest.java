package com.amazonaws.sample.lambda;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Map;

class HandlerTest {
    @Test
    @Disabled("Only for local debugging.")
    void testExecution() {
        EnvVariablesHelper.setTimestreamWriterThreadPoolSize(5);
        EnvVariablesHelper.setTimestreamWriterQueueSize(500000);
        EnvVariablesHelper.setS3Region("us-east-1");
        EnvVariablesHelper.setTimestreamRegion("us-east-1");
        EnvVariablesHelper.setTargetTimestreamDatabase("mt2");
        EnvVariablesHelper.setTargetTimestreamTable("testo8");

        final Handler h = new Handler();
        final Map<String, Object> event = new java.util.HashMap<>();
        event.put("local-test-path", "..\\..\\SampleLocalCsvIngestion\\src\\main\\resources\\sample_withHeader.csv");
        final String response = h.handleRequest(event, null);

        Assertions.assertEquals("OK", response);
    }
}