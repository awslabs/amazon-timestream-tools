package com.amazonaws.samples.kinesis2timestream.model;

import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class TimestreamRecordDeserializer implements DeserializationSchema<MyHostBase> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public MyHostBase deserialize(byte[] messageBytes) {
        try {
            return objectMapper.readValue(messageBytes, MyHostBase.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize message", e);
        }
    }

    @Override
    public boolean isEndOfStream(MyHostBase nextElement) {
        return false;
    }

    @Override
    public TypeInformation<MyHostBase> getProducedType() {
        return TypeInformation.of(MyHostBase.class);
    }
}
