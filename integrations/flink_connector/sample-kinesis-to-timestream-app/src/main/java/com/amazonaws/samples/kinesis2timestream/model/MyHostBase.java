package com.amazonaws.samples.kinesis2timestream.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
@JsonSubTypes({
        @JsonSubTypes.Type(value = MyHostMetric.class, name = "metrics"),
        @JsonSubTypes.Type(value = MyHostEvent.class, name = "events")
})
@Data
public class MyHostBase {
    @JsonProperty(value = "microservice_name", required = true)
    private String microserviceName;

    @JsonProperty(value = "instance_name", required = true)
    private String instanceName;

    @JsonProperty(value = "time", required = true)
    private Long time;

    @JsonProperty(value = "region", required = true)
    private String region;

    @JsonProperty(value = "cell", required = true)
    private String cell;

    @JsonProperty(value = "memory_free", required = true)
    private Double memoryFree;

    @JsonProperty(value = "silo", required = true)
    private String silo;

    @JsonProperty(value = "availability_zone", required = true)
    private String availabilityZone;
}
