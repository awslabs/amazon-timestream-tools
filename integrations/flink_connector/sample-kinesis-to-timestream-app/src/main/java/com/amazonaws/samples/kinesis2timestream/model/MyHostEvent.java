package com.amazonaws.samples.kinesis2timestream.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class MyHostEvent extends MyHostBase {
    @JsonProperty(value = "task_completed", required = true)
    private Integer taskCompleted;

    @JsonProperty(value = "task_end_state", required = true)
    private String taskEndState;

    @JsonProperty(value = "gc_reclaimed", required = true)
    private Double gcReclaimed;

    @JsonProperty(value = "gc_pause", required = true)
    private Double gcPause;

    @JsonProperty(value = "process_name", required = true)
    private String processName;

    @JsonProperty(value = "jdk_version", required = true)
    private String jdkVersion;
}
