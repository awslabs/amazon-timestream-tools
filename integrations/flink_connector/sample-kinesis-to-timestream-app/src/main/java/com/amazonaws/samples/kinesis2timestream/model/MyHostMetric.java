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
public class MyHostMetric extends MyHostBase{
    @JsonProperty(value = "cpu_user", required = true)
    private Double cpuUser;

    @JsonProperty(value = "cpu_system", required = true)
    private Double cpuSystem;

    @JsonProperty(value = "cpu_steal", required = true)
    private Double cpuSteal;

    @JsonProperty(value = "cpu_iowait", required = true)
    private Double cpuIowait;

    @JsonProperty(value = "cpu_nice", required = true)
    private Double cpuNice;

    @JsonProperty(value = "cpu_hi", required = true)
    private Double cpuHi;

    @JsonProperty(value = "cpu_si", required = true)
    private Double cpuSi;

    @JsonProperty(value = "cpu_idle", required = true)
    private Double cpuIdle;

    @JsonProperty(value = "memory_used", required = true)
    private Double memoryUsed;

    @JsonProperty(value = "memory_cached", required = true)
    private Double memoryCached;

    @JsonProperty(value = "disk_io_reads", required = true)
    private Double diskIOReads;

    @JsonProperty(value = "disk_io_writes", required = true)
    private Double diskIOWrites;

    @JsonProperty(value = "latency_per_read", required = true)
    private Double latencyPerRead;

    @JsonProperty(value = "latency_per_write", required = true)
    private Double latencyPerWrite;

    @JsonProperty(value = "network_bytes_in", required = true)
    private Double networkBytesIn;

    @JsonProperty(value = "network_bytes_out", required = true)
    private Double networkBytesOut;

    @JsonProperty(value = "disk_used", required = true)
    private Double diskUsed;

    @JsonProperty(value = "disk_free", required = true)
    private Double diskFree;

    @JsonProperty(value = "file_descriptors_in_use", required = true)
    private Double fileDescriptorInUse;

    @JsonProperty(value = "instance_type", required = true)
    private String instanceType;

    @JsonProperty(value = "os_version", required = true)
    private String osVersion;
}
