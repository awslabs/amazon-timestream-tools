package com.amazonaws.samples.connectors.timestream;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TestDataObject {
    private long timestamp;
    private String region;
    private String az;
    private String hostname;
    private String os;
    private String osVersion;
    private double cpu_utilization;
    private int cpu_processes;
    private int cpu_threads;
    private double memory_utilization;
    private double memory_committed;
    private double memory_cached;
}
