package com.amazonaws.sample.csv.mapping;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

// TODO: adjust model to your CSV file
@RequiredArgsConstructor
@ToString
@Data
public class SampleCsvRow {
    private final String region;
    private final String az;
    private final String hostname;
    private final String measure_name;
    private final String measure_value;
    private final String timestamp;
}
