// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

use crate::metric::{self, Metric};
use anyhow::{anyhow, Error};
use influxdb_line_protocol::{self, parse_lines, ParsedLine};

/// Parses a string of line protocol to a vector of Metric structs.
#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
pub fn parse_line_protocol(line_protocol: &str) -> Result<Vec<Metric>, Error> {
    let parsed_lines = parse_lines(line_protocol);
    let mut output_metrics: Vec<Metric> = Vec::new();
    for line_result in parsed_lines {
        match line_result {
            Ok(line) => {
                let new_metric = parsed_line_to_metric(line)?;
                output_metrics.push(new_metric);
            }

            Err(error) => {
                return Err(anyhow!(
                    "Non-retryable error: Failed to parse line: {}",
                    error.to_string()
                ));
            }
        }
    }

    Ok(output_metrics)
}

/// Converts an influxdb_line_protocol ParsedLine struct to a Metric struct.
#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
pub fn parsed_line_to_metric(parsed_line: ParsedLine) -> Result<Metric, Error> {
    let mut new_tags: Vec<(String, String)> = Vec::new();
    if let Some(tag_set) = parsed_line.series.tag_set.as_ref() {
        for (tag_key, tag_value) in tag_set {
            new_tags.push((tag_key.to_string(), tag_value.to_string()));
        }
    }

    let mut new_fields: Vec<(String, metric::FieldValue)> = Vec::new();
    for (field_key, field_value) in parsed_line.field_set.as_ref() {
        match field_value {
            influxdb_line_protocol::FieldValue::I64(int_value) => {
                new_fields.push((field_key.to_string(), metric::FieldValue::I64(*int_value)));
            }

            influxdb_line_protocol::FieldValue::U64(uint_value) => {
                new_fields.push((field_key.to_string(), metric::FieldValue::U64(*uint_value)));
            }

            influxdb_line_protocol::FieldValue::F64(float_value) => {
                new_fields.push((field_key.to_string(), metric::FieldValue::F64(*float_value)));
            }

            influxdb_line_protocol::FieldValue::String(string_value) => {
                new_fields.push((
                    field_key.to_string(),
                    metric::FieldValue::String(string_value.to_string()),
                ));
            }

            influxdb_line_protocol::FieldValue::Boolean(bool_value) => {
                new_fields.push((
                    field_key.to_string(),
                    metric::FieldValue::Boolean(*bool_value),
                ));
            }
        }
    }

    match parsed_line.timestamp {
        Some(timestamp) => Ok(Metric::new(
            parsed_line.series.measurement.to_string(),
            Some(new_tags),
            new_fields,
            timestamp,
        )),
        None => Err(anyhow!("Non-retryable error: Failed to parse timestamp")),
    }
}

#[cfg(test)]
pub mod tests;
