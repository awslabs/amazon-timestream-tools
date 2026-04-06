// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

use crate::metric::Metric;
use anyhow::Error;
use async_trait::async_trait;
use aws_sdk_timestreamwrite::types as timestream_types;
use std::{collections::HashMap, fmt::Debug};

mod multi_measure_builder;

#[derive(Debug)]
pub enum SchemaType {
    MultiTableMultiMeasure,
    SingleTableMultiMeasure,
}

/// A group of records sharing common attributes.
#[derive(Debug, Clone)]
pub struct AttributeGroupedRecords {
    /// A Record comprised of a measure name, dimensions, and timestamp precision.
    /// All records within the "records" field share these attributes.
    pub common_attributes: timestream_types::Record,
    /// Records containing measure values and timestamps. These records share
    /// attributes contained in the "common_attributes" field.
    pub records: Vec<timestream_types::Record>,
}

impl AttributeGroupedRecords {
    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn insert_record(&mut self, record: timestream_types::Record) {
        self.records.push(record);
    }
}

/// A single Record split into its common attributes and measure value(s).
#[derive(Debug, Clone)]
pub struct RecordPair {
    /// A Record comprised of a measure name, dimensions, and timestamp precision.
    /// The Record in the "record" field shares these attributes.
    common_attributes: timestream_types::Record,
    /// A Record containing measure value(s) and a timestamp.
    record: timestream_types::Record,
}

#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
pub fn get_builder(schema: SchemaType, measure_name: String) -> impl BuildRecords {
    match schema {
        SchemaType::SingleTableMultiMeasure => {
            return multi_measure_builder::MultiMeasureBuilder {
                measure_name: None,
                schema_type: SchemaType::SingleTableMultiMeasure,
            }
        }
        SchemaType::MultiTableMultiMeasure => {
            return multi_measure_builder::MultiMeasureBuilder {
                measure_name: Some(measure_name.to_string()),
                schema_type: SchemaType::MultiTableMultiMeasure,
            }
        }
    }
}

#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
pub async fn build_records(
    records_builder: &impl BuildRecords,
    metrics: &[Metric],
    precision: &timestream_types::TimeUnit,
) -> Result<HashMap<String, Vec<AttributeGroupedRecords>>, Error> {
    records_builder.build_records(metrics, precision).await
}

#[async_trait]
pub trait BuildRecords: Debug {
    async fn build_records(
        &self,
        metrics: &[Metric],
        precision: &timestream_types::TimeUnit,
    ) -> Result<HashMap<String, Vec<AttributeGroupedRecords>>, Error>;
}

#[cfg(test)]
pub mod tests;
