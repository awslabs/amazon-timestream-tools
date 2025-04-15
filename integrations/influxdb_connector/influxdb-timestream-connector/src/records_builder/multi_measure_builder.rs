use super::{AttributeGroupedRecords, BuildRecords, RecordPair};
use crate::{
    metric::{FieldValue, Metric},
    timestream_utils::TimestreamEnvConfig,
    SchemaType,
};
use anyhow::{anyhow, Error, Result};
use async_trait::async_trait;
use aws_sdk_timestreamwrite::{self as timestream_write};
use std::collections::HashMap;

pub struct MultiMeasureBuilder {
    pub measure_name: Option<String>,
    pub schema_type: SchemaType,
}

/// Trait implementation to support multi-measure records Timestream.
#[async_trait]
impl BuildRecords for MultiMeasureBuilder {
    #[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
    async fn build_records(
        &self,
        metrics: &[Metric],
        precision: &timestream_write::types::TimeUnit,
    ) -> Result<HashMap<String, Vec<AttributeGroupedRecords>>, Error> {
        match self.schema_type {
            SchemaType::SingleTableMultiMeasure => {
                let records = build_single_table_multi_measure_records(metrics, precision).await;
                return records;
            }
            SchemaType::MultiTableMultiMeasure => {
                return build_multi_table_multi_measure_records(
                    metrics,
                    self.measure_name.as_deref(),
                    precision,
                )
            }
        }
    }
}

impl std::fmt::Debug for MultiMeasureBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            formatter,
            "{}",
            self.measure_name
                .as_deref()
                .expect("Non-retryable error: Failed to unwrap MultiMeasureBuilder.measure_name")
                .to_owned()
        )
    }
}

/// Builds multi-measure records HashMap to be ingested to one table.
/// The HashMap's key is the table name and its value is a Vec where each entry
/// is an AttributeGroupedRecords struct, comprised of a common attribute record
/// and another Vec containing records that share that common attribute.
#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
async fn build_single_table_multi_measure_records(
    metrics: &[Metric],
    precision: &timestream_write::types::TimeUnit,
) -> Result<HashMap<String, Vec<AttributeGroupedRecords>>, Error> {
    let timestream_env_config = TimestreamEnvConfig::get().await?;

    // Records grouped according to table name
    let mut records_batch: HashMap<String, Vec<AttributeGroupedRecords>> = HashMap::new();
    // Records grouped according to common attributes.
    // Key: a common attribute Record as a String, value: index of an existing AttributeGroupedRecords in
    // records_batch
    let mut group_indices_map: HashMap<String, usize> = HashMap::new();

    let table_name = timestream_env_config.single_table_name.ok_or(anyhow!(
        "Non-retryable error: Failed to get single_table_name"
    ))?;

    for metric in metrics.iter() {
        let record_pair = metric_to_timestream_record_pair(metric.name(), metric, precision)?;
        // Check for existing table entry
        if let Some(table_group) = records_batch.get_mut(table_name.as_str()) {
            // Check for existing AttributeGroupedRecords
            if let Some(attribute_grouped_records_index) =
                group_indices_map.get(format!("{:#?}", record_pair.common_attributes).as_str())
            {
                if let Some(attribute_grouped_records) =
                    table_group.get_mut(*attribute_grouped_records_index)
                {
                    // Add record to group of records with the same common attributes
                    attribute_grouped_records.records.push(record_pair.record);
                } else {
                    // Index was incorrect. Add a new AttributeGroupedRecords and update the incorrect index
                    // .insert will update the value for the existing entry
                    group_indices_map.insert(
                        format!("{:#?}", record_pair.common_attributes),
                        table_group.len(),
                    );
                    table_group.push(AttributeGroupedRecords {
                        common_attributes: record_pair.common_attributes,
                        records: vec![record_pair.record],
                    });
                }
            } else {
                // AttributeGroupedRecords doesn't exist, create it
                group_indices_map.insert(
                    format!("{:#?}", record_pair.common_attributes),
                    table_group.len(),
                );
                table_group.push(AttributeGroupedRecords {
                    common_attributes: record_pair.common_attributes,
                    records: vec![record_pair.record],
                });
            }
        } else {
            // Table entry doesn't exist, create it
            group_indices_map.insert(format!("{:#?}", record_pair.common_attributes), 0);
            records_batch.insert(
                table_name.clone(),
                vec![AttributeGroupedRecords {
                    common_attributes: record_pair.common_attributes,
                    records: vec![record_pair.record],
                }],
            );
        }
    }

    Ok(records_batch)
}

/// Builds multi-measure records HashMap to be ingested to multiple tables.
/// The HashMap's key is the table name and its value is a Vec where each entry
/// is an AttributeGroupedRecords struct, comprised of a common attribute record
/// and another Vec containing records that share that common attribute.
#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
fn build_multi_table_multi_measure_records(
    metrics: &[Metric],
    measure_name: Option<&str>,
    precision: &timestream_write::types::TimeUnit,
) -> Result<HashMap<String, Vec<AttributeGroupedRecords>>, Error> {
    // Records grouped according to table name
    let mut records_batch: HashMap<String, Vec<AttributeGroupedRecords>> = HashMap::new();
    // Records grouped according to common attributes.
    // Key: a common attribute Record as a String, value: index of an existing AttributeGroupedRecords in
    // records_batch
    let mut group_indices_map: HashMap<String, usize> = HashMap::new();

    for metric in metrics.iter() {
        let record_pair = metric_to_timestream_record_pair(
            measure_name.expect("Non-retryable error: Failed to unwrap measure_name"),
            metric,
            precision,
        )?;
        let table_name = metric.name();

        // Check for existing table entry
        if let Some(table_group) = records_batch.get_mut(table_name) {
            // Check for existing AttributeGroupedRecords
            if let Some(attribute_grouped_records_index) =
                group_indices_map.get(format!("{:#?}", record_pair.common_attributes).as_str())
            {
                if let Some(attribute_grouped_records) =
                    table_group.get_mut(*attribute_grouped_records_index)
                {
                    // Add record to group of records with the same common attributes
                    attribute_grouped_records.records.push(record_pair.record);
                } else {
                    // Index was incorrect. Add a new AttributeGroupedRecords and update the incorrect index
                    // .insert will update the value for the existing entry
                    group_indices_map.insert(
                        format!("{:#?}", record_pair.common_attributes),
                        table_group.len(),
                    );
                    table_group.push(AttributeGroupedRecords {
                        common_attributes: record_pair.common_attributes,
                        records: vec![record_pair.record],
                    });
                }
            } else {
                // AttributeGroupedRecords doesn't exist, create it
                group_indices_map.insert(
                    format!("{:#?}", record_pair.common_attributes),
                    table_group.len(),
                );
                table_group.push(AttributeGroupedRecords {
                    common_attributes: record_pair.common_attributes,
                    records: vec![record_pair.record],
                });
            }
        } else {
            // Table entry doesn't exist, create it
            group_indices_map.insert(format!("{:#?}", record_pair.common_attributes), 0);
            records_batch.insert(
                table_name.to_string(),
                vec![AttributeGroupedRecords {
                    common_attributes: record_pair.common_attributes,
                    records: vec![record_pair.record],
                }],
            );
        }
    }

    Ok(records_batch)
}

/// Converts a Metric struct to a tuple containing a timestream multi-measure Record and its
/// common attribute, comprised of the record's dimensions, measure name, and timestamp precision.
#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
pub fn metric_to_timestream_record_pair(
    measure_name: &str,
    metric: &Metric,
    precision: &timestream_write::types::TimeUnit,
) -> Result<RecordPair, Error> {
    let mut dimensions: Vec<timestream_write::types::Dimension> = Vec::new();
    for tag in metric.tags().iter().flatten() {
        dimensions.push(
            timestream_write::types::Dimension::builder()
                .name(tag.0.to_owned())
                .value(tag.1.to_owned())
                .build()
                .expect("Non-retryable error: Failed to build dimension"),
        )
    }

    let mut measure_values: Vec<timestream_write::types::MeasureValue> = Vec::new();
    for field in metric.fields() {
        let measure_type = get_timestream_measure_type(&field.1)?;
        measure_values.push(
            timestream_write::types::MeasureValue::builder()
                .name(field.0.to_owned())
                .value(field.1.to_string())
                .r#type(measure_type)
                .build()
                .expect("Non-retryable error: Failed to build measure"),
        );
    }

    let common_attributes = timestream_write::types::Record::builder()
        .measure_name(measure_name)
        .set_dimensions(Some(dimensions))
        .set_measure_value_type(Some(timestream_write::types::MeasureValueType::Multi))
        .set_time_unit(Some(precision.clone()))
        .build();

    let record = timestream_write::types::Record::builder()
        .set_measure_values(Some(measure_values))
        .time(metric.timestamp().to_string())
        .build();

    Ok(RecordPair {
        common_attributes,
        record,
    })
}

/// Converts a Metric struct type to a timestream MeasureValue type.
#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
pub fn get_timestream_measure_type(
    field_value: &FieldValue,
) -> Result<timestream_write::types::MeasureValueType, Error> {
    match field_value {
        FieldValue::Boolean(_) => Ok(timestream_write::types::MeasureValueType::Boolean),
        FieldValue::I64(_) => Ok(timestream_write::types::MeasureValueType::Bigint),
        FieldValue::U64(_) => Ok(timestream_write::types::MeasureValueType::Bigint),
        FieldValue::F64(_) => Ok(timestream_write::types::MeasureValueType::Double),
        FieldValue::String(_) => Ok(timestream_write::types::MeasureValueType::Varchar),
    }
}
