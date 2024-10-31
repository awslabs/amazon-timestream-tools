use super::{validate_env_variables, BuildRecords};
use crate::{
    metric::{FieldValue, Metric},
    SchemaType,
};
use anyhow::{Error, Result};
use aws_sdk_timestreamwrite as timestream_write;
use std::collections::HashMap;

pub struct MultiMeasureBuilder {
    pub measure_name: Option<String>,
    pub schema_type: SchemaType,
}

impl BuildRecords for MultiMeasureBuilder {
    // trait implementation to support multi-measure records Timestream

    #[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
    fn build_records(
        &self,
        metrics: &[Metric],
        precision: &timestream_write::types::TimeUnit,
    ) -> Result<HashMap<String, Vec<timestream_write::types::Record>>, Error> {
        validate_env_variables()?;
        match self.schema_type {
            SchemaType::SingleTableMultiMeasure => {
                return build_single_table_multi_measure_records(metrics, precision)
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
                .expect("Failed to unwrap")
                .to_owned()
        )
    }
}

#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
fn build_single_table_multi_measure_records(
    metrics: &[Metric],
    precision: &timestream_write::types::TimeUnit,
) -> Result<HashMap<String, Vec<timestream_write::types::Record>>, Error> {
    // Builds multi-measure records hashmap to be ingested to one table

    let mut records_batch: HashMap<String, Vec<aws_sdk_timestreamwrite::types::Record>> =
        HashMap::new();
    let table_name = std::env::var("single_table_name")?;
    for metric in metrics.iter() {
        let new_record = metric_to_timestream_record(metric.name(), metric, precision)?;
        if let Some(record_vec) = records_batch.get_mut(&table_name) {
            record_vec.push(new_record);
        } else {
            records_batch.insert(table_name.to_string(), vec![new_record]);
        }
    }

    Ok(records_batch)
}

#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
fn build_multi_table_multi_measure_records(
    metrics: &[Metric],
    measure_name: Option<&str>,
    precision: &timestream_write::types::TimeUnit,
) -> Result<HashMap<String, Vec<timestream_write::types::Record>>, Error> {
    // Builds multi-measure records hashmap to be ingested to multiple tables

    let mut records_batch: HashMap<String, Vec<aws_sdk_timestreamwrite::types::Record>> =
        HashMap::new();
    for metric in metrics.iter() {
        let new_record = metric_to_timestream_record(
            measure_name.expect("Failed to unwrap"),
            metric,
            precision,
        )?;
        let table_name = metric.name();
        if let Some(record_vec) = records_batch.get_mut(table_name) {
            record_vec.push(new_record);
        } else {
            records_batch.insert(table_name.to_string(), vec![new_record]);
        }
    }

    Ok(records_batch)
}

#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
pub fn metric_to_timestream_record(
    measure_name: &str,
    metric: &Metric,
    precision: &timestream_write::types::TimeUnit,
) -> Result<timestream_write::types::Record, Error> {
    // Converts the metric struct to a timestream multi-measure record

    let mut dimensions: Vec<timestream_write::types::Dimension> = Vec::new();
    for tag in metric.tags().iter().flatten() {
        dimensions.push(
            timestream_write::types::Dimension::builder()
                .name(tag.0.to_owned())
                .value(tag.1.to_owned())
                .build()
                .expect("Failed to build dimension"),
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
                .expect("Failed to build measure"),
        );
    }

    let new_record = timestream_write::types::Record::builder()
        .measure_name(measure_name)
        .set_measure_values(Some(measure_values))
        .set_measure_value_type(Some(timestream_write::types::MeasureValueType::Multi))
        .set_time_unit(Some(precision.clone()))
        .time(metric.timestamp().to_string())
        .set_dimensions(Some(dimensions))
        .build();

    Ok(new_record)
}

#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
pub fn get_timestream_measure_type(
    field_value: &FieldValue,
) -> Result<timestream_write::types::MeasureValueType, Error> {
    // Converts a metric struct type to a timestream measure value type

    match field_value {
        FieldValue::Boolean(_) => Ok(timestream_write::types::MeasureValueType::Boolean),
        FieldValue::I64(_) => Ok(timestream_write::types::MeasureValueType::Bigint),
        FieldValue::U64(_) => Ok(timestream_write::types::MeasureValueType::Bigint),
        FieldValue::F64(_) => Ok(timestream_write::types::MeasureValueType::Double),
        FieldValue::String(_) => Ok(timestream_write::types::MeasureValueType::Varchar),
    }
}
