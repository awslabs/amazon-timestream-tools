use crate::metric::Metric;
use crate::timestream_utils::{DIMENSION_PARTITION_KEY_TYPE, MEASURE_PARTITION_KEY_TYPE};
use anyhow::{anyhow, Error};
use aws_sdk_timestreamwrite::types as timestream_types;
use std::{collections::HashMap, fmt::Debug};

mod multi_measure_builder;

#[derive(Debug)]
pub enum SchemaType {
    MultiTableMultiMeasure,
    SingleTableMultiMeasure,
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
pub fn build_records(
    records_builder: &impl BuildRecords,
    metrics: &[Metric],
    precision: &timestream_types::TimeUnit,
) -> Result<HashMap<String, Vec<timestream_types::Record>>, Error> {
    records_builder.build_records(metrics, precision)
}

#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
pub fn table_creation_enabled() -> Result<bool, Error> {
    // Convert the env var table_creation_enabled to bool

    match std::env::var("enable_table_creation") {
        Ok(enabled) => Ok(env_var_to_bool(enabled)),
        Err(_) => Err(anyhow!(
            "enable_table_creation environment variable is not defined"
        )),
    }
}

#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
pub fn database_creation_enabled() -> Result<bool, Error> {
    // Convert the env var database_creation_enabled to bool
    match std::env::var("enable_database_creation") {
        Ok(enabled) => Ok(env_var_to_bool(enabled)),
        Err(_) => Err(anyhow!(
            "enable_database_creation environment variable is not defined"
        )),
    }
}

#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
pub fn env_var_to_bool(env_var: String) -> bool {
    // Convert the env var to bool

    matches!(env_var.as_str(), "true" | "t" | "1")
}

#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
pub fn validate_env_variables() -> Result<(), Error> {
    // Validate environment variables for all schema types

    if std::env::var("region").is_err() {
        return Err(anyhow!("region environment variable is not defined"));
    }
    if std::env::var("database_name").is_err() {
        return Err(anyhow!("database_name environment variable is not defined"));
    }
    if std::env::var("enable_database_creation").is_err() {
        return Err(anyhow!(
            "enable_database_creation environment variable is not defined"
        ));
    }
    let enable_table_creation = std::env::var("enable_table_creation");

    if enable_table_creation.is_err() {
        return Err(anyhow!(
            "enable_table_creation environment variable is not defined"
        ));
    }

    if env_var_to_bool(enable_table_creation?) {
        if std::env::var("enable_mag_store_writes").is_err() {
            return Err(anyhow!(
                "enable_mag_store_writes environment variable is not defined"
            ));
        }
        if std::env::var("mag_store_retention_period").is_err() {
            return Err(anyhow!(
                "mag_store_retention_period environment variable is not defined"
            ));
        }
        if std::env::var("mem_store_retention_period").is_err() {
            return Err(anyhow!(
                "mem_store_retention_period environment variable is not defined"
            ));
        }
    }

    // Validate environment variables for table mapping
    match std::env::var("table_mapping") {
        Ok(table_mapping) => match table_mapping.as_str() {
            "single-table" => {
                if std::env::var("single_table_name").is_err() {
                    return Err(anyhow!(
                        "single_table_name environment variable is not defined"
                    ));
                }
            }
            "multi-table" => {
                if std::env::var("measure_name_for_multi_measure_records").is_err() {
                    return Err(anyhow!(
                        "measure_name_for_multi_measure_records environment variable is not defined"
                    ));
                }
            }
            table_mapping => {
                return Err(anyhow!(
                    "{:?} is an invalid value for the table_mapping environment variable",
                    table_mapping
                ))
            }
        },
        Err(_) => return Err(anyhow!("table_mapping environment variable is not defined")),
    }

    // Customer-defined partition key environment variables
    let custom_partition_key_type = std::env::var("custom_partition_key_type");

    if let Ok(custom_partition_key_type) = custom_partition_key_type {
        if custom_partition_key_type != DIMENSION_PARTITION_KEY_TYPE
            && custom_partition_key_type != MEASURE_PARTITION_KEY_TYPE
        {
            return Err(anyhow!(
                format!("custom_partition_key_type can only be {DIMENSION_PARTITION_KEY_TYPE} or {MEASURE_PARTITION_KEY_TYPE}")
            ));
        }

        // Check required environment variables for when custom partition key type is "dimension." If it is "measure,"
        // no other environment variables are necessary.

        let custom_partition_key_dimension = std::env::var("custom_partition_key_dimension");

        if custom_partition_key_type == DIMENSION_PARTITION_KEY_TYPE
            && custom_partition_key_dimension.is_err()
        {
            return Err(anyhow!(
                format!("If custom_partition_key_type is {DIMENSION_PARTITION_KEY_TYPE}, then custom_partition_key_dimension must be defined")
            ));
        }

        let enforce_custom_partition_key = std::env::var("enforce_custom_partition_key");

        if custom_partition_key_type == DIMENSION_PARTITION_KEY_TYPE
            && enforce_custom_partition_key.is_err()
        {
            return Err(anyhow!(
                format!("enforce_custom_partition_key value must be specified (true or false) when custom_partition_key_type is {DIMENSION_PARTITION_KEY_TYPE}")
            ));
        }
    }

    Ok(())
}

pub trait BuildRecords: Debug {
    fn build_records(
        &self,
        metrics: &[Metric],
        precision: &timestream_types::TimeUnit,
    ) -> Result<HashMap<String, Vec<timestream_types::Record>>, Error>;
}

#[cfg(test)]
pub mod tests;
