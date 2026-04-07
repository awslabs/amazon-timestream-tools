// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

use anyhow::{anyhow, Error, Result};
use aws_sdk_timestreamwrite as timestream_write;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use lambda_runtime::LambdaEvent;
use line_protocol_parser::parse_line_protocol;
use log::{error, trace};
use once_cell::sync::OnceCell;
use records_builder::{build_records, get_builder, AttributeGroupedRecords, SchemaType};
use serde_json::{json, Value};
use std::{
    collections::{HashMap, HashSet},
    str,
    sync::Arc,
    time::Instant,
};
use timestream_utils::{
    create_database, create_table, database_exists, get_table_config, ingest_records,
    TimestreamEnvConfig,
};
use tokio::sync::{Mutex, Semaphore};
use tokio::task;

pub mod line_protocol_parser;
pub mod metric;
pub mod records_builder;
pub mod timestream_utils;

/// The number of batches processed at the same time.
/// For multi-table multi measure schema, batches are a combination of
/// a table name and a Vec of records bound for that table
pub static NUM_BATCH_THREADS: usize = 16;

/// Environment variables common to all inputs and outputs.
#[derive(Debug, Clone)]
pub struct LibEnvConfig {
    /// Whether the connector is being invoked locally, not as a Lambda function.
    pub local_invocation: bool,
}

impl LibEnvConfig {
    fn new() -> Result<Self, Error> {
        Ok(Self {
            local_invocation: std::env::var("local_invocation")
                .map(|val| matches!(val.to_lowercase().as_str(), "true" | "t" | "1"))
                .unwrap_or(false),
        })
    }

    /// Gets LIB_ENV_CONFIG.
    ///
    /// # Examples
    ///
    /// ```
    /// use influxdb_timestream_connector::LibEnvConfig;
    /// # use tokio_test::block_on;
    ///
    /// # block_on(async {
    /// let lib_env_config_result = LibEnvConfig::get().await;
    /// assert!(lib_env_config_result.is_ok());
    /// # })
    /// ```
    pub async fn get() -> Result<LibEnvConfig, Error> {
        let config_lock = LIB_ENV_CONFIG.get_or_init(|| Mutex::new(None));
        let mut config = config_lock.lock().await;

        if config.is_none() {
            *config = Some(LibEnvConfig::new());
        }

        match config.take() {
            Some(Ok(env_config)) => Ok(env_config),
            Some(Err(err)) => Err(anyhow!("{}", err)),
            None => Err(anyhow!(
                "Retryable error: LibEnvConfig configuration has not been initialized"
            )),
        }
    }

    /// Resets LIB_ENV_CONFIG, requiring it to be reinitialized for any future use.
    /// This function may return an Err, if a lock cannot be acquired on
    /// LIB_ENV_CONFIG.
    ///
    /// # Examples
    ///
    /// ```
    /// use influxdb_timestream_connector::LibEnvConfig;
    /// use std::env;
    /// # use tokio_test::block_on;
    ///
    /// // Here, reset_lib_env_config is used to reset environment variables during
    /// // testing.
    /// // Changes to environment variables are only picked up by the connector after
    /// // reset_lib_env_config is called
    /// # block_on(async {
    /// env::remove_var("some_environment_variable_removed_for_test");
    ///
    /// // . . . test code
    ///
    /// // Reset LibEnvConfig so that the next test has its environment variables
    /// // picked up
    /// LibEnvConfig::reset();
    /// # })
    /// ```
    pub async fn reset() {
        if let Some(config_lock) = LIB_ENV_CONFIG.get() {
            let mut config = config_lock.lock().await;
            *config = None;
        }
    }
}

/// Library environment variable configuration, making sure that environment
/// variables are read once.
/// This being a OnceCell<Mutex<Option<Result<LibEnvConfig, Error>>>> means
/// that it can handle checking for required environment variables and is
/// thread safe.
static LIB_ENV_CONFIG: OnceCell<Mutex<Option<Result<LibEnvConfig, Error>>>> = OnceCell::new();

/// Handles parsing body in request.
#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
async fn handle_body(
    client: &Arc<timestream_write::Client>,
    body: &[u8],
    precision: &timestream_write::types::TimeUnit,
) -> Result<(), Error> {
    let timestream_env_config = TimestreamEnvConfig::get().await?;

    let line_protocol = match str::from_utf8(body) {
        Ok(line_protocol) => line_protocol,
        Err(err) => {
            error!("Failed to decode line protocol body as UTF-8: {}", err);
            return Err(anyhow!(err));
        }
    };
    let metric_data = parse_line_protocol(line_protocol)?;

    let multi_measure_builder = match timestream_env_config.table_mapping.as_str() {
        "multi-table" => {
            let measure_name_for_multi_measure_records = match &timestream_env_config
                .measure_name_for_multi_measure_records
            {
                Some(val) => val.clone(),
                None => {
                    let err_message = "Non-retryable error: measure_name_for_multi_measure_records is not defined";
                    error!("{}", err_message);
                    return Err(anyhow!(err_message));
                }
            };
            get_builder(
                SchemaType::MultiTableMultiMeasure,
                measure_name_for_multi_measure_records,
            )
        }
        _ => {
            let single_table_name = match &timestream_env_config.single_table_name {
                Some(val) => val.clone(),
                None => {
                    let err_message = "Non-retryable error: single_table_name is not defined";
                    error!("{}", err_message);
                    return Err(anyhow!(err_message));
                }
            };
            get_builder(SchemaType::SingleTableMultiMeasure, single_table_name)
        }
    };

    // Only currently supports multi-measure
    let multi_table_batch = build_records(&multi_measure_builder, &metric_data, precision).await?;
    handle_ingestion(client, multi_table_batch).await?;
    Ok(())
}

/// Ingests records for multi-measure schema type.
#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
async fn handle_ingestion(
    client: &Arc<timestream_write::Client>,
    records: HashMap<String, Vec<AttributeGroupedRecords>>,
) -> Result<(), Error> {
    let timestream_env_config = TimestreamEnvConfig::get().await?;

    let database_name = timestream_env_config.database_name.clone();
    let kms_key_id = timestream_env_config.kms_key_id.clone();
    let database_tags = timestream_env_config.database_tags.clone();
    let database_name = Arc::new(database_name);

    if timestream_env_config.enable_database_creation {
        match database_exists(client, &database_name).await {
            Ok(true) => (),
            Ok(false) => {
                create_database(client, &database_name, kms_key_id.as_deref(), database_tags)
                    .await?;
            }
            Err(error) => return Err(anyhow!(error)),
        }
    }

    // Use a semaphore to limit the maximum number of threads used to process batches in parallel
    let ingestion_semaphore = Arc::new(Semaphore::new(NUM_BATCH_THREADS));
    let mut batch_ingestion_futures = FuturesUnordered::new();

    // Keep track of created tables to avoid unnecessary API calls to verify tables exist
    let created_table_names = Arc::new(Mutex::new(HashSet::<String>::new()));

    // Track total time taken to create tables and ingest records
    let ingestion_start = Instant::now();

    // Ingest records for each table, in parallel
    for (table_name, attribute_grouped_records_vec) in records {
        for attribute_grouped_records in attribute_grouped_records_vec {
            let created_table_names_clone = Arc::clone(&created_table_names);

            let permit = ingestion_semaphore
                .clone()
                .acquire_owned()
                .await
                .expect("Retryable error: Failed to get semaphore permit");

            // Use Arc::clone to create a shallow clone of the client
            let client_clone = Arc::clone(client);
            let database_name_clone = Arc::clone(&database_name);
            let table_name_clone = table_name.clone();
            let table_tags = timestream_env_config.table_tags.clone();

            // Create a future for ingesting to the current table
            let future = task::spawn(async move {
                if timestream_env_config.enable_table_creation {
                    let mut created_table_names = created_table_names_clone.lock().await;

                    // If the table name wasn't in the created_table_names HashSet, create the table.
                    if created_table_names.insert(table_name_clone.to_string()) {
                        create_table(
                            &client_clone,
                            &database_name_clone,
                            &table_name_clone,
                            get_table_config().await?,
                            table_tags,
                        )
                        .await?;
                    }
                }

                // Destructuring common_attributes_grouped_records in order to
                // use it for ingestion without cloning
                let AttributeGroupedRecords {
                    common_attributes,
                    records,
                } = attribute_grouped_records;

                // Ingest the data to the table
                let result = ingest_records(
                    client_clone,
                    database_name_clone,
                    table_name_clone,
                    common_attributes,
                    records,
                )
                .await;
                drop(permit);
                result
            });
            batch_ingestion_futures.push(future);
        }
    }

    while let Some(result) = batch_ingestion_futures.next().await {
        // result will be Result<Result<(), Error>>
        // This means the nested Result needs to be checked
        match result {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                return Err(anyhow!(error));
            }
            Err(error) => {
                return Err(anyhow!(error));
            }
        }
    }

    trace!(
        "Total asynchronous ingestion duration: {:?}",
        ingestion_start.elapsed()
    );
    Ok(())
}

/// Retrieves the optional "precision" query string parameter from a serde_json::Value.
#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
pub fn get_precision(event: &Value) -> Option<&str> {
    // Query string parameters may be included as "queryStringParameters"
    if let Some(precision) = event
        .get("queryStringParameters")
        .or_else(|| event.get("queryParameters"))
        .and_then(|query_string_parameters| query_string_parameters.get("precision"))
    {
        // event["queryStringParameters"]["precision"] may be an object
        if let Some(precision_str) = precision.as_str() {
            return Some(precision_str);
        // event["queryStringParameters"]["precision"] may be an array. This is common from requests
        // originating from AWS services, such as when the connector is ran with the cargo lambda watch command
        } else if let Some(precision_array) = precision.as_array() {
            if let Some(precision_value) = precision_array.first().and_then(|value| value.as_str())
            {
                return Some(precision_value);
            }
        }
    }

    None
}

/// Handler for lambda runtime.
#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
pub async fn lambda_handler(
    client: &Arc<timestream_write::Client>,
    event: LambdaEvent<Value>,
) -> Result<Value, Error> {
    let lib_env_config = LibEnvConfig::get().await?;

    let (event, _context) = event.into_parts();

    let precision = match get_precision(&event) {
        Some("ms") => timestream_write::types::TimeUnit::Milliseconds,
        Some("us") => timestream_write::types::TimeUnit::Microseconds,
        Some("s") => timestream_write::types::TimeUnit::Seconds,
        _ => timestream_write::types::TimeUnit::Nanoseconds,
    };

    let data = event
        .get("body")
        .expect("Non-retryable error: No body was included in the request")
        .as_str()
        .expect("Non-retryable error: Failed to convert body to &str")
        .as_bytes();

    match handle_body(client, data, &precision).await {
        // This is the format required for custom Lambda 1.0 responses
        // https://docs.aws.amazon.com/apigateway/latest/developerguide/http-api-develop-integrations-lambda.html
        Ok(_) => {
            let mut response = json!({
                "statusCode": 200,
                "body": "{\"message\": \"Success\"}",
                "isBase64Encoded": false,
                "headers": {
                    "Content-Type": "application/json"
                }
            });
            // cargo lambda watch expects a Lambda response in 2.0 format.
            // This means a "cookies" array must be added to the response.
            // If this "cookies" array is present and the connector is deployed
            // with synchronous invocation in a stack, users will receive a
            // 502 error
            if lib_env_config.local_invocation {
                response["cookies"] = json!([]);
            }
            Ok(response)
        }
        // An Err is required in order to send messages to the Lambda's
        // dead letter queue, when the connector is deployed as part of a stack
        // with asynchronous invocation
        Err(error) => Err(anyhow!(error.to_string())),
    }
}

#[cfg(test)]
#[test]
pub fn test_get_precision_query_string_parameters_array() -> Result<(), Error> {
    let fake_event_value = json!({ "queryStringParameters": { "precision": ["ms"] } });
    let precision = get_precision(&fake_event_value);
    assert!(precision.is_some());
    assert!(precision.expect("Failed to get precision") == "ms");
    Ok(())
}

#[test]
pub fn test_get_precision_query_string_parameters_object() -> Result<(), Error> {
    let fake_event_value = json!({ "queryStringParameters": { "precision": "ms" } });
    let precision = get_precision(&fake_event_value);
    assert!(precision.is_some());
    assert!(precision.expect("Failed to get precision") == "ms");
    Ok(())
}

#[test]
pub fn test_get_precision_query_string_parameters_object_nanoseconds() -> Result<(), Error> {
    let fake_event_value = json!({ "queryStringParameters": { "precision": "ns" } });
    let precision = get_precision(&fake_event_value);
    assert!(precision.is_some());
    assert!(precision.expect("Failed to get precision") == "ns");
    Ok(())
}

#[test]
pub fn test_get_precision_query_string_parameters_object_microseconds() -> Result<(), Error> {
    let fake_event_value = json!({ "queryStringParameters": { "precision": "us" } });
    let precision = get_precision(&fake_event_value);
    assert!(precision.is_some());
    assert!(precision.expect("Failed to get precision") == "us");
    Ok(())
}

#[test]
pub fn test_get_precision_query_string_parameters_object_seconds() -> Result<(), Error> {
    let fake_event_value = json!({ "queryStringParameters": { "precision": "s" } });
    let precision = get_precision(&fake_event_value);
    assert!(precision.is_some());
    assert!(precision.expect("Failed to get precision") == "s");
    Ok(())
}

#[test]
pub fn test_get_precision_query_parameters_array() -> Result<(), Error> {
    let fake_event_value = json!({ "queryParameters": { "precision": ["ms"] } });
    let precision = get_precision(&fake_event_value);
    assert!(precision.is_some());
    assert!(precision.expect("Failed to get precision") == "ms");
    Ok(())
}

#[test]
pub fn test_get_precision_query_parameters_object() -> Result<(), Error> {
    let fake_event_value = json!({ "queryParameters": { "precision": "ms" } });
    let precision = get_precision(&fake_event_value);
    assert!(precision.is_some());
    assert!(precision.expect("Failed to get precision") == "ms");
    Ok(())
}

#[test]
pub fn test_get_precision_incorrect_query_parameters_key() -> Result<(), Error> {
    let fake_event_value = json!({ "nomatch": { "precision": "ms" } });
    assert!(get_precision(&fake_event_value).is_none());
    Ok(())
}

#[test]
pub fn test_get_precision_incorrect_precision_key() -> Result<(), Error> {
    let fake_event_value = json!({ "queryStringParameters": { "nomatch": "ms" } });
    assert!(get_precision(&fake_event_value).is_none());
    Ok(())
}
