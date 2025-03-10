use anyhow::{anyhow, Error, Result};
use aws_sdk_timestreamwrite as timestream_write;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use lambda_runtime::LambdaEvent;
use line_protocol_parser::parse_line_protocol;
use log::{info, trace};
use records_builder::{
    build_records, database_creation_enabled, env_var_to_bool, get_builder, SchemaType,
};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use std::{str, thread, time};
use timestream_utils::{
    create_database, create_table, database_exists, get_table_config, ingest_records,
    parse_tags_from_str, table_exists,
};
use tokio::sync::Semaphore;
use tokio::task;

pub mod line_protocol_parser;
pub mod metric;
pub mod records_builder;
pub mod timestream_utils;

// The maximum number of database/table creation/delete API calls
// that can be made per second is 1.
pub static TIMESTREAM_API_WAIT_SECONDS: u64 = 1;

// The number of batches processed at the same time.
// For multi-table multi measure schema, batches are a combination of
// a table name and a Vec of records bound for that table
pub static NUM_BATCH_THREADS: usize = 16;

#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
async fn handle_body(
    client: &Arc<timestream_write::Client>,
    body: &[u8],
    precision: &timestream_write::types::TimeUnit,
) -> Result<(), Error> {
    // Handle parsing body in request

    let line_protocol = str::from_utf8(body).unwrap();
    let metric_data = parse_line_protocol(line_protocol)?;

    let multi_measure_builder = match std::env::var("table_mapping")?.to_lowercase().as_str() {
        "multi-table" => get_builder(
            SchemaType::MultiTableMultiMeasure,
            std::env::var("measure_name_for_multi_measure_records")?,
        ),
        _ => get_builder(
            SchemaType::SingleTableMultiMeasure,
            std::env::var("single_table_name")?,
        ),
    };

    // Only currently supports multi-measure
    let multi_table_batch = build_records(&multi_measure_builder, &metric_data, precision)?;
    handle_ingestion(client, multi_table_batch).await?;
    Ok(())
}

#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
async fn handle_ingestion(
    client: &Arc<timestream_write::Client>,
    records: HashMap<String, Vec<timestream_write::types::Record>>,
) -> Result<(), Error> {
    // Ingestion for multi-measure schema type

    let database_name = std::env::var("database_name")?;
    let database_name = Arc::new(database_name);

    let kms_key_id = std::env::var("kms_key_id").ok();

    let database_tags = match std::env::var("database_tags") {
        Ok(database_tags_str) => match parse_tags_from_str(&database_tags_str) {
            Ok(tags) => Some(tags),
            Err(_) => None,
        },
        Err(_) => None,
    };

    if let Ok(true) = std::env::var("enable_database_creation").map(env_var_to_bool) {
        match database_exists(client, &database_name).await {
            Ok(true) => (),
            Ok(false) => {
                if database_creation_enabled()? {
                    thread::sleep(time::Duration::from_secs(TIMESTREAM_API_WAIT_SECONDS));
                    create_database(client, &database_name, kms_key_id.as_deref(), database_tags)
                        .await?;
                } else {
                    return Err(anyhow!(
                        "Database {} does not exist and database creation is not enabled",
                        database_name
                    ));
                }
            }
            Err(error) => return Err(anyhow!(error)),
        }
    }

    // Use a semaphore to limit the maximum number of threads used to process batches in parallel
    let ingestion_semaphore = Arc::new(Semaphore::new(NUM_BATCH_THREADS));
    let mut batch_ingestion_futures = FuturesUnordered::new();

    // Track total time taken to check existence of tables and ingest records
    let ingestion_start = Instant::now();

    // Ingest records for each table, in parallel
    for (table_name, records) in records {
        let permit = ingestion_semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("Failed to get semaphore permit");

        // Use Arc::clone to create a shallow clone of the client
        let client_clone = Arc::clone(client);
        let database_name_clone = Arc::clone(&database_name);

        // Create a future for ingesting to the current table
        let future = task::spawn(async move {
            if let Ok(true) = std::env::var("enable_table_creation").map(env_var_to_bool) {
                let _ =
                    create_table_if_non_existent(&client_clone, &database_name_clone, &table_name)
                        .await;
            }

            // Ingest the data to the table
            let result =
                ingest_records(client_clone, database_name_clone, table_name, records).await;
            drop(permit);
            result
        });
        batch_ingestion_futures.push(future);
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

#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
pub async fn create_table_if_non_existent(
    client: &Arc<timestream_write::Client>,
    database_name: &Arc<String>,
    table_name: &str,
) -> Result<(), Error> {
    let table_tags = match std::env::var("table_tags") {
        Ok(table_tags_str) => match parse_tags_from_str(&table_tags_str) {
            Ok(tags) => Some(tags),
            Err(_) => None,
        },
        Err(_) => None,
    };
    match table_exists(client, database_name, table_name).await {
        Ok(true) => (),
        Ok(false) => {
            thread::sleep(time::Duration::from_secs(TIMESTREAM_API_WAIT_SECONDS));
            create_table(
                client,
                database_name,
                table_name,
                get_table_config()?,
                table_tags,
            )
            .await?
        }
        Err(error) => info!("error checking table exists: {:?}", error),
    }

    Ok(())
}

#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
pub fn get_precision(event: &Value) -> Option<&str> {
    // Retrieves the optional "precision" query string parameter from a serde_json::Value

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

#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
pub async fn lambda_handler(
    client: &Arc<timestream_write::Client>,
    event: LambdaEvent<Value>,
) -> Result<Value, Error> {
    // Handler for lambda runtime

    let (event, _context) = event.into_parts();

    let precision = match get_precision(&event) {
        Some("ms") => timestream_write::types::TimeUnit::Milliseconds,
        Some("us") => timestream_write::types::TimeUnit::Microseconds,
        Some("s") => timestream_write::types::TimeUnit::Seconds,
        _ => timestream_write::types::TimeUnit::Nanoseconds,
    };

    let data = event
        .get("body")
        .expect("No body was included in the request")
        .as_str()
        .expect("Failed to convert body to &str")
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
            if std::env::var("local_invocation").is_ok() {
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

#[test]
pub fn test_parse_tags_from_str_empty_string() -> Result<(), Error> {
    let tags_str = "";
    let tags = parse_tags_from_str(tags_str)?;
    assert!(tags.is_empty());
    Ok(())
}

#[test]
pub fn test_parse_tags_from_str_whitespace_only() -> Result<(), Error> {
    let tags_str = "   ";
    let tags = parse_tags_from_str(tags_str)?;
    assert!(tags.is_empty());
    Ok(())
}

#[test]
pub fn test_parse_tags_from_str_tag_with_no_value() -> Result<(), Error> {
    let tags_str = "key1";
    let tags = parse_tags_from_str(tags_str)?;
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0].key, "key1");
    assert_eq!(tags[0].value, "");
    Ok(())
}

#[test]
pub fn test_parse_tags_from_str_multiple_tags() -> Result<(), Error> {
    let tags_str = "key1=value1, key2=value2, key3=value3";
    let tags = parse_tags_from_str(tags_str)?;
    assert_eq!(tags.len(), 3);
    assert_eq!(tags[0].key, "key1");
    assert_eq!(tags[0].value, "value1");
    assert_eq!(tags[1].key, "key2");
    assert_eq!(tags[1].value, "value2");
    assert_eq!(tags[2].key, "key3");
    assert_eq!(tags[2].value, "value3");
    Ok(())
}

#[test]
pub fn test_parse_tags_from_str_extra_commas() -> Result<(), Error> {
    let tags_str = "key1=value1, , key2=value2,";
    let tags = parse_tags_from_str(tags_str)?;
    assert_eq!(tags.len(), 2);
    assert_eq!(tags[0].key, "key1");
    assert_eq!(tags[0].value, "value1");
    assert_eq!(tags[1].key, "key2");
    assert_eq!(tags[1].value, "value2");
    Ok(())
}

#[test]
pub fn test_parse_tags_from_str_multiple_equals() -> Result<(), Error> {
    let tags_str = "key1=value=with=equals";
    let tags = parse_tags_from_str(tags_str)?;
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0].key, "key1");
    assert_eq!(tags[0].value, "value=with=equals");
    Ok(())
}

#[test]
pub fn test_parse_tags_from_str_trimming_whitespace() -> Result<(), Error> {
    let tags_str = "  key1  =  value1  ,   key2=   value2  ";
    let tags = parse_tags_from_str(tags_str)?;
    assert_eq!(tags.len(), 2);
    assert_eq!(tags[0].key, "key1");
    assert_eq!(tags[0].value, "value1");
    assert_eq!(tags[1].key, "key2");
    assert_eq!(tags[1].value, "value2");
    Ok(())
}

#[test]
pub fn test_parse_tags_from_str_tag_with_empty_value_explicit() -> Result<(), Error> {
    let tags_str = "key1=,key2=2,key3";
    let tags = parse_tags_from_str(tags_str)?;
    assert_eq!(tags.len(), 3);
    assert_eq!(tags[0].key, "key1");
    assert_eq!(tags[0].value, "");
    assert_eq!(tags[1].key, "key2");
    assert_eq!(tags[1].value, "2");
    assert_eq!(tags[2].key, "key3");
    assert_eq!(tags[2].value, "");
    Ok(())
}

#[test]
pub fn test_parse_tags_from_str_error_empty_key() {
    let tags_str = "=value";
    let err = parse_tags_from_str(tags_str).unwrap_err();
    assert!(err.to_string().contains("Tag key must not be empty"));
}

#[test]
pub fn test_parse_tags_from_str_error_only_equals() {
    let tags_str = "   =   ";
    let err = parse_tags_from_str(tags_str).unwrap_err();
    assert!(err.to_string().contains("Tag key must not be empty"));
}
