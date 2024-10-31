use anyhow::Error;
use aws_credential_types::Credentials;
use aws_sdk_timestreamwrite as timestream_write;
use aws_types::region::Region;
use influxdb_timestream_connector::records_builder::SchemaType;
use lambda_runtime::{Context, LambdaEvent};
use rand::{distributions::uniform::SampleUniform, distributions::Alphanumeric, Rng};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::{env, thread, time};

static DATABASE_NAME: &str = "influxdb_timestream_connector_integ_db";
static REGION: &str = "us-west-2";
static MAX_TIMESTREAM_TABLE_NAME_LENGTH: usize = 256;

// A batch of resources to be deleted at the end of a test.
struct CleanupBatch {
    database_name: String,
    table_names_to_delete: Vec<String>,
}

impl CleanupBatch {
    pub fn new(database_name: String, table_names_to_delete: Vec<String>) -> CleanupBatch {
        CleanupBatch {
            database_name,
            table_names_to_delete,
        }
    }

    async fn cleanup(&mut self, client: &timestream_write::Client) {
        for table_name_to_delete in self.table_names_to_delete.iter() {
            println!(
                "Deleting table {} in database {}",
                table_name_to_delete, self.database_name
            );
            thread::sleep(time::Duration::from_secs(
                influxdb_timestream_connector::TIMESTREAM_API_WAIT_SECONDS,
            ));
            let result = client
                .delete_table()
                .database_name(&self.database_name)
                .table_name(table_name_to_delete)
                .send()
                .await;
            match result {
                Ok(_) => (),

                Err(error) => {
                    println!(
                        "Table deletion failed for table {}: {:?}",
                        table_name_to_delete,
                        error.raw_response()
                    );
                }
            }
        }
    }
}

fn set_table_mapping_env_variables(schema_type: SchemaType) {
    match schema_type {
        SchemaType::MultiTableMultiMeasure => {
            env::set_var("table_mapping", "multi-table");
        }
        SchemaType::SingleTableMultiMeasure => {
            env::set_var("table_mapping", "multi-table");
            env::set_var("single_table_name", "influxdb-measures");
        }
    }
}

fn set_base_environment_variables() {
    env::set_var("database_name", DATABASE_NAME);
    env::set_var("enable_database_creation", "true");
    env::set_var("enable_table_creation", "true");
    env::set_var("enable_mag_store_writes", "true");
    // A value of 30,000 allows single-digit timestamps to be ingested.
    env::set_var("mag_store_retention_period", "30000");
    env::set_var("mem_store_retention_period", "12");
    env::set_var("region", REGION);
    env::set_var(
        "measure_name_for_multi_measure_records",
        "test_measure_name",
    );
    env::remove_var("custom_partition_key_type");
    env::remove_var("custom_partition_key_dimension");
    env::remove_var("enforce_custom_partition_key");
}

fn random_string(n: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(n)
        .map(char::from)
        .collect()
}

fn random_number<T: PartialOrd + SampleUniform>(low: T, high: T) -> T {
    rand::thread_rng().gen_range(low, high)
}

// These integration tests use the InfluxDB Timestream connector as a library
// instead of deploying the connector as a lambda and then making
// requests to the connector. Each test builds a lambda_http::http::Request and
// passes it to the connector's lambda_handler function. This means integration
// testing has minimal overhead.

#[tokio::test]
async fn test_mtmm_basic() -> Result<(), Error> {
    // Tests ingesting a single point.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::MultiTableMultiMeasure);
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let client = Arc::new(client);

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},tag1={} field1={}i {}\n",
        lp_measurement_name,
        random_string(9),
        random_number(0, 100001),
        chrono::offset::Utc::now().timestamp_millis()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = LambdaEvent::<Value>::new(
        json!({ "queryStringParameters": query_parameters, "body": point }),
        Context::default(),
    );

    let response = influxdb_timestream_connector::lambda_handler(&client, request).await;
    println!("Response {:?}", response);

    let mut cleanup_batch = CleanupBatch::new(DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup(&client).await;

    assert!(response.is_ok());
    assert!(response?["statusCode"] == 200);
    Ok(())
}

#[tokio::test]
async fn test_mtmm_create_database() -> Result<(), Error> {
    // Tests ingesting a single point and creating a database.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::MultiTableMultiMeasure);
    let test_create_database_name = "test_create_database_influxdb_timestream_connector_integ";
    env::set_var("database_name", test_create_database_name);
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let client = Arc::new(client);

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},tag1={} field1={}i {}\n",
        lp_measurement_name,
        random_string(9),
        random_number(0, 100001),
        chrono::offset::Utc::now().timestamp_millis()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = LambdaEvent::<Value>::new(
        json!({ "queryStringParameters": query_parameters, "body": point }),
        Context::default(),
    );

    let response = influxdb_timestream_connector::lambda_handler(&client, request).await;
    println!("Response: {:?}", response);

    let mut cleanup_batch = CleanupBatch::new(
        test_create_database_name.to_string(),
        vec![lp_measurement_name],
    );
    cleanup_batch.cleanup(&client).await;
    let database_delete_response = client
        .delete_database()
        .database_name(test_create_database_name)
        .send()
        .await;
    if database_delete_response.is_err() {
        println!(
            "The database {} failed to delete",
            test_create_database_name
        );
    }

    assert!(response.is_ok());
    assert!(response?["statusCode"] == 200);
    Ok(())
}

#[tokio::test]
async fn test_mtmm_unusual_query_parameters() -> Result<(), Error> {
    // Tests ingesting a single point with a query parameters key with unusual spelling.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::MultiTableMultiMeasure);
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let client = Arc::new(client);

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},tag1={} field1={}i {}\n",
        lp_measurement_name,
        random_string(9),
        random_number(0, 100001),
        chrono::offset::Utc::now().timestamp_millis()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = LambdaEvent::<Value>::new(
        json!({ "QUERYtestStrparaMETERS": query_parameters, "body": point }),
        Context::default(),
    );

    let response = influxdb_timestream_connector::lambda_handler(&client, request).await;
    println!("Response {:?}", response);

    let mut cleanup_batch = CleanupBatch::new(DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup(&client).await;

    assert!(response.is_ok());
    assert!(response?["statusCode"] == 200);
    Ok(())
}

#[tokio::test]
async fn test_mtmm_no_query_parameters() -> Result<(), Error> {
    // Tests ingesting a single point without query parameters.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::MultiTableMultiMeasure);
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let client = Arc::new(client);

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},tag1={} field1={}i {}\n",
        lp_measurement_name,
        random_string(9),
        random_number(0, 100001),
        chrono::offset::Utc::now().timestamp_millis()
    );

    let request = LambdaEvent::<Value>::new(json!({ "body": point }), Context::default());

    let response = influxdb_timestream_connector::lambda_handler(&client, request).await;
    println!("Response {:?}", response);

    let mut cleanup_batch = CleanupBatch::new(DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup(&client).await;

    assert!(response.is_ok());
    assert!(response?["statusCode"] == 200);
    Ok(())
}

#[tokio::test]
async fn test_mtmm_multiple_timestamps() -> Result<(), Error> {
    // Tests ingesting a single point with two timestamps.
    // Note, the connector either returns JSON with a 200 status code or an Error. This is so that
    // the dead letter queue works when the connector is deployed as part of a stack.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::MultiTableMultiMeasure);
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let client = Arc::new(client);

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},tag1={} field1={}i {} {}\n",
        lp_measurement_name,
        random_string(9),
        random_number(0, 100001),
        chrono::offset::Utc::now().timestamp_millis(),
        chrono::offset::Utc::now().timestamp_millis()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = LambdaEvent::<Value>::new(
        json!({ "queryStringParameters": query_parameters, "body": point }),
        Context::default(),
    );

    let response = influxdb_timestream_connector::lambda_handler(&client, request).await;

    let mut cleanup_batch = CleanupBatch::new(DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup(&client).await;

    assert!(response.is_err());
    Ok(())
}

#[tokio::test]
async fn test_mtmm_many_tags_many_fields() -> Result<(), Error> {
    // Tests ingesting a single point with 50 tags and 50 fields.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::MultiTableMultiMeasure);
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let client = Arc::new(client);

    let lp_measurement_name = String::from("readings");

    let mut point = format!("{},", lp_measurement_name);

    for i in 0..50 {
        point.push_str(&format!("tag{}={}", i, random_string(9)));
        if i != 49 {
            point.push(',');
        }
    }
    point.push(' ');
    for i in 0..50 {
        point.push_str(&format!("field{}={}i", i, random_number(0, 100001)));
        if i != 49 {
            point.push(',');
        }
    }
    point.push_str(&format!(" {}\n", chrono::Utc::now().timestamp_millis()));

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = LambdaEvent::<Value>::new(
        json!({ "queryStringParameters": query_parameters, "body": point }),
        Context::default(),
    );

    let response = influxdb_timestream_connector::lambda_handler(&client, request).await;
    println!("Response {:?}", response);

    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let mut cleanup_batch = CleanupBatch::new(DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup(&client).await;

    assert!(response.is_ok());
    assert!(response?["statusCode"] == 200);
    Ok(())
}

#[tokio::test]
async fn test_mtmm_float() -> Result<(), Error> {
    // Tests ingesting a single point with a float value for the field.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::MultiTableMultiMeasure);
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let client = Arc::new(client);

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},tag1={} field1={} {}\n",
        lp_measurement_name,
        random_string(9),
        random_number(0.0, 100001.0),
        chrono::offset::Utc::now().timestamp_millis()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = LambdaEvent::<Value>::new(
        json!({ "queryStringParameters": query_parameters, "body": point }),
        Context::default(),
    );

    let response = influxdb_timestream_connector::lambda_handler(&client, request).await;
    println!("Response {:?}", response);

    let mut cleanup_batch = CleanupBatch::new(DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup(&client).await;

    assert!(response.is_ok());
    assert!(response?["statusCode"] == 200);
    Ok(())
}

#[tokio::test]
async fn test_mtmm_string() -> Result<(), Error> {
    // Tests ingesting a single point with a string value for the field.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::MultiTableMultiMeasure);
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let client = Arc::new(client);

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},tag1={} field1=\"{}\" {}\n",
        lp_measurement_name,
        random_string(9),
        random_string(9),
        chrono::offset::Utc::now().timestamp_millis()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = LambdaEvent::<Value>::new(
        json!({ "queryStringParameters": query_parameters, "body": point }),
        Context::default(),
    );

    let response = influxdb_timestream_connector::lambda_handler(&client, request).await;
    println!("Response {:?}", response);

    let mut cleanup_batch = CleanupBatch::new(DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup(&client).await;

    assert!(response.is_ok());
    assert!(response?["statusCode"] == 200);
    Ok(())
}

#[tokio::test]
async fn test_mtmm_bool() -> Result<(), Error> {
    // Tests ingesting a single point with a bool value for the field.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::MultiTableMultiMeasure);
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let client = Arc::new(client);

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},tag1={} field1={} {}\n",
        lp_measurement_name,
        random_string(9),
        true,
        chrono::offset::Utc::now().timestamp_millis()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = LambdaEvent::<Value>::new(
        json!({ "queryStringParameters": query_parameters, "body": point }),
        Context::default(),
    );

    let response = influxdb_timestream_connector::lambda_handler(&client, request).await;
    println!("Response {:?}", response);

    let mut cleanup_batch = CleanupBatch::new(DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup(&client).await;

    assert!(response.is_ok());
    assert!(response?["statusCode"] == 200);
    Ok(())
}

#[tokio::test]
async fn test_mtmm_max_tag_length() -> Result<(), Error> {
    // Tests ingesting a single point with a tag where the tag's key
    // is 60, the maximum allowed dimension name length, and its value
    // is 1988 characters long. The length of the tag key and tag value
    // together amount to the maximum size for a dimension pair, 2 kilobytes.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::MultiTableMultiMeasure);
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let client = Arc::new(client);

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},{}={} field1={}i {}\n",
        lp_measurement_name,
        random_string(60),
        random_string(1988),
        random_number(0, 100001),
        chrono::offset::Utc::now().timestamp_millis()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = LambdaEvent::<Value>::new(
        json!({ "queryStringParameters": query_parameters, "body": point }),
        Context::default(),
    );

    let response = influxdb_timestream_connector::lambda_handler(&client, request).await;
    println!("Response {:?}", response);

    let mut cleanup_batch = CleanupBatch::new(DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup(&client).await;

    assert!(response.is_ok());
    assert!(response?["statusCode"] == 200);
    Ok(())
}

#[tokio::test]
async fn test_mtmm_beyond_max_tag_length() -> Result<(), Error> {
    // Tests ingesting a single point with a tag where the tag's key
    // is 60, the maximum allowed dimension name length, and its value
    // is 1989 characters long. The length of the tag key and tag value
    // together exceed the maximum size for a dimension pair, 2 kilobytes.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::MultiTableMultiMeasure);
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let client = Arc::new(client);

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},{}={} field1={}i {}\n",
        lp_measurement_name,
        random_string(60),
        random_string(1989),
        random_number(0, 100001),
        chrono::offset::Utc::now().timestamp_millis()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = LambdaEvent::<Value>::new(
        json!({ "queryStringParameters": query_parameters, "body": point }),
        Context::default(),
    );

    let response = influxdb_timestream_connector::lambda_handler(&client, request).await;

    let mut cleanup_batch = CleanupBatch::new(DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup(&client).await;

    assert!(response.is_err());
    Ok(())
}

#[tokio::test]
async fn test_mtmm_max_field_length() -> Result<(), Error> {
    // Tests ingesting a single point with a field where the length
    // of the field key is the maximum measure name, 256, and the length
    // of the field value is the maximum measure value size, 2048.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::MultiTableMultiMeasure);
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let client = Arc::new(client);

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},field1={} {}=\"{}\" {}\n",
        lp_measurement_name,
        random_string(9),
        random_string(256),
        random_string(2048),
        chrono::offset::Utc::now().timestamp_millis()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = LambdaEvent::<Value>::new(
        json!({ "queryStringParameters": query_parameters, "body": point }),
        Context::default(),
    );

    let response = influxdb_timestream_connector::lambda_handler(&client, request).await;
    println!("Response {:?}", response);

    let mut cleanup_batch = CleanupBatch::new(DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup(&client).await;

    assert!(response.is_ok());
    assert!(response?["statusCode"] == 200);
    Ok(())
}

#[tokio::test]
async fn test_mtmm_beyond_max_field_length() -> Result<(), Error> {
    // Tests ingesting a single point with a field where the length
    // of the field key is the maximum measure name, 256, and the length
    // of the field value is beyond the maximum measure value size, 2048.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::MultiTableMultiMeasure);
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let client = Arc::new(client);

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},field1={} {}=\"{}\" {}\n",
        lp_measurement_name,
        random_string(9),
        random_string(256),
        random_string(2049),
        chrono::offset::Utc::now().timestamp_millis()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = LambdaEvent::<Value>::new(
        json!({ "queryStringParameters": query_parameters, "body": point }),
        Context::default(),
    );

    let response = influxdb_timestream_connector::lambda_handler(&client, request).await;

    let mut cleanup_batch = CleanupBatch::new(DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup(&client).await;

    assert!(response.is_err());
    Ok(())
}

#[tokio::test]
async fn test_mtmm_max_unique_field_keys() -> Result<(), Error> {
    // Tests ingesting a batch of points where the number of unique field keys
    // in the batch equals the maximum number of unique measures for a single
    // table, 1024.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::MultiTableMultiMeasure);
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let client = Arc::new(client);

    let lp_measurement_name = String::from("readings");

    let mut lp_batch = String::new();
    for i in 0..1024 {
        let point = format!(
            "{},tag1={} field{}={}i {}\n",
            lp_measurement_name,
            random_string(9),
            i,
            random_number(0, 100001),
            chrono::offset::Utc::now().timestamp_millis()
        );
        lp_batch.push_str(&point);
    }

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = LambdaEvent::<Value>::new(
        json!({ "queryStringParameters": query_parameters, "body": lp_batch }),
        Context::default(),
    );

    let response = influxdb_timestream_connector::lambda_handler(&client, request).await;
    println!("Response {:?}", response);

    let mut cleanup_batch = CleanupBatch::new(DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup(&client).await;

    assert!(response.is_ok());
    assert!(response?["statusCode"] == 200);
    Ok(())
}

#[tokio::test]
async fn test_mtmm_beyond_max_unique_field_keys() -> Result<(), Error> {
    // Tests ingesting a batch of points where the number of unique field keys
    // in the batch exceeds the maximum number of unique measures for a single
    // table, 1024.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::MultiTableMultiMeasure);
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let client = Arc::new(client);

    let lp_measurement_name = String::from("readings");

    let mut lp_batch = String::new();
    for i in 0..1025 {
        let point = format!(
            "{},tag1={} field{}={}i {}\n",
            lp_measurement_name,
            random_string(9),
            i,
            random_number(0, 100001),
            chrono::offset::Utc::now().timestamp_millis()
        );
        lp_batch.push_str(&point);
    }

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = LambdaEvent::<Value>::new(
        json!({ "queryStringParameters": query_parameters, "body": lp_batch }),
        Context::default(),
    );

    let response = influxdb_timestream_connector::lambda_handler(&client, request).await;
    println!("Response: {:?}", response);

    let mut cleanup_batch = CleanupBatch::new(DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup(&client).await;

    assert!(response.is_err());
    Ok(())
}

#[tokio::test]
async fn test_mtmm_max_unique_tag_keys() -> Result<(), Error> {
    // Tests ingesting a batch of points where the number of unique tag keys
    // in the batch equals the maximum number of unique dimensions for a single
    // table, 128.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::MultiTableMultiMeasure);
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let client = Arc::new(client);

    let lp_measurement_name = String::from("readings");

    let mut lp_batch = String::new();
    for i in 0..128 {
        let point = format!(
            "{},tag{}={} field1={}i {}\n",
            lp_measurement_name,
            i,
            random_string(9),
            random_number(0, 100001),
            chrono::offset::Utc::now().timestamp_millis()
        );
        lp_batch.push_str(&point);
    }

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = LambdaEvent::<Value>::new(
        json!({ "queryStringParameters": query_parameters, "body": lp_batch }),
        Context::default(),
    );

    let response = influxdb_timestream_connector::lambda_handler(&client, request).await;
    println!("Response {:?}", response);

    let mut cleanup_batch = CleanupBatch::new(DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup(&client).await;

    assert!(response.is_ok());
    assert!(response?["statusCode"] == 200);
    Ok(())
}

#[tokio::test]
async fn test_mtmm_beyond_max_unique_tag_keys() -> Result<(), Error> {
    // Tests ingesting a batch of points where the number of unique tag keys
    // in the batch exceeds the maximum number of unique dimensions for a single
    // table, 128.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::MultiTableMultiMeasure);
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let client = Arc::new(client);

    let lp_measurement_name = String::from("readings");

    let mut lp_batch = String::new();
    for i in 0..129 {
        let point = format!(
            "{},tag{}={} field1={}i {}\n",
            lp_measurement_name,
            i,
            random_string(9),
            random_number(0, 100001),
            chrono::offset::Utc::now().timestamp_millis()
        );
        lp_batch.push_str(&point);
    }

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = LambdaEvent::<Value>::new(
        json!({ "queryStringParameters": query_parameters, "body": lp_batch }),
        Context::default(),
    );

    let response = influxdb_timestream_connector::lambda_handler(&client, request).await;
    println!("Response: {:?}", response);

    let mut cleanup_batch = CleanupBatch::new(DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup(&client).await;

    assert!(response.is_err());
    Ok(())
}

#[tokio::test]
async fn test_mtmm_max_table_name_length() -> Result<(), Error> {
    // Tests ingesting a single point with measurement with length
    // equal to the maximum number of bytes a Timestream table name can
    // have.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::MultiTableMultiMeasure);
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let client = Arc::new(client);

    let lp_measurement_name = random_string(MAX_TIMESTREAM_TABLE_NAME_LENGTH);

    let point = format!(
        "{},tag1={} field1={}i {}\n",
        lp_measurement_name,
        random_string(9),
        random_number(0, 100001),
        chrono::offset::Utc::now().timestamp_millis()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = LambdaEvent::<Value>::new(
        json!({ "queryStringParameters": query_parameters, "body": point }),
        Context::default(),
    );

    let response = influxdb_timestream_connector::lambda_handler(&client, request).await;
    println!("Response {:?}", response);

    let mut cleanup_batch = CleanupBatch::new(DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup(&client).await;

    assert!(response.is_ok());
    assert!(response?["statusCode"] == 200);
    Ok(())
}

#[tokio::test]
async fn test_mtmm_beyond_max_table_name_length() -> Result<(), Error> {
    // Tests ingesting a single point with measurement with length
    // exceeding the maximum number of bytes a Timestream table name can
    // have.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::MultiTableMultiMeasure);
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let client = Arc::new(client);

    let lp_measurement_name = random_string(MAX_TIMESTREAM_TABLE_NAME_LENGTH + 1);

    let point = format!(
        "{},tag1={} field1={}i {}\n",
        lp_measurement_name,
        random_string(9),
        random_number(0, 100001),
        chrono::offset::Utc::now().timestamp_millis()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = LambdaEvent::<Value>::new(
        json!({ "queryStringParameters": query_parameters, "body": point }),
        Context::default(),
    );

    let response = influxdb_timestream_connector::lambda_handler(&client, request).await;

    let mut cleanup_batch = CleanupBatch::new(DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup(&client).await;

    assert!(response.is_err());
    Ok(())
}

#[tokio::test]
async fn test_mtmm_nanosecond_precision() -> Result<(), Error> {
    // Tests ingesting a single point with nanosecond precision.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::MultiTableMultiMeasure);
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let client = Arc::new(client);

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},tag1={} field1={}i {}\n",
        lp_measurement_name,
        random_string(9),
        random_number(0, 100001),
        chrono::offset::Utc::now()
            .timestamp_nanos_opt()
            .expect("Failed to create nanosecond timestamp")
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ns".to_string())]);
    let request = LambdaEvent::<Value>::new(
        json!({ "queryStringParameters": query_parameters, "body": point }),
        Context::default(),
    );

    let response = influxdb_timestream_connector::lambda_handler(&client, request).await;
    println!("Response {:?}", response);

    let mut cleanup_batch = CleanupBatch::new(DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup(&client).await;

    assert!(response.is_ok());
    assert!(response?["statusCode"] == 200);
    Ok(())
}

#[tokio::test]
async fn test_mtmm_microsecond_precision() -> Result<(), Error> {
    // Tests ingesting a single point with microsecond precision.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::MultiTableMultiMeasure);
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let client = Arc::new(client);

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},tag1={} field1={}i {}\n",
        lp_measurement_name,
        random_string(9),
        random_number(0, 100001),
        chrono::offset::Utc::now().timestamp_micros()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "us".to_string())]);
    let request = LambdaEvent::<Value>::new(
        json!({ "queryStringParameters": query_parameters, "body": point }),
        Context::default(),
    );

    let response = influxdb_timestream_connector::lambda_handler(&client, request).await;
    println!("Response {:?}", response);

    let mut cleanup_batch = CleanupBatch::new(DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup(&client).await;

    assert!(response.is_ok());
    assert!(response?["statusCode"] == 200);
    Ok(())
}

#[tokio::test]
async fn test_mtmm_second_precision() -> Result<(), Error> {
    // Tests ingesting a single point with second precision.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::MultiTableMultiMeasure);
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let client = Arc::new(client);

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},tag1={} field1={}i {}\n",
        lp_measurement_name,
        random_string(9),
        random_number(0, 100001),
        chrono::offset::Utc::now().timestamp()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "s".to_string())]);
    let request = LambdaEvent::<Value>::new(
        json!({ "queryStringParameters": query_parameters, "body": point }),
        Context::default(),
    );

    let response = influxdb_timestream_connector::lambda_handler(&client, request).await;
    println!("Response {:?}", response);

    let mut cleanup_batch = CleanupBatch::new(DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup(&client).await;

    assert!(response.is_ok());
    assert!(response?["statusCode"] == 200);
    Ok(())
}

#[tokio::test]
async fn test_mtmm_no_precision() -> Result<(), Error> {
    // Tests ingesting a single point without precision supplied.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::MultiTableMultiMeasure);
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let client = Arc::new(client);

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},tag1={} field1={}i {}\n",
        lp_measurement_name,
        random_string(9),
        random_number(0, 100001),
        // Without a precision provided, the connector should assume the precision
        // is nanoseconds.
        chrono::offset::Utc::now()
            .timestamp_nanos_opt()
            .expect("Failed to create nanosecond timestamp")
    );

    let request = LambdaEvent::<Value>::new(
        json!({ "queryStringParameters": "", "body": point }),
        Context::default(),
    );

    let response = influxdb_timestream_connector::lambda_handler(&client, request).await;
    println!("Response {:?}", response);

    let mut cleanup_batch = CleanupBatch::new(DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup(&client).await;

    assert!(response.is_ok());
    assert!(response?["statusCode"] == 200);
    Ok(())
}

#[tokio::test]
async fn test_mtmm_empty_point() -> Result<(), Error> {
    // Tests ingesting an empty string.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::MultiTableMultiMeasure);
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let client = Arc::new(client);

    let point = String::new();

    let request = LambdaEvent::<Value>::new(
        json!({ "queryStringParameters": "", "body": point }),
        Context::default(),
    );

    let response = influxdb_timestream_connector::lambda_handler(&client, request).await;
    println!("Response {:?}", response);

    assert!(response.is_ok());
    assert!(response?["statusCode"] == 200);
    Ok(())
}

#[tokio::test]
pub async fn test_mtmm_small_timestamp() -> Result<(), Error> {
    // Tests ingesting with a single-digit millisecond timestamp.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::MultiTableMultiMeasure);
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let client = Arc::new(client);

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},tag1={} field1={}i {}\n",
        lp_measurement_name,
        random_string(9),
        random_number(0, 100001),
        3
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = LambdaEvent::<Value>::new(
        json!({ "queryStringParameters": query_parameters, "body": point }),
        Context::default(),
    );

    let response = influxdb_timestream_connector::lambda_handler(&client, request).await;
    println!("Response {:?}", response);

    let mut cleanup_batch = CleanupBatch::new(DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup(&client).await;

    assert!(response.is_ok());
    assert!(response?["statusCode"] == 200);
    Ok(())
}

#[tokio::test]
async fn test_mtmm_5_measurements() -> Result<(), Error> {
    // Tests ingesting a batch with five measurements.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::MultiTableMultiMeasure);
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let client = Arc::new(client);

    let mut table_names_to_delete = Vec::<String>::new();
    let lp_measurement_name = String::from("readings");

    let mut lp_batch = String::new();
    for i in 0..5 {
        let lp_measurement_name = format!("{lp_measurement_name}{i}").to_string();
        table_names_to_delete.push(lp_measurement_name.clone());

        let point = format!(
            "{},tag1={} field1={}i {}\n",
            lp_measurement_name,
            random_string(9),
            random_number(0, 100001),
            chrono::Utc::now().timestamp_millis()
        );
        lp_batch.push_str(&point);
    }

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = LambdaEvent::<Value>::new(
        json!({ "queryStringParameters": query_parameters, "body": lp_batch }),
        Context::default(),
    );

    let response = influxdb_timestream_connector::lambda_handler(&client, request).await;
    println!("Response {:?}", response);

    let mut cleanup_batch = CleanupBatch::new(DATABASE_NAME.to_string(), table_names_to_delete);
    cleanup_batch.cleanup(&client).await;

    assert!(response.is_ok());
    assert!(response?["statusCode"] == 200);
    Ok(())
}

#[tokio::test]
async fn test_mtmm_100_measurements() -> Result<(), Error> {
    // Tests ingesting a batch with 100 measurements.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::MultiTableMultiMeasure);
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let client = Arc::new(client);

    let mut table_names_to_delete = Vec::<String>::new();

    let lp_measurement_name = String::from("readings");
    let mut lp_batch = String::new();
    for i in 0..100 {
        let lp_measurement_name = format!("{lp_measurement_name}{i}").to_string();
        table_names_to_delete.push(lp_measurement_name.clone());

        let point = format!(
            "{},tag1={} field1={}i {}\n",
            lp_measurement_name,
            random_string(9),
            random_number(0, 100001),
            chrono::Utc::now().timestamp_millis()
        );
        lp_batch.push_str(&point);
    }

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = LambdaEvent::<Value>::new(
        json!({ "queryStringParameters": query_parameters, "body": lp_batch }),
        Context::default(),
    );

    let response = influxdb_timestream_connector::lambda_handler(&client, request).await;
    println!("Response {:?}", response);

    let mut cleanup_batch = CleanupBatch::new(DATABASE_NAME.to_string(), table_names_to_delete);
    cleanup_batch.cleanup(&client).await;

    assert!(response.is_ok());
    assert!(response?["statusCode"] == 200);
    Ok(())
}

#[tokio::test]
async fn test_mtmm_5000_batch() -> Result<(), Error> {
    // Tests ingesting a batch of 5000 points with a single measurement.
    // 5000 is the recommended batch size for InfluxDB v2 OSS.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::MultiTableMultiMeasure);
    // Cleanup
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let client = Arc::new(client);

    let lp_measurement_name = String::from("readings");
    let mut lp_batch = String::new();

    for _ in 0..5000 {
        let point = format!(
            "{},tag1={} field1={}i {}\n",
            lp_measurement_name,
            random_string(9),
            random_number(0, 100001),
            chrono::offset::Utc::now().timestamp_millis()
        );
        lp_batch.push_str(&point);
    }

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = LambdaEvent::<Value>::new(
        json!({ "queryStringParameters": query_parameters, "body": lp_batch }),
        Context::default(),
    );

    let response = influxdb_timestream_connector::lambda_handler(&client, request).await;
    println!("Response {:?}", response);

    let mut cleanup_batch = CleanupBatch::new(DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup(&client).await;

    assert!(response.is_ok());
    assert!(response?["statusCode"] == 200);
    Ok(())
}

#[tokio::test]
#[should_panic]
async fn test_mtmm_no_credentials() {
    // Tests ingesting without AWS credentials. This test should panic.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::MultiTableMultiMeasure);

    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .credentials_provider(Credentials::new("", "", None, None, "test"))
        .region(Region::new(REGION))
        .load()
        .await;

    let (client, reload) = timestream_write::Client::new(&config)
        .with_endpoint_discovery_enabled()
        .await
        .expect("Failed to get the write client connection with Timestream");
    let client = Arc::new(client);
    tokio::task::spawn(reload.reload_task());

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},tag1={} field1={}i {}\n",
        lp_measurement_name,
        random_string(9),
        random_number(0, 100001),
        chrono::offset::Utc::now().timestamp_millis()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = LambdaEvent::<Value>::new(
        json!({ "queryStringParameters": query_parameters, "body": point }),
        Context::default(),
    );

    let _ = influxdb_timestream_connector::lambda_handler(&client, request).await;
}

#[tokio::test]
#[should_panic]
async fn test_mtmm_incorrect_credentials() {
    // Tests ingesting with incorrect AWS credentials. This test should panic.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::MultiTableMultiMeasure);

    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .credentials_provider(Credentials::new(
            "ANOTREAL",
            "notrealrnrELgWzOk3IfjzDKtFBhDby",
            None,
            None,
            "test",
        ))
        .region(Region::new(REGION))
        .load()
        .await;

    let (client, reload) = timestream_write::Client::new(&config)
        .with_endpoint_discovery_enabled()
        .await
        .expect("Failed to get the write client connection with Timestream");
    let client = Arc::new(client);
    tokio::task::spawn(reload.reload_task());

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},tag1={} field1={}i {}\n",
        lp_measurement_name,
        random_string(9),
        random_number(0, 100001),
        chrono::offset::Utc::now().timestamp_millis()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = LambdaEvent::<Value>::new(
        json!({ "queryStringParameters": query_parameters, "body": point }),
        Context::default(),
    );

    let _ = influxdb_timestream_connector::lambda_handler(&client, request).await;
}

#[tokio::test]
async fn test_mtmm_custom_dimension_partition_key_optional_enforcement() -> Result<(), Error> {
    // Tests ingesting a single point and specifying a valid configuration for
    // a custom dimension partition key with optional enforcement.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::MultiTableMultiMeasure);
    env::set_var("custom_partition_key_type", "dimension");
    env::set_var("custom_partition_key_dimension", "nomatch");
    env::set_var("enforce_custom_partition_key", "false");
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let client = Arc::new(client);

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},tag1={} field1={}i {}\n",
        lp_measurement_name,
        random_string(9),
        random_number(0, 100001),
        chrono::offset::Utc::now().timestamp_millis()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = LambdaEvent::<Value>::new(
        json!({ "queryStringParameters": query_parameters, "body": point }),
        Context::default(),
    );

    let response = influxdb_timestream_connector::lambda_handler(&client, request).await;
    println!("Response {:?}", response);

    let mut cleanup_batch = CleanupBatch::new(DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup(&client).await;

    assert!(response.is_ok());
    assert!(response?["statusCode"] == 200);
    Ok(())
}

#[tokio::test]
async fn test_mtmm_custom_dimension_partition_key_required_enforcement_accepted(
) -> Result<(), Error> {
    // Tests ingesting a single point and specifying a valid configuration for
    // a custom dimension partition key with required enforcement and a successful ingestion.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::MultiTableMultiMeasure);
    env::set_var("custom_partition_key_type", "dimension");
    env::set_var("custom_partition_key_dimension", "tag1");
    env::set_var("enforce_custom_partition_key", "true");
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let client = Arc::new(client);

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},tag1={} field1={}i {}\n",
        lp_measurement_name,
        random_string(9),
        random_number(0, 100001),
        chrono::offset::Utc::now().timestamp_millis()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = LambdaEvent::<Value>::new(
        json!({ "queryStringParameters": query_parameters, "body": point }),
        Context::default(),
    );

    let response = influxdb_timestream_connector::lambda_handler(&client, request).await;
    println!("Response {:?}", response);

    let mut cleanup_batch = CleanupBatch::new(DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup(&client).await;

    assert!(response.is_ok());
    assert!(response?["statusCode"] == 200);
    Ok(())
}

#[tokio::test]
async fn test_mtmm_custom_dimension_partition_key_required_enforcement_rejected(
) -> Result<(), Error> {
    // Tests ingesting a single point and specifying a valid configuration for
    // a custom dimension partition key with required enforcement and an unsuccessful ingestion.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::MultiTableMultiMeasure);
    env::set_var("custom_partition_key_type", "dimension");
    env::set_var("custom_partition_key_dimension", "nomatch");
    env::set_var("enforce_custom_partition_key", "true");
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let client = Arc::new(client);

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},tag1={} field1={}i {}\n",
        lp_measurement_name,
        random_string(9),
        random_number(0, 100001),
        chrono::offset::Utc::now().timestamp_millis()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = LambdaEvent::<Value>::new(
        json!({ "queryStringParameters": query_parameters, "body": point }),
        Context::default(),
    );

    let response = influxdb_timestream_connector::lambda_handler(&client, request).await;
    println!("Response: {:?}", response);

    let mut cleanup_batch = CleanupBatch::new(DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup(&client).await;

    assert!(response.is_err());
    Ok(())
}

#[tokio::test]
async fn test_mtmm_custom_dimension_partition_key_no_dimension() -> Result<(), Error> {
    // Tests ingesting a single point and specifying a configuration for
    // a custom dimension partition key without a dimension specified.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::MultiTableMultiMeasure);
    env::set_var("custom_partition_key_type", "dimension");
    env::remove_var("custom_partition_key_dimension");
    env::set_var("enforce_custom_partition_key", "false");
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let client = Arc::new(client);

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},tag1={} field1={}i {}\n",
        lp_measurement_name,
        random_string(9),
        random_number(0, 100001),
        chrono::offset::Utc::now().timestamp_millis()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = LambdaEvent::<Value>::new(
        json!({ "queryStringParameters": query_parameters, "body": point }),
        Context::default(),
    );

    let response = influxdb_timestream_connector::lambda_handler(&client, request).await;
    println!("Response: {:?}", response);

    let mut cleanup_batch = CleanupBatch::new(DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup(&client).await;

    assert!(response.is_err());
    Ok(())
}

#[tokio::test]
async fn test_mtmm_custom_dimension_partition_key_no_enforcement() -> Result<(), Error> {
    // Tests ingesting a single point and specifying a configuration for
    // a custom dimension partition key without an enforcement configuration specified.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::MultiTableMultiMeasure);
    env::set_var("custom_partition_key_type", "dimension");
    env::set_var("custom_partition_key_dimension", "tag1");
    env::remove_var("enforce_custom_partition_key");
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let client = Arc::new(client);

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},tag1={} field1={}i {}\n",
        lp_measurement_name,
        random_string(9),
        random_number(0, 100001),
        chrono::offset::Utc::now().timestamp_millis()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = LambdaEvent::<Value>::new(
        json!({ "queryStringParameters": query_parameters, "body": point }),
        Context::default(),
    );

    let response = influxdb_timestream_connector::lambda_handler(&client, request).await;
    println!("Response: {:?}", response);

    let mut cleanup_batch = CleanupBatch::new(DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup(&client).await;

    assert!(response.is_err());
    Ok(())
}

#[tokio::test]
async fn test_mtmm_custom_measure_partition_key() -> Result<(), Error> {
    // Tests ingesting a single point and specifying a valid configuration for
    // a custom measure partition key.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::MultiTableMultiMeasure);
    env::set_var("custom_partition_key_type", "measure");
    env::remove_var("custom_partition_key_dimension");
    env::remove_var("enforce_custom_partition_key");
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let client = Arc::new(client);

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},tag1={} field1={}i {}\n",
        lp_measurement_name,
        random_string(9),
        random_number(0, 100001),
        chrono::offset::Utc::now().timestamp_millis()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = LambdaEvent::<Value>::new(
        json!({ "queryStringParameters": query_parameters, "body": point }),
        Context::default(),
    );

    let response = influxdb_timestream_connector::lambda_handler(&client, request).await;
    println!("Response: {:?}", response);

    let mut cleanup_batch = CleanupBatch::new(DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup(&client).await;

    assert!(response.is_ok());
    assert!(response?["statusCode"] == 200);
    Ok(())
}

#[tokio::test]
async fn test_mtmm_custom_measure_partition_key_with_dimension() -> Result<(), Error> {
    // Tests ingesting a single point and specifying a valid configuration for
    // a custom measure partition key with a dimension specified. The dimension should be ignored.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::MultiTableMultiMeasure);
    env::set_var("custom_partition_key_type", "measure");
    env::set_var("custom_partition_key_dimension", "should_be_ignored");
    env::remove_var("enforce_custom_partition_key");
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let client = Arc::new(client);

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},tag1={} field1={}i {}\n",
        lp_measurement_name,
        random_string(9),
        random_number(0, 100001),
        chrono::offset::Utc::now().timestamp_millis()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = LambdaEvent::<Value>::new(
        json!({ "queryStringParameters": query_parameters, "body": point }),
        Context::default(),
    );

    let response = influxdb_timestream_connector::lambda_handler(&client, request).await;
    println!("Response: {:?}", response);

    let mut cleanup_batch = CleanupBatch::new(DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup(&client).await;

    assert!(response.is_ok());
    assert!(response?["statusCode"] == 200);
    Ok(())
}

#[tokio::test]
async fn test_mtmm_custom_measure_partition_key_with_enforcement() -> Result<(), Error> {
    // Tests ingesting a single point and specifying a valid configuration for
    // a custom measure partition key with an enforcement configuration specified.
    // The enforcement configuration should be ignored.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::MultiTableMultiMeasure);
    env::set_var("custom_partition_key_type", "measure");
    env::remove_var("custom_partition_key_dimension");
    env::set_var("enforce_custom_partition_key", "false");
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let client = Arc::new(client);

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},tag1={} field1={}i {}\n",
        lp_measurement_name,
        random_string(9),
        random_number(0, 100001),
        chrono::offset::Utc::now().timestamp_millis()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = LambdaEvent::<Value>::new(
        json!({ "queryStringParameters": query_parameters, "body": point }),
        Context::default(),
    );

    let response = influxdb_timestream_connector::lambda_handler(&client, request).await;
    println!("Response: {:?}", response);

    let mut cleanup_batch = CleanupBatch::new(DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup(&client).await;

    assert!(response.is_ok());
    assert!(response?["statusCode"] == 200);
    Ok(())
}

#[tokio::test]
async fn test_stmm_basic() -> Result<(), Error> {
    // Tests ingesting a single point.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::SingleTableMultiMeasure);
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let client = Arc::new(client);

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},tag1={} field1={}i {}\n",
        lp_measurement_name,
        random_string(9),
        random_number(0, 100001),
        chrono::offset::Utc::now().timestamp_millis()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = LambdaEvent::<Value>::new(
        json!({ "queryStringParameters": query_parameters, "body": point }),
        Context::default(),
    );

    let response = influxdb_timestream_connector::lambda_handler(&client, request).await;
    println!("Response {:?}", response);

    let mut cleanup_batch = CleanupBatch::new(DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup(&client).await;

    assert!(response.is_ok());
    assert!(response?["statusCode"] == 200);
    Ok(())
}

#[tokio::test]
async fn test_stmm_varying_metrics() -> Result<(), Error> {
    // Tests ingesting a batch with 100 measurements.
    set_base_environment_variables();
    set_table_mapping_env_variables(SchemaType::SingleTableMultiMeasure);
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let client = Arc::new(client);

    let mut table_names_to_delete = Vec::<String>::new();

    let lp_readings_measurement_name = String::from("readings");
    let mut lp_batch = String::new();
    for i in 0..10 {
        let lp_readings_measurement_name = format!("{lp_readings_measurement_name}{i}").to_string();
        table_names_to_delete.push(lp_readings_measurement_name.clone());

        let point = format!(
            "{},tag1={} field1={}i {}\n",
            lp_readings_measurement_name,
            random_string(9),
            random_number(0, 100001),
            chrono::Utc::now().timestamp_millis()
        );
        lp_batch.push_str(&point);
    }

    let lp_velocity_measurement_name = String::from("velocity");
    for i in 0..10 {
        let lp_velocity_measurement_name = format!("{lp_velocity_measurement_name}{i}").to_string();
        table_names_to_delete.push(lp_readings_measurement_name.clone());

        let point = format!(
            "{},tag1={} field1={}i {}\n",
            lp_velocity_measurement_name,
            random_string(9),
            random_number(0, 100001),
            chrono::Utc::now().timestamp_millis()
        );
        lp_batch.push_str(&point);
    }

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = LambdaEvent::<Value>::new(
        json!({ "queryStringParameters": query_parameters, "body": lp_batch }),
        Context::default(),
    );

    let response = influxdb_timestream_connector::lambda_handler(&client, request).await;
    println!("Response {:?}", response);

    let mut cleanup_batch = CleanupBatch::new(DATABASE_NAME.to_string(), table_names_to_delete);
    cleanup_batch.cleanup(&client).await;

    assert!(response.is_ok());
    assert!(response?["statusCode"] == 200);
    Ok(())
}
