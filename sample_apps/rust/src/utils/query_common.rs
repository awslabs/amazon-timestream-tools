use anyhow::{anyhow, Error, Result};
use aws_sdk_timestreamquery as timestream_query;
use aws_sdk_timestreamquery::types;
use aws_types::region::Region;
use clap::Parser;
use std::fs;
use std::io::Write;

const DEFAULT_DATABASE_NAME: &str = "devops_multi_sample_application";
const DEFAULT_OUTPUT_FILE: &str = "query_results.log";
const DEFAULT_REGION: &str = "us-east-1";
const DEFAULT_TABLE_NAME: &str = "host_metrics_sample_application";

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    // The Timestream for LiveAnalytics database name to use for all queries
    #[arg(short, long, default_value = DEFAULT_DATABASE_NAME)]
    pub database_name: String,

    // The full name of the output file to write query results to.
    // For example, query_results.log
    #[arg(short, long, default_value = DEFAULT_OUTPUT_FILE)]
    pub output_file: String,

    // The name of the AWS region to use for queries
    #[arg(short, long, default_value = DEFAULT_REGION)]
    pub region: String,

    // The Timestream for LiveAnalytics table name to use for all queries
    #[arg(short, long, default_value = DEFAULT_TABLE_NAME)]
    pub table_name: String,
}

pub async fn get_connection(
    region: &str,
) -> Result<timestream_query::Client, timestream_query::Error> {
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new(region.to_owned()))
        .load()
        .await;
    let (client, reload) = timestream_query::Client::new(&config)
        .with_endpoint_discovery_enabled()
        .await
        .expect("Failure to get the query client connection with Timestream");
    tokio::task::spawn(reload.reload_task());
    Ok(client)
}

pub fn write(mut file: &fs::File, s: String) -> Result<(), Error> {
    let s_formatted = format!("{}\n", s);
    file.write(s_formatted.as_bytes())
        .map_err(|_| anyhow!("Failed to write to file"))?;
    file.flush().map_err(|_| anyhow!("Failed to flush file"))?;
    Ok(())
}

#[allow(dead_code)]
pub fn process_scalar_type(data: &types::Datum) -> Result<String, Error> {
    data.scalar_value
        .clone()
        .ok_or(anyhow!("Scalar value is None"))
}

#[allow(dead_code)]
pub fn process_time_series_type(
    data: &[types::TimeSeriesDataPoint],
    column_info: &types::ColumnInfo,
) -> Result<String, Error> {
    let joined_string = data
        .iter()
        .map(|datum| -> Result<String, Error> {
            let mut datum_string = String::new();
            datum_string.push_str(&datum.time);
            datum_string.push(':');

            let column_type = column_info.r#type();
            let column_type_ref = column_type.as_ref().ok_or(anyhow!("Column type is None"))?;
            let scalar_type = column_type_ref.scalar_type.to_owned();
            let scalar_type_ref = scalar_type.ok_or(anyhow!("Scalar type is None"));

            let datum_value = datum.value.as_ref().ok_or(anyhow!("Datum value is None"))?;

            if scalar_type_ref.is_ok() && !scalar_type_ref?.as_str().is_empty() {
                datum_string.push_str(&process_scalar_type(datum_value)?);
            } else if let Some(array_column_info) = &column_type_ref.array_column_info {
                let array_value = datum_value
                    .array_value
                    .as_ref()
                    .ok_or(anyhow!("Array value is None"))?;
                datum_string.push_str(&process_array_type(array_value, array_column_info)?);
            } else if let Some(row_column_info) = &column_type_ref.row_column_info {
                let row_value = datum_value
                    .row_value
                    .as_ref()
                    .ok_or(anyhow!("Row value is None"))?;
                datum_string.push_str(&process_row_type(&row_value.data, row_column_info)?);
            } else {
                return Err(anyhow!("Bad data type"));
            }
            Ok(datum_string)
        })
        .collect::<Result<Vec<String>, Error>>()?
        .join(", ");

    Ok(joined_string)
}

#[allow(dead_code)]
pub fn process_array_type(
    datum_list: &[types::Datum],
    column_info: &types::ColumnInfo,
) -> Result<String, Error> {
    let joined_string = datum_list
        .iter()
        .map(|datum| {
            let mut datum_string = String::new();
            let column_type = column_info.r#type();
            let column_type_ref = column_type.as_ref().ok_or(anyhow!("Column type is None"))?;

            let scalar_type = column_type_ref.scalar_type.to_owned();
            let scalar_type_ref = scalar_type.ok_or(anyhow!("Scalar type is None"));

            if scalar_type_ref.is_ok() && scalar_type_ref?.as_str().is_empty() {
                datum_string.push_str(&process_scalar_type(datum)?);
            } else if let Some(time_series_measure_value_column_info) =
                &column_type_ref.time_series_measure_value_column_info
            {
                let time_series_value = datum
                    .time_series_value
                    .as_ref()
                    .ok_or(anyhow!("Time series value is None"))?;
                datum_string.push_str(&process_time_series_type(
                    time_series_value,
                    time_series_measure_value_column_info,
                )?);
            } else if let Some(array_column_info) = &column_type_ref.array_column_info {
                let array_value = datum
                    .array_value
                    .as_ref()
                    .ok_or(anyhow!("Array value is None"))?;
                datum_string.push('[');
                datum_string.push_str(&process_array_type(array_value, array_column_info)?);
                datum_string.push(']');
            } else if let Some(row_column_info) = &column_type_ref.row_column_info {
                let row_value = datum
                    .row_value
                    .as_ref()
                    .ok_or(anyhow!("Row value is None"))?;
                datum_string.push('[');
                datum_string.push_str(&process_row_type(&row_value.data, row_column_info)?);
                datum_string.push(']');
            } else {
                return Err(anyhow!("Bad column type"));
            }
            Ok(datum_string)
        })
        .collect::<Result<Vec<String>, Error>>()?
        .join(", ");

    Ok(joined_string)
}

#[allow(dead_code)]
pub fn process_row_type(
    data: &[types::Datum],
    metadata: &[types::ColumnInfo],
) -> Result<String, Error> {
    let joined_string = data
        .iter()
        .enumerate()
        .map(|(i, datum)| {
            let mut datum_string = String::new();
            let column_info = metadata[i].clone();
            let column_type = column_info.r#type();
            let column_type_ref = column_type.as_ref().ok_or(anyhow!("Column type is None"))?;
            let scalar_type = column_type_ref.scalar_type.to_owned();
            let scalar_type_ref = scalar_type.ok_or(anyhow!("Scalar type is None"));

            if scalar_type_ref.is_ok() && !scalar_type_ref?.as_str().is_empty() {
                // process simple data types
                datum_string.push_str(&process_scalar_type(datum)?);
            } else if let Some(time_series_measure_value_column_info) =
                &column_type_ref.time_series_measure_value_column_info
            {
                let datapoint_list = datum
                    .time_series_value
                    .as_ref()
                    .ok_or(anyhow!("Time series value is None"))?;
                datum_string.push('[');
                datum_string.push_str(&process_time_series_type(
                    datapoint_list,
                    time_series_measure_value_column_info,
                )?);
                datum_string.push(']');
            } else if let Some(array_column_info) = &column_type_ref.array_column_info {
                let array_value = datum
                    .array_value
                    .as_ref()
                    .ok_or(anyhow!("Array value is None"))?;
                datum_string.push('[');
                datum_string.push_str(&process_array_type(array_value, array_column_info)?);
                datum_string.push(']');
            } else if let Some(row_column_info) = &column_type_ref.row_column_info {
                let row_value = datum
                    .row_value
                    .as_ref()
                    .ok_or(anyhow!("Row value is None"))?;
                datum_string.push('[');
                datum_string.push_str(&process_row_type(&row_value.data, row_column_info)?);
                datum_string.push(']');
            } else {
                return Err(anyhow!("Bad column type"));
            }
            Ok(datum_string)
        })
        .collect::<Result<Vec<String>, Error>>()?
        .join(", ");

    Ok(joined_string)
}

pub async fn run_query(
    query: String,
    client: &timestream_query::Client,
    f: &std::fs::File,
    max_rows: i32,
) -> Result<(), Error> {
    let query_client = client.query().clone();

    let mut query_result = query_client
        .clone()
        .max_rows(max_rows)
        .query_string(&query)
        .send()
        .await;

    let mut token: String;
    let mut num_rows = 0;
    loop {
        match query_result {
            Ok(query_success) => {
                num_rows += query_success.rows.len();
                if let Some(new_next_token) = query_success.next_token {
                    // Set token to paginate through results
                    token = new_next_token;
                    query_result = query_client
                        .clone()
                        .max_rows(max_rows)
                        .query_string(&query)
                        .next_token(token)
                        .send()
                        .await;
                } else {
                    break;
                }
            }

            Err(error) => {
                let error_string = error.to_string();
                let message = format!(
                    "Error while querying the query {} : {}",
                    &query, error_string
                );
                println!("{}", message);
                write(f, message)?;
                return Err(anyhow!(error_string));
            }
        }
    }
    let message = format!("Number of rows: {}", num_rows).to_string();
    println!("{}", message);
    write(f, message)?;
    Ok(())
}
