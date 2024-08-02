use anyhow::{anyhow, Error, Result};
use aws_sdk_timestreamwrite as timestream_write;
use chrono::NaiveDateTime;
use clap::Parser;
use csv::Reader;
use sample_timestream_app::utils::timestream_helper;
use std::io::Write;
use std::str::FromStr;
use std::{thread, time::Duration};

async fn ingest_data(
    client: &timestream_write::Client,
    args: &timestream_helper::Args,
) -> Result<(), Error> {
    let mut records: Vec<timestream_write::types::Record> = Vec::new();
    let mut csv_reader = Reader::from_path("../data/sample-multi.csv")?;
    let mut records_ingested: usize = 0;
    const MAX_TIMESTREAM_BATCH_SIZE: usize = 100;

    for record in csv_reader.records() {
        let record_result = record.expect("Failed to read csv record");

        let measure_values = vec![
            aws_sdk_timestreamwrite::types::MeasureValue::builder()
                .name(&record_result[8])
                .value(&record_result[9])
                .r#type(
                    timestream_write::types::MeasureValueType::from_str(&record_result[10])
                        .expect("Failed to enumerate measure value type"),
                )
                .build()
                .expect("Failed to build measure value"),
            aws_sdk_timestreamwrite::types::MeasureValue::builder()
                .name(&record_result[11])
                .value(&record_result[12])
                .r#type(
                    timestream_write::types::MeasureValueType::from_str(&record_result[13])
                        .expect("Failed to enumerate measure value type"),
                )
                .build()
                .expect("Failed to build measure value"),
        ];

        records.push(
            timestream_write::types::Record::builder()
                .set_dimensions(Some(vec![
                    timestream_write::types::Dimension::builder()
                        .name(&record_result[0])
                        .value(&record_result[1])
                        .build()
                        .expect("Failed to build dimension"),
                    timestream_write::types::Dimension::builder()
                        .name(&record_result[2])
                        .value(&record_result[3])
                        .build()
                        .expect("Failed to build dimension"),
                    timestream_write::types::Dimension::builder()
                        .name(&record_result[4])
                        .value(&record_result[5])
                        .build()
                        .expect("Failed to build dimension"),
                ]))
                .measure_name(String::from("metrics"))
                .set_measure_values(Some(measure_values))
                .set_measure_value_type(Some(timestream_write::types::MeasureValueType::Multi))
                .time(
                    NaiveDateTime::parse_from_str(&record_result[6], "%Y-%m-%d %H:%M:%S%.9f")?
                        .and_utc()
                        .timestamp_millis()
                        .to_string(),
                )
                .time_unit(
                    timestream_write::types::TimeUnit::from_str(&record_result[7])
                        .expect("Failed to parse time unit"),
                )
                .build(),
        );

        if records.len() == MAX_TIMESTREAM_BATCH_SIZE {
            client
                .write_records()
                .database_name(&args.database_name)
                .table_name(&args.table_name)
                .set_records(Some(std::mem::take(&mut records)))
                .send()
                .await?;

            records_ingested += MAX_TIMESTREAM_BATCH_SIZE;
            print!("\r{} records ingested", records_ingested);
            std::io::stdout().flush()?;
        }
    }

    if !records.is_empty() {
        records_ingested += records.len();
        client
            .write_records()
            .database_name(&args.database_name)
            .table_name(&args.table_name)
            .set_records(Some(records))
            .send()
            .await?;

        print!("\r{} records ingested", records_ingested);
        std::io::stdout().flush()?;
    }

    println!("\nMulti-measure record ingestion complete");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = timestream_helper::Args::parse();

    let client = timestream_helper::get_connection(&args.region)
        .await
        .expect("Failed to get connection to Timestream");

    if let Err(describe_db_error) = client
        .describe_database()
        .database_name(&args.database_name)
        .send()
        .await
    {
        if describe_db_error
            .as_service_error()
            .map(|e| e.is_resource_not_found_exception())
            == Some(true)
        {
            timestream_helper::create_database(&client, &args.database_name).await?;
        } else {
            return Err(anyhow!(
                "Failed to describe the database {:?}, Error: {:?}",
                args.database_name,
                describe_db_error
            ));
        }
    }

    if let Err(describe_table_error) = client
        .describe_table()
        .database_name(&args.database_name)
        .table_name(&args.table_name)
        .send()
        .await
    {
        if describe_table_error
            .as_service_error()
            .map(|e| e.is_resource_not_found_exception())
            == Some(true)
        {
            // Sleep to avoid any errors shortly after table creation
            timestream_helper::create_table(&client, &args.database_name, &args.table_name).await?;
            for i in (0..30).rev() {
                print!("\rcountdown: {} to avoid any errors on table creation", i);
                std::io::stdout().flush()?;
                thread::sleep(Duration::from_millis(1000));
            }
            println!("\nBeginning ingestion of multi-measure records");
        } else {
            return Err(anyhow!(
                "Failed to describe the table {:?}, Error: {:?}",
                args.table_name,
                describe_table_error
            ));
        }
    }

    ingest_data(&client, &args).await?;
    Ok(())
}
