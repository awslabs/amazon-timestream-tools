use anyhow::{anyhow, Error, Result};
use aws_sdk_timestreamwrite as timestream_write;
use aws_sdk_timestreamwrite::operation::describe_table::DescribeTableOutput;
use clap::Parser;
use sample_timestream_app::utils::timestream_helper;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = timestream_helper::Args::parse();

    let client = timestream_helper::get_connection(&args.region)
        .await
        .expect("Failed to get connection to Timestream");

    match client
        .describe_table()
        .database_name(&args.database_name)
        .table_name(&args.table_name)
        .send()
        .await
    {
        Ok(describe_table_output) => {
            if let Some(s3_config) = get_magnetic_storage_configuration(&describe_table_output) {
                let bucket_name = s3_config
                    .bucket_name()
                    .expect("Failed to retrieve bucket name");
                match timestream_helper::delete_s3_bucket(bucket_name, &args.region).await {
                    Ok(_) => {
                        println!("Successfully deleted s3 bucket {:?} ", bucket_name)
                    }
                    Err(err) => println!(
                        "Failed to delete s3 bucket {:?}, err: {:?}",
                        bucket_name, err
                    ),
                }
            }

            client
                .delete_table()
                .database_name(&args.database_name)
                .table_name(&args.table_name)
                .send()
                .await?;
            println!("Successfully deleted table {:?}", args.table_name);
        }

        Err(describe_table_error) => {
            if describe_table_error
                .as_service_error()
                .map(|e| e.is_resource_not_found_exception())
                == Some(true)
            {
                println!(
                    "Skipping table deletion as the table {:?} does not exist",
                    args.table_name
                );
            } else {
                return Err(anyhow!(
                    "Failed to describe the table {:?}, Error: {:?}",
                    args.table_name,
                    describe_table_error
                ));
            }
        }
    }

    match client
        .describe_database()
        .database_name(&args.database_name)
        .send()
        .await
    {
        Ok(_) => match client
            .delete_database()
            .database_name(&args.database_name)
            .send()
            .await
        {
            Ok(_) => println!("Successfully deleted database {:?}", &args.database_name),
            Err(err) => println!(
                "Failed to delete database {:?}, err: {:?}",
                &args.database_name, err
            ),
        },
        Err(describe_database_error) => {
            if describe_database_error
                .as_service_error()
                .map(|e| e.is_resource_not_found_exception())
                == Some(true)
            {
                println!(
                    "Skipping database deletion as the database {:?} does not exist",
                    args.database_name
                );
            } else {
                return Err(anyhow!(
                    "Failed to describe the database {:?}, Error: {:?}",
                    args.database_name,
                    describe_database_error
                ));
            }
        }
    }

    Ok(())
}

fn get_magnetic_storage_configuration(
    describe_table_output: &DescribeTableOutput,
) -> Option<&timestream_write::types::S3Configuration> {
    let table = describe_table_output.table()?;
    let magnetic_store_write_properties = table.magnetic_store_write_properties()?;

    if magnetic_store_write_properties.enable_magnetic_store_writes() {
        let rejected_data_location =
            magnetic_store_write_properties.magnetic_store_rejected_data_location()?;
        return rejected_data_location.s3_configuration();
    }
    None
}
