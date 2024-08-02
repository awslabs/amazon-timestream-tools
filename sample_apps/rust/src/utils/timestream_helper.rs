use anyhow::Result;
use aws_sdk_timestreamwrite as timestream_write;
use aws_types::region::Region;
use clap::Parser;

const DEFAULT_DATABASE_NAME: &str = "devops_multi_sample_application";
const DEFAULT_REGION: &str = "us-east-1";
const DEFAULT_TABLE_NAME: &str = "host_metrics_sample_application";

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    // The Timestream for LiveAnalytics database name to use for all writes
    #[arg(short, long, default_value = DEFAULT_DATABASE_NAME)]
    pub database_name: String,

    // The name of the AWS region to use for writes
    #[arg(short, long, default_value = DEFAULT_REGION)]
    pub region: String,

    // The Timestream for LiveAnalytics table name to use for all writes
    #[arg(short, long, default_value = DEFAULT_TABLE_NAME)]
    pub table_name: String,
}

pub async fn get_connection(
    region: &str,
) -> Result<timestream_write::Client, timestream_write::Error> {
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new(region.to_owned()))
        .load()
        .await;
    let (client, reload) = timestream_write::Client::new(&config)
        .with_endpoint_discovery_enabled()
        .await
        .expect("Failed to get the write client connection with Timestream");

    tokio::task::spawn(reload.reload_task());
    Ok(client)
}

pub async fn create_database(
    client: &timestream_write::Client,
    database_name: &str,
) -> Result<(), timestream_write::Error> {
    println!("Creating new database {:?}", database_name);

    client
        .create_database()
        .set_database_name(Some(database_name.to_owned()))
        .send()
        .await?;

    Ok(())
}

pub async fn create_table(
    client: &timestream_write::Client,
    database_name: &str,
    table_name: &str,
) -> Result<(), timestream_write::Error> {
    println!("Creating new table {:?}", table_name);

    let retention_properties = timestream_write::types::RetentionProperties::builder()
        .set_magnetic_store_retention_period_in_days(Some(8000))
        .set_memory_store_retention_period_in_hours(Some(12))
        .build()?;

    let magnetic_store_properties =
        timestream_write::types::MagneticStoreWriteProperties::builder()
            .set_enable_magnetic_store_writes(Some(true))
            .build()?;

    client
        .create_table()
        .set_table_name(Some(table_name.to_owned()))
        .set_database_name(Some(database_name.to_owned()))
        .set_retention_properties(Some(retention_properties))
        .set_magnetic_store_write_properties(Some(magnetic_store_properties))
        .send()
        .await?;

    Ok(())
}

pub async fn delete_s3_bucket(bucket_name: &str, region: &str) -> Result<(), aws_sdk_s3::Error> {
    println!("Deleting s3 bucket {:?}", bucket_name);
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new(region.to_owned()))
        .load()
        .await;
    let client = aws_sdk_s3::Client::new(&config);

    for object in client
        .list_objects_v2()
        .set_bucket(Some(bucket_name.to_owned()))
        .send()
        .await?
        .contents()
    {
        client
            .delete_object()
            .set_bucket(Some(bucket_name.to_owned()))
            .set_key(Some(
                object.key().expect("Failed to get object key").to_owned(),
            ))
            .send()
            .await?;
    }

    let mut object_versions = client
        .list_object_versions()
        .set_bucket(Some(bucket_name.to_owned()))
        .send()
        .await?;

    loop {
        for delete_marker in object_versions.delete_markers() {
            client
                .delete_object()
                .set_bucket(Some(bucket_name.to_owned()))
                .set_key(Some(
                    delete_marker
                        .key()
                        .expect("Failed to get delete marker key")
                        .to_owned(),
                ))
                .send()
                .await?;
        }

        for version in object_versions.versions() {
            client
                .delete_object()
                .set_bucket(Some(bucket_name.to_owned()))
                .set_key(Some(
                    version.key().expect("Failed to get version key").to_owned(),
                ))
                .send()
                .await?;
        }

        if object_versions
            .is_truncated()
            .expect("Failed to get object versions is_truncated value")
        {
            object_versions = client
                .list_object_versions()
                .set_bucket(Some(bucket_name.to_owned()))
                .key_marker(
                    object_versions
                        .next_key_marker()
                        .expect("Failed to get next key marker"),
                )
                .version_id_marker(
                    object_versions
                        .next_version_id_marker()
                        .expect("Failed to get next version ID marker"),
                )
                .send()
                .await?;
        } else {
            break;
        }
    }

    client
        .delete_bucket()
        .set_bucket(Some(bucket_name.to_owned()))
        .send()
        .await?;

    Ok(())
}
