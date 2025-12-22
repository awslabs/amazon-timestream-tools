from typing import Any
import boto3
from influxdb_client_3.write_client.client.write_api import WriteType
from mypy_boto3_s3 import S3Client
from pyarrow import Table
import os
from influxdb_client_3 import (
    InfluxDBClient3,
    write_client_options,
    WriteOptions,
)

write_options: WriteOptions = WriteOptions(
    write_type=WriteType.synchronous,
    batch_size=5000,
    flush_interval=10_000,
    jitter_interval=2_000,
    retry_interval=5_000,
    max_retries=5,
    max_retry_delay=30_000,
    exponential_base=2,
)


wco: dict[str, Any] = write_client_options(
    write_options=write_options,
)

# Download CSV data from S3.
session: boto3.Session = boto3.Session()
s3_client: S3Client = session.client("s3")

s3_bucket_name: str = os.environ["S3_BUCKET_NAME"]
s3_object_key: str = os.environ.get(
    "S3_OBJECT_KEY", "influxdb_v3_sample_data/sample_data.csv"
)
local_file_path: str = "./data/downloaded_sample_data.csv"

try:
    s3_client.download_file(s3_bucket_name, s3_object_key, local_file_path)
    print(f"Downloaded {s3_object_key} to location {local_file_path}")
except Exception as e:
    print(f"Error downloading {s3_object_key} from {s3_bucket_name}")
    exit(1)

with InfluxDBClient3(
    host=os.environ["INFLUX_HOST"],
    database=os.environ["INFLUX_DATABASE"],
    token=os.environ["INFLUX_TOKEN"],
    write_client_options=wco,
) as client:
    print("Ingesting CSV data")
    client.write_file(
        file="./data/downloaded_sample_data.csv",
        measurement_name="csv_measurement",
        timestamp_column="timestamp_utc",
        tag_columns=["region", "meter_id", "project_id", "olc_id"],
    )

    query: str = "SELECT * FROM csv_measurement"
    print(f'Querying ingested data: "{query}"')
    table: Table = client.query(query)
    for row in table.to_pylist():
        print(row)

# Delete the downloaded CSV data.
os.remove(local_file_path)
print(f"Deleted {local_file_path}")
