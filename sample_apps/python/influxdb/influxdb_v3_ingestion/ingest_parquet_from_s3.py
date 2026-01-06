import argparse
from typing import Any
import boto3
from pyarrow import Table
from mypy_boto3_s3 import S3Client
import os
from influxdb_client_3 import (
    InfluxDBClient3,
    write_client_options,
    WriteOptions,
)
from influxdb_client_3.write_client.client.write_api import WriteType

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="ingest_parquet_from_s3")
    parser.add_argument(
        "--host",
        required=True,
        help=(
            "The InfluxDB v3 host to ingest data "
            'to, including scheme and port. For example, "https://example.com:8181"'
        ),
    )
    parser.add_argument(
        "--database-name",
        required=True,
        help=(
            "The name of a database in your InfluxDB v3 instance to ingest data into. If the database does not exist, it will be created."
        ),
    )
    parser.add_argument(
        "--measurement-name",
        default="parquet_measurement",
        required=False,
        help="The name to use as the measurment. In InfluxDB v3, this becomes the table name.",
    )
    parser.add_argument(
        "--timestamp-column",
        default="timestamp_utc",
        required=False,
        help="The name of the column to use as the time column in the data file.",
    )
    parser.add_argument(
        "--tag-columns",
        nargs="+",
        default=["region", "meter_id", "project_id", "olc_id"],
        required=False,
        help='The names of the columns to use as tags in the data file, as a list. For example, --tag-columns "region" "meter_id".',
    )
    parser.add_argument(
        "--s3-bucket-name",
        required=True,
        help="The name of the S3 bucket that holds Parquet data.",
    )
    parser.add_argument(
        "--s3-object-key",
        default="influxdb_v3_sample_data/sample_data.parquet",
        required=False,
        help='The object key of the Parquet data file within the S3 bucket. For example, "influxdb_v3_sample_data/sample_data.parquet".',
    )

    args = parser.parse_args()

    host: str = args.host
    database_name: str = args.database_name
    measurement_name: str = args.measurement_name
    timestamp_column: str = args.timestamp_column
    tag_columns: list[str] = args.tag_columns

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

    # Download Parquet data from S3.
    session: boto3.Session = boto3.Session()
    s3_client: S3Client = session.client("s3")

    s3_bucket_name: str = args.s3_bucket_name
    s3_object_key: str = args.s3_object_key
    local_file_path: str = "./data/downloaded_sample_data.parquet"

    try:
        s3_client.download_file(s3_bucket_name, s3_object_key, local_file_path)
        print(f"Downloaded {s3_object_key} to location {local_file_path}")
    except Exception as e:
        print(f"Error downloading {s3_object_key} from {s3_bucket_name}: {e}")
        exit(1)

    with InfluxDBClient3(
        host=host,
        database=database_name,
        token=os.environ["INFLUX_TOKEN"],
        write_client_options=wco,
    ) as client:
        print("Ingesting Parquet data")
        client.write_file(
            file=local_file_path,
            measurement_name=measurement_name,
            timestamp_column=timestamp_column,
            tag_columns=tag_columns,
        )

        query: str = f'SELECT * FROM "{measurement_name}"'
        print(f'Querying ingested data: "{query}"')
        table: Table = client.query(query)
        for row in table.to_pylist():
            print(row)

    # Delete the downloaded Parquet data.
    os.remove(local_file_path)
    print(f"Deleted {local_file_path}")
