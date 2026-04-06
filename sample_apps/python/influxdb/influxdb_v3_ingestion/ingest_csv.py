# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import argparse
from pathlib import Path
from typing import Any
from pyarrow import Table
import os
from influxdb_client_3 import (
    InfluxDBClient3,
    write_client_options,
    WriteOptions,
)
from influxdb_client_3.write_client.client.write_api import WriteType

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="ingest_csv")
    parser.add_argument(
        "--file-path",
        required=False,
        default="./data/sample_data.csv",
        help=(
            "The path to the CSV file data "
            'to ingest. For example, "./data/sample_data.csv".'
        ),
    )
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
        default="csv_measurement",
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

    args = parser.parse_args()

    file_path: Path = Path(args.file_path)
    if not file_path.exists():
        print(f"File {args.file_path} does not exist. Exiting.")
        exit(1)
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

    with InfluxDBClient3(
        host=host,
        database=database_name,
        token=os.environ["INFLUX_TOKEN"],
        write_client_options=wco,
    ) as client:
        print("Ingesting CSV data")
        client.write_file(
            file=str(file_path.expanduser()),
            measurement_name=measurement_name,
            timestamp_column=timestamp_column,
            tag_columns=tag_columns,
        )

        query: str = f'SELECT * FROM "{measurement_name}"'
        print(f'Querying ingested data: "{query}"')
        table: Table = client.query(query)
        for row in table.to_pylist():
            print(row)
