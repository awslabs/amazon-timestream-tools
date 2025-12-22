from typing import Any
from pyarrow import Table
import os
from influxdb_client_3 import InfluxDBClient3, write_client_options, WriteOptions
from influxdb_client_3.write_client.client.write_api import WriteType

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


wco: dit[str, Any] = write_client_options(
    write_options=write_options,
)


with InfluxDBClient3(
    host=os.environ["INFLUX_HOST"],
    database=os.environ["INFLUX_DATABASE"],
    token=os.environ["INFLUX_TOKEN"],
    write_client_options=wco,
) as client:
    print("Ingesting Parquet data")
    client.write_file(
        file="./data/sample_data.parquet",
        measurement_name="parquet_measurement",
        timestamp_column="timestamp_utc",
        tag_columns=["region", "meter_id", "project_id", "olc_id"],
    )

    query: str = "SELECT * FROM parquet_measurement"
    print(f'Querying ingested data: "{query}"')
    table: Table = client.query(query)
    for row in table.to_pylist():
        print(row)
