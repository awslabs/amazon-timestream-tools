from pyarrow import Table
import os
from influxdb_client_3 import (
    InfluxDBClient3,
    write_client_options,
    WriteOptions,
    InfluxDBError,
)
from influxdb_client_3.write_client.client.write_api import WriteType

# Define the result object
result = {"config": None, "status": None, "data": None, "error": None}


# Define callbacks for write responses
def success_callback(self, data: str):
    result["config"] = self
    result["status"] = "success"
    result["data"] = data

    assert result["data"] != None, f"Expected {result['data']}"
    # print(f"Successfully wrote data: {result['data'].decode("utf-8")}")
    print(f"Successfully wrote data")


def error_callback(self, data: str, exception: InfluxDBError):
    result["config"] = self
    result["status"] = "error"
    result["data"] = data
    result["error"] = exception

    assert result["status"] == "success", (
        f"Expected {result['error']} to be success for {result['config']}"
    )


def retry_callback(self, data: str, exception: InfluxDBError):
    result["config"] = self
    result["status"] = "retry_error"
    result["data"] = data
    result["error"] = exception

    assert result["status"] == "success", (
        f"Expected {result['status']} to be success for {result['config']}"
    )


write_options = WriteOptions(
    # write_type=WriteType.batching, # To make async calls and use callbacks.
    write_type=WriteType.synchronous,
    batch_size=5000,
    flush_interval=10_000,
    jitter_interval=2_000,
    retry_interval=5_000,
    max_retries=5,
    max_retry_delay=30_000,
    exponential_base=2,
)


wco = write_client_options(
    success_callback=success_callback,  # Callbacks won't be used with write_type = synchronous.
    error_callback=error_callback,
    retry_callback=retry_callback,
    write_options=write_options,
)


with InfluxDBClient3(
    host=os.environ["INFLUX_HOST"],
    database=os.environ["INFLUX_DATABASE"],
    token=os.environ["INFLUX_TOKEN"],
    write_client_options=wco,
) as client:
    client.write_file(
        file="./data/sample_data.csv",
        measurement_name="csv_measurement",
        timestamp_column="timestamp_utc",
        tag_columns=["region", "meter_id", "project_id", "olc_id"],
    )

    query: str = "SELECT * FROM csv_measurement"
    print(f'Querying ingested data: "{query}"')
    table: Table = client.query(query)
    for row in table.to_pylist():
        print(row)
