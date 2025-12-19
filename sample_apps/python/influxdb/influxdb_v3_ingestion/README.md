# InfluxDB v3 Ingestion Samples

## Overview

The samples in this directory demonstrate how to use the [InfluxDB v3 Python client](https://github.com/InfluxCommunity/influxdb3-python) to write Parquet and CSV files to InfluxDB v3. Additionally, samples are included in which data is downloaded from S3 and ingested. These samples are demonstrative, and show how the client can be used, they are not full-featured tools.

If you are familiar with InfluxDB v3, you will notice (as of December 2025) that neither InfluxDB v3's [Core](https://docs.influxdata.com/influxdb3/core/api/v3/#operation/PostV2Write) nor [Enterprise](https://docs.influxdata.com/influxdb3/enterprise/api/v3/#operation/PostV2Write) APIs support writing CSV or Parquet data. CSV and Parquet data ingestion is accomplished using [InfluxDB v3 client libraries](https://docs.influxdata.com/influxdb3/cloud-dedicated/reference/client-libraries/v3/). The clients vary in their support of writing from file; these samples uses the [InfluxDB v3 Python client](https://docs.influxdata.com/influxdb3/cloud-dedicated/reference/client-libraries/v3/python/) as it provides many options for writing from file.

Be sure to view the contents of each sample. Writing data from file to InfluxDB v3 can be done with a few lines of code:

```python
with InfluxDBClient3(
    host=influxdb_v3_host
    database=influxdb_v3_database_name,
    token=influxdb_v3_token,
) as client:
    client.write_file(
        file="./data/sample_data.parquet",
        measurement_name="parquet_measurement",
        timestamp_column="timestamp_utc",
        tag_columns=["region", "meter_id", "project_id", "olc_id"],
    )
```
Documentation for `InfluxDBClient3.write_file` can be found [here](https://docs.influxdata.com/influxdb3/cloud-dedicated/reference/client-libraries/v3/python/#influxdbclient3write_file).

Much of the content of each sample includes callbacks. These callbacks are not used as data is ingested synchronously, but serve to show how callbacks could be implemented.

These samples build off of [InfluxData's guide for ingesting CSV](https://www.influxdata.com/blog/csv-data-influxdb-3/) and its [InfluxDB v3 Python client documentation](https://docs.influxdata.com/influxdb3/cloud-dedicated/reference/client-libraries/v3/python/).

## Prerequisites

Before running any of the samples, you must do the following:

1. [Create a Timestream for InfluxDB v3 instance](https://docs.aws.amazon.com/timestream/latest/developerguide/getting-started-with-timestream-for-influxdb-3.html) and retrieve its admin token.
2. [Download and install Python](https://www.python.org/downloads/).
3. [Create a database in Timestream for InfluxDB v3](https://docs.influxdata.com/influxdb3/core/reference/cli/influxdb3/create/database/).
3. The samples operate entirely with environment variables. Set the following environment variables:
   - `INFLUX_HOST`: The URL of your Timestream for InfluxDB v3 instance, including scheme (such as `https`) and port. For example, `"https://example.com:8181"`.
   - `INFLUX_TOKEN`: The admin token from your Timestream for InfluxDB v3 instance.
   - `INFLUX_DATABASE`: The name of the database that you created in your Timestream for InfluxDB v3 instance.
4. For the samples that use data from S3:

   a. [Create an S3 bucket](https://docs.aws.amazon.com/AmazonS3/latest/userguide/create-bucket-overview.html).

   b. Upload all files from `./data` to your bucket. You can use any path within the bucket. By default, the path `influxdb_v3_sample_data/` will be searched within the bucket. For example, CSV data would be searched for within the S3 bucket in `influxdb_v3_sample_data/sample_data.csv`.

   c. In addition to the above environment variables, set the following environment variables:
      - `S3_BUCKET_NAME`: The name of the S3 bucket you created.
      - `S3_OBJECT_KEY`: The object key for the sample data within your S3 bucket. This defaults to `influxdb_v3_sample_data/sample_data.csv` and `influxdb_v3_sample_data/sample_data.parquet` for the respective samples.
5. Create a Python virtual environment:
   ```shell
   python3 -m venv .env
   source .env/bin/activate
   ```
6. Download and install all Python dependencies:
   ```shell
   python3 -m pip install -r requirements.txt
   ```

## Run Samples

After meeting the above prerequisites, run one of the samples:

```shell
python3 ingest_csv.py
```

Data will be read from a file in the `./data/` directory (or possibly S3, if you run one of the samples that use S3), ingested to your Timestream for InfluxDB v3 instance, and the ingested data will be queried. If any data was downloaded, the data will be deleted from disk.
