# InfluxDB v3 Ingestion Samples

## Overview

The samples in this directory demonstrate how to use the [InfluxDB v3 Python client](https://github.com/InfluxCommunity/influxdb3-python) to write Parquet and CSV files to InfluxDB v3. Additionally, samples are included in which data is downloaded from S3 and ingested. These samples are demonstrative, and show how the client can be used, they are not full-featured tools.

If you are familiar with InfluxDB v3, you will notice (as of December 2025) that neither InfluxDB v3's [Core](https://docs.influxdata.com/influxdb3/core/api/v3/#operation/PostV2Write) nor [Enterprise](https://docs.influxdata.com/influxdb3/enterprise/api/v3/#operation/PostV2Write) HTTP APIs support writing CSV or Parquet data. CSV and Parquet data ingestion is accomplished using [InfluxDB v3 client libraries](https://docs.influxdata.com/influxdb3/cloud-dedicated/reference/client-libraries/v3/). The clients vary in their support of writing from file; these samples use the [InfluxDB v3 Python client](https://docs.influxdata.com/influxdb3/cloud-dedicated/reference/client-libraries/v3/python/) as it provides many options for writing from file.

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

These samples build off of [InfluxData's guide for ingesting CSV](https://www.influxdata.com/blog/csv-data-influxdb-3/) and its [InfluxDB v3 Python client documentation](https://docs.influxdata.com/influxdb3/cloud-dedicated/reference/client-libraries/v3/python/). The samples in this directory ingest synchronously. The InfluxDB v3 Python client documentation shows how asynchronous ingestion using callbacks can be accomplished.

## Prerequisites

Before running any of the samples, you must do the following:

1. [Create a Timestream for InfluxDB v3 instance](https://docs.aws.amazon.com/timestream/latest/developerguide/getting-started-with-timestream-for-influxdb-3.html) and retrieve its admin token.
2. [Download and install Python](https://www.python.org/downloads/).
3. Set the following environment variable:
   - `INFLUX_TOKEN`: The admin token from your Timestream for InfluxDB v3 instance.
4. For the samples that use data from S3:

   a. [Create an S3 bucket](https://docs.aws.amazon.com/AmazonS3/latest/userguide/create-bucket-overview.html). This can be done using the [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html):
      ```shell
      aws s3api create-bucket --bucket <bucket name> --region us-west-2 --create-bucket-configuration LocationConstraint=us-west-2
      ```

   b. Upload all files from `./data` to your bucket. You can use any path within the bucket. By default, the path `influxdb_v3_sample_data/` will be searched within the bucket. For example, CSV data would be searched for within the S3 bucket in `influxdb_v3_sample_data/sample_data.csv`. This can be done with the AWS CLI:
      ```shell
      # Uploading a single file.
      aws s3 cp ./data/sample_data.csv s3://<bucket name>/influxdb_v3_sample_data/

      # Bash, uploading all files in the ./data/ directory.
      for f in ./data/*; do aws s3 cp $f s3://<bucket name>/influxdb_v3_sample_data; done
      ```

5. Create a Python virtual environment:
   ```shell
   python3 -m venv .env
   source .env/bin/activate
   ```
6. Download and install all Python dependencies:
   ```shell
   python3 -m pip install -r requirements.txt
   ```

## Sample Data

A few sample data files are provided in `./data/`. Additionally, the Python script `generate_csv_file.py` allows you to generate a CSV file with a few configurable options, such as setting column names and the timestamp start time.

`generate_csv_file.py` offers the following command-line arguments:
- `--output-path`: The path to place the generated CSV file. For example, `./data/generated_data.csv`.
- `--timestamp-column`: The name of the column to use as the time column in the data file. Defaults to `timestamp_utc`.
- `--tag-columns`: The names of the columns to use as tags in the data file, as a list. For example, `--tag-columns region meter_id`. Values will be random strings.
- `--field-columns`: The names of the columns to use as fields in the data file, as a list. For example, `--field-columns region meter_id`. Values will be random integers.
- `--num-rows`: The number of rows to generate.
- `--start-time`: The time to use as the initial generation point as an RFC 3339 timestamp. Defaults to 24 hours ago. For example, '2026-01-01T00:00:00Z' for UTC or '2026-01-01T00:00:00-08:00' for PST.
- `--time-increment`: The amount of time to increment between records. Defaults to one minute (`1m`). Supported time formats are hr, m, and s.

If you want to create a Parquet file from your generated CSV file, the script `csv_to_parquet.py` allows you to do this. It offers the following command-line argument:
- `--csv-file`: The path to the CSV file to transform to Parquet. The new Parquet file will be created in the same directory.

## Running Samples

After meeting the above prerequisites, run one of the samples, providing the following command-line arguments:
- `--host`: The URL of your Timestream for InfluxDB v3 instance, including scheme (such as `https`) and port. For example, `"https://example.com:8181"`.
- `--database-name`: The name of the database that you want to ingest data into in your Timestream for InfluxDB v3 instance. If this database does not exist, it will be created.
- `--measurement-name`: Optional. The name to use as the measurment. In InfluxDB v3, this becomes the table name.
- `--timestamp-column`: The name of the column to use as the time column in the data file. Defaults to `timestamp_utc`.
- `--tag-columns`: The names of the columns to use as tags in the data file, as a list. For example, `--tag-columns region meter_id`.
- For samples that do not use an S3 bucket, provide:
   - `--file-path`: The path to the file to ingest. Defaults to a file in `./data/`, either CSV or Parquet depending on the sample.
- For samples that use S3 buckets, provide:
   - `--s3-bucket-name`: The name of the S3 bucket you created.
   - `--s3-object-key`: The object key for the sample data within your S3 bucket. This defaults to `influxdb_v3_sample_data/sample_data.csv` and `influxdb_v3_sample_data/sample_data.parquet` for the respective samples.

```shell
python3 ingest_csv.py --host <host> --database-name <database name>
```

Data will be read from a file (or possibly S3, if you run one of the samples that use S3), ingested to your Timestream for InfluxDB v3 instance, and then the ingested data will be queried. If any data was downloaded, the data will be deleted from disk.
