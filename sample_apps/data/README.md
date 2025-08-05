# Sample Data

This folder contains sample data to help you get started with Timestream.

## Datasets

### Lap Times

[`lap_times.lp`](./lap_times.lp) contains line protocol data of lap times from the [2012 British Grand Prix](https://en.wikipedia.org/wiki/2012_British_Grand_Prix). This data was assembled from the [Formula 1 World Championship (1950 - 2024) Kaggle dataset](https://www.kaggle.com/datasets/rohanrao/formula-1-world-championship-1950-2020/data?select=lap_times.csv) and includes calculated timestamps for each lap.

To ingest this dataset into Timestream for InfluxDB, run the following command, using the [Influx CLI](https://docs.influxdata.com/influxdb/v2/reference/cli/influx/):

```
influx write \
    --host https://<endpoint>:<port> \
    -t <token> \
    --org <org name> \
    --bucket <bucket name> \
    --format lp \
    --file lap_times.lp \
    --precision s
```

Where:
- `<endpoint>` is your Timestream for InfluxDB endpoint.
- `<port>` is the port your Timestream for InfluxDB instance uses. The default port for InfluxDB v2 is `8086`.
- `<token>` is a token with write permission for your bucket.
- `<org name>` is the name of the organization that your bucket resides in.
- `<bucket name>` is the name of an existent bucket that you want to ingest data to.

To view your ingested data, run the following Flux query:

```
from(bucket: "<bucket name>")
    |> range(start: 2012-07-08T11:00:00Z)
```

This query can be run in [InfluxDB's data explorer](https://docs.influxdata.com/influxdb/v2/query-data/execute-queries/data-explorer/) or with the Influx CLI:

```
influx query \
    --host https://<endpoint>:<port> \
    -t <token> \
    --org <org name> \
    "from(bucket: \"<bucket name>\") \
        |> range(start: 2012-07-08T11:00:00Z)"
```

### Free Zip Code Database Primary

[`free-zipcode-database-Primary.csv`](./free-zipcode-database-Primary.csv) contains time-series data for various US zip codes with estimated population and the number of tax returns filed.

### Sample Unload

[`sample_unload.csv`](./sample_unload.csv) contains e-commerce time-series data and is used primarily in sample applications that insert data into Timestream for LiveAnalytics and then unload data into S3. For example, this data can be used with [`../js/main.js`](../js/main.js):

```shell
node main.js --type unload --csvFilePath=../data/sample_unload.csv
```

### Sample Multi

[`sample-multi.csv`](./sample-multi.csv) contains DevOps multi-measure Timestream for LiveAnalytics data.

This data can be ingested using the Python sample application, in [`../python/live_analytics/basic_example/SampleApplication.py`](../python/live_analytics/basic_example/SampleApplication.py):

```python
python SampleApplication.py \
--type csv \
--csv_file_path ../../../data/sample-multi.csv \
--region us-east-2 \
--skip_deletion false
```

### Sample

[`sample.csv`](./sample.csv) contains DevOps single-measure Timestream for LiveAnalytics data.

This data can be ingested using the Python sample application, in [`../python/live_analytics/basic_example/SampleApplication.py`](../python/live_analytics/basic_example/SampleApplication.py):

```python
python SampleApplication.py \
--type csv \
--csv_file_path ../../../data/sample.csv \
--region us-east-2 \
--skip_deletion false
```

### User Data

[`user_data.csv`](./user_data.csv) contains mock user data, including first and last names, ZIP code, job, and age.
