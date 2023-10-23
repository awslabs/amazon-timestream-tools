# Gap filling with last value

## Query uses multiple steps

1. create time sequence (not limited to just 10,000 data points)
2. select raw data binned at same intervals
3. join time sequence with raw data as data set that contains NULL values now
4. use LAST_VALUE in filled dataset, this query lists justs 2 measures: orignal temperature that can contain NULL and the filled column

The result set shows both original value containing NULL and the filled value in a separate column

```shell
cd <github-clone-root>/amazon-timestream-tools/sample_apps/sql/utils
python ../create_batch_load_task.py \
     region=<your_region> \
     mapping=../last_value_fill_forward/datamodel.json \
     input_bucket=<s3-bucket-name> \
     object_key_prefix=<upload_folder> \
     data_file=../last_value_fill_forward/sensor_with_gaps.csv \
     database=amazon-timestream-tools \
     table=sensordata
```

:::alert[Important]{type="info"}
It is recommended to use the database name `amazon-timestream-tools` and table name `sensordata` as described below.
The SQL statement uses `"amazon-timestream-tools"."sensorddata"` in WHERE clause and would not need to be modified.
If you use a different database name and table combination, please adjust the SQL query as needed.
:::

Parameters used:

Parameter        | Description | Recommended Value
-----------------|-------------|-------------------
**region**       | Region where database is deployed | any region where Timestream is available
**mapping**      | File used to map CSV data columns to Timestream attributes | `../last_value_fill_forward/datamodel.json`
**input_bucket** | S3 bucket used to upload data file and report Batch Load Status | existing S3 bucket
**object_key**   | Folder where data file will be uploaded | any string, should not be root folder
**data_file**    | CSV file for this example | `sensor_with_gaps.csv`
**database**     | Existing database in region. | `amazon-timestream-tools`
**table**        | Table where data is loaded. If this table does not exist, the table will be created | `sensordata`
