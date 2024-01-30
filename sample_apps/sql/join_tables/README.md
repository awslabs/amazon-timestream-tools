# Join tables in Amazon Timestream SQL query

## This example joins 2 tables
The goal for this join is that there are 2 tables
1. Sensor events for sensor measurements
2. Sensor status events and other details
The JOIN examples enriches the sensor events with data from the status event. Common use is to only filter sensor events for sensors that are active. The active/stopped informaiton is here stored in the second table of sensor details.

### 1. Sensor Event table
Contains measure events (temperature, humidity)

| Column | Type | Description |
|--------|------|-------------|
|gpio|Dimension| Sensor channel identifier |
|time|Timestamp| Time measure event was taken|
|measure_name|measure_name|Measure type|
|temperature|DOUBLE|Temperature in F|
|humidity|DOUBLE|Humidity in % |

### 2. Sensor Details table
Additional details about the sensor. These are time based change events

| Column | Type | Description                             |
|--------|------|-----------------------------------------|
|gpio|Dimension| Sensor channel identifier               |
|time|Timestamp| Time details were created/changed       |
|measure_name|measure_name| event type (initial_event/change_event) |
|name|VARCHAR|readable name of sensor|
|status|VARCHAR|active/stopped status of sensor|

## SQL query

Two SQL example statements are included in the file ```query_example_join_tables.sql```
1. Standard SQL JOIN to illustrate SQL compatibility
2. Performance optimized CTE JOIN

## Loading example data

The following commands is using the ```create_batch_load_task.py``` utility to load the example csv files used in this
query example. Please adjust as needed

###  1. Load sensor events data

```shell
cd <github-clone-root>/amazon-timestream-tools/sample_apps/sql/utils
```

```shell
python3 ./create_batch_load_task.py \
     region=<your_region> \
     mapping=../join_tables/datamodel_events.json \
     input_bucket=<s3-bucket-name> \
     object_key_prefix=<upload_folder> \
     data_file=../join_tables/sensor_events.csv \
     database=amazon-timestream-tools \
     table=sensor_events \
     partition_key=gpio
```

###  2. Load sensor details data

```shell
python3 ./create_batch_load_task.py \
     region=<your_region> \
     mapping=../join_tables/datamodel_details.json \
     input_bucket=<s3-bucket-name> \
     object_key_prefix=<upload_folder> \
     data_file=../join_tables/sensor_details.csv \
     database=amazon-timestream-tools \
     table=sensor_details \
     partition_key=gpio

```

| **⚠ Note**:                                                                                                                                                                                                                                                                                                                                          |
|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| It is recommended to use the database name `amazon-timestream-tools` and table names `sensor_events` and `sensor_details` as described below. The SQL statement uses `"amazon-timestream-tools"."sensorddata"` in WHERE clause and would not need to be modified. If you use a different database name and table combination, please adjust the SQL query as needed. |

Parameters used:

Parameter        | Description                                                                                                                            | Recommended Value
-----------------|----------------------------------------------------------------------------------------------------------------------------------------|-------------------
**region**       | Region where database is deployed                                                                                                      | any region where Timestream is available
**mapping**      | File used to map CSV data columns to Timestream attributes                                                                             | `../join_tables/datamodel_events.json` <br/> `../join_tables/datamodel_details.json`
**input_bucket** | S3 bucket used to upload data file and report Batch Load Status                                                                        | existing S3 bucket
**object_key**   | Folder where data file will be uploaded                                                                                                | any string, should not be root folder
**data_file**    | CSV file for this example                                                                                                              | `../join_tables/sensor_events.csv ` <br/> `../join_tables/sensor_details.csv`
**database**     | Database in region. Database will be created if not exists. | `amazon-timestream-tools`
**table**        | Table where data is loaded. If this table does not exist, the table will be created                                                    | `sensor_events` <br/>`sensor_details`
**partition_key**| Custome defined partition key (CDPK) used for this data example | `gpio`
