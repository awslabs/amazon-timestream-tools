# Timestream for LiveAnalytics to Line Protocol Transformation Script

## Overview

The script in this directory converts [Amazon Timestream](https://aws.amazon.com/timestream/) for LiveAnalytics data into [line protocol](https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/), allowing ingestion to any database that supports line protocol.

Specifically, the script does the following:
- Loads exported Timestream for LiveAnalytics [data](https://docs.aws.amazon.com/timestream/latest/developerguide/API_Record.html) from an [Amazon S3](https://aws.amazon.com/s3/) bucket into an [Amazon Athena](https://aws.amazon.com/athena/) table.
- Transforms the data stored in the Athena table into line protocol and stores it in the S3 bucket.

This script assumes that the path `<Timestream database name>/<Timestream table name>/unload-<%Y-%m-%d-%H:%M:%S>/results` exists in your S3 bucket and contains data unloaded by the [unload script](../../../unload/README.md). Line protocol data will be exported to `<Timestream database name>/<Timestream table name>/unload-<%Y-%m-%d-%H:%M:%S>/line-protocol-output` in your S3 bucket.

## Data Mapping

The following table shows how Timestream for LiveAnalytics data is mapped to line protocol data.

| Timestream for LiveAnalytics Concept | Line Protocol Concept |
|--------------------------------------|-----------------------|
| [Table](https://docs.aws.amazon.com/timestream/latest/developerguide/API_Table.html)                                | [Measurement](https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/#measurement)           |
| [Dimensions](https://docs.aws.amazon.com/timestream/latest/developerguide/API_Dimension.html)                           | [Tags](https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/#tag-set)                  |
| [Measure name](https://docs.aws.amazon.com/timestream/latest/developerguide/data-modeling.html#data-modeling-measurenamemulti)                         | Tag                   |
| [Measures](https://docs.aws.amazon.com/timestream/latest/developerguide/API_MeasureValue.html)                             | [Fields](https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/#field-set)                |
| [Time](https://docs.aws.amazon.com/timestream/latest/developerguide/writes.html#writes.data-types)                                 | [Timestamp](https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/#timestamp)             |

### Single-Measure Record Transformation

The following is a single-measure record in Timestream for LiveAnalytics in the table `example_table`:

| host  | region     | request_id       | measure_name | time                          | measure_value::double |
|-------|------------|------------------|--------------|-------------------------------|-----------------------|
| host1 | us-west-2  | saio3242ovnfk    | cpu_usage    | 2025-04-17 16:42:54.702394001 | 0.66                  |

This record will be transformed to:

```
example_table,host=host1,region=us-west-2,request_id=saio3242ovnfk,measure_name=cpu_usage measure_value::double=0.66 1744933374702
```

### Multi-Measure Record Transformation

The following is a multi-measure record in Timestream for LiveAnalytics in the table `example_table` with everything to the right of `time` being measures:

| host  | region     | request_id       | measure_name | time                          | cpu_usage             | memory_usage |
|-------|------------|------------------|--------------|-------------------------------|-----------------------|--------------|
| host1 | us-west-2  | saio3242ovnfk    | metrics      | 2025-04-17 16:42:54.702394001 | 0.66                  | 0.21         |

This record will be transformed to:

```
example_table,host=host1,region=us-west-2,request_id=saio3242ovnfk,measure_name=metrics cpu_usage=0.66,memory_usage=0.21 1744933374702
```

## Prerequisites

The following prerequisites must be met before using the script:

1. A Timestream for LiveAnalytics table [created](https://docs.aws.amazon.com/timestream/latest/developerguide/console_timestream.html#console_timestream.table.using-console).
2. Data from the Timestream for LiveAnalytics table having been unloaded to an S3 bucket, within the path `<S3 bucket name>/<Timestream database name>/<Timestream table name>/unload-<%Y-%m-%d-%H:%M:%S>/results`.


## Installation

See [README.md#Installation](../../../README.md#installation).


## Usage

### Options

`transform.py` provides the following options:

- `-h`, `--help`: Show this help message and exit.
- `--tables TABLES`: Optional. A comma-separated list of Timestream for LiveAnalytics tables to transform.
- `--database-name DATABASE_NAME`: The Timestream for LiveAnalytics database that your table(s) resides in.
- `--all-tables`: Optional. Whether to transform all tables in the database.
- `--s3-bucket-path S3_BUCKET_PATH`: The S3 bucket path in which to load data from. This bucket must already exist. If this is an S3 bucket name or URI, for example, `s3://example_bucket`, then the path `s3://example_bucket/database_name/table_name/unload-latest-timestamp/results` will be used to load data.
- `--athena-database-name ATHENA_DATABASE_NAME`: Optional. The name of the Athena database to use when creating any new Athena tables. Defaults to "`default`".
- `--athena-table-name ATHENA_TABLE_NAME`: Optional. The name to use for a new Athena table, used for the transformation of LiveAnalytics records to line protocol. Defaults to the Timestream for LiveAnalytics database and table name connected with an underscore, without dashes.
- `--dimensions-to-fields DIMENSIONS_TO_FIELDS`: Optional. The tables and names of dimensions within to change to fields in resulting line protocol. Dimensions are usually mapped to tags. Mapping dimensions to fields can lower cardinality. The required format is `--dimensions-to-fields table1=dimension1,dimension2 --dimensions-to-fields table2=dimension3,dimension4`.
- `--add-validation-field BOOLEAN`: Whether to add an additional field to all transformed line protocol points to help with post-migration validation. The field will be `la_unload=1`.
- `--add-time-ns BOOLEAN`: Optional. Whether to add and use `time_ns` column during transformation for achieving nanosecond timestamp precision. Defaults to "`false`".

### Basic Usage

To transform data stored in the bucket, `example_s3_bucket` from the Timestream for LiveAnalytics table `example_table` in `example_database`, run the following command:
```shell
python3 transform.py \
    --database-name example_database \
    --tables example_table \
    --s3-bucket-path example_s3_bucket \
    --add-validation-field false
```

After the script has finished running:
- In Athena, the table `example_database_example_table` will be created, containing Timestream for LiveAnalytics data.
- In Athena, the table `lp_example_database_example_table` will be created, containing Timestream for LiveAnalytics data transformed to line protocol points.
    - **NOTE**: The name of this table is needed later if [validating ingested records](../validation/README.md).
- In the S3 bucket `example_s3_bucket`, within the path `example_database/example_table/unload-<%Y-%m-%d-%H:%M:%S>/line-protocol-output`, line protocol data will be stored.

### Multiple Tables

The `--tables` argument accepts any number of table names, where each named table belongs to the same database:

```shell
python3 transform.py \
    --database-name example_database \
    --tables example_table_1,example_table_2,example_table_3 \
    --s3-bucket-path example_s3_bucket \
    --add-validation-field false
```

### Using Dimensions as Fields

In Timestream for InfluxDB, [cardinality](https://docs.influxdata.com/influxdb/v2/reference/glossary/#series-cardinality) is the "number of unique measurement, tag set, and field key combinations in an InfluxDB bucket". By default, the script maps dimensions to tags. To reduce cardinality, dimensions can instead be mapped to fields. This should only be done if a dimension is not expected to be queried often, as fields are [not indexed](https://docs.influxdata.com/influxdb/v1/concepts/glossary/#field-value).

Dimensions belonging to a specific table can be changed to fields in the following way:
```shell
python3 transform.py \
    --database-name example_database \
    --tables example_table_1,example_table_2,example_table_3 \
    --s3-bucket-path example_s3_bucket \
    --dimensions-to-fields example_table1=dimension_1,dimension_2 \
    --dimensions-to-fields example_table2=dimension_3,dimension_4 \
    --add-validation-field true
```

The following is an example output for a transformed table with `hostname` and `region` dimensions converted to fields.
```
Tags:
    rack,service_environment,os,service,datacenter,arch,service_version,team,measure_name
Fields:
    hostname,region,usage_nice,usage_system,usage_irq,usage_guest,usage_user,usage_guest_nice,usage_idle,usage_steal,usage_iowait,usage_softirq,la_unload
```

### Adding a Field for Validation

To help validate that all data from a Timestream for LiveAnalytics table has been migrated to Timestream for InfluxDB, an additional field can be added to all line protocol points. Adding an additional field ensures each data point has a unique identifier, preventing InfluxDB's deduplication logic from merging or omitting migrated records during validation. This field is `la_unload=1`.

To verify data in Timestream for InfluxDB, the following Flux query can be used, replacing `<Timestream table name>` with the name of your Timestream for LiveAnalytics table:
```
from(bucket: "example_influxdb_bucket")
    |> range(start: 0)
    |> filter(fn: (r) => r._measurement == "<Timestream table name>")
    |> filter(fn: (r) => r._field == "la_unload")
    |> group()
    |> count()
```

Flux queries can be executed using the [Influx CLI](https://docs.influxdata.com/influxdb/v2/reference/cli/influx/) or in the [InfluxDB UI](https://docs.influxdata.com/influxdb/v2/query-data/execute-queries/data-explorer/).

The `la_unload` field can be added in the following way:

```shell
python3 transform.py \
    --database-name example_database \
    --tables example_table \
    --s3-bucket-path example_s3_bucket \
    --add-validation-field true
```

## Cleanup

After transforming Timestream for LiveAnalytics data to line protocol, three resources/artifacts will be created:
- An Athena table, containing Timestream for LiveAnalytics data. By default, this is `<Timestream database name>_<Timestream table name>` in the `default` Athena database.
- An Athena table, containing transformed line protocol data. By default, this is `lp_<Athena table name>` in the `default` Athena database.
- Line protocol data within your S3 bucket, with the path `<Timestream database name>/<Timestream table name>/line-protocol-output`.

To delete any Athena table, run the following [AWS CLI](https://aws.amazon.com/cli/) command, replacing `<Athena table name>` with the name of the table that you want to delete and `<Athena database name>` with the name of the Athena database that the table resides in:

```shell
aws glue delete-table \
    --database-name <Athena database name> \
    --name <Athena table name>
```

To delete line protocol data within your S3 bucket, run the following AWS CLI command, replacing `<S3 bucket name>` with the name of your S3 bucket, `<Timestream database name>` with the name of your Timestream for LiveAnalytics database, `<Timestream table name>` with the name of your Timestream for LiveAnalytics table, and `<timestamp>` with the timestamp that forms the `unload-<%Y-%m-%d-%H:%M:%S>` path in your S3 bucket:

```shell
aws s3 rm s3://<S3 bucket name>/<Timestream database name>/<Timestream table name>/unload-<timestamp>/line-protocol-output --recursive
```

Note that if [validation](../validation/README.md) is required, it is recommended to perform cleanup after the final validation stage.

## Limitations

The following limitations should be considered before transforming Timestream for LiveAnalytics records to line protocol:
- The finest timestamp precision that Athena supports is **milliseconds**. If you need greater timestamp precision, such as nanosecond precision, ensure your data is unloaded and transformed with `--add-time-ns` set to `true`.

## Troubleshooting

### Table Already Exists Error

As part of the transformation process, two Athena tables are created. By default, these are `<Timestream database name>_<Timestream table name>` and `lp_<Timestream database name>_<Timestream table name>`. If these tables already exist, for example, from a previous run of the transformation script, the following exception will be raised:

```
RuntimeError: Athena query failed with state: FAILED, error: Table <table name> already exists.
```

To solve this error, delete the table indicated in the error and try running the transformation script again. To delete a table in Athena, see the [Cleanup](#cleanup) section.
