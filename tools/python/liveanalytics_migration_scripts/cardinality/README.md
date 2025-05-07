# Cardinality Calculation Script

## Overview

[Cardinality](https://docs.influxdata.com/influxdb/v2/reference/glossary/#series-cardinality) in InfluxDB is the "number of unique measurement, tag set, and field key combinations in an InfluxDB bucket." When migrating from Timestream for LiveAnalytics, carefully select your InfluxDB instance specifications based on your dataset's cardinality as this directly impacts performance and resource requirements and consider migrating to a destination other than InfluxDB if your cardinality **exceeds ten million**.

Refer to [Timestream for InfluxDB's documentation on cardinality management](https://docs.aws.amazon.com/timestream/latest/developerguide/timestream-for-influxdb.html#timestream-for-influx-getting-started-security-best-practices) to understand how exceeding recommended limits can degrade query performance and increase memory consumption. Benchmark your anticipated query patterns against representative data samples before finalizing your instance selection to ensure your analytics remain performant post-migration, paying particular attention to memory-intensive aggregation queries that might behave differently than in Timestream.

## Calculating Cardinality

This script calculates the cardinality of a Timestream for LiveAnalytics table when mapped to Timestream for InfluxDB using the [Timestream for InfluxDB ingestion script](../targets/timestream_for_influxdb/ingestion/README.md). If the cardinality is under **ten million**, you can determine which Timestream for InfluxDB instance type to migrate to, otherwise how to adjust the schema to reduce cardinality, for example, by using the [Timestream for LiveAnalytics to line protocol translation script](../targets/timestream_for_influxdb/transform/README.md) with the `--dimensions-to-fields` argument to change particular dimensions to fields. Using the default schema mapping, cardinality is calculated by computing the total unique combinations of dimensions and measure name. The script executes the following query to do this:

```sql
SELECT 
    COUNT(
        DISTINCT(
            measure_name, dimension_name1, dimension_name2, 
            dimension_name3
        )
    ) AS cardinality
FROM 
    "database_name"."table_name"
```

### Low Cardinality Example

As an example of cardinality, consider the following Timestream for LiveAnalytics table, which has a cardinality of $`4`$.

| host  | region     | request_id       | measure_name | time                          | measure_value::double |
|-------|------------|------------------|--------------|-------------------------------|-----------------------|
| host1 | us-west-2  | saio3242ovnfk    | cpu_usage    | 2025-04-17 16:42:54.702394001 | 0.66                  |
| host2 | us-west-3  | 9213oijfnkalf    | cpu_usage    | 2025-04-17 16:42:54.702394002 | 0.70                  |
| host3 | us-west-4  | saklj213mhl33    | cpu_usage    | 2025-04-17 16:42:54.702394003 | 0.73                  |
| host4 | us-west-5  | 12b129flnbalf    | cpu_usage    | 2025-04-17 16:42:54.702394004 | 0.68                  |

The following is the number of possible values for each dimension, considering the data that exists in the table:
- `host`: $`4`$.
- `region`: $`4`$.
- `request_id`: $`4`$.

And for `measure_value`, there is only $`1`$ possible value, `cpu_usage`.

This gives us a total possible number of combinations of dimensions and the measure name as

```math
4 \cdot 4 \cdot 4 \cdot 1 = 64
```

However, this is not the cardinality since this is not the number of **actual** existing combinations. For the actual combinations in the table, each value of `host` is mapped to one region, since a server can only exist in one region, and each record has exactly $`1`$ unique `request_id`, meaning the actual number of combinations is

```math
4 \cdot 1 \cdot 1 \cdot 1 = 4
```

### Runaway Cardinality Example

As an example of runaway cardinality, where cardinality grows to a performance-impacting amount (over 7 million), consider the following partial Timestream for LiveAnalytics table, which has a cardinality of 20,000,000:

| host  | region    | request_id    | measure_name | time                          | measure_value::double |
|-------|-----------|---------------|--------------|-------------------------------|-----------------------|
| host1 | us-west-2 | saio3242ovnfk | cpu_usage    | 2025-04-17 16:42:54.702394001 | 0.66                  |
| host1 | us-west-2 | 9213oijfnkalf | cpu_usage    | 2025-04-17 16:42:54.702394002 | 0.70                  |
| host1 | us-west-2 | saklj213mhl33 | cpu_usage    | 2025-04-17 16:42:54.702394003 | 0.73                  |
| host1 | us-west-2 | 12b129flnbalf | cpu_usage    | 2025-04-17 16:42:54.702394004 | 0.68                  |
| . . .                                                                                                    |
| host1 | us-west-2 | 213oiasjfaosc | cpu_usage    | 2025-04-17 20:05:32.003108710 | 0.41                  |

This table is similar to the table above, except it has $`20,000,000`$ records from the same server (`host1`). In this case, each record has a unique `request_id` value. This unique value means that the total unique combinations of `host`, `region`, `request_id`, and `measure_name` is:

```math
1 \cdot 1 \cdot 20,000,000 \cdot 1 = 20,000,000
```

In this case, `request_id` should be changed to a field when migrating to Timestream for InfluxDB, after which the cardinality would be $`1`$.

If you decide to migrate to Timestream for InfluxDB and decide to translate any dimensions to InfluxDB fields, see [InfluxData's documentation for schema design best practices](https://docs.influxdata.com/influxdb/v2/write-data/best-practices/schema-design).

**NOTE:** in InfluxDB, fields are not indexed. Consider this before choosing to translate a dimension to a field during the migration process. If a dimension is often queried on, consider changing a different dimension to a field.

## Prerequisites

The following prerequisites must be met before running the script:
1. [AWS credentials configured for use with boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#using-a-configuration-file).
2. A Timestream for LiveAnalytics table [created](https://docs.aws.amazon.com/timestream/latest/developerguide/console_timestream.html#console_timestream.table.using-console) and loaded with data.
3. [Python 3.13 installed](https://www.python.org/downloads/).

## Installation

Optionally, a [Python virtual environment](https://docs.python.org/3/library/venv.html), with all packages in `requirements.txt` installed. The following command can be used to create a virtual environment, activate it, and install all necessary packages:
   ```shell
   python3 -m venv env && \
   source env/bin/activate && \
   python3 -m pip install -r requirements.txt
   ```

## Usage

### Options

`cardinality.py` provides the following options:

- `-h`, `--help`: Show this help message and exit.
- `--table-name TABLE_NAME`: The Timestream for LiveAnalytics table to determine the cardinality of.
- `--database-name DATABASE_NAME`: The Timestream for LiveAnalytics database that your table resides in.
- `--exclude-dimensions EXCLUDE_DIMENSIONS`: Optional. A list of dimension names to exclude from the cardinality calculation separated by commas. In a real-world scenario, changing Timestream for LiveAnalytics dimensions to InfluxDB fields rather than InfluxDB tags when translating Timestream for LiveAnalytics records to line protocol lowers the cardinality.

### Basic Usage

To determine the cardinality of a table, example_table, in the database example_database the script can be used in the following way:

```shell
python3 cardinality.py \
    --table-name example_table \
    --database-name example_database
```

This produces the following output:

```console
Cardinality of "example_database"."example_table": 160
Your recommended Timestream for InfluxDB type is: db.influx.medium
```

### Excluding Dimensions

If you plan to later change particular dimensions to fields, for example, `dimension_1` and `dimension_2`, using the Timestream for LiveAnalytics to Line Protocol Translation Script, detailed below, the script provides the `--exclude-dimensions` argument to calculate cardinality if these dimensions were fields. To do this, run the script in the following way:

```shell
python3 cardinality.py \
    --table-name example_table \
    --database-name example_database \
    --exclude-dimensions dimension_1,dimension_2
```

This produces the following output:

```console
Cardinality of "example_database"."example_table": 160
Your recommended Timestream for InfluxDB type is: db.influx.medium
Hypothetical cardinality of "example_database"."example_table" if the
dimensions dimension_1 and dimension_2 became fields: 16
Your hypothetical recommended Timestream for InfluxDB instance type is: db.influx.medium
```
