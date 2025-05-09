# Migration Validation Script

The migration validation script compares **logical row/point counts** between a source table (Amazon Timestream or Amazon Athena) and an InfluxDB bucket measurement, with optional time-range specifications. This tool helps ensure data integrity during migration processes by running parallel queries against both systems and comparing the results.

## Overview

The migration from Timestream for LiveAnalytics to Timestream for InfluxDB involves three stages:

1. Timestream for LiveAnalytics -> Unload to S3 [[#README]](../../../unload/README.md)
2. Load from S3 into Athena -> Transform to line protocol (LP)  [[#README]](../transform/README.md)
3. Download transformed dataset and ingest to Timestream for InfluxDB  [[#README]](../ingestion/README.md)

The validation script supports queries against either the exported dataset in Athena or the original Timestream database/table. Be aware that querying Timestream directly may lead to inaccurate comparisons if additional data has been written since the export.

The validation script can be run anytime after ingestion has begun. The script first polls InfluxDB's [metrics endpoint](https://docs.influxdata.com/influxdb/v2/reference/internals/metrics/) to wait for the [WAL](https://docs.influxdata.com/influxdb/v2/reference/internals/storage-engine/#write-ahead-log-wal) to flush completely, accounting for post-ingestion [data file merging and de-duplication](https://www.influxdata.com/blog/compactor-hidden-engine-database-performance/#:~:text=loading%20and%20reading.-,Tasks%20of%20post%2Dingestion%20and%20pre%2Dquery,delete%20application%2C%20and%20data%20deduplication.). The script then:

- Executes count‑only queries over an identical time window  
- Compares results and highlights matches or mismatches  
- Supports optional schema/tag filtering for [transformed schemas](../transform/README.md### Using Dimensions as Fields) 
- Produces human‑readable timing and result summaries  


## Prerequisites

1. Complete the previous migration stages as highlighted above. The validation script will exit early if there are no points ingested to the target InfluxDB instance.
2. [AWS credentials configured for use with boto3.](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#using-a-configuration-file)
3. [An InfluxDB access token](https://docs.influxdata.com/influxdb/cloud/admin/tokens/create-token/) for the target Timestream for InfluxDB instance.
3. One of:
    - Python 3.8+
    - Docker

## Installation

1. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate
```

2. Install the required dependencies:
```bash
pip install -r requirements.txt
```

#### With Docker

Build the container with 
```bash
make build
```

## Usage

```
python validator.py [options]
```

All settings can be supplied as CLI flags **or** environment variables. See [example.env](../example.env) for reference.

### Required arguments

- `--source-engine` / `SOURCE_ENGINE` – Data source engine: `athena` *(default)* or `timestream`  
- **Athena‑specific**  
  - `--athena-database-name` / `ATHENA_DATABASE_NAME` – Database name  
  - `--athena-table-name` / `ATHENA_TABLE_NAME` – Table name  
  - `--athena-output` / `ATHENA_OUTPUT` – S3 output path for query results  
- **Timestream‑specific**  
  - `--timestream-database-name` / `TIMESTREAM_DATABASE_NAME` – Database name  
  - `--timestream-table-name` / `TIMESTREAM_TABLE_NAME` – Table name  
- **InfluxDB**  
  - `--influxdb-v2-url` / `INFLUXDB_V2_URL` – e.g. `https://example.com:8086`  
  - `--influxdb-v2-token` / `INFLUXDB_V2_TOKEN` – API token with read scope  
  - `--influxdb-v2-org` / `INFLUXDB_V2_ORG` – Organisation  
  - `--influxdb-v2-bucket` / `INFLUXDB_V2_BUCKET` – Bucket  
  - `--influxdb-v2-measurement` / `INFLUXDB_V2_MEASUREMENT` – Measurement to validate  

### Optional arguments

- `--schema-tags` / `SCHEMA_TAGS` – Comma‑separated dimension/tag list  
- `--start-time` / `START_TIME` – Inclusive lower ISO‑8601 bound (e.g., `2024-08-01T00:00:00Z`) 
- `--end-time` / `END_TIME` – Exclusive upper ISO‑8601 bound (e.g., `2024-08-04T00:00:00Z`) 
- `--poll-metrics-interval` / `POLL_METRICS_INTERVAL` – Seconds between `/metrics` polls *(default: 30)*  
- `--skip-wal-check` / `SKIP_WAL_CHECK` – Skip WAL‑flush wait *(default: false)*  
- `--influx-only` / `INFLUX_ONLY` – Skip source query; return Influx count only *(default: false)*  

#### With Docker

For full validation:
```bash
make validate
```

To query only InfluxDB:
```bash
make influx_only
```

To track ingestion (skips WAL check & queries only InfluxDB)
```bash
make track_influx
```

## Use Cases

| Intent | Command |
|--------|---------|
| Validate **entire** migration of `benchmark3.cpu` | `python validator.py` |
| Validate migration for a **date partition** | `python validator.py --start-time 2025-01-01T00:00:00Z --end-time 2025-02-01T00:00:00Z` |
| Validate migration against **Timestream** | `python validator.py --source-engine timestream` |
| Validate migration for a **transformed table** | `python validator.py --schema-tags=service_environment,os,arch,service_version,team,region`<br/><br/>If changes were made to the original table schema during the transformation to Line Protocol (i.e., using the [`--dimensions-to-fields` flag](../transform/README.md#using-dimensions-as-fields)), pass the full list of tags from the new table schema to `--schema-tags` as a comma-separated list.|
| Track an **ongoing migration** | `python validator.py --influx-only --skip-wal-check`<br/><br/>During active ingestion, the validation script allows you to check progress by displaying the current record count in InfluxDB without querying the source engine. This provides visibility into raw data ingestion but excludes any records that may still be undergoing post-ingestion processing.


## Output Examples

### Success Output
```
--------------------
Starting validation
--------------------

Polling https://xxx-yyy.timestream-influxdb.us-west-2.on.aws:8086/metrics to wait for WAL to complete flushing ... 

2025-05-06 20:45:59  WAL empty on all shards — ready for validation.

Starting validation ...

--- InfluxDB ---
Total LP points in bucket-big.cpu (2024-01-01T00:00:00Z – 2025-02-01T00:00:00Z): 975661

--- Athena ---
Total records in default.cpu_usage (2024-01-01T00:00:00Z – 2025-02-01T00:00:00Z): 975661

⏱ Athena query time: 8.62 s
⏱ InfluxDB query time:   0.18 s

--------- Migration Results ---------

🎉  Athena and InfluxDB row counts match.
```

### Mismatch Output
```
--------------------
Starting validation
--------------------

Polling https://zzz-yyy.timestream-influxdb.us-west-2.on.aws:8086/metrics to wait for WAL to complete flushing ... 

2025-05-06 20:45:59  WAL empty on all shards — ready for validation.

Starting validation ...

--- InfluxDB ---
Total LP points in bucket3.cpu (2025-03-01T00:00:00Z – 2025-03-02T00:00:00Z): 10634255

--- Athena ---
Total records in default.cpu_usage (2025-03-01T00:00:00Z – 2025-03-02T00:00:00Z): 11180160

⏱ Athena query time: 12.88 s
⏱ InfluxDB query time:   0.68 s

--------- Migration Results ---------

⚠️  Athena (11180160) > InfluxDB (10634255)
```

## Troubleshooting & Tips

- When ingesting large amounts of data to InfluxDB, [post-ingestion processing and compaction](https://www.influxdata.com/blog/compactor-hidden-engine-database-performance/#:~:text=loading%20and%20reading.-,Tasks%20of%20post%2Dingestion%20and%20pre%2Dquery,delete%20application%2C%20and%20data%20deduplication.) in the InfluxDB [storage engine](https://docs.influxdata.com/influxdb/v2/reference/internals/storage-engine/) can take some time. To skip waiting for the WAL to flush, run with `--skip-wal-check` to retrieve the total count without accounting for post-ingestion processing.

- If data is still being written to InfluxDB at time of validation (i.e., simultaneous migrations to the same InfluxDB instance in different buckets or measurements), the WAL will remain non-empty until it has fully processed all ingested points. Ensure that ingestion is fully complete before running comparisons between a target database/table and bucket/measurement, or use the `--skip-wal-check` flag.

- If the total row count exported from Timestream is known at time of validation (i.e., unloaded with [DynamoDB logging enabled on export to S3](../../../unload/README.md#export-with-dynamodb-logging-enabled)), avoid querying the source engine using the `--influx-only` flag.

- For large datasets, consider using time range filters to validate in smaller chunks.
- When validating transformed tables (with converted dimensions as fields), ensure you specify all schema tags for accurate comparison. See output of [the transformation script](../transform/README.md#using-dimensions-as-fields) for the full list of tags.
