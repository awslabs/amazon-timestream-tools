# Ingest to Timestream for InfluxDB

## Overview

Migration tooling for Timestream for InfluxDB allows you to easily transform your Timestream for LiveAnalytics data and ingest it to a Timestream for InfluxDB instance. Prior to performing the migration, ensure you have completed a cardinality assessment of the transformed schema to ensure Timestream for InfluxDB is a suitable migration target. See [../../cardinality/README.md](../../cardinality/README.md) for how to perform the cardinality assessment, and any potential schema alterations before starting the transformation process.

Migrating to Timestream for InfluxDB takes multiple steps as the data models differ in how the data model is represented. See the Influx documentation for [getting started](https://docs.influxdata.com/influxdb/v2/get-started/) with InfluxDB V2 for an overview of the database key concepts.

The workflow for completing a migration is separated into four stages:
- [**Unload**](../../unload/README.md): Export your Timestream for LiveAnalytics dataset to S3
- [**Data transformation**](./transform/README.md): Convert your Timestream for LiveAnalytics data to line protocol format (Based on the schema defined after the cardinality assessment)
- [**Data ingestion**](./ingestion/README.md): Ingest the line protocol dataset to your Timestream for InfluxDB instance
- [**Validation**](./validation/README.md): Optionally you can validate that every line protocol point has been ingested (Requires `--add-validation-field true` during transformation)

See [End to end migration](#end-to-end-migration) for additional details on each step, and the migration diagram below for a visual workflow.

```mermaid
stateDiagram-v2
direction LR
    unload --> dataTransformation: Success
    dataTransformation--> dataIngestion: Success
    dataIngestion --> validation: Success
    state "Step: Unload data" as unload
    state unload {
        InfluxDB: Source Database
        SingleTable: Single Table
        AllTables: All Tables in Database
        AllDatabases: All Databases
        InfluxDB --> SingleTable: Option 1
        InfluxDB --> AllTables: Option 2
        InfluxDB --> AllDatabases: Option 3
    }
    state "Step: Data transformation" as dataTransformation
    state  dataTransformation {
        SourceData: Source Data
        SchemaMapping: Apply Schema Mapping
        AddMetadata: Add la_unload=1
        StoreInS3: Store in S3
        SourceData --> SchemaMapping: Transform
        SchemaMapping --> AddMetadata: Add metadata field
        AddMetadata --> StoreInS3
    }
    state "Step: Data ingestion" as dataIngestion
    state dataIngestion {
        UploadScript: Upload Ingestion Script to EC2
        DataTransformationS3Sync: S3 Sync Line Protocol Dataset
        ExecuteScript: Execute Ingestion Script
        UploadScript --> DataTransformationS3Sync: Script Ready
        DataTransformationS3Sync --> ExecuteScript: Data Ready
    }
    state "Step: Validation" as validation
    state validation {
        QuerySource: Query Original Database(s)
        QueryDestination: Query Ingestion Destination
        CompareResults: Compare Results
        QuerySource --> CompareResults
        QueryDestination --> CompareResults
    }
    classDef green fill:#32cd32
    classDef orange fill:#f96
    classDef yellow fill:#eeff1b
    classDef blue fill:#87cefa
    class Acceptable green
    class TooHigh orange
    class CompareResults yellow
    class AddMetadata blue
```


## End-to-end Migration

The following is an end-to-end example for migrating from a Timestream for LiveAnalytics database `benchmark` and table `cpu` to bucket `benchmark-bucket` in Timestream for InfluxDB (as defined in [example.env](example.env)).

###  1. Transform data from Timestream

Transform the unloaded data from Timestream for LiveAnalytics to line protocol (LP) using Athena.

```
cd transform
python3 transform.py --database-name benchmark --tables cpu --s3-bucket-path <s3_bucket_path> --add-validation-field true
```

- To transform all tables, use the `--all-tables` flag.
- If end-to-end validation (comparing logical row counts between source and destination) is not required, set `--add-validation-field` flag to `false`.
- To convert dimensions to fields during transformation, use the `--dimensions-to-fields` flag.

See [transform/README.md](./transform/README.md) for more details.

### 2. Ingest line protocol to Timestream for InfluxDB

Download transformed LP dataset from S3:
```
aws s3 sync s3://<s3_bucket_name>/benchmark/cpu/unload-<%Y-%m-%d-%H-%M-%S>/line-protocol-output ./line-protocol-output
```

Run the ingestion script with the target Timestream for InfluxDB bucket and path to your downloaded LP dataset:
```
python3 ingestion/influxdb_ingestion.py benchmark-bucket ./line-protocol-output
```

- Optionally configure the number of workers (`-w`), batch size (`-l`), and I/O multiplier (`-m`) 
- Run ingestion with the `--continue-on-error` flag to continue ingesting remaining files even if one fails.
- On failure or disruption to ingestion, you can resume from a previous run by using the `--resume-from` flag. Specify the path to the tracking directory from a previous run to skip already ingested files.
    ```
    python3 ingestion/influxdb_ingestion.py benchmark-bucket ./line-protocol-output --resume-from  ./influxdb-ingestion-logs/tracking_<run_id>
    ```

See [ingestion/README.md](./ingestion/README.md) for more details.

### 3. Validation

Validate that all records have been ingested to InfluxDB:
```
python3 validation/validator.py
```

- Optionally configure `--start-time` and `--end-time` to validate row counts in time ranges.

- If any dimensions were converted to fields during transformation to LP, provide the full list of tags from the new schema. Refer to the output of the [transformation](transform/README.md#using-dimensions-as-fields) to retrieve tags from the transformed schema.

- To check current ingestion progress without impacting the migration, use the validator with `--influx-only` and `--skip-wal-check` flags. This provides a real-time count of points in Timestream for InfluxDB without querying the source database (Athena/Timestream for LiveAnalytics) and excludes records still in post-processing.

    ```
    python3 validation/validator.py --skip-wal-check --influx-only
    ```

See [validation/README.md](./validation/README.md) for more details.

## Troubleshooting

- If you are experiencing unexpected Python errors, ensure your Python virtual environment is properly activated:

    1. Activate the virtual environment:
    ```bash
    source venv/bin/activate
    ```

    You should see (venv)
    appear at the beginning of your command prompt.

    2. If you're experiencing command path issues, refresh your shell's command hash table:
    ```bash
    hash -r
    ```

    3. Verify Python path points to the virtual environment directory:

    ```bash
    which python
    ```

- Ensure required environment variables are defined, or `.env` (see [example.env](example.env)) is present when running scripts in this directory. Note that ingestion  requires the following environment variables to be defined:
    - `INFLUXDB_V2_URL`
    - `INFLUXDB_V2_ORG`
    - `INFLUXDB_V2_TOKEN`
