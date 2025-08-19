# Rust Sample Application

This sample application shows how to:
1. Create a table enabled with magnetic tier upsert.
2. Ingest data with multi measure records.
3. Execute queries with your terminal inside the `amazon-timestream-tools/sample_apps/rust` directory and interpret their results.

This application populates a table with ~63K rows of sample multi-measure-value data (provided as csv data), and runs sample queries to jumpstart your evaluation and/or proof-of-concept applications with Amazon Timestream for LiveAnalytics.

## Sample Application Usage

### Prerequisites

1. Install [Rust](https://www.rust-lang.org/tools/install).
2. Configure [SDK authentication with AWS](https://docs.aws.amazon.com/sdk-for-rust/latest/dg/credentials.html).

Once the `ingestion_csv_sample` sample application is run, if it doesn't already exist, the database `devops_multi_sample_application` with the table `host_metrics_sample_application` will be created and will contain the CPU utilization and memory utilization of EC2 instances to simulate DevOps use cases. The timestamps for the data points in the table use millisecond granularity.

### Sample Application to Ingest & Query Multi-Measure Data

1. To insert ~63K rows in the table `host_metrics_sample_application`.

   ```
   cargo run --example ingestion_csv_sample
   ```

2. Run the `ingestion_csv_sample` application in a given region. Without the `--region` parameter, the region defaults to `us-east-1`.

   ```
   cargo run --example ingestion_csv_sample -- --region=us-east-2
   ```

   > **_NOTE:_**: If the `--region` parameter is used for the ingestion of data, use the same region for the remaining steps.

3. For any sample application, the `-h` or `--help` parameter displays the list of arguments.

   ```
   cargo run --example <sample application> -- --help
   ```

4. To execute sample queries on the above ingested data. See `query_sample.rs` for the Rust code for parsing query results.

   ```
   cargo run --example query_sample
   ```

5. Run the `query_sample` application in a given region. Without the `--region` parameter, the region defaults to `us-east-1`.

   ```
   cargo run --example query_sample -- --region=us-east-2
   ```

6. By default, responses and the number of rows returned from queries are printed to stdout as well as to an output file. The default output file is `query_results.log`.

   ```
   cargo run --example query_sample -- --output-file=custom_output_file.log
   ```

7. To clean up resources created for the sample applications, which are an S3 bucket and a Timestream for LiveAnalytics database and table.

   ```
   cargo run --example cleanup_sample
   ```

8. Run the sample application in a given region

   ```
   cargo run --example cleanup_sample -- --region=us-east-2
   ```
