# Getting started with Amazon Timestream with Go
This sample application shows to
1. Create table enabled with magnetic tier upsert
2. Ingest data with multi measure records
3. Create ScheduledQuery and interpret its run.

This application populates the table with ~63K rows of sample  multi measure value data (provided as part of csv) , and run sample queries to jumpstart your evaluation and/or proof-of-concept applications with Amazon Timestream.

Run the command `chmod 777 *.sh` on the .sh files in go_sample to make them executable files

#### Sample application to create scheduled query and run
- View list of arguments
  ```
  go run scheduled-query-sample.go --help
   ```
- Run example to create scheduled query and run
   ```
  go run scheduled-query-sample.go 
   ```
- Run example to ingest the multi measure data in `sample-multi.csv`. DEFAULT value is `../sample-multi.csv`
  ```
  go run scheduled-query-sample.go --csv_file_path="../data/sample-multi.csv"
  ```

- Run example to create scheduled query that fails on execution and generates an error report. DEFAULT value is false,
  so it runs a valid query in the scheduled query sample
  ```
  go run scheduled-query-sample.go --run_invalid_scheduled_query_flag=true
  ```
- Delete resources created by this script for creating/executing E2E scheduled query. DEFAULT value is true
  ```
  go run scheduled-query-sample.go --delete_resources_after_execution_flag=false
  ```
- Skip deleting resources created by this script for further manual testing. DEFAULT value is true
  ```
  go run scheduled-query-sample.go --delete_resources_after_execution_flag=false
  ```
-  Run the sample application in a given region
  ```
  go run scheduled-query-sample.go --region=us-east-2
  ```
>**_NOTE:_** : Only when the CSV file path provided is valid, sample will run the ScheduledQuery E2E example.

#### Sample application to run basic example

- Create a Timestream database called devops_multi_sample_application with a table named
  host_metrics_sample_application and inserts 4 records in the table.
  The table host_metrics_sample_application contains the cpu utilization and memory utilization of many EC2 instances to
  address devops use cases. The timestamps for the data points in the table have millisecond granularity. Set the flag
  with a valid kms key to verify the updateDatabase API works:
  `go run crud-ingestion-sample.go --kms_key_id updatedKmsKeyId`
  ```
  go run crud-ingestion-sample.go
  ```

#### Sample application to ingest the multi measure data in `sample-multi.csv`
- To insert ~63K rows in the table host_metrics_sample_application.
  ```
  go run ingestion-csv-sample.go
  ```

#### Sample queries on ingested  multi measure data from `sample-multi.csv`

- Execute the shell file to list the unique hostnames in the table `host_metrics_sample_application` and the count of data points for each host. Copy one of the host names from the result.
  ```
  ./devops_init.sh devops_multi_sample_application host_metrics_sample_application
  ```
- Now run the following command to execute a shell file with 12 sample queries, followed by demonstration of functionality like query with multiple pages and cancelling a query. See `query-sample.go` for the GO code for parsing query results.
  ```
  ./devops.sh devops_multi_sample_application host_metrics_sample_application <host name copied from above>
  ```
