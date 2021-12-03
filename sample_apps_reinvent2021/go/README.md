# Getting started with Amazon Timestream with Go

This sample application shows to
1. Create table enabled with magnetic tier upsert
2. Ingest data with multi measure records
3. Create ScheduledQuery and interpret its run.

This application populates the table with ~63K rows of sample  multi measure value data (provided as part of csv) , and run sample queries to jumpstart your evaluation and/or proof-of-concept applications with Amazon Timestream.

### Sample Applications Usage
1. #### Sample application to create scheduled query and run
   1. View list of arguments
       ```
       go run scheduled-query-sample.go --help
       ```   
   2. Run example to create scheduled query and run
      ```
      go run scheduled-query-sample.go
      ```
   3. Run example to ingest the multi measure data in `sample-multi.csv`. DEFAULT value is `../data/sample-multi.csv`
      ```
      go run scheduled-query-sample.go --csv_file_path="../data/sample-multi.csv"
      ```
   4. Run example to create scheduled query that fails on execution and generates an error report. DEFAULT value is false,
       so it runs a valid query in the scheduled query sample
      ```
      go run scheduled-query-sample.go --run_invalid_scheduled_query_flag=true
      ```
   5. Delete resources created by this script for creating/executing E2E scheduled query. DEFAULT value is true
      ```
      go run scheduled-query-sample.go --delete_resources_after_execution_flag=true
      ```
   6. Skip deleting resources created by this script for further manual testing. DEFAULT value is true
      ```
      go run scheduled-query-sample.go --delete_resources_after_execution_flag=false
      ```
   7. Run the sample application in a given region
      ```
      go run scheduled-query-sample.go --region=us-east-2
      ```
   >**_NOTE:_** : Only when the CSV file path provided is valid, sample will run the ScheduledQuery E2E example.

2. #### Sample application to run basic example
   1. Create a Timestream database called devops_multi_sample_application with a table named
     host_metrics_sample_application and inserts 4 records in the table.
     The table host_metrics_sample_application contains the cpu utilization and memory utilization of many EC2 instances to
     address devops use cases. The timestamps for the data points in the table have millisecond granularity. Set the flag
     with a valid kms key to verify the updateDatabase API works:
     `go run crud-ingestion-sample.go --kms_key_id updatedKmsKeyId`
      ```
      go run crud-ingestion-sample.go
      ```

3. #### Sample application to ingest/query the multi measure data from `sample-multi.csv`
   1. To insert ~63K rows in the table host_metrics_sample_application.
      ```
      go run ingestion-csv-sample.go
      ```
   2. Run the sample application in a given region
      ```
      go run ingestion-csv-sample.go --region=us-east-2
      ```
      >**_NOTE:_** : If -region parameter is used for the ingesting data, please use the same region for rest of the two steps
   
   3. To execute sample queries on the above ingested data. See `query-sample.go` for the GO code for parsing query results.
       ```
        go run query-sample.go
       ```
   4. View list of arguments
      ```
       go run query-sample.go --help
      ```
   5. Run the sample application in a given region
      ```
      go run query-sample.go --region=us-east-2
      ```
   6. By default, responses from queries are printed on the cli and as well as to an output file, DEFAULT output file is `query_results.log`
      ```
      go run query-sample.go --outputfile="custom_output_file.log"
      ```
   7. Run the cancel query in a given region
      ```
      go run query-with-cancel.go --region=us-east-2 --query "select * from devops_multi_sample_application.host_metrics_sample_application"
      ```
   
   8. To clean up resources created in STEP-3.1 which are S3 bucket, Timestream database and table.
       ```
       go run cleanup-sample.go
      ```
   9. Run the sample application in a given region
       ```
       go run cleanup-sample.go --region=us-east-2
      ```
