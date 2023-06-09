# Getting started with Amazon Timestream with Go

This sample application shows how you can create a database and table, populate the table with ~126K rows of sample data, and run sample queries to jumpstart your evaluation and/or proof-of-concept applications with Amazon Timestream.
1. Install [Go](https://go.dev/doc/install)

1. To run the sample application, you can use the following commands. This will create a Timestream database called devops with a table named host_metrics and inserts 4 records in the table.
    ```
    go mod init go_sample
    go mod tidy
    ```
1. To run with sample application and ingest data from sample csv data file, you can use the following command:
    ```
    go run ingestion-csv-sample.go
    ```
1. To run with sample application and include database CMK update to a kms "valid-kms-id" registered in your account run
    ```
    go run crud-ingestion-sample.go --kms_key_id updatedKmsKeyId
    ```

1. To run sample queries,  
    1. Make .sh files executable:
        ```
        chmod 777 *.sh
        ```
    1.  List the unique hostnames in the table host_metrics and the count of data points for each host. Copy one of the host names from the result.
        ```
        ./devops_init.sh devops host_metrics
        ```
    1. Run 12 sample queries with shell. 
        ```
        ./devops.sh devops host_metrics <host name copied from above>
       ```

The following sample application will insert ~13k rows into a Timestream table and run queries to export data to S3

1. To run the sample application and execute Unload queries without deleting the Timestream resources, you can run the following command
    ```
    go run unload-sample.go -skip_deletion=true -region=us-east-1 -csv_file_path=../data/sample_unload.csv
    ```

The following sample application will demonstrate creating tables with composite partition keys (dimension partition key & measure partition key) and ingest records. Also demonstrate that records will be rejected if table has a dimension partition key with REQUIRED as enforcement and records are ingested without the partition key dimension

1. Create S3 bucket with bucket name `timestream-sample-bucket`
2. To run the sample application for composite partition key
    ```
    go run composite-partition-key-sample.go
    ```