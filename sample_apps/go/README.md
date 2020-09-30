1. To run the sample application. This will create a Timestream database called devops with a table named host_metrics and inserts 4 records in the table.
    ```
    go mod init go_sample
    go run crud-ingestion-sample.go
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
