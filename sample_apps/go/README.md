1. Create folder for Timestream go workspace and export the path to the folder as TIMESTREAM_GO_HOME.
export TIMESTREAM_GO_HOME=<path_to_timestream>
1. Unzip the GO SDK into $TIMESTREAM_GO_HOME.
1. Unzip sample_apps.zip into $TIMESTREAM_GO_HOME. 
1. Run the command "chmod 777 *.sh" on the .sh files in go_sample to make them executable files
1. Run the command "go run crud-ingestion-sample.go". This will create a Timestream database called devops with a table named host_metrics and inserts 4 records in the table.The table host_metrics contains the cpu utilization and memory utilization of many EC2 instances to address devops use cases. The timestamps for the data points in the table have millisecond granularity. Set the flag with a valid kms key to verify the updateDatabase API works: go run crud-ingestion-sample.go --kms_key_id updatedKmsKeyId 
1. Run the command "go run ingestion-csv-sample.go". This will insert ~125K rows in the table host_metrics. This operation can take a few minutes.
1. Execute the shell file ./devops_init.sh devops host_metrics. This will list the unique hostnames in the table host_metrics and the count of data points for each host. Copy one of the host names from the result.
1. Now run ./devops.sh devops host_metrics <host name copied from above>. This will execute a shell file with 12 sample queries, followed by demonstration of functionality like query with multiple pages and cancelling a query. See query-sample.go for the GO code for parsing query results.
