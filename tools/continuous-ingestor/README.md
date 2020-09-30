## Sample Continuous Data Ingestor Python application for AWS SDK V2

A script to generate a continuous stream of records that are ingested into Timestream. This script mimics a DevOps scenario where an application is emitting different types of events at a regular cadence. The script continuously generates data until the execution is interrupted with a SIGINT (or `CTRL + C`).

--- 
## Dependencies
- Boto3
- numpy (Tested with version 1.18.5)
- Python3 (Tested with version 3.5.2)

----
## How to use it

1. Install and configure Boto3 set up following the instructions at https://boto3.amazonaws.com/v1/documentation/api/latest/index.html

1. Install numpy 
	```
	pip3 install numpy
	```

1. Run the following command to continuously generate and ingest sample data into Timestream. 
   
    ```    
    python3 timestream_sample_continuous_data_ingestor_application.py --database-name <db_name> --table-name <table_name> --endpoint <endpoint e.g. 'us-east-1'>
    ```
    
    - The ingestion can be stopped with a SIGINT signal (typically, `Ctrl + C` on most systems).
    - If the `database-name` and `table-name` do not pre-exist in Timestream, the application will stop with an error message.
    - Use `--help` argument to see more options available in the Sample Continuous Data Ingestor Python Application.
     E.g. ```python3 timestream_sample_continuous_data_ingestor_application.py --help```
    
### Examples
#### Single-threaded ingest
Starts a single-threaded ingest process the continues until SIGINT signal (CTRL + C) is received.
```
python3 timestream_sample_continuous_data_ingestor_application.py -c 1 --host-scale 1 -d testDb -t testTable -e 'us-east-1'
```

#### Concurrent ingest

Starts a multi-threaded ingest process the continues until SIGINT signal (CTRL + C) is received. The number of threads is controlled by the option -c or --concurrency.
```
python3 timestream_sample_continuous_data_ingestor_application.py -c 30 --host-scale 1 -d testDb -t testTable -e 'us-east-1'
```

#### Higher number of hosts and time series

Starts a multi-threaded ingest process the continues until SIGINT signal (CTRL + C) is received. The time series count is controlled by the option --host-scale.

```
python3 timestream_sample_continuous_data_ingestor_application.py -c 30 --host-scale 3 -d testDb -t testTable -e 'us-east-1'
```





