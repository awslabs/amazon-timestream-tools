# Ingesting data to Amazon Kinesis for Amazon Timestream Sink Sample Application

This folder contains a script to generate a continuous stream of records that are ingested into Kinesis, and then can be consumed by Amazon Timestream Sink Sample Application. You can refer to the [Amazon Timestream documentation on working with Apache Flink](https://docs.aws.amazon.com/timestream/latest/developerguide/ApacheFlink.html) for additional information. This script mimics a DevOps scenario where an application is emitting different types of events at a regular cadence. The script continuously generates data until the execution is interrupted with a SIGINT (or `CTRL + C`).

--- 
## Dependencies
- Boto3
- numpy (Tested with version 1.18.5)
- Python3 (Tested with version 3.5.2)

----
## Getting Started

0. (Optional) Setup Python virtual environment
```
python3 -m venv venv
. venv/bin/activate
```

1. Install and configure Boto3 set up following the instructions at https://boto3.amazonaws.com/v1/documentation/api/latest/index.html or executing the following command:
   ```
   pip3 install boto3
   ```

2. Install numpy
   ```
   pip3 install numpy
   ```

3. Run the following command to continuously generate and ingest sample data into Kinesis.

    ```    
    python3 kinesis_data_gen.py --stream <name_of_the_kinesis_stream> --region <the_region_of_kinesis_stream> 
    ```

    - The ingestion can be stopped with a SIGINT signal (`Ctrl + C` or `⌘ + C`).
    - Use `--help` argument to see more options available in the Sample Continuous Data Ingestor Python Application.
      E.g. ```python3 kinesis_data_gen.py --help```

**Note:** Two instances of the script may produce data on exactly the same timestamp, with same dimensions, but different measure values. Such records will be rejected on Timestream side without specifying a version. Therefore, it's unsafe from concurrency point of view to run multiple instances of the same python generation script without modifying dimensions.

## Generated Data

### Metric Data

Example metric structure is below, which will be parsed as [MyHostMetrics](../sample-kinesis-to-timestream-app/src/main/java/com/amazonaws/samples/kinesis2timestream/model/MyHostMetrics.java) object in sample application:
```json
{
  "availability_zone": "us-west-2-1",
  "cell": "us-west-2-cell-1",
  "cpu_hi": 0.36,
  "cpu_idle": 90.79,
  "cpu_iowait": 0.25,
  "cpu_nice": 0.82,
  "cpu_si": 0.09,
  "cpu_steal": 0.19,
  "cpu_system": 0.86,
  "cpu_user": 6.64,
  "disk_free": 95.07,
  "disk_io_reads": 26.76,
  "disk_io_writes": 28.96,
  "disk_used": 50.95,
  "file_descriptors_in_use": 0.67,
  "instance_name": "i-zaZswmJk-athena-0000.amazonaws.com",
  "instance_type": "m5.8xlarge",
  "latency_per_read": 40.71,
  "latency_per_write": 80.2,
  "memory_cached": 58.27,
  "memory_free": 85.05,
  "memory_used": 11.31,
  "microservice_name": "athena",
  "network_bytes_in": 35.81,
  "network_bytes_out": 74.71,
  "@type": "metrics",
  "os_version": "AL2012",
  "region": "us-west-2",
  "silo": "us-west-2-cell-1-silo-2",
  "time": 1642626987
}
```

### Event data

Example event structure is below, which will be parsed as [MyHostEvents](../sample-kinesis-to-timestream-app/src/main/java/com/amazonaws/samples/kinesis2timestream/model/MyHostEvents.java) object in sample application: 

```json
{
 "availability_zone": "us_east_1-1",
 "cell": "us_east_1-cell-4",
 "gc_pause": 77.26,
 "gc_reclaimed": 66.99,
 "instance_name": "i-zaZswmJk-apollo-0000.amazonaws.com",
 "jdk_version": "JDK_11",
 "memory_free": 87.03,
 "microservice_name": "apollo",
 "@type": "events",
 "process_name": "server",
 "region": "us_east_1",
 "silo": "us_east_1-cell-4-silo-3",
 "task_completed": 324,
 "task_end_state": "SUCCESS_WITH_RESULT",
 "time": 1642626987
}
```

## Usage Examples

- Start sending a stream of events to Kinesis stream called **TimestreamTestStream**:

    ```
    python3 kinesis_data_gen.py --stream TimestreamTestStream --region us-east-1
    ```

- Start sending a stream of events where a percentage of the events will be late arriving by ten minutes:

    ```
    python3 timestream_kinesis_data_gen.py --stream TimestreamTestStream --region us-east-1 --late-percent 25 --late-time 600
    ```

- Generate only event data:

    ```
    python3 timestream_kinesis_data_gen.py --stream TimestreamTestStream --region us-east-1 --generate-events 1 --generate-metrics 0

    ```