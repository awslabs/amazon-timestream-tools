# Timestream Compute Units Performance Testing

## Performance testing 

We provided scripts to create Timestream for LiveAnalytics resource (database and table) and ingestion script to load data and finally querying scripts for testing the tcu configuration with different queries concurrently.

## Requirements

1. Make sure you have latest pip package installed
    ```bash
    python3 -m ensurepip --upgrade
    ```
2. Install Python packages if testing outside of sagemaker notebooks 
    ```bash
    python3 -m pip install boto3 numpy matplotlib
    ```
3. Tested on Python3.8 

## How to use it 

1. Create Timestream for LiveAnalytics Database and Table, you can use create_timestream_resource [python](./create_timestream_resource.py), [cloudformation template](./create_timestream_resource.yaml) or [terraform](./create_timestream_resource.tf) depending upon your choice. 
    - You can change the database name, table name, memory and magentic retention within the script or template
2. After resources are created, Execute the  [ingestion script](./ingestion.py), python3.8 ingestion.py. If required [You can change the database name,table name, region](https://github.com/awslabs/amazon-timestream-tools/blob/tcu-testing/tools/python/timestream-compute-units-testing/ingestion.py#L183). Script will ingest 100 devops metrics into Timestream for LiveAnalytics table every second. Let the ingestion run for atleast couple hours before you start querying. 
3. [Configure the Timestream Compute Unit (TCU)](https://docs.aws.amazon.com/timestream/latest/developerguide/tcu.html), we tested for 4 and 8 TCUs and shared the results and insights in the **[blog](https://aws.amazon.com/blogs/database/understanding-and-optimizing-amazon-timestream-compute-units-for-efficient-time-series-data-management)
4. Run the [lastpoint-query.ipynb](./lastpoint-query.ipynb) and [single-groupby-orderby.ipynb](./single-groupby-orderby-query.ipynb) notebooks to capture the perfomance metrics for different TCU configuration. These notebooks run one minute, increasing the users from 1 to 21 concurrently and capture p50, p90, p99, total number of queries per minute, throttles and plot graphs (latency percentiles, Queries Per Minute, Throttling Counts vs number of workers in three different graphs). 

    ## lastpoint-query 
    Retrieves the most recent memory Utilization for a given host
    ```sql
    select memory from "devops"."sample_devops" where time > ago(10m) and hostname='host1' order by time desc limit 1
    ```

    ## single-groupby-orderby
    Binning, grouping, and ordering for given host. A relatively more resource intensive query than lastpoint-query 
    ```sql
    select bin(time, 1m) AS binned_time, max(cpu_utilization) as max_cpu_utilization from "devops"."sample_devops" where time > ago(10m) and hostname='host2' group by bin(time, 1m) order by binned_time asc
    ```

we simulated two popular observability query patterns with more than one user monitoring the metrics, increasing the users from 1 to 21. We then varied MaxQueryTCU settings (4 and 8) to monitor the volume of queries handled and corresponding latencies as the users increase. We explained about TCUs and test results in the the **[blog](https://aws.amazon.com/blogs/database/understanding-and-optimizing-amazon-timestream-compute-units-for-efficient-time-series-data-management)**, please refer the blog for more insights. 