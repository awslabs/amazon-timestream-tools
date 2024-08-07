# Timestream compute units testing

## Performance testing 

We provided scripts to create timestream resource (database and table) and ingestion script to load data and finally querying scripts for testing the tcu configuration with different queries concurrently.

## Requirements

1. Make sure you have latest pip package installed
    ```bash
    python3 -m ensurepip --upgrade
    ```
2. Install Python packages if testing outside of sagemaker notebooks 
    ```bash
    python3 -m pip install boto3 numpy matplotlib
    ```

## How to use it 

1. Create Timestream for LiveAnalytics Database and Table, you can use create_timestream_resource python, terraform or cloudformation template depending upon your choice. 
    - You can change the **database name,table name, memory and magentic retention** within the script or template
2. After resources are created start the ingestion script (ingestion.py), script will ingest 100 devops metrics into Timestream for LiveAnalytics Table **every second**. 
3. [Configure the Timestream Compute Unit (TCU)](https://docs.aws.amazon.com/timestream/latest/developerguide/tcu.html), we tested for 4 and 8 TCUs and shared the results and insights in the **blog** (once the blog is published this will be hyperlink to blog-- circular dependency, this needs to published so github link can be referenced in the blog)
4. Run the notebooks lastpoint-query and single-groupby-orderby to capture the perfomance metrics for different TCU configuration.
    ## lastpoint-query - select memory from "devops"."sample_devops" where time > ago(10m) and hostname='host1' order by time desc limit 1
    ## single-groupby-orderby - select bin(time, 1m) AS binned_time, max(cpu_utilization) as max_cpu_utilization from "devops"."sample_devops" where time > ago(10m) and hostname='host2' group by bin(time, 1m) order by binned_time asc',
    - lastpoint-query and single-groupby-orderby run one minute for each worker 7, 14, 21, 28, 42, 50, 60 concurrently and  produce p50,p90,p99,total queries,throttles and plot graphs (latency percentiles,Queries Per Minute,Throttling Counts vs workers in three different graphs)