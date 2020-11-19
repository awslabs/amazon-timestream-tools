# Sample Ingestion and Query Workload Generator for Amazon Timestream

A continuous data ingester and query workload generator mimicking a monitoring application in a DevOps scenario. A sample application, comprising of many micro-services, is deployed across multiple regions, silos, cells, etc. The application instances emit different metrics which are ingested into a database and table in [Amazon Timestream](https://aws.amazon.com/timestream/). An ingestion workload generator creates the specified database and table (if they don't exist) in the specified Timestream region and ingests the metrics into the table. A query workload generator emulates users who want to alert on anomalous resource usage, create dashboards on aggregate fleet utilization, and perform sophisticated analysis on recent and historical data to find correlations.

## Prerequistes

* **Ingestion host**: An EC2 instance on the same region as that of the Timestream database.
* **Query host**: An EC2 instance on the same region as that of the Timestream database.

Refer to the [EC2 documentation](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/LaunchingAndUsingInstances.html) to launch the EC2 instances. We recommend creating an instance with sufficient resources to drive the concurrent ingestion and query workload. Make sure to follow the security best practices to secure the instances.

The following scripts are tested on an EC2 instance with Amazon Linux 2 with Python v3.7.9. The setup used was an ``m5.24xlarge`` instance for ingestion and ``m5.12xlarge`` for query.

## Basic setup

    The following instructions are for an Amazon Linux 2 host. Instructions need to be appropriately adapted for other hosts.

* Launch a new instance by [following instructions to launch EC2 instances](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/LaunchingAndUsingInstances.html)
* Check the python version. If a version lower than 3.7.9 is installed, then either install or upgrade python 3.

```
yum list installed | grep -i python3

whereis python3
```

**Install Python 3**
```
sudo yum install python3
```

**Upgrade Python 3**

```
sudo yum update python3
```

The following setup can also be completed within a virtual env. [Follow instructions](https://aws.amazon.com/premiumsupport/knowledge-center/ec2-linux-python3-boto3/) to set up a virtual env.

* [Install or upgrade AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html)

```
pip3 install --upgrade --user awscli
```
* Configure the AWS CLI with the appropriate credentials, region, and output format.

```
aws configure
```

* Install the dependencies. The dependencies necessary are specified in the ``requirements.txt`` file.

```
pip3 install --user -r requirements.txt
```

If you encounter error: ``error: command 'gcc' failed with exit status 1``, then the development tools might be missing.

```
sudo yum groupinstall "Development Tools"
sudo yum install python3-devel
```

## Authentication and Authorization

Timestream uses IAM-based authentication and authorization. Each of the driver scripts here is configured to obtain the credentials using the default credentials provider. They also support providing an explicit profile (configured through ``aws configure``) using the ``--profile`` argument. [Follow the instructions](https://docs.aws.amazon.com/timestream/latest/developerguide/accessing.html#getting-started.prereqs.iam-user) to make sure that IAM profile as the necessary access.

## Ingestion Workload

The sample application workload mimics a DevOps monitoring scenario for a highly scaled out service which is deployed in several *regions* across the globe. Each region is further sub-divided into a number of scaling units called *cells* that have a level of isolation in terms of infrastructure within the region. Each cell is further sub-divided into *silos* which represent a level of software isolation. Each silo has five *microservices* that comprise one isolated instance of the service. Each microservice has several servers with different instance type and os version which are deployed across three availability zones. These attributes that identify the servers emitting the metrics are modeled as [dimensions](https://docs.aws.amazon.com/timestream/latest/developerguide/concepts.html) in Timestream. Here, we have a hierarchy of dimensions (e.g., region, cell, silo, microservice) as well as other dimensions that cut across the hierarchy (e.g., instance_type, availability_zone, etc.).

A multi-process and multi-threaded application models the metrics and events emitted by the service. The main ingestion driver is the file ``devops_ingestion_driver.py``. This script creates the database and table if they don't exist, and spawn a configurable number of processes and configurable number of threads per process which ingest data into Timestream. The effective concurrency is the product of the number of processes and the number of threads. This file takes several arguments which define the scale of the workload. Use the ``--help`` option to list these arguments.

```
python3 devops_ingestion_driver.py --help
```

In addition to specifying the Timestream database and table, the table's [retention bounds](https://docs.aws.amazon.com/timestream/latest/developerguide/storage.html), and the AWS region, the arguments also control the data scale, ingestion volume, and the concurrency of the workload driver. The ``--memory-store-retention-hours`` and ``--magnetic-store-retention-days`` options allow overriding the retention bounds desired. Note that these options are only effective if the table is created by the script. If the table already exists, the script down not modify its existing retention bounds.

At a high level, the data generator models (defined in ``model.py``) multiple hosts within the service. The option ``--host-scale`` determines the number of hosts emitting metrics. ``--host-scale 1`` corresponds to 1,000 hosts emitting metrics, ``--host-scale 100`` corresponds to 100,000 hosts and so on.

Each host emits 26 periodic metrics and events that correspond to resource usage, GC events, etc. Therefore, ``--host-scale 100`` corresponds to 2.6M time series being stored in the table.

The other parameter that controls the ingestion volume is the interval at which metrics and events are emitted. The argument ``--interval-millis`` controls this interval. ``--interval-millis 60000`` implies that each host emits its set of metrics and events once every 60 seconds on average.

The table below summarizes some example configurations for ``--host-scale`` and ``--interval-millis``:

### Table summarizing sample parameters

|   |Interval Millis (ms)	|Host scale	|Number of hosts	|Num of Time series	|Avg. Data points/sec	|Avg. Data points/hr	|Avg. Ingestion volume (MB/s)	|Data size per hour (GB)	|Data size per day (GB)	|Data size per year (TB)	|
|---    |---	|---	|---	|---	|---	|---	|---	|---	|---	|---	|
|**Small**|60,000	|100	|100,000	|2,600,000	|43,333	|155,998,800	|12.86	|45	|1,085	|387	|
|**Medium**|300,000	|2,000	|2,000,000	|52,000,000	|173,333	|623,998,800	|51.45	|181	|4,341	|1,547	|
|**Large**|120,000	|4,000	|4,000,000	|104,000,000	|866,667	|3,120,001,200	|257.26	|904	|21,706	|7,737	|

The above output can also be generated using the ``--print-model-summary`` argument for the script. Note that passing this argument does not execute the ingestion process, it only prints the summary and the script exits. For example:

```
python3 devops_ingestion_driver.py -d perf_scale -t test -e us-east-1 -c 10 -p 5 --host-scale 100 --interval-millis 60000 --print-model-summary
```

The above command generates the following output summarizing the (approximate) ingestion and data volume characteristics.

```
Dimensions for metrics: 100,000
Dimensions for events: 120,000
avg row size: 310.72 Bytes
Number of timeseries: 2,600,000. Avg. data points per second: 43,333. Avg. data points per hour: 155,998,800
Avg. Ingestion volume: 12.86 MB/s. Data size per hour: 45.21 GB. Data size per day: 1,085.04 GB. Data size per year: 386.76 TB
```

### Example ingestion command line

Below are example command lines to start ingestion for the first two rows of the table above.

**Small scale**
```
python3 devops_ingestion_driver.py -d perf_scale -t test -e us-east-1 -c 30 -p 20 --host-scale 100 --interval-millis 60000
```
**Medium scale**

```
python3 devops_ingestion_driver.py -d perf_scale -t test -e us-east-1 -c 50 -p 50 --host-scale 2000 --interval-millis 300000
```

This command will launch 50 processes, which in turn will launch 50 threads to generate the ingestion load. Each thread synchronously ingests data using the Python SDK of Timestream. The ingester continues to run continuously until a ``SIGINT`` signal is sent to the process. It prints summary statistics as it continues to ingest.

As the ingestion workload is scaling up, Timestream will automatically scale resources. During the scaling, Timestream will throttle the write records. As these throttles are received, it is important to keep the ingestion load to ensure the system scales up. Follow the [best practices recommendations](https://docs.aws.amazon.com/timestream/latest/developerguide/best-practices.html#data-ingest) and adapt the SDK configurations appropriately in the file ``timestreamwrite.py``.

### Note

If during ingestion, the driver prints a steady stream of messages of the ``Can't keep up ingestion to the desired inter-event interval ...``, it implies more parallelism is needed in the workload generator. This can be modified using the arguments ``-p`` and ``-c`` or launch multiple instances of the ingester (see below). Since Python multi-threading is still serialized through the global interpreter lock, ``-c`` higher than 50 does not result in increasing the effective parallelism. In such cases, it is recommended to increase the value of ``-p``. The number of processes that can be launch is dependent on the CPU and memory available on the EC2 instance. If the EC2 instance is already high in CPU and memory utilization, consider splitting up the ingester into multiple instances across multiple EC2 instances (see below).

### Multiple instances of the ingester

If a single ingester is not able to achieve the target ingestion volume, the ingester is also amenable to having multiple instances running on multiple hosts. The recommended approach to have multiple instances is to partition the workload into multiple instances.

For instance, if the goal for ingestion is ``--host-scale 4000``, and to partition on two hosts, then follow the following steps:

* Create two EC2 instances following the instructions above.
* On the two instances, execute the following command.

**Host 1**

```
python3 devops_ingestion_driver.py -d perf_scale -t test -e us-east-1 -c 50 -p 50 --host-scale 2000 --interval-millis 120000 --instance-name-seed 12345
```

**Host 2**

```
python3 devops_ingestion_driver.py -d perf_scale -t test -e us-east-1 -c 50 -p 50 --host-scale 2000 --interval-millis 120000 --instance-name-seed 56789
```

Note that only difference in the command lines on the two hosts is the ``--instance-name-seed`` option. The data generator uses the seed to generate a psuedo-random string in the instance name. Different seeds allow different sets of instance names to be created, which essentially allows generating metrics for two sets of hosts. In this case, it creates two sets of hosts, thus resulting in the overall equivalent host scale of 4,000.

## Query Workload

A multi-process and multi-threaded application. It models a query workload that executes a steady stream of queries. The main query driver is the file ``devops_query_driver.py``. This script spawns a configurable number of processes and configurable number of threads per process which runs queries against Timestream. This file takes several arguments which define the scale of the workload. Use the ``--help`` option to list these arguments.

```
python3 devops_query_driver.py --help
```

In addition to specifying the Timestream database and table, there are options to control the concurrency. The workload configurations, that control the number of executions and the weights of the queries is controlled by a configuration file. An example is the ``config.ini`` or the ``config_row_count.ini`` file. The query weights specify the probability with which a query is executed. Each thread of the workload driver executes queries in a loop and uses the weight to randomly select queries to execute.

Note that the example queries defined in ``devops_query_driver.py`` use up to 3 days of data. Therefore, to get representative numbers, it is important to have the ingestion driver ingest data for at least three days before starting the query driver. It is also possible to have the ingestion driver execute for several weeks to get query performance numbers with weeks of data.

Following is an example command line to execute 10 concurrent sessions.

```
python3 devops_query_driver.py -d perf_scale -t test -e us-east-1 --config config.ini -c 5 -p 2 -l ~/experiment_log/r1 --think-time-milliseconds 300000 --randomized-think-time
```

The above command line executes six queries, defined in the file ``devops_query_driver.py`` that correspond to *alerting*, *dashboards*, and *analysis* queries.

During the script's execution, the terminal window also prints the execution statistics across all the threads. In addition, the script also also generates detailed query execution logs on the terminal as well as in the log directory specified (using argument ``--log-dir``). The log directory will have multiple sub-directories created, one for each thread launched. The sub-directories have a prefix of the region and the timestamp. For instance, a run may create a directory with the name  ``us-east-1-2020-11-13-07-04-19-1-1`` for a run launched on ``2020/11/13 07:04:19`` timestamp on the EC2 instance. The suffix ``1-1`` corresponds to the process ID and thread ID. If the script was executed with ``-p 5 -c 2``, there will be 10 sub-directories with the suffix ``{1-1, 1-2, 2-1, 2-2, ..., 5-2}``.

Each sub-directory will have several files with query names. For example: ``do-q1.*`` correspond to files for the first query ``do-q1`` in the script. The file with extension ``.sql`` logs the actual ``SQL`` which is executed, ``.log`` file logs the query execution details, such as the Query Execution ID, the execution time, etc., and the ``.err`` file will log any error messages for exceptions that a specific query's execution may have encountered. At the end of execution, the script creates a ``csv`` file (e.g., ``us-east-1-2020-11-13-07-04-19-1-1-summary.csv`) with a summary of execution latency (client-side elapsed time from when the query is sent to Timestream to the last row of result read from Timestream) per query. The file reports several summary statistics for each query, e.g., Avg, 50th percentile, 99th percentile, Geometric mean, etc.

An example of the summary file:

```
Query type, Total Count, Successful Count, Avg. latency (in secs), Std dev latency (in secs), Median, 90th perc (in secs), 99th Perc (in secs), Geo Mean (in secs)
do-q1, 2956, 2956, 1.55, 0.6, 1.493, 1.94, 2.544, 1.502
```

### Note

Timestream automatically scales resources allocated to queries. Hence, during load spikes, the requests may encounter occasional throttling exceptions with the message ``Request rate limit exceeded``. It is recommended to retry the requests with a back-off. If requests continue to be throttled, consider reaching out through usual support channels.

## Cleanup

Once the performance experiments are completed, it is important to clean up the resources. A simple utility cleans up Timestream resources.

```
python3 devops_cleanup_resources.py -d perf_scale -t test -e us-east-1
```

Follow the [instructions](https://aws.amazon.com/premiumsupport/knowledge-center/delete-terminate-ec2/) to delete the EC2 instances to also avoid billing for those instance.