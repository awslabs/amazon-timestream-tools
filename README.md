## Amazon Timestream Tools and Samples 

[Amazon Timestream](https://aws.amazon.com/timestream/) is a fast, scalable, fully managed, purpose-built time series database that makes it easy to store and
 analyze trillions of time series data points per day. Amazon Timestream saves you time and cost in managing the 
 lifecycle of time series data by keeping recent data in memory and moving historical data to a cost optimized storage 
 tier based upon user defined policies. Amazon Timestream’s purpose-built query engine lets you access and analyze 
 recent and historical data together, without having to specify its location. Amazon Timestream has built-in time series
  analytics functions, helping you identify trends and patterns in your data in near real-time. Timestream is serverless
   and automatically scales up or down to adjust capacity and performance. Because you don’t need to manage the 
   underlying infrastructure, you can focus on optimizing and building your applications.

Amazon Timestream also integrates with commonly used services for data collection, visualization, and machine learning. 
You can send data to Amazon Timestream using AWS IoT Core, Amazon Kinesis, Amazon MSK, and open source Telegraf. 
You can visualize data using Amazon QuickSight, Grafana, and business intelligence tools through JDBC. You can also use
Amazon SageMaker with Amazon Timestream for machine learning. For more information on how to use Amazon Timestream see the [AWS documentation](https://docs.aws.amazon.com/timestream/latest/developerguide/index.html).

This repository contains sample applications, plugins, notebooks, data connectors, and adapters to help you get 
started with Amazon Timestream and to enable you to use Amazon Timestream with other tools and services. 


## Sample applications
This repository contains fully functional sample applications to help you get started with Amazon Timestream. 

The getting started application shows how to create a database and table, populate the table with ~126K rows of sample data, and run sample queries. 
This sample application is currently available for the following programming languages:

* [Getting started with Java](https://github.com/awslabs/amazon-timestream-tools/blob/master/sample_apps/java/)
* [Getting started with Java v2](https://github.com/awslabs/amazon-timestream-tools/blob/master/sample_apps/javaV2/)
* [Getting started with Python](https://github.com/awslabs/amazon-timestream-tools/blob/master/sample_apps/python/)
* [Getting started with Go](https://github.com/awslabs/amazon-timestream-tools/blob/master/sample_apps/go/)
* [Getting started with Node.js](https://github.com/awslabs/amazon-timestream-tools/blob/master/sample_apps/js/)
* [Getting started with .NET](https://github.com/awslabs/amazon-timestream-tools/blob/master/sample_apps/dotnet/)

To query time series data using Amazon Timestream's JDBC driver, refer to the following application:
* [Querying data with JDBC](https://github.com/awslabs/amazon-timestream-tools/tree/master/sample_apps/jdbc)


## Working with other tools and services
To continue to use your preferred data collection, analytics, visualization, and machine learning tools with Amazon Timestream, refer to the following:

* [Analyzing time series data with Amazon SageMaker Notebooks](https://github.com/awslabs/amazon-timestream-tools/blob/master/integrations/sagemaker/)
* [Sending data to Amazon Timestream using AWS IoT Core](https://github.com/awslabs/amazon-timestream-tools/blob/master/integrations/iot_core/)
* [Sending data to Amazon Timestream using open source Telegraf](https://github.com/awslabs/amazon-timestream-tools/tree/master/integrations/telegraf/)
* [Sending data to Amazon Timestream using Apache Flink](https://github.com/awslabs/amazon-timestream-tools/blob/master/integrations/flink_connector/)
* [Writing and Querying with Pandas (AWS Data Wrangler)](https://github.com/awslabs/amazon-timestream-tools/blob/master/integrations/pandas/)


## Data ingestion and query tools
To understand the performance and scale capabilities of Amazon Timestream, you can run the following workload:
* [Running large scale workloads with Amazon Timestream](https://github.com/awslabs/amazon-timestream-tools/tree/master/tools/perf-scale-workload/)

You can use the following tools to continuously send data to Amazon Timestream:
* [Publishing data with Amazon Kinesis to send to Amazon Timestream](https://github.com/awslabs/amazon-timestream-tools/blob/master/tools/kinesis_ingestor/)
* [Multi-threaded continuous data generator for writing DevOps metrics into Amazon Timestream](https://github.com/awslabs/amazon-timestream-tools/blob/master/tools/continuous-ingestor/)


