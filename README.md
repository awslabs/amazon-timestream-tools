## Amazon Timestream Tools and Samples 

Amazon Timestream is a fast, scalable, fully managed, purpose-built time series database that makes it easy to store and
 analyze trillions of time series data points per day. Amazon Timestream saves you time and cost in managing the 
 lifecycle of time series data by keeping recent data in memory and moving historical data to a cost optimized storage 
 tier based upon user defined policies. Amazon Timestream’s purpose-built query engine lets you access and analyze 
 recent and historical data together, without having to specify its location. Amazon Timestream has built-in time series
  analytics functions, helping you identify trends and patterns in your data in near real-time. Timestream is serverless
   and automatically scales up or down to adjust capacity and performance. Because you don’t need to manage the 
   underlying infrastructure, you can focus on optimizing and building your applications.

Timestream also integrates with commonly used services for data collection, visualization, and machine learning. 
You can send data to Amazon Timestream using AWS IoT Core, Amazon Kinesis, Amazon MSK, and open source Telegraf. 
You can visualize data using Amazon QuickSight, Grafana, and business intelligence tools through JDBC. You can also use
Amazon SageMaker with Timestream for machine learning.
This repository contains sample applications, plugins, notebooks, data connectors, and adapters to help you get 
started with Amazon Timestream and to enable you to use Amazon Timestream with other tools and services. 


## Sample application
The following are sample data connectors, adapters, and notebooks that show how to use Amazon Timestream with 
and popular tools and services used for data collection, visualization, and machine learning.:

* [Java](https://github.com/awslabs/amazon-timestream-tools/blob/master/sample_apps/java/)
* [Java v2](https://github.com/awslabs/amazon-timestream-tools/blob/master/sample_apps/javaV2/)
* [Python](https://github.com/awslabs/amazon-timestream-tools/blob/master/sample_apps/python/)
* [Go](https://github.com/awslabs/amazon-timestream-tools/blob/master/sample_apps/go/)
* [Node.js](https://github.com/awslabs/amazon-timestream-tools/blob/master/sample_apps/js/)
* [.NET](https://github.com/awslabs/amazon-timestream-tools/blob/master/sample_apps/dotnet/)


## Integrations
The following are sample data connectors, adapters, and notebooks show how to use Amazon Timestream with 
and popular tools and services used for data collection, visualization, and machine learning:

* [Working with Amazon SageMaker Notebook](https://github.com/awslabs/amazon-timestream-tools/blob/master/integrations/sagemaker/)
* [Publishing data with AWS IoT Core](https://github.com/awslabs/amazon-timestream-tools/blob/master/integrations/iot_core/)
* [Output data connector for open source Telegraf](https://github.com/aws/telegraf/blob/telegraf_v1.15.3_with_Timestream/plugins/outputs/timestream/)
* [Apache Flink sample data connector](https://github.com/awslabs/amazon-timestream-tools/blob/master/integrations/flink_connector/)

## Data ingestion tools
The following tools can be used to continuously send data into Amazon Timestream:
* [Publishing data with Amazon Kinesis](https://github.com/awslabs/amazon-timestream-tools/blob/master/tools/kinesis_ingestor/)
* [Writing data using a multi-thread Python DevOps data generator](https://github.com/awslabs/amazon-timestream-tools/blob/master/tools/continuous-ingestor/)


