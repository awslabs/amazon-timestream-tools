<!-- This sample application is part of the Timestream prerelease documentation. The prerelease documentation is confidential and is provided under the terms of your nondisclosure agreement with Amazon Web Services (AWS) or other agreement governing your receipt of AWS confidential information. -->

# Amazon Timestream Sink for Apache Flink

Write records to Timestream from Flink.

----
## Overview

This is the root directory for samples which show you end-to-end process of working with Kinesis and Timestream.
The directory contains:
 - [sample data generator](/integrations/flink_connector/sample-data-generator) - python script which generates and ingests sample data to Kinesis
 - [sample Kinesis to Timestream Application](/integrations/flink_connector/sample-kinesis-to-timestream-app) - sample Flink application which reads records from Kinesis and uses the Timestream Sink to ingest data to Timestream
 - [Amazon Timestream Flink Sink](/integrations/flink_connector/flink-connector-timestream) - Timestream Flink Sink as Maven module


![design](images/root-diagram.png)

## Getting Started

### 1. Install prerequisites

 - Java 11 is the recommended version for using Kinesis Data Analytics for Apache Flink Application. If you have multiple Java versions ensure to export Java 11 to your `JAVA_HOME` environment variable.
 - Install [Apache Maven](https://maven.apache.org/install.html). You can test your Apache Maven install with the following command:
```
mvn -version
```

### 2. Setup Environment

Create an Amazon Kinesis Data Stream with the name "TimestreamTestStream". You can use the below AWS CLI command:
```
aws kinesis create-stream --stream-name TimestreamTestStream --shard-count 1
```

### 3 (option A). Run Sample Flink Application locally
1. Compile and run the sample application. The application will create target Timestream database/table upon launch automatically, and will start pooling records from Kinesis and writing them to Timestream:
```
cd ../sample-kinesis-to-timestream-app
mvn clean compile && mvn package
mvn install exec:java -Dexec.mainClass="com.amazonaws.samples.kinesis2timestream.StreamingJob" -Dexec.args="--InputStreamName TimestreamTestStream --Region us-east-1 --TimestreamDbName kdaflink --TimestreamTableName kinesisdata" -Dexec.classpathScope=test
```

2. Follow **Getting Started** section from [sample data generator](/integrations/flink_connector/sample-data-generator) to send records to Kinesis.
3. The records now should be consumed by the sample application and written to Timestream table.
4. Query Timestream table using [AWS Console](https://docs.aws.amazon.com/timestream/latest/developerguide/console_timestream.html#console_timestream.queries.using-console) or AWS CLI:
```
aws timestream-query query --query-string "SELECT * FROM kdaflink.kinesisdata WHERE time >= ago (15m) LIMIT 10"
```

## 3 (option B). Run Sample Flink Application on Amazon Kinesis Data Analytics
1. Compile the sample application:
```shell
cd ../sample-kinesis-to-timestream-app
mvn clean compile && mvn package
```
2. Upload Flink Application Jar file to S3 bucket: 
```shell
cd ../sample-kinesis-to-timestream-app
aws s3 cp target/sample-kinesis-to-timestream-app-0.1-SNAPSHOT.jar s3://YOUR_BUCKET_NAME/sample-kinesis-to-timestream-app-0.1-SNAPSHOT.jar
```
3. Follow the steps in [Create and Run the Kinesis Data Analytics Application](https://docs.aws.amazon.com/kinesisanalytics/latest/java/get-started-exercise.html#get-started-exercise-7)
 - pick Apache Flink version 1.13.2
 - in "Edit the IAM Policy" step, add Timestream Write permissions to the created policy

4. Follow **Getting Started** section from [sample data generator](/integrations/flink_connector/sample-data-generator) to send records to Kinesis.
5. The records now should be consumed by the sample application and written to Timestream table.
6. Query Timestream table using [AWS Console](https://docs.aws.amazon.com/timestream/latest/developerguide/console_timestream.html#console_timestream.queries.using-console) or AWS CLI:
```
aws timestream-query query --query-string "SELECT * FROM kdaflink.kinesisdata WHERE time >= ago (15m) LIMIT 10"
```