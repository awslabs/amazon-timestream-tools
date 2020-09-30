<!-- This sample application is part of the Timestream prerelease documentation. The prerelease documentation is confidential and is provided under the terms of your nondisclosure agreement with Amazon Web Services (AWS) or other agreement governing your receipt of AWS confidential information. -->

# Apache Flink sample data connector

Sample application that reads data from Flink and writes to Amazon Timestream

----
## How to test it

Java 1.8 is the recommended version for using Kinesis Data Analytics for Apache Flink Application. If you have multiple Java versions ensure to export Java 1.8 to your `JAVA_HOME` environment variable.

1. Ensure that you have [Apache Maven](https://maven.apache.org/install.html) installed. You can test your Apache Maven install with the following command:
   ```
   mvn -version
   ```
   
1. The latest version of Apache Flink that Kinesis Data Analytics supports is **1.8.2**. To download and install Apache Flink version 1.8.2 you can follow these steps:

   1. Download the Apache Flink version 1.8.2 source code:
      ```
      wget https://archive.apache.org/dist/flink/flink-1.8.2/flink-1.8.2-src.tgz
      ```
   
   1. Uncompress the Apache Flink source code:
      ```
      tar -xvf flink-1.8.2-src.tgz
      ```
   
   1. Change to the Apache Flink source code directory:
      ```
      cd flink-1.8.2
      ```
   
   1. Compile and install Apache Flink:
      ```
      mvn clean install -Pinclude-kinesis -DskipTests
      ```    
   1. Go back to the sample app folder
      ```
      cd ..
      ```
1. Create an Amazon Kinesis Data Stream with the name "TimestreamTestStream". You can use the below AWS CLI command:
   ```
   aws kinesis create-stream --stream-name TimestreamTestStream --shard-count 1
   ```

1. Compile and run the sample app.
   ```shell
   mvn clean compile
   mvn exec:java -Dexec.mainClass="com.amazonaws.services.kinesisanalytics.StreamingJob" -Dexec.args="--InputStreamName TimestreamTestStream --Region us-east-1 --TimestreamDbName kdaflink --TimestreamTableName kinesisdata1"
   ``` 
   NOTE: You might need to change the version of timestreamwrite and timestreamquery dependencies in `pom.xml` file based on the version of SDK jar you are using.
   
   By default this sample app batches Timestream ingest records in batch of 50. This can be adjusted using `--TimestreamIngestBatchSize` option.
   ```shell
   mvn clean compile
   mvn exec:java -Dexec.mainClass="com.amazonaws.services.kinesisanalytics.StreamingJob" -Dexec.args="--InputStreamName TimestreamTestStream --Region us-east-1 --TimestreamDbName kdaflink --TimestreamTableName kinesisdata1 --TimestreamIngestBatchSize 75"
   ```    

## For sending data into the Amazon Kinesis Data Stream
You can follow the instructions on https://github.com/awslabs/amazon-timestream-tools/tree/master/tools/kinesis_ingestor

## For deploying the sample application to Kinesis Data Analytics for Apache Flink

This sample application is part of the setup for transfering your time series data from Amazon Kinesis, Amazon MSK, Apache Kafka, and other streaming technologies directly into Amazon Timestream.

For the full set of instructions: https://docs.aws.amazon.com/timestream/latest/developerguide/ApacheFlink.html

