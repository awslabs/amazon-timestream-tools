<!-- This sample application is part of the Timestream prerelease documentation. The prerelease documentation is confidential and is provided under the terms of your nondisclosure agreement with Amazon Web Services (AWS) or other agreement governing your receipt of AWS confidential information. -->

# TimestreamCustomerSampleJavaFlinkAdapter

Sample application that reads data from Flink and writes to Amazon Timestream

----
## How to test it

Java 1.8 is the recommended version for using Kinesis Data Analytics for Apache Flink Application. If you have multiple Java versions ensure to export Java 1.8 to your `JAVA_HOME` environment variable.

1. Ensure that you have [Apache Maven](https://maven.apache.org/install.html) installed. You can test your Apache Maven install with the following command:
   ```
   mvn -version
   ```
   
2. The latest version of Apache Flink that Kinesis Data Analytics supports is **1.8.2**. To download and install Apache Flink version 1.8.2 you can follow these steps:

   a. Download the Apache Flink version 1.8.2 source code:
   ```
   wget https://archive.apache.org/dist/flink/flink-1.8.2/flink-1.8.2-src.tgz
   ```
   
   b. Uncompress the Apache Flink source code:
   ```
   tar -xvf flink-1.8.2-src.tgz
   ```
   
   c. Change to the Apache Flink source code directory:
   ```
   cd flink-1.8.2
   ```
   
   d. Compile and install Apache Flink:
   ```
   mvn clean install -Pinclude-kinesis -DskipTests
   ```    

3. Export path to workspace
    ```shell
    export PATH_TO_WORKSPACE=<path to workspace>
    ```

4. Download and copy Timestream Java SDKs into a folder of your choice and export this path into TIMESTREAM_JAVA_SDK_PATH environment variable.
   ```
   export TIMESTREAM_JAVA_SDK_PATH=<path to folder containing java_sdk>/java_sdk
   ```

5. Install Timestream jar to maven 
   ```shell
   mvn install:install-file -Dfile=${TIMESTREAM_JAVA_SDK_PATH}/aws-sdk-java-timestreamwrite-1.11.805-SNAPSHOT.jar -DgroupId=com.amazonaws -DartifactId=timestreamwrite -Dversion=1.11.805-SNAPSHOT -Dpackaging=jar
   ```
   NOTE: You might need to change the version of SDK jar in this command based on the version of SDK jar you are using.

5. Go the `TimestreamCustomerSampleJavaFlinkAdapter` sample app workspace directory

6. Compile and run the sample app.
   ```shell
   mvn clean compile
   mvn exec:java -Dexec.mainClass="com.amazonaws.services.kinesisanalytics.StreamingJob" -Dexec.args="--InputStreamName TimestreamTestStream --Region us-east-1 --TimestreamDbName kdaflink --TimestreamTableName kinesisdata2"
   ``` 
   NOTE: You might need to change the version of timestreamwrite and timestreamquery dependencies in `pom.xml` file based on the version of SDK jar you are using.
   
   By default this sample app batches Timestream ingest records in batch of 50. This can be adjusted using `--TimestreamIngestBatchSize` option.
   ```shell
   mvn clean compile
   mvn exec:java -Dexec.mainClass="com.amazonaws.services.kinesisanalytics.StreamingJob" -Dexec.args="--InputStreamName TimestreamTestStream --Region us-east-1 --TimestreamDbName kdaflink --TimestreamTableName kinesisdata2 --TimestreamIngestBatchSize 75"
   ```    
