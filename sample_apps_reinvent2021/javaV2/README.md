
# Getting started with Amazon Timestream with Java V2

This sample application shows to
1. Create table enabled with magnetic tier upsert
2. Ingest data with multi measure records
3. Create ScheduledQuery and interpret its run.

This application populates the table with ~63K rows of sample  multi measure value data (provided as part of csv) , and run sample queries to jumpstart your evaluation and/or proof-of-concept applications with Amazon Timestream.

----
## How to use it

Ensure your Java SDK and runtime are 1.8 or higher.

1. Install maven: https://maven.apache.org/install.html

1. Go to Timestream Java sample app directory

1. You can compile and run your source code with the below command:
    ```shell
   mvn clean compile
   mvn exec:java -Dexec.mainClass="com.amazonaws.services.timestream.Main"
    ``` 
   NOTE: 
   1. You might need to change the version of timestreamwrite and timestreamquery dependencies in `pom.xml` file based on the version of SDK jar you are using. 

1. To run with sample application and ingest data from sample csv data file, you can use the following command: 
   ```shell
   export PATH_TO_SAMPLE_DATA_FILE=<path to sample csv data file>
   mvn clean compile
   mvn exec:java -Dexec.mainClass="com.amazonaws.services.timestream.Main" -Dexec.args="--inputFile ${PATH_TO_SAMPLE_DATA_FILE}/sample-multi.csv"
   ``` 
   
   NOTE:
   1. Only when the CSV file is provided, the sample will run all queries, and the ScheduledQuery examples.

1. To run with sample application and include database CMK update to a kms "valid-kms-id" registered in your account run  
   ```shell
   mvn clean compile
   mvn exec:java -Dexec.mainClass="com.amazonaws.services.timestream.Main" -Dexec.args="--kmsId valid-kms-id"
   ``` 
