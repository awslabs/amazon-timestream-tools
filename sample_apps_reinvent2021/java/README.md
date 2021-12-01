<!-- This sample application is part of the Timestream prerelease documentation. The prerelease documentation is confidential and is provided under the terms of your nondisclosure agreement with Amazon Web Services (AWS) or other agreement governing your receipt of AWS confidential information. -->

# TimestreamCustomerSampleJava

Sample Java application for AWS SDK

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
