# Getting started with Amazon Timestream with Node.js

This sample application shows to
1. Create table enabled with magnetic tier upsert
2. Ingest data with multi measure records
3. Create ScheduledQuery and interpret its run.

This application populates the table with ~63K rows of sample  multi measure value data (provided as part of csv) , and run sample queries to jumpstart your evaluation and/or proof-of-concept applications with Amazon Timestream.

-----
## How to use

 1. Install Node.js if not already installed.
    ```
    https://nodejs.org/en/
    ```
 1. Initialize node application by calling following command in your sample application folder. You can use default values for details asked while initializing the application. 
    ```shell
    npm init
    ```
    Install minimist library for flag support (https://github.com/substack/minimist)
    ```shell
    npm install minimist
    ```
    Install @js-joda/core for localDate Library (https://www.npmjs.com/package/@js-joda/core)
    ```shell
    npm install @js-joda/core
    ```
    NOTE: Sometimes you may see issues if your `npm` repository is not configured to `https://registry.npmjs.org`. You can check that by calling
    ```shell script
    npm config get registry
    ``` 
    Simplest way to solve this is:
    ```shell
    npm config set registry https://registry.npmjs.org
    ```
 1. Install SDK by following the steps: https://aws.amazon.com/sdk-for-node-js/   
 1. You can compile and run your source code with the below command
    ```shell
    node main.js
    ```
 1. To run with sample application and include database CMK update to a kms "valid-kms-id" registered in your account run  
     ```shell
     node main.js --kmsKeyId=updatedKmsKeyId
     ```
 1. To run with sample application and ingest data from sample csv data file, you can use the following command:
    ```shell
    node main.js --csvFilePath=../data/sample-multi.csv
    ``` 
    Note:
    Only when the CSV file is provided, the sample will run all queries, and the ScheduledQuery examples.
1. To run with specific region:
   ```shell
   node main.js --region="eu-west-1"
   ```
   Note:
   Default "us-east-1" will be used, if region is not passed.
1. By default we do not delete database and table which can be enabled by un-commenting below line in main.js
   ```shell
   // await crudAndSimpleIngestionExample.deleteTable(constants.DATABASE_NAME, constants.TABLE_NAME);
   // await crudAndSimpleIngestionExample.deleteDatabase(constants.DATABASE_NAME);
   ```
