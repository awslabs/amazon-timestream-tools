# Getting started with Amazon Timestream with Node.js

This sample application shows how you can create a database and table, populate the table with ~126K rows of sample data, and run sample queries to jumpstart your evaluation and/or proof-of-concept applications with Amazon Timestream.

-----
## How to use

 1. Install Node.js if not already installed.
    ```
    https://nodejs.org/en/
    ```
 1. Install necessary node dependencies. 
    ```shell
    npm install
    ```
 1. Run the sample application
    ```shell
    node main.js
    ```
 1. UpdateDatabase will be skipped unless --kmsKeyId flag is given
     ```shell
     node main.js --kmsKeyId=updatedKmsKeyId
     ```
 1. Run the sample application with sample data by adding --csvFilePath flag
    ```shell
    node main.js --csvFilePath=../data/sample.csv
    ``` 
         
---
# Release Note

1. To run `writeRecordsWithUpsert()`, please update sdk to [v2.799.0](https://github.com/aws/aws-sdk-js) or above.
    ```
    npm update aws-sdk
    ```
