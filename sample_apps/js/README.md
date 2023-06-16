# Getting started with Amazon Timestream with Node.js

This sample application shows how you can create a database and table, populate the table with ~126K rows of sample data, and run sample queries to jumpstart your evaluation and/or proof-of-concept applications with Amazon Timestream.

-----
## How to use

1. Install Node.js if not already installed.
   ```
   https://nodejs.org/en/
   ```
2. Install necessary node dependencies.
   ```shell
   npm install
   ```
3. Run the sample application
   ```shell
   node main.js
   ```
4. UpdateDatabase will be skipped unless --kmsKeyId flag is given
    ```shell
    node main.js --kmsKeyId=updatedKmsKeyId
    ```
5. Run the sample application with sample data by adding --csvFilePath flag
   ```shell
   node main.js --csvFilePath=../data/sample.csv
   ``` 
6. Run sample application to remove table and database created
   ```shell
   node main.js --type cleanup
   ```
7. Run sample application to execute Unload queries
   ```shell
   node main.js --type unload --csvFilePath=../data/sample_unload.csv
   ```
8. Run sample application with composite partition key
   ```shell
   node main.js --type compositePartitionKey
   ```
         

