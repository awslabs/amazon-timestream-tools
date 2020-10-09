# TimestreamCustomerSampleNodeJs

Sample NodeJs application to use Timestream

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
         

