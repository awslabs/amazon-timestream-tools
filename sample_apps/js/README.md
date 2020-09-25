# TimestreamCustomerSampleNodeJs

Sample NodeJs application to use Timestream

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
    Also install minimist library for flag support (https://github.com/substack/minimist)
    ```shell
    npm install minimist
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
         

