# Getting started with Amazon Timestream with Python

This sample application shows how you can create a database and table, populate the table with ~126K rows of sample data, and run sample queries to jumpstart your evaluation and/or proof-of-concept applications with Amazon Timestream.

----
## How to use it

1. Install and configure Boto3 set up following the instructions at https://boto3.amazonaws.com/v1/documentation/api/latest/index.html
   ```
   pip install boto3
   ```
2. Install Pandas and its associated dependencies for reading parquet files from S3
   ```
   pip install pandas pyarrow s3fs
   ```

3. Run the following commands to insert data into Timestream and to query the data
    ```
    python SampleApplication.py -t basic --csv_file_path ../data/sample.csv --kmsId ${kmsId}
    ```

   Both --kmsId and --csv_file_path are optional parameters.

   UpdateDatabase API will be called if the kmsId is provided. This kmsId should refer to a valid kms key present in your account. If the kmsId is not provided, UpdateDatabase will not be called.

   Data ingestion through csv file will happen if csv_file_path is provided.

4. Run the following commands to insert data into Timestream and export the data into S3 using Unload
   ```
   python SampleApplication.py -t unload --csv_file_path ../data/sample_unload.csv
   ```
5. Run the following command to run sample application for composite partition key:
   ```
   python SampleApplication.py -t composite_partition_key
   ```
