# Getting started with Amazon Timestream with Python

This sample application shows to
1. Create table enabled with magnetic tier upsert
2. Ingest data with multi measure records
3. Create ScheduledQuery and interpret its run.

This application populates the table with ~63K rows of sample  multi measure value data (provided as part of csv) , and run sample queries to jumpstart your evaluation and/or proof-of-concept applications with Amazon Timestream.

----

## Dependencies
- Boto3 (Version 1.20.13+)
- Python3 (Version 3.6+)

## How to use it

0. (Optional) You can work within a virtual environment
    ```
    python3 -m venv venv
    . venv/bin/activate
    ```

1. Install and configure Boto3 (>= 1.20.13) set up following the instructions at https://boto3.amazonaws.com/v1/documentation/api/latest/index.html or executing the following command:
   ```
   pip3 install -r requirements.txt --upgrade
   ```


## How to run the sample application

View list of arguments
```
python SampleApplication.py --help
```

### Run basic example
```
python SampleApplication.py \
--type basic \
--region us-east-2 \
--skip_deletion false
--kmsId ${kmsId}
```

--kmsId, skip_deletion are optional parameters.

UpdateDatabase API will be called if the kmsId is provided. This kmsId should refer to a valid kms key present in your account. If the kmsId is not provided, UpdateDatabase will not be called.

skip_deletion controls whether the database and table are deleted afterward the query finishes running. By default it doesn't delete, which may lead to duplicate data being ingested the next run. Be wary of this argument in real production environment.

--region is required field based on the region the application will be hosted in your AWS account.

### Run example to ingest the multi measure data in `sample-multi.csv`
```
python SampleApplication.py \
--type csv \
--csv_file_path ../data/sample-multi.csv \
--region us-east-2 \
--skip_deletion false
```

csv_file_path is a required field controlling the path of csv ingestion. Note it must be multi measure format data (example given in data directory) 

### Run example to create scheduled query and run
```
python3 SampleApplication.py \
--type sq \
--region us-east-2 \
--skip_deletion false
```
Note: If skip_deletion is true then ScheduledQuery resources (Timestream scheduled query, Timestream database, Timestream table, SNS, SQS, IAM role, IAM policy, S3) need to be deleted manually.

### Run example to create scheduled query that fails on execution and generates an error report
```
python3 SampleApplication.py \
--type sq-error \
--region us-east-2 \
--skip_deletion false
```

### Delete database and table created by this script
```
python SampleApplication.py \
--type cleanup
```
If your database is in an inconsistent state, you could use this command to hard delete tables. Be wary of this argument in real production environment.
