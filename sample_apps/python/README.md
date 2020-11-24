# Getting started with Amazon Timestream with Python

This sample application shows how you can create a database and table, populate the table with ~126K rows of sample data, and run sample queries to jumpstart your evaluation and/or proof-of-concept applications with Amazon Timestream.

----

## Dependencies
- Boto3
- Python3 (Tested with version 3.5.2)

## How to use it

0. (Optional) You can work within a virtual environment
    ```
    python3 -m venv venv
    . venv/bin/activate
    ```

1. Install and configure Boto3 set up following the instructions at https://boto3.amazonaws.com/v1/documentation/api/latest/index.html or executing the following command:
	```
	pip3 install -r requirements.txt --upgrade
	```

1. Run the following commands to insert data into Timestream and to query the data
    ```
    python3 SampleApplication.py --csv_file_path ../data/sample.csv --kmsId ${kmsId}
    ```

    Both --kmsId and --csv_file_path are optional parameters.
	
    UpdateDatabase API will be called if the kmsId is provided. This kmsId should refer to a valid kms key present in your account. If the kmsId is not provided, UpdateDatabase will not be called.
	
    Provide a csv file to perform data ingestion.
