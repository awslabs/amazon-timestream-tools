Sample Python application for AWS SDK V2

----
## How to use it

1. Install and configure Boto3 set up following the instructions at https://boto3.amazonaws.com/v1/documentation/api/latest/index.html

1. Run the following commands to insert data into Timestream and to query the data
    ```
    python SampleApplication.py --csv_file_path ../data/sample.csv --kmsId ${kmsId}
    ```

    Both --kmsId and --csv_file_path are optional parameters.
	
    UpdateDatabase API will be called if the kmsId is provided. This kmsId should refer to a valid kms key present in your account. If the kmsId is not provided, UpdateDatabase will not be called.
	
    Data ingestion through csv file will happen if csv_file_path is provided.


