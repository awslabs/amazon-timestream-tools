# Lambda function ingesting S3 CSV file to Timestream

See [parent readme](../) for description.

## Running the code

### Requirements

 - [Amazon Corretto 11](https://docs.aws.amazon.com/corretto/latest/corretto-11-ug/what-is-corretto-11.html)
 - [Maven](https://docs.aws.amazon.com/corretto/latest/corretto-11-ug/what-is-corretto-11.html)
 - The Bash shell. For Linux and macOS, this is included by default. In Windows 10, you can install the [Windows Subsystem for Linux](https://docs.microsoft.com/en-us/windows/wsl/install-win10) to get a Windows-integrated version of Ubuntu and Bash.
 - [The AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html).
 
### Setup
 
 1. Download or clone this repository.
 2. Go to `SampleLambdaFromS3CsvIngestion\deployment` folder.
 3. Run `1-create-bucket.sh` to create a new bucket for deployment artifacts.
 4. Create Timestream database and table where records will be ingested.
 
### Deploy

In `2-build-upload.sh` script modify the following strings:

 - REPLACE_ME_BUCKET_NAME - S3 bucket name from which lambda will be allowed to read data
 - REPLACE_ME_TIMESTREAM_DATABASE_NAME - Timestream database name to which lambda will write the records
 - REPLACE_ME_TIMESTREAM_TABLE_NAME - Timestream table name to which lambda will write the records
 
Run `2-build-upload.sh` to build the application with Maven and deploy it. This script uses AWS CloudFormation to deploy the Lambda functions and an IAM role. If the AWS CloudFormation stack that contains the resources already exists, the script updates it with any changes to the template or function code.


### Run the lambda function

Run the following AWS CLI command, replacing **function-name**, **bucket.name**, **bucket.key** with parameters of your choice:

```
aws lambda invoke --function-name sample-lambda-from-s3-csv-ingestion out --log-type Tail --query 'LogResult' --output text --payload '{ "bucket.name": "my-bucket-name", "bucket.key": "sample_withHeader.csv" }' |  base64 -d
```