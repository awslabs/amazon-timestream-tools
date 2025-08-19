# Timestream Unload & BatchLoad Automation

## When to use this automation?

This automation can be used for migrating **Timestream for LiveAnalytics** table to new table. Automation is divided into two parts:
1. unloading the data from Timestream using different partitioning choices 
2. batchloading the data into Timestream, it also covers S3 copy functionality if unload was run on different account or same account with different region. Data modelling changes can be applied as part of batchload. 

You can use this automation in following use-cases:
-  Migrating Timestream for LiveAnalytics table to different AWS Organization. 
-  Migrating Timestream for LiveAnalytics table to different region or different account and need data model changes in destination account/region. If data model changes are not required (and accounts belong to same AWS Organization) try to make use of AWS Backups for Timestream (ref. [Amazon Timestream backups](https://docs.aws.amazon.com/aws-backup/latest/devguide/timestream-backup.html) and [Restore an Amazon Timestream table](https://docs.aws.amazon.com/aws-backup/latest/devguide/timestream-restore.html)).
- Migrating Timestream for LiveAnalytics table to new table with [customer defined partition key](https://docs.aws.amazon.com/timestream/latest/developerguide/customer-defined-partition-keys.html)


## Usage and Requirements

These are the full steps to execute the script in your AWS Account.

1. Log into your AWS account and select the AWS Region in which your Timestream table is stored

2. Launch [AWS CloudShell](https://console.aws.amazon.com/cloudshell/home) or your local shell (Python 3.9 or newer is **required**)

3. Clone this source code project using [git](https://git-scm.com/) or download it manually

4. Make sure you have latest pip package installed
    ```bash
    python3 -m ensurepip --upgrade
    ```
5. Install Python [boto3](https://pypi.org/project/boto3/), [backoff](https://pypi.org/project/backoff/) and [tqdm](https://pypi.org/project/tqdm/) packages
    ```bash
    python3 -m pip install boto3 backoff tqdm
    ```
6. Run the unload.py or the batch_load.py as described below.

## Getting started with UNLOAD

### Key considerations/limits and best practices
- **Recommendation is to have your UNLOAD not exceed 80 GB**, consider to split the process into multiple UNLOADS leveraging scripts start/end time parameters (e.g. if you have 1 TB, run 13 UNLOADS) to avoid any interruptions.
- **Queries containing UNLOAD statement can export at most 100 partitions per query.**, hence consider to split the process into multiple UNLOADS leveraging scripts start/end and partition parameters
- **Maximum file size in a batch load task cannot exceed 5 GB**.Unload files as part of this automation will not exceed that size.
- **Concurrency for queries using the UNLOAD statement is 1 query per second (QPS)**. Exceeding the query rate will result in throttling.
- **Queries containing UNLOAD statement time out after 60 minutes.**
- **CSV Files with headers (column names) are created in S3 as part of Unload script**. This is requirement for Timestream batch load.   

Unload official Documentation: (https://docs.aws.amazon.com/timestream/latest/developerguide/export-unload.html)
Check the following guide to learn more: [Limits for UNLOAD from Timestream for LiveAnalytics](https://docs.aws.amazon.com/timestream/latest/developerguide/export-unload-limits.html)

### Usage Parameters
- **region** [OPTIONAL]: AWS region of your Timestream table to be unloaded, if not provided the current region of your session will be chosen (e.g.: *us-west-1*)
- **database** [REQUIRED]: Timestream database where the table to be unloaded is located
- **table** [REQUIRED]: Timestream table to be unloaded
- **s3_uri** [OPTIONAL]: S3 Bucket URI to store unload data, if not provided a new S3 bucket will be created with the following name 'timestream-unload-<database>-<table>-<account_id> (e.g.: *s3://timestream-unload-sourcedb-mytable-account_id/unload*)
- **from_time** [OPTIONAL]: Timestamp (extreme included) from which you want to select data to unload (e.g.: *2024-02-26 17:24:38.270000000*)
- **end_time** [OPTIONAL]: Timestamp (extreme excluded) to which you want to select data to unload (e.g.: *2024-03-15 19:26:31.304000000*)
- **partition** [OPTIONAL]: Time partition you want to use (possible values: *day, month, year*)
- **iam_role_arn** [OPTIONAL]: {Applies for cross account migrations} Grants destination IAM Role access to S3 Bucket (e.g.: *arn:aws:iam::123456789123:role/BatchLoadRole*)

### Examples

Example to unload the Timestream table *myTable* in the database *sourcedb* to the folder *unload* in the *timestream-unload-sourcedb-mytable* S3 bucket.
Also, it applies an S3 bucket policy to allow the IAM Role *BatchLoadRole* of account *123456789123* to allow the copy. It does day level partitions.
 ```bash
python3 unload.py --region eu-west-1 --s3_uri s3://timestream-unload-sourcedb-mytable/unload --database sourcedb --table myTable --iam_role_arn arn:aws:iam::123456789123:role/BatchLoadRole --partition day
```

## Getting started with BATCH LOAD

### Key considerations and best practices
  
- **A table cannot have more than 5 active batch load tasks and an account cannot have more than 10 active batch load tasks. Timestream for LiveAnalytics will throttle new batch load tasks until more resources are available.** batch load script allows only 5 as max limit for batchload threads (table level).  

**Additional details**
- [Batch load prerequisites](https://docs.aws.amazon.com/timestream/latest/developerguide/batch-load-prerequisites.html) 
- [Batch load best practices](https://docs.aws.amazon.com/timestream/latest/developerguide/batch-load-best-practices.html)
- [Batchload official documentation](https://docs.aws.amazon.com/timestream/latest/developerguide/batch-load.html)
- [Batchload Quotas](https://docs.aws.amazon.com/timestream/latest/developerguide/ts-limits.html)

### Usage Parameters

- **region** [OPTIONAL]: AWS region of your Timestream table for batchload, if not provided the current region of your session will be chosen  (e.g.: *us-east-1*)
- **database_name** [OPTIONAL]: Timestream database name for batchload (default: batch_load_test)
- **create_timestream_resource**[OPTIONAL]:  Provide this if Timestream database and table have to be created (default: False)
- **table_name** [OPTIONAL]: Timestream table name (default: batch_load_test)
- **partition_key** [OPTIONAL]: Partition key for Timestream table, provide partition_key it if option create_timestream_resource is set(default: None)
- **memory_store_retenion_in_hours** [OPTIONAL]: Memory store retention in **hours** for Timestream table (default: 24)
- **magnetic_store_retention_in_days** [OPTIONAL]:  Magnetic store retention in **days** for Timestream table (default: 3655)
- **create_error_logging_bucket** [OPTIONAL]: Provide this option if error logging bucket for batchload has to be created (default: False).
- **create_destination_bucket** [OPTIONAL]: Provide this option if bucket for batchload target has to be created (default: False)
- **copy_s3_bucket** [OPTIONAL]: Provide this option if unload files have to copied from source bucket (default: False)
- **s3_source_bucket_location** [OPTIONAL]: Source S3 bucket, if copy_s3_bucket is set to true (default: None). Example : timestream-unload-sourcedb
- **data_model_file** [REQUIRED]:  Data model JSON file location for batchload, [data modelling reference](https://docs.aws.amazon.com/timestream/latest/developerguide/batch-load-data-model-mappings.html)
- **s3_target_bucket** [OPTIONAL]: Target bucket for batchload, if not provided  defaults to bucket name: timestream-batchload-{database}-{table}-{account_id}-{region} 
- **s3_target_error_bucket** [OPTIONAL]: Target bucket for batchload errors, if not provided  defaults to bucket name: timestream-batchload-error-{database}-{table}-{account_id}-{region} 
- **source_s3_prefix** [OPTIONAL]:  Source bucket prefix if copy_s3_bucket is set true (default: results/)
- **destination_s3_prefix** [OPTIONAL]: Desctination bucket prefix if copy_s3_bucket is set true (default: dest/)
- **sns_topic_arn** [OPTIONAL]: SNS topic ARN for sending any batchload failures (default: None), SNS topic should be in same account and region. Example: arn:aws:sns:us-east-2:123456789012:MyTopic
- **num_of_batchload_threads** [OPTIONAL]: Number of parallel batchloads threads (default: 5 and maximum: 5)
- **multi_part_upload_chunk**  [OPTIONAL]: Multi part upload chunk size in bytes, default is 500MB (default: 524288000)

### Examples

#### With S3 Copy
Example to execute a batch load to the target Timestream table *myTable* with partition key *city* in the database *targetdb* with  *us-west-2* region. 
Timestream objects are created by this script as per *create_timestream_resource* parameter. 
Source data are located in the S3 bucket *timestream-unload-sourcedb-mytable* with prefix *unload/results/*.
S3 batch target and error buckets(for error logs) are created by this script as per *create_destination_bucket* and *create_error_logging_bucket* parameter.
Target bucket and error bucket names are given by *s3_target_bucket* and *s3_target_error_bucket* parameter. Error logs are stored into S3 bucket *timestream-batchload-error-logs*.
Destination prefix will be created with prefix dest/ given by *destination_s3_prefix*. Desired data model file is chosen as *data_model_sample.json* in the current location of the script. 

 ```bash
python3 batch_load.py --region us-west-2 --create_timestream_resource --database=targetdb --table=myTable --partition_key city --copy_s3_bucket --s3_source_bucket_location  timestream-unload-sourcedb-mytable --source_s3_prefix unload/results/ --create_destination_bucket --s3_target_bucket timestream-batchload-targetdb-mytable --destination_s3_prefix dest/ --create_error_logging_bucket --s3_target_error_bucket timestream-batchload-error-logs --data_model_file "data_model_sample.json"      
```
 
#### Without S3 Copy
Example to execute a batch load to the target Timestream table *myTable* with partition key *city* in the database *targetdb* with  *eu-west-1* region.
Timestream objects are created by this script as per *create_timestream_resource* parameter. Source data are located in the S3 bucket *timestream-unload-sourcedb-mytable* with prefix *unload/results/*.
Error logs are stored into S3 bucket *timestream-batchload-error-logs*. If you need error log buckets to be created specify --create_error_logging_bucket.
 ```bash
python3 batch_load.py --region eu-west-1 --database=targetdb --table=myTable  --s3_target_bucket timestream-unload-sourcedb-mytable --destination_s3_prefix unload/results/  --data_model_file "data_model_sample.json" --create_timestream_resource --partition_key city   --s3_target_error_bucket timestream-batchload-error-logs
```



