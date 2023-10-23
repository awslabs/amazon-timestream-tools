# Upload utility

This utility allows to upload the sample data set using Batch Load

This utility will

1. Create a table for the example data. Note: It is recommended to use the same name as used in query to minimize rework of test query
2. Upload sample CSV file to S3 bucket
3. Create **Batch Load** task for upload

Usage:

```shell
cd <github-clone-root>/amazon-timestream-tools/sample_apps/sql/utils
python3 ./create_batch_load_task.py \
     region=<your_region> \
     mapping=../<example_folder>/datamodel.json \
     input_bucket=<s3-bucket-name> \
     object_key_prefix=<upload_folder> \
     data_file=../<example_folder>/<example_data>.csv \
     database=amazon-timestream-tools \
     table=<example_table>
```

| **⚠ Note**:                                                                                                                                                                                                                                                                                                                                  |
|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| It is recommended to use the database name `amazon-timestream-tools` and table name as described in each example. The SQL statement uses `"amazon-timestream-tools"."<example_table>"` in WHERE clause and would not need to be modified. If you use a different database name and table combination, please adjust the SQL query as needed. |

Parameters used:

Parameter        | Description                                                                                                                                             | Recommended Value
-----------------|---------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------
**region**       | Region where database is deployed                                                                                                                       | any region where Timestream is available
**mapping**      | File used to map CSV data columns to Timestream attributes                                                                                              | `../<example_folder>/datamodel.json`
**input_bucket** | S3 bucket used to upload data file and report Batch Load Status                                                                                         | existing S3 bucket
**object_key**   | Folder where data file will be uploaded                                                                                                                 | any string, should not be root folder
**data_file**    | CSV file for this example                                                                                                                               | csv file as specified in example folder
**database**     | Existing database in region. All examples are designed to use the same database. **⚠ Note** A database will not be created, it has to be created first. | `amazon-timestream-tools`
**table**        | Table where data is loaded. If this table does not exist, the table will be created                                                                     | Table name as specified in example folder
