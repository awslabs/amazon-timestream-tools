# Upload utility

This utility allows to upload the sample data set using Batch Load

For each of the SQL examples this utility will

1. Create Database (if not exists)
2. Create a table for the example data. Note: It is recommended to use the same name as used in query to minimize rework of test query
2. Upload sample CSV file to S3 bucket
3. Create **Batch Load** task for upload

This utility is used in the examples below and each invocation is listed in the examples with parameters:
* [Gap filling with last value](../last_value_fill_forward)

