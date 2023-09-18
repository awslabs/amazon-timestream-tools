# Create Amazon Timestream table and scheduled query in single CloudFormation 

## Summary

With scheduled queries, you define the real-time analytics queries that compute aggregates, rollups and store results into another Amazon Timestream table. 
As tables are schemaless and schema is creating during ingestion, Scheduled queries creation relies on data being already loaded for SQL validation to succeed.

## How to create scheduled query in CloudFormation 

This cloudformation illustrates stack creation in following order:

1. Create database and table; Table will be empty
2. Create Lambda to ingest 2 example records
3. Create Custom resources that will invoke Lambda
4. Create Scheduled query

## Content
- ``schedule_query.yaml`` - Amazon CloudFormation Template
- ``runme.sh`` - Shell script to launch CloudFormation creation 

To run the CloudFormation with the shell script a S3 bucket needs to be specified where the template is uploaded
how to use:

```
sh ./run.me <existing S3 bucket name>
```



