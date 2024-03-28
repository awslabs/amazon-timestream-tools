#!/usr/bin/python

import argparse
import boto3
import json
from botocore.config import Config
from utils.logger_utils import create_logger
from utils.s3_utils import s3Utility

def main(logger, region, database, table, bucket_s3_uri, from_time, end_time, partition, iam_role_arn):

    session = boto3.Session()
    if (region is None or len(region) == 0):
        region = session.region_name #set current region

    timestream_client = session.client('timestream-query', config=Config(region_name=region))
    s3_client = boto3.client('s3')
    sts_client = boto3.client('sts')

    s3_utility = s3Utility(region, None)
    
    account_id = sts_client.get_caller_identity().get('Account')

    # Create S3 bucket if S3 URI is not provided
    if (bucket_s3_uri is None or len(bucket_s3_uri) == 0):
        bucket_name = 'timestream-unload-' + database + '-' + table + '-' + account_id
        bucket_name = bucket_name.lower()

        bucket_s3_uri = s3_utility.create_s3_bucket(bucket_name)

    # Create bucket policy for accessing data if IAM Role is provided 
    if (iam_role_arn):
        bucket_name = bucket_s3_uri.split('s3://')[1]
        bucket_name = bucket_name.split('/')[0]
        
        bucket_policy = {
            'Version': '2012-10-17',
            'Statement': [{
                'Sid': 'PermissionS3CopyGetObj',
                'Effect': 'Allow',
                'Principal': {'AWS': f'{iam_role_arn}'},
                'Action': ['s3:GetObject'],
                'Resource': f'arn:aws:s3:::{bucket_name}/*'
            },{
                'Sid': 'PermissionS3CopyListBucket',
                'Effect': 'Allow',
                'Principal': {'AWS': f'{iam_role_arn}'},
                'Action': ['s3:ListBucket'],
                'Resource': f'arn:aws:s3:::{bucket_name}'
            }
            ]
        }

        bucket_policy = json.dumps(bucket_policy)

        s3_client.put_bucket_policy(Bucket=bucket_name, Policy=bucket_policy)
   
    query = build_query(database, table, bucket_s3_uri, from_time, end_time, partition)
    run_query(logger, timestream_client, query)

def build_query(database, table, bucket_s3_uri, from_time, end_time, partition):
    unload_query = "UNLOAD("
    unload_query += " SELECT *, to_nanoseconds(time) as nanoseconds"
    if (partition):
        if (partition == "day"):
            unload_query += ", DATE_FORMAT(time,'%Y-%m-%d') as partition_date"
        elif (partition == "month"):
            unload_query += ", DATE_FORMAT(time,'%Y-%m') as partition_date"
        elif (partition == "year"):
            unload_query += ", DATE_FORMAT(time,'%Y') as partition_date"  
    unload_query += " FROM " + database + "." + table
    if (from_time and end_time):
        unload_query += " WHERE time >= '" + from_time + "' AND time < '" + end_time + "'"
    elif (from_time):
        unload_query += " WHERE time >= '" + from_time + "'"
    elif (end_time):
        unload_query += " WHERE time < '" + end_time + "'"
    unload_query += " ORDER BY "
    unload_query += " time asc )"
        
    unload_query += " TO '" + bucket_s3_uri + "'"
    unload_query += " WITH ("
    if (partition):
        unload_query += " partitioned_by = ARRAY['partition_date'],"
    unload_query += " format='csv',"
    unload_query += " compression='none',"
    unload_query += " max_file_size = '4.9GB',"
    unload_query += " include_header='true')"

    return unload_query
        
def run_query(logger, client, query):
    paginator = client.get_paginator('query')
    try:
        logger.info("QUERY EXECUTED: " + query)
        logger.info("UNLOAD IN PROGRESS...")
        page_iterator = paginator.paginate(QueryString=query)
        for page in page_iterator:
            logger.info("Progress Percentage: " + str(page['QueryStatus']['ProgressPercentage']) + "%")
            logger.debug(page)
    except Exception as err:
        logger.error("Exception while running query: ", err)
        raise
        
if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    
    parser.add_argument("-r", "--region", help="AWS region of your Timestream table to be unloaded", required=False)
    parser.add_argument("-d", "--database", help="Timestream database name where is located the table to be unloaded", required=True)
    parser.add_argument("-t", "--table", help="Timestream table name to be unloaded", required=True)
    parser.add_argument("-s", "--s3_uri", help="S3 Bucket URI to store unload data", required=False)
    parser.add_argument("-f", "--from_time", help="Timestamp from which you want to unload data (included)", required=False)
    parser.add_argument("-e", "--end_time", help="Timestamp to which you want to unload data (not included)", required=False)
    parser.add_argument("-p", "--partition", help="Partition data by 'day', 'month' or 'year'", required=False, choices=['day', 'month', 'year'])
    parser.add_argument("-i", "--iam_role_arn", help="IAM Role ARN used in the S3 Bucket policy that is applied to the S3 Bucket where unload data is stored", required=False)
   
    #assign arguments to args variable
    args = parser.parse_args()

    #create logger
    logger = create_logger("Unload Logger")

    main(logger, args.region, args.database, args.table, args.s3_uri, args.from_time, args.end_time, args.partition, args.iam_role_arn)

    logger.info("COMPLETED SUCCESSFULLY")

