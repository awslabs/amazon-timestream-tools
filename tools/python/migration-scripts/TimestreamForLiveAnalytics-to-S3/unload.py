#!/usr/bin/python

import argparse
import boto3
import json
from botocore.config import Config
from utils.logger_utils import create_logger
from utils.timestream_utils import *
from utils.s3_utils import s3Utility
from datetime import timezone
   
if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    
    parser.add_argument("-r", "--region", help="AWS region of your Timestream table to be unloaded",default=None,required=False)
    parser.add_argument("-d", "--database", help="Timestream database name", required=False)
    parser.add_argument("-t", "--table", help="Timestream table name to be unloaded", required=False)
    parser.add_argument("-s", "--s3_uri", help="S3 Bucket URI to store unload data. Example s3://abc", required=False)
    parser.add_argument("-f", "--start_time", help="UTC start timestamp for data unload.Format: 'YYYY-MM-DD HH:MM:SS'Example: '2024-03-26 17:24:38' Note: Must be in UTC timezone", required=True)
    parser.add_argument("-e", "--end_time", help="UTC end timestamp for data unload.Format: 'YYYY-MM-DD HH:MM:SS'Example: '2025-03-26 17:24:38' Note: Must be in UTC timezone", required=False)
    parser.add_argument("-p", "--partition", help="Partition data by 'hour','day', 'month' or 'year'", required=False, choices=['hour','day', 'month', 'year'])
    parser.add_argument("-ed", "--export_database", help="Number of records to unload in each batch", default=False, required=False, action='store_true')
    parser.add_argument("-et", "--export_table", help="Number of records to unload in each batch", default=False, required=False, action='store_true')
    parser.add_argument("-ead", "--export_all_databases", help="Unload all databases from a region", default=False, required=False, action='store_true')
    parser.add_argument("-sns","--sns_topic_arn", help="SNS topic ARN for sending any batchload failures", default=None, required=False)
    parser.add_argument("-edl", "--enable_dynamodb_logger", default=False,type=lambda x: x.lower() in ['true', '1', 'yes'],help="Enable DynamoDB logger.Example False",required=False)
    parser.add_argument("-mt", "--migration_tag", help="Migration tag-used as sort key to store DynamoDB", default=None, required=False)
    parser.add_argument("-ef","--export_format", help="export format", default='PARQUET',choices=['PARQUET','CSV'], required=False)
    parser.add_argument("-c","--compression", help="Compress the export files", default='NONE',choices=['NONE','GZIP'], required=False)
    parser.add_argument("-ms", "--max_file_size", help="Max individual file size in GB or MB for unload", default='78GB', required=False)
    parser.add_argument("-eb", "--escaped_by", 
                   default="\\",
                   help="""Character used for escaping in CSV files. Examples:
                   - If value is 'Time"stream' → becomes 'Time\"stream'
                   - If value is 'Time\stream' → becomes 'Time\\stream'""")
    parser.add_argument("--field_delimiter", default=",",help="Character used to separate fields in CSV files (default: comma)")
    parser.add_argument("-ik", "--kms_key", help="KMS key to be used to encrypt the data in S3", default=None, required=False)
    parser.add_argument("-en", "--encryption", help="Encryption type", default='SSE_S3', choices=['SSE_KMS', 'SSE_S3'], required=False)
    parser.add_argument("-rf", "--recent_first", default=False,type=lambda x: x.lower() in ['true', '1', 'yes'],help="et to true to load data in reverse chronological order (most recent batch first)",required=False)
 
    #assign arguments to args variable
    args = parser.parse_args()
    start_time= args.start_time 
    end_time= args.end_time
    migration_tag = args.migration_tag or f"unload-{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}"
    database = args.database
    table = args.table
    compression = args.compression
    export_format = args.export_format
    max_file_size = args.max_file_size
    kms_key = args.kms_key
    encryption = args.encryption
    escaped_by = args.escaped_by
    field_delimiter = args.field_delimiter
    recent_first = args.recent_first

    sts_client = boto3.client('sts')
    region = args.region if args.region else sts_client.meta.region_name
    partition = args.partition if args.partition is not None else 'day'


    # if args.export_database and args.export_table and args.export_database_all all are false, throw error needs to be atleast one
    if not (args.export_database or args.export_table or args.export_all_databases):
        raise ValueError("At least one of --export_database, --export_table, or --export_all_databases must be specified.")
    else:
        unload_type = 'database' if args.export_database else 'table' if args.export_table else 'all_databases'

    #create logger
    logger = create_logger("Unload Logger")

    #parse bucket
    bucket_s3_uri = args.s3_uri
    s3_utility = s3Utility(region)

    #parse sns 
    sns_topic_arn=args.sns_topic_arn
    if args.sns_topic_arn is not None:
        assert sns_topic_arn.startswith('arn:aws:sns:'), "Invalid SNS topic ARN format."
        sns_region = sns_topic_arn.split(":")[3]
        assert sns_region == region, f"The specified SNS topic ARN does not match the provided region. {region}"

    #create bucked if not provided
    if (bucket_s3_uri is None):
        account_id = sts_client.get_caller_identity().get('Account')
        bucket_name = f'timestream-dump-{account_id}-{region}'
        bucket_name = bucket_name.lower()
        bucket_s3_uri = s3_utility.create_s3_bucket(bucket_name)

    #initiate timestream utility 
    timestream_utility = timestreamUtility(region, sns_topic_arn, args.enable_dynamodb_logger)

    #Validations
    timestream_utility.validate_timestamp(start_time)

    if (end_time):
        timestream_utility.validate_timestamp(end_time)
    else:
         end_time = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

    if (start_time >= end_time):
        raise ValueError("start_time must be less than end_time")
    

    #log inputs
    logger.info(f'bucket_s3_uri {bucket_s3_uri}')
    logger.info(f'sns_topic_arn {sns_topic_arn}')
    logger.info(f'export_format {args.export_format}')
    logger.info(f'compression {args.compression}')
    logger.info(f'start_time {start_time} ')
    logger.info(f'end_time {end_time} ')
    logger.info(f'partition by {partition}')
    logger.info(f'migration_tag {migration_tag}')
    logger.info(f'unload_type {unload_type}')
    logger.info(f'max_file_size {args.max_file_size}')

    unload_params = {
        'database': database,
        'table': table,
        'bucket_s3_uri': bucket_s3_uri,
        'partition': partition,
        'export_format': export_format,
        'start_time': start_time,
        'end_time': end_time,
        'compression': compression,
        'migration_tag': migration_tag,
        'max_file_size': max_file_size,
        'kms_key': kms_key,
        'encryption': encryption,
        'escaped_by': escaped_by,
        'field_delimiter': field_delimiter,
        'recent_first' : recent_first
    }

     #Create dynamodb logging table if dynamodb logging is enabled
    timestream_utility.create_dynamodb_logger_table(table_name='timestream_unload_tracker',partition_key='DatabaseName.TableName', sort_key='timestamp_epoch')
    logging_params = {**unload_params, 'unload_type': unload_type}
    timestream_utility.log_unload(configuration=logging_params,migration_tag=migration_tag)

    # export database
    if (args.export_table):
        assert args.database is not None, "Database name is required."
        assert args.table is not None, "Table name is required."
        logger.info(f'unloading  {database}.{table}')
        timestream_utility.timestream_unload(**unload_params)
     # export database 
    elif (args.export_database):
        assert args.database is not None, "Database name is required."
        #table_list
        logger.info(f'unloading database {database}')
        tables = timestream_utility.get_all_tables(database)
        logger.info(f'all tables {tables}')
        logger.info(f'total count of tables is {len(tables)}')
        for table in tables:
            unload_params.update({
                    'table': table,
            })
            timestream_utility.timestream_unload(**unload_params)
    #export all databases 
    elif (args.export_all_databases): 
        #database list 
        all_databases = timestream_utility.get_all_databases()
        logger.info(f'all databases {all_databases}')
        logger.info(f'total count of databases is {len(all_databases)}')
        #table_list 
        total_table_count=0
        database_tables_map ={}
        for database in all_databases:
            tables = timestream_utility.get_all_tables(database)
            count_tables = len(tables)
            total_table_count += count_tables
            database_tables_map[database] = tables
        for database, tables in database_tables_map.items():
            for table in tables:
                unload_params.update({
                    'database': database,
                    'table': table,
                })
                timestream_utility.timestream_unload(**unload_params)


    message = f'Unload script completed for {unload_type} with migration tag {migration_tag}'
    if sns_topic_arn is not None:
        timestream_utility.sns_publish_message(message,f"Unload Script Completed")
    logger.info(message)



