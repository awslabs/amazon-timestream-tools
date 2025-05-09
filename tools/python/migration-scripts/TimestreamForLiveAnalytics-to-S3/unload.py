#!/usr/bin/python

import argparse
import boto3
import json
from botocore.config import Config
from utils.logger_utils import create_logger
from utils.timestream_utils import *
from utils.s3_utils import s3Utility
from datetime import timezone
import sys
   
if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    
    parser.add_argument("-r", "--region", help="AWS region of your Timestream table to be unloaded",default=None,required=False)
    parser.add_argument("-d", "--database", help="Timestream database name", required=False)
    parser.add_argument("-t", "--table", help="Timestream table name to be unloaded", required=False)
    parser.add_argument("-s", "--s3-uri", help="S3 Bucket URI to store unload data. Example s3://abc", required=False)
    parser.add_argument("-f", "--start-time", help="UTC start timestamp for data unload.Format: 'YYYY-MM-DD HH:MM:SS'Example: '2024-03-26 17:24:38' Note: Must be in UTC timezone", required=True)
    parser.add_argument("-e", "--end-time", help="UTC end timestamp for data unload.Format: 'YYYY-MM-DD HH:MM:SS'Example: '2025-03-26 17:24:38' Note: Must be in UTC timezone", required=False)
    parser.add_argument("-p", "--partition", help="Partition data by 'hour','day', 'month' or 'year'", required=False, choices=['hour','day', 'month', 'year'])
    parser.add_argument("-ed", "--export-database", help="export all tables within a database", default=False, required=False, action='store_true')
    parser.add_argument("-et", "--export-table", help="export a single table", default=False, required=False, action='store_true')
    parser.add_argument("-ead", "--export-all-databases", help="export all databases in a region and account", default=False, required=False, action='store_true')
    parser.add_argument("-sns","--sns-topic-arn", help="SNS topic ARN for sending any batchload failures", default=None, required=False)
    parser.add_argument("-edl", "--enable-dynamodb-logger", default=False,type=lambda x: x.lower() in ['true', '1', 'yes'],help="Enable DynamoDB logger.Example False",required=False)
    parser.add_argument("-mt", "--migration-tag", help="Migration tag-used as sort key to store DynamoDB", default=None, required=False)
    parser.add_argument("-ef","--export-format", help="export format", default='PARQUET',choices=['PARQUET','CSV'], required=False)
    parser.add_argument("-c","--compression", help="Compress the export files", default='NONE',choices=['NONE','GZIP'], required=False)
    parser.add_argument("-ms", "--max-file-size", help="Max individual file size in GB or MB for unload", default='78GB', required=False)
    parser.add_argument("-eb", "--escaped-by", 
                   default="\\",
                   help="""Character used for escaping in CSV files. Examples:
                   - If value is 'Time"stream' → becomes 'Time\"stream'
                   - If value is 'Time\stream' → becomes 'Time\\stream'""")
    parser.add_argument("--field-delimiter", default=",",help="Character used to separate fields in CSV files (default: comma)")
    parser.add_argument("-ik", "--kms-key", help="KMS key to be used to encrypt the data in S3", default=None, required=False)
    parser.add_argument("-en", "--encryption", help="Encryption type", default='SSE_S3', choices=['SSE_KMS', 'SSE_S3'], required=False)
    parser.add_argument("-rf", "--recent-first", default=False,type=lambda x: x.lower() in ['true', '1', 'yes'],help="Set to true to load data in reverse chronological order (most recent batch first)",required=False)
    parser.add_argument("-cp", "--custom-partition-count", help="Custom partition count", default=99, required=False)
    parser.add_argument("-ob", "--order-by-asc", help="data order by ascending", default=False, type=lambda x: x.lower() in ['true', '1', 'yes'], required=False)

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
    custom_partition_count = args.custom_partition_count
    order_by_asc = args.order_by_asc 

    sts_client = boto3.client('sts')
    region = args.region if args.region else sts_client.meta.region_name
    partition = args.partition if args.partition is not None else 'day'


    # Check that exactly one export option is selected
    export_options_count = sum([args.export_database, args.export_table, args.export_all_databases])
    if export_options_count == 0:
        raise ValueError("At least one of --export_database, --export_table, or --export_all_databases must be specified.")
    elif export_options_count > 1:
        raise ValueError("Only one of --export_database, --export_table, or --export_all_databases can be specified at a time.")
    else:
        unload_type = 'database' if args.export_database else 'table' if args.export_table else 'all_databases'

    #create logger
    logger = create_logger("Unload Logger")

    #parse bucket
    bucket_s3_uri = args.s3_uri
    s3_utility = s3Utility(region)

    #parse sns 
    sns_topic_arn=args.sns_topic_arn
    timestream_utility = timestreamUtility(region, sns_topic_arn, args.enable_dynamodb_logger)
    if args.sns_topic_arn is not None:
        if not timestream_utility.validate_sns_topic(sns_topic_arn):
            sys.exit(1)
   
    #create bucked if not provided
    if (bucket_s3_uri is None):
        account_id = sts_client.get_caller_identity().get('Account')
        bucket_name = f'timestream-dump-{account_id}-{region}'
        bucket_name = bucket_name.lower()
        bucket_s3_uri = s3_utility.create_s3_bucket(bucket_name)
    else:
        logger.info(f"Using provided S3 URI: {bucket_s3_uri}")

    #initiate timestream utility 
    

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
    logger.info(f'encryption {encryption}')
    logger.info(f'escaped_by {escaped_by}')
    logger.info(f'field_delimiter {field_delimiter}')
    logger.info(f'recent_first {recent_first}')
    logger.info(f'kms_key {kms_key}')
    logger.info(f'enable_dynamodb_logger {args.enable_dynamodb_logger}')
    logger.info(f'region {region}')
    logger.info(f'database {database}, it may print None if exporting all databases for specific region')
    logger.info(f'table {table}, it may print None if exporting a database or all databases')
    logger.info(f'custom_partition_count {custom_partition_count}')
    logger.info(f'order_by_asc {order_by_asc}')

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
        'recent_first' : recent_first,
        'custom_partition_count' : custom_partition_count,
        'order_by_asc' : order_by_asc
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


