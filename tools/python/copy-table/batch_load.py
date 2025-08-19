import json
import boto3 
from backoff import expo
import argparse
from utils.s3_utils import *
from utils.timestream_utils import *
from utils.logger_utils import create_logger
import sys


if __name__ == '__main__':

   parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

   #parser.add_argument("-k", "--kmsId", help="KMS key for updating the database")
   parser.add_argument("--region", help="provide the aws region",default=None,required=False)
   parser.add_argument("--database_name", help="timestream database name",default='batch_load_test', required=False)
   parser.add_argument("--table_name", help="timestream table name",default='batch_load_test', required=False)
   parser.add_argument("--partition_key", help="partition key for timestream table",default=None, required=False)
   parser.add_argument("--create_timestream_resource", help="provide this if timestream database and table have to be created", action='store_true', required=False, default=False)
   parser.add_argument("--memory_store_retenion_in_hours", type=int, help="memory store retention in hours for timestream table", required=False, default=24)
   parser.add_argument("--magnetic_store_retention_in_days", type=int, help="magnetic store retention in days for timestream table", required=False, default=3655)
   parser.add_argument("--create_error_logging_bucket", help ="provide this option if error logging bucket for batchload has to be created", action='store_true', required=False, default=False)
   parser.add_argument("--create_destination_bucket", help ="provide this option if bucket for batchload target bucket has to be created", action='store_true', required=False, default=False)
   parser.add_argument("--copy_s3_bucket", help ="provide this option if unload files have to copied from source bucket",action='store_true', required=False, default=False)
   parser.add_argument("--s3_source_bucket_location", help ="location of source s3 bucket, if copy_s3_bucket is set to true",default=None, required=False)
   parser.add_argument("--data_model_file", help ="data model JSON file location for batchload", required=True)
   parser.add_argument("--s3_target_bucket", help ="target bucket for batchload", default=None, required=False)
   parser.add_argument("--s3_target_error_bucket", help ="target bucket for batchload errors",  default=None, required=False)
   parser.add_argument("--source_s3_prefix", help ="source bucket prefix if copy_s3_bucket is set true", default="results/", required=False)
   parser.add_argument("--destination_s3_prefix", help ="desctination bucket prefix ifcopy_s3_bucket is set true", default="dest/", required=False)
   parser.add_argument("--sns_topic_arn", help="SNS topic ARN for sending any batchload failures", default=None, required=False)
   parser.add_argument("--num_of_batchload_threads", type=int,help="number of parallel batchloads threads", default=5, choices=(1,5), required=False) #nargs and const can be added. 
   parser.add_argument("--multi_part_upload_chunk", type=int, help="multi part upload chunk size in bytes, default is 500MB", default=524288000, required=False)
 
   #assign arguments to args variable
   args = parser.parse_args()

   #create logger
   logger = create_logger("migration_logger")
   

   #parse region 
   sts_client = boto3.client('sts')
   if args.region is None:
      region=sts_client.meta.region_name
   else:
      region=args.region

   logger.info(f'region {region}')

   #assign few required variable. 
   account_id = sts_client.get_caller_identity().get('Account')
   database = args.database_name
   table = args.table_name

   #sns region check 
   sns_topic_arn=args.sns_topic_arn
   if args.sns_topic_arn is not None:
      assert sns_topic_arn.startswith('arn:aws:sns:'), "Invalid SNS topic ARN format."
      sns_region = sns_topic_arn.split(":")[3]
      assert sns_region == region, f"The specified SNS topic ARN does not match the provided region. {region}"


   #Initiate s3 and timestream utilities 
   s3_utility = s3Utility(region,args.multi_part_upload_chunk)
   timestream_utility = timestreamUtility(region, database, table, sns_topic_arn)


   #assign default bucket names if not provided 
   bucket_suffix = f'{database}-{table}-{account_id}-{region}'
   s3_target_bucket = args.s3_target_bucket if args.s3_target_bucket is not None else f'timestream-batchload-{bucket_suffix}'
   logger.info(f's3_target_bucket_location  {s3_target_bucket}')
   s3_target_error_bucket = args.s3_target_error_bucket if args.s3_target_error_bucket is not None else f'timestream-batchload-error-{bucket_suffix}'
   logger.info(f's3_target_error_bucket_location {s3_target_error_bucket}')
   

   #create destination buckets
   if args.create_destination_bucket:
      s3_utility.create_s3_bucket(s3_target_bucket)

   if args.create_error_logging_bucket:
      s3_utility.create_s3_bucket(s3_target_error_bucket)

   #create database and required  if required
   if args.create_timestream_resource and args.partition_key is None:
      raise ValueError("Partition key must be provided if create_timestream_resource is set to true.")
   elif args.create_timestream_resource and args.partition_key is not None:
      timestream_utility.create_timestream_res(args.partition_key, args.memory_store_retenion_in_hours, args.magnetic_store_retention_in_days)
   


   #append "/" if user misses providing in the end for source and target prefix
   if not args.source_s3_prefix.endswith('/'):
      args.source_s3_prefix += '/'
    
   if not args.destination_s3_prefix.endswith('/'):
      args.destination_s3_prefix += '/'

   source_s3_prefix = f"{args.source_s3_prefix}"    
   dest_s3_prefix = f"{args.destination_s3_prefix}" 
   #final_dest_s3_prefix = f'{dest_s3_prefix}'  


   #copy source S3 content to target only CSV files.
   if args.copy_s3_bucket:
      if args.s3_source_bucket_location is None:
         logger.error(f'Provide the source bucket name with argument s3_source_bucket_location')
         sys.exit()
      else:
         s3_utility.copy_multiple_s3_objects(args.s3_source_bucket_location, s3_target_bucket, source_s3_prefix, dest_s3_prefix)
   

   #load data model file 
   try:
      f = open(args.data_model_file)
      data_model = json.load(f)  
      logger.info(f'Using datamodel {data_model}')
   except:
      logger.error(f'File {args.data_model_file} cannot be loaded, please check files exists and is correct path is provided')

   #batchload
   all_csv_files_list,sorted_list_s3_partitions= s3_utility.list_s3_object_custom(s3_target_bucket, dest_s3_prefix)
   #make sure you empty the target folder before retrying for any error to write in README.
   logger.info(f'all destination CSV files : {all_csv_files_list}')
   logger.info(f'sorted partition list for batchload : {sorted_list_s3_partitions}')
   timestream_utility.multi_thread_handler(args.num_of_batchload_threads, sorted_list_s3_partitions, data_model, s3_target_bucket, s3_target_error_bucket)

