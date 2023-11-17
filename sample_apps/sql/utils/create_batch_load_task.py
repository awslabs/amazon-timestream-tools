import boto3
import json
from botocore.config import Config
import sys
import os

# WRITE_ENDPOINT = "https://gamma-ingest-cell1.timestream.us-west-2.amazonaws.com"

##### Default values, if no arguments given in command line

REGION = "us-west-2"
DATABASE_NAME = "amazon-timestream-tools"
HT_TTL_HOURS = 1 # not need more than 1 hour, as data is likely older than 1 day anyway
CT_TTL_DAYS = 3650 # 10 years, as data can age

#TABLE_NAME = "<table name>"
#INPUT_BUCKET_NAME = "<S3 location>"
#INPUT_OBJECT_KEY_PREFIX = "<CSV file name>"
#REPORT_BUCKET_NAME = "<S3 location>"

#MAPPING_FILE = '../last_value_fill_forward/datamodel.json'

def print_usage():
    print('Usage here')
def process_arguments(argv):
    print(f"Arguments count: {len(argv)}")
    parameters = {
        'region': REGION,
        'database': DATABASE_NAME,
        'table': None,
        'input_bucket': None,
        'object_key_prefix': '/',
        'report_bucket': None,
        'mapping': None,
        'data_file': None
    }


    for i, arg in enumerate(argv):
        kv = arg.split('=')
        if len(kv) == 2:
            key = kv[0].lower()
            value = kv[1]
            if key is not None and value is not None:
                parameters[key] = value
            # print(f"Argument {i:>6}: {arg}")

    if parameters['input_bucket'] is None:
        print('input_bucket missing')
        print_usage()
        return None
    elif parameters['table'] is None:
        print('table missing')
        print_usage()
        return None
    elif parameters['data_file'] is None:
        print('table missing')
        print_usage()
        return None

    if parameters['report_bucket'] is None:
        parameters['report_bucket'] = parameters['input_bucket']

    return parameters
def load_mapping(file_name):
    try:
        f = open(file_name)
        data = json.load(f)
    except:
        print(f'File {file_name} cannot be loaded')
        return None
    return data

def create_resources(client, region, database_name, table_name, input_bucket_name, input_object_key_prefix, data_file, partition_key):

    # Upload data file
    s3 = boto3.resource('s3')
    s3c = boto3.client("s3",
                       region_name=region)
    bucket = s3.Bucket(input_bucket_name)
    head_tail = os.path.split(data_file)
    file_name = head_tail[1]
    key = input_object_key_prefix.rstrip('/') + '/' + file_name
    bucket.upload_file(data_file, key)

    #create database if not exists
    print("Creating Database")
    try:
        client.create_database(DatabaseName=database_name)
        print("Database [%s] created successfully." % database_name)
    except client.exceptions.ConflictException:
        print("Database [%s] exists. Skipping database creation" % database_name)
    except Exception as err:
        print("Create database failed:", err)
        return False

    #create table
    print("Creating table")
    retention_properties = {
        'MemoryStoreRetentionPeriodInHours': HT_TTL_HOURS,
        'MagneticStoreRetentionPeriodInDays': CT_TTL_DAYS
    }
    magnetic_store_write_properties = {
        'EnableMagneticStoreWrites': True
    }

    schema = {
        "CompositePartitionKey": [
            {
                "EnforcementInRecord": "REQUIRED",
                "Name": partition_key,
                "Type": "DIMENSION"
            }
        ]
    }

    try:
        client.create_table(DatabaseName=database_name, TableName=table_name,
                                RetentionProperties=retention_properties,
                                MagneticStoreWriteProperties=magnetic_store_write_properties,
                                Schema=schema
                            )
        print("Table [%s] successfully created." % table_name)
    except client.exceptions.ConflictException:
        print("Table [%s] exists on database [%s]. Skipping table creation" % (
            table_name, database_name))
    except Exception as err:
        print("Create table failed:", err)
        return False


    return True

def create_batch_load_task(client, database_name, table_name, input_bucket_name, input_object_key_prefix, report_bucket_name, mapping_file):
    data_model = load_mapping(mapping_file)

    if data_model is not None:

        try:
            result = client.create_batch_load_task(TargetDatabaseName=database_name, TargetTableName=table_name,
                                                   DataModelConfiguration={"DataModel": data_model
                                                   },
                                                   DataSourceConfiguration={
                                                       "DataSourceS3Configuration": {
                                                           "BucketName": input_bucket_name,
                                                           "ObjectKeyPrefix": input_object_key_prefix
                                                       },
                                                       "DataFormat": "CSV"
                                                   },
                                                   ReportConfiguration={
                                                       "ReportS3Configuration": {
                                                           "BucketName":  report_bucket_name,
                                                           "EncryptionOption": "SSE_S3"
                                                       }
                                                   }
                                                   )

            task_id = result["TaskId"]
            print("Successfully created batch load task: ", task_id)
            return task_id
        except Exception as err:
            print("Create batch load task job failed:", err)
            return None


if __name__ == '__main__':

    parameters = process_arguments(sys.argv)

    if parameters is not None:

        session = boto3.Session(region_name=parameters['region'])
        write_client = session.client('timestream-write',
    #                                  endpoint_url=WRITE_ENDPOINT,
                                      region_name=parameters['region'],
                                      config=Config(read_timeout=20, max_pool_connections=5000, retries={'max_attempts': 10}))
        print(write_client.meta.endpoint_url)
        print(write_client.meta.config.user_agent)
        print(write_client._service_model.operation_names)

        success = create_resources(write_client,
                                   parameters['region'],
                                   parameters['database'],
                                   parameters['table'],
                                   parameters['input_bucket'],
                                   parameters['object_key_prefix'],
                                   parameters['data_file'],
                                   parameters['partition_key']
                                   )



        task_id = create_batch_load_task(write_client, parameters['database'], parameters['table'],
                                         parameters['input_bucket'], parameters['object_key_prefix'], parameters['report_bucket'], parameters['mapping'])
