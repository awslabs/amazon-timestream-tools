import json
import os
import boto3
from botocore.exceptions import ClientError
import time

def lambda_handler(event, context):
    """
    Lambda function to process the request and ingest records into Timestream.
    The function accepts a list of records, handles MULTI measure types,
    and sends data in batches to Timestream.

    :param event: dict: The Lambda event.
    :param context: LambdaContext: The AWS context for the event.
    :returns: A dict containing an HTTP status code and body as a result of processing the event.
    """

    query_params = event.get('queryStringParameters', {})
    precision = query_params.get('precision', 'MILLISECONDS')

    data_source_s3_bucket_name = query_params.get('dataSourceS3BucketName', '')
    report_s3_bucket_name = query_params.get('reportS3BucketName', '')

    # Creating a batch load task requires both a data source S3 bucket and a reporting S3 bucket
    if data_source_s3_bucket_name and not report_s3_bucket_name:
        return {
            "statusCode": 400,
            "body": json.dumps("The data source S3 bucket name was provided but the report S3 bucket name was missing")
        }
    if not data_source_s3_bucket_name and report_s3_bucket_name:
        return {
            "statusCode": 400,
            "body": json.dumps("The report S3 bucket name was provided but data source S3 bucket name was missing")
        }

    DATABASE_NAME = os.environ['DATABASE_NAME']
    TABLE_NAME = os.environ['TABLE_NAME']
    MEM_STORE_RETENTION_PERIOD_IN_HOURS = int(os.environ['MEM_STORE_RETENTION_PERIOD_IN_HOURS'])
    MAG_STORE_RETENTION_PERIOD_IN_DAYS = int(os.environ['MAG_STORE_RETENTION_PERIOD_IN_DAYS'])
    DIMENSION_PARTITION_KEY = os.environ.get('DIMENSION_PARTITION_KEY', None)
    PARTITION_KEY_ENFORCEMENT = os.environ.get('PARTITION_KEY_ENFORCEMENT', 'OPTIONAL')
    DATABASE_KMS_KEY_ID = os.environ.get('DATABASE_KMS_KEY_ID', None)

    BATCH_SIZE = int(os.environ.get('BATCH_SIZE', 100))

    session = boto3.session.Session()

    # Define your table retention properties
    RETENTION_PROPERTIES = {
        'MemoryStoreRetentionPeriodInHours': MEM_STORE_RETENTION_PERIOD_IN_HOURS,
        'MagneticStoreRetentionPeriodInDays': MAG_STORE_RETENTION_PERIOD_IN_DAYS
    }

    # Create the database and table if they do not exist
    create_timestream_database_and_table(session, database_name=DATABASE_NAME,
                                         table_name=TABLE_NAME,
                                         retention_properties=RETENTION_PROPERTIES,
                                         partition_key_enforcement=PARTITION_KEY_ENFORCEMENT,
                                         dimension_partition_key=DIMENSION_PARTITION_KEY,
                                         database_kms_key_id=DATABASE_KMS_KEY_ID)

    try:
        # Extract the records from the event
        body = event.get('body', '{}')
        parsed_body = json.loads(body)
        records = parsed_body.get('records', [])
        if not records:
            return {
                "statusCode": 400,
                "body": json.dumps("No records found in the request.")
            }

        # Creating an asynchronous batch load task to ingest records
        if data_source_s3_bucket_name and report_s3_bucket_name:
            data_model = get_data_model_from_record(record=records[0], precision=precision)
            data_filename = 'data.csv'
            data_filepath = write_data_file(records, data_filename)
            upload_data_file(session, data_filepath, data_filename, data_source_s3_bucket_name)
            task_id = create_batch_load_task(session, data_model, DATABASE_NAME, TABLE_NAME, data_source_s3_bucket_name, report_s3_bucket_name)

            return {
                "statusCode": 202,
                "body": json.dumps(f"Successfully created batch load task with ID {task_id} to ingest {len(records)} records")
            }

        # Ingesting records directly into Timestream for LiveAnalytics
        else:
            ingest_records(session, records=records, database_name=DATABASE_NAME, table_name=TABLE_NAME,
                           precision=precision, batch_size=BATCH_SIZE)

        query_table_metadata(session, database_name=DATABASE_NAME, table_name=TABLE_NAME)
        query_table_statistics(session, database_name=DATABASE_NAME, table_name=TABLE_NAME)

        return {
            "statusCode": 200,
            "body": json.dumps(f"Successfully ingested {len(records)} records into Timestream.")
        }
    
    except ClientError as e:
        print(f"Failed to write to Timestream: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps(f"Error writing to Timestream: {str(e)}")
        }

def create_timestream_database_and_table(session: boto3.session.Session, database_name: str, table_name: str,
                                         retention_properties: dict, partition_key_enforcement='OPTIONAL',
                                         dimension_partition_key=None, database_kms_key_id=None):
    """
    Create Timestream database and table if they do not exist.

    :param partition_key_enforcement: str: Whether to require that all records contain the partition key. Options
        are 'OPTIONAL' or 'REQUIRED'. (Default value = 'OPTIONAL')
    :param dimension_partition_key: str: The name of the dimension to use for the partition key. If not provided,
        the default partition key for the new table is 'MEASURE'. (Default value = None)
    :param database_kms_key_id: str: The ID of the KMS key to use to encrypt the newly-created Timestream for
        LiveAnalytics database. (Default value = None)
    """

    timestream_client = session.client('timestream-write')

    try:
        # Create database if it does not exist
        if database_kms_key_id is not None:
            timestream_client.create_database(DatabaseName=database_name, KmsKeyId=database_kms_key_id)
        else:
            timestream_client.create_database(DatabaseName=database_name)
        print(f"Database '{database_name}' created successfully.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConflictException':
            print(f"Database '{database_name}' already exists.")
        else:
            raise e

    if dimension_partition_key is not None:
        schema = {
            'CompositePartitionKey': [
                {
                    'Type': 'DIMENSION',
                    'Name': dimension_partition_key,
                    'EnforcementInRecord': partition_key_enforcement
                }
            ]
        }
    else:
        schema = {
            'CompositePartitionKey': [
                {
                    'Type': 'MEASURE'
                }
            ]
        }

    try:
        # Create table if it does not exist
        timestream_client.create_table(
            DatabaseName=database_name,
            TableName=table_name,
            RetentionProperties=retention_properties,
            Schema=schema
        )
        print(f"Table '{table_name}' created successfully in database '{database_name}'.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConflictException':
            print(f"Table '{table_name}' already exists in database '{database_name}'.")
        else:
            raise e

def get_data_model_from_record(record: dict, precision: str) -> dict:
    """
    Gets a data model using a single record. A data model is required to create a batch load task.

    :param record: dict: A record to use to create the data model, originating from a data generator. The record
        must be in the form:
            {
                "Dimensions": [
                    {
                        "Name": "some_dimension_name"
                        "Value": "some_dimension_value",
                        "DimensionValueType": "VARCHAR"
                    }
                ],
                "Measures": [
                    {
                        "MeasureName": "some_measure_name",
                        "MeasureValueType": "DOUBLE",
                        "MeasureValue": 90.2
                    }
                ],
                "Time": "1735689062"
            }
    :param precision: str: The Unix timestamp precision used in the record. This cannot be determined by simply examining a record.
    :returns: A dict data model to be used to create a batch load task.
    """

    data_model = {
        'TimeColumn': 'time',
        'TimeUnit': precision,
        'DimensionMappings': []
    }

    for dimension in record.get("Dimensions", []):
        dimension_mapping = { 'SourceColumn': dimension['Name'] }
        data_model['DimensionMappings'].append(dimension_mapping)

    measures = record.get("Measures", [])
    if len(measures) > 1:
        data_model['MultiMeasureMappings'] = {
            "TargetMultiMeasureName": 'metrics',
            'MultiMeasureAttributeMappings': []
        }
        for measure in measures:
            measure_mapping = {
                'SourceColumn': measure['MeasureName'],
                'MeasureValueType': measure['MeasureValueType']
            }
            data_model['MultiMeasureMappings']['MultiMeasureAttributeMappings'].append(measure_mapping)
    else:
        data_model['MixedMeasureMappings'] = []
        for measure in measures:
            measure_mapping = {
                'MeasureName': measure['MeasureName'],
                'MeasureValueType': measure['MeasureValueType']
            }
            data_model['MixedMeasureMappings'].append(measure_mapping)

    return data_model

def write_data_file(records: list, data_filename: str) -> str:
    """
    Writes a data file to temporary storage. A data file is used for creating a batch load task.

    :param records: list: A list of records, originating from a data generator.
    :param data_filename: str: The name of the file to write to, with its type extension, for example data.csv.
    :returns: The absolute path to the data file as a string.
    """

    data_filepath = f'/tmp/{data_filename}'
    with open(data_filepath, 'w', newline='') as csv_file:
        example_record = records[0]
        for dimension in example_record.get("Dimensions", []):
            csv_file.write(dimension['Name'] + ',')
        for measure in example_record.get("Measures", []):
            csv_file.write(measure['MeasureName'] + ",")
        csv_file.write("time\n")

        for record in records:
            dimensions = record.get("Dimensions", [])
            time_value = record.get("Time")
            measures = record.get("Measures", [])

            for dimension in dimensions:
                csv_file.write(dimension['Value'] + ",")
            for measure in measures:
                csv_file.write(measure['MeasureValue'] + ",")
            csv_file.write(time_value + "\n")

    return data_filepath

def upload_data_file(session: boto3.session.Session, data_filepath: str, data_filename: str, data_source_s3_bucket_name: str):
    """
    Uploads a file to an S3 bucket.

    :param session: boto3.session.Session: The session to use to make requests.
    :param data_filepath: The path to the file to be uploaded, including its name.
    :param data_filename: The name of the file to be uploaded, for example, data.csv.
    :param data_source_s3_bucket_name: The S3 bucket to upload to.
    """

    s3_client = session.client('s3')
    s3_client.upload_file(data_filepath, data_source_s3_bucket_name, data_filename)

def create_batch_load_task(session: boto3.session.Session, data_model: dict, database_name: str, table_name: str, data_source_s3_bucket_name: str, report_s3_bucket_name: str):
    """
    Creates a batch load task to asynchronously ingest data into Timestream for LiveAnalytics.

    :param session: boto3.session.Session: The session to use to make requests.
    :param data_model: dict: The data model to use to make the batch load task. This defines the dimensions, measures, and time column
        for data already existing in a CSV file in an S3 bucket.
    :param database_name: str: The name of the Timestream for LiveAnalytics database to ingest into.
    :param table_name: str: The name of the Timestream for LiveAnalytics table to ingest into.
    :param data_source_s3_bucket_name: str: The name of the S3 bucket containing the data to ingest.
    :param report_s3_bucket_name: str: An S3 bucket used to log errors during batch load ingestion.
    """

    timestream_client = session.client('timestream-write')
    try:
        response = timestream_client.create_batch_load_task(
            DataModelConfiguration={
                'DataModel': data_model
            },
            DataSourceConfiguration={
                    'DataSourceS3Configuration': {
                        'BucketName': data_source_s3_bucket_name
                    },
                    'DataFormat': 'CSV'
            },
            ReportConfiguration={
                'ReportS3Configuration': {
                    'BucketName': report_s3_bucket_name
                }
            },
            TargetDatabaseName=database_name,
            TargetTableName=table_name
        )
    except Exception:
        print("Failed to create a batch load task")
        raise

    return response['TaskId']

def ingest_records(session: boto3.session.Session, records: list, database_name: str, table_name: str, precision='MILLISECONDS', batch_size=100):
    """
    Ingests records into Timestream for LiveAnalytics.

    :param session: boto3.session.Session: The session to use to make requests.
    :param records: list: A list of records to ingest into Timestream for LiveAnalytics, formatted by a DataGenerator.
    :param database_name: str: The Timestream for LiveAnalytics database to ingest into.
    :param table_name: str: The Timestream for LiveAnalytics table to ingest into.
    :param precision: str: The time precision of the timestamps the records use. (Default value = 'MILLISECONDS')
    :param batch_size: int: The number of records to send at a time to Timestream for LiveAnalytics. 100 is the maximum
        that Timestream for LiveAnalytics allows. (Default value = 100)
    """

    timestream_client = session.client('timestream-write')

    for i in range(0, len(records), batch_size):
        records_batch = records[i:i + 100]
        # Prepare the records for Timestream
        prepared_records = []

        for record in records_batch:
            dimensions = record.get("Dimensions", [])
            time_value = record.get("Time")
            measures = record.get("Measures", [])

            # Check if there are multiple measures, in which case we'll use MULTI
            if len(measures) > 1:
                measure_value_type = 'MULTI'
                multi_value_measure = {
                    'MeasureName': 'metrics',  # General measure name, used for any multi-measure dataset
                    'MeasureValues': [
                        {
                            'Name': m['MeasureName'],
                            'Value': m['MeasureValue'],
                            'Type': m['MeasureValueType']
                        }
                        for m in measures
                    ]
                }
                prepared_record = {
                    'Dimensions': dimensions,
                    'Time': time_value,
                    'TimeUnit': precision,
                    'MeasureName': multi_value_measure['MeasureName'],
                    'MeasureValueType': measure_value_type,
                    'MeasureValues': multi_value_measure['MeasureValues']
                }
            else:
                # Handle the case where there is only one measure
                measure = measures[0]
                prepared_record = {
                    'Dimensions': dimensions,
                    'Time': time_value,
                    'TimeUnit': precision,
                    'MeasureName': measure['MeasureName'],
                    'MeasureValue': measure['MeasureValue'],
                    'MeasureValueType': measure['MeasureValueType']
                }

            prepared_records.append(prepared_record)

        # Write to Timestream using the `write_records` API
        response = timestream_client.write_records(
            DatabaseName=database_name,
            TableName=table_name,
            Records=prepared_records
        )
        print(f"Batch write successful for records {i} to {i + len(records_batch) - 1}: {response}")

def query_table_metadata(session: boto3.session.Session, database_name: str, table_name: str):
    """
    Executes a query to describe a Timestream for LiveAnalytics table.

    :param session: boto3.session.Session: The session to use for requests.
    :param database: str: The name of the Timestream for LiveAnalytics database to query.
    :param table: str: The name of the Timestream for LiveAnalytics table to query.
    """

    timestream_client = session.client('timestream-query')

    query_string = f"DESCRIBE \"{database_name}\".\"{table_name}\""

    print("Executing metadata query: " + query_string)
    query_result = timestream_client.query(QueryString=query_string)
    for row in query_result['Rows']:
        print(row)

def query_table_statistics(session: boto3.session.Session, database_name: str, table_name: str):
    """
    Executes a query to retrieve statistics for a Timestream for LiveAnalytics table.

    :param session: boto3.session.Session: The session to use for requests.
    :param database: str: The name of the Timestream for LiveAnalytics database to query.
    :param table: str: The name of the Timestream for LiveAnalytics table to query.
    """

    timestream_client = session.client('timestream-query')

    query_string = f"SELECT MIN(time), MAX(time), COUNT(*) FROM \"{database_name}\".\"{table_name}\""

    print("Executing statistics query: " + query_string)
    query_result = timestream_client.query(QueryString=query_string)
    for row in query_result['Rows']:
        print(row)
