import json
import os
import boto3
from botocore.exceptions import ClientError

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

    DATABASE_NAME = os.environ['DATABASE_NAME']
    TABLE_NAME = os.environ['TABLE_NAME']
    MEM_STORE_RETENTION_PERIOD_IN_HOURS = int(os.environ['MEM_STORE_RETENTION_PERIOD_IN_HOURS'])
    MAG_STORE_RETENTION_PERIOD_IN_DAYS = int(os.environ['MAG_STORE_RETENTION_PERIOD_IN_DAYS'])
    DIMENSION_PARTITION_KEY = os.environ.get('DIMENSION_PARTITION_KEY', None)
    PARTITION_KEY_ENFORCEMENT = os.environ.get('PARTITION_KEY_ENFORCEMENT', 'OPTIONAL')
    DATABASE_KMS_KEY_ID = os.environ.get('DATABASE_KMS_KEY_ID', None)

    BATCH_SIZE = int(os.environ['BATCH_SIZE'])

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
        ingest_records(session, records=records, database_name=DATABASE_NAME, table_name=TABLE_NAME,
                       precision=precision, batch_size=BATCH_SIZE)

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