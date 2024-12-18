import json
import os
import boto3
from botocore.exceptions import ClientError

REGION_NAME = os.environ['REGION_NAME']
DATABASE_NAME = os.environ['DATABASE_NAME']
TABLE_NAME = os.environ['TABLE_NAME']
BATCH_SIZE = int(os.environ['BATCH_SIZE'])
MEM_STORE_RETENTION_PERIOD_IN_HOURS = int(os.environ['MEM_STORE_RETENTION_PERIOD_IN_HOURS'])
MAG_STORE_RETENTION_PERIOD_IN_DAYS = int(os.environ['MAG_STORE_RETENTION_PERIOD_IN_DAYS'])

# Initialize the Timestream client
timestream_client = boto3.client('timestream-write', REGION_NAME)

# Define your table retention properties
RETENTION_PROPERTIES = {
    'MemoryStoreRetentionPeriodInHours': MEM_STORE_RETENTION_PERIOD_IN_HOURS,
    'MagneticStoreRetentionPeriodInDays': MAG_STORE_RETENTION_PERIOD_IN_DAYS
}

def create_timestream_database_and_table():
    """Create Timestream database and table if they do not exist."""
    try:
        # Create database if it does not exist
        timestream_client.create_database(DatabaseName=DATABASE_NAME)
        print(f"Database '{DATABASE_NAME}' created successfully.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConflictException':
            print(f"Database '{DATABASE_NAME}' already exists.")
        else:
            raise e

    try:
        # Create table if it does not exist
        timestream_client.create_table(
            DatabaseName=DATABASE_NAME,
            TableName=TABLE_NAME,
            RetentionProperties=RETENTION_PROPERTIES
        )
        print(f"Table '{TABLE_NAME}' created successfully in database '{DATABASE_NAME}'.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConflictException':
            print(f"Table '{TABLE_NAME}' already exists in database '{DATABASE_NAME}'.")
        else:
            raise e

def lambda_handler(event, context):
    """Lambda function to process the request and ingest records into Timestream.
    The function accepts a list of records, handles MULTI measure types,
    and sends data in batches to Timestream.

    :param event: 
    :param context: 

    """

    query_params = event.get('queryStringParameters', {})
    precision = query_params.get('precision', 'MILLISECONDS')

    # Create the database and table if they do not exist
    create_timestream_database_and_table()

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

        # Process records in batches
        for i in range(0, len(records), BATCH_SIZE):
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
                DatabaseName=DATABASE_NAME,
                TableName=TABLE_NAME,
                Records=prepared_records
            )
            print(f"Batch write successful for records {i} to {i + len(records_batch) - 1}: {response}")

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
