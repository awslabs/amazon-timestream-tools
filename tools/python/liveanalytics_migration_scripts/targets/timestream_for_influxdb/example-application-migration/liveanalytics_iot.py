#!/usr/bin/env python3
"""
A simple application providing a basic example of how to use Timestream for LiveAnalytics APIs.
This application reads a sample JSON dataset and creates Timestream records to ingest into Timestream for LiveAnalytics.
"""

import boto3
from botocore.config import Config
import json
from datetime import datetime

# Constants
MAX_BATCH_SIZE = 100  # Timestream has a max batch size of 100 records

def create_timestream_client():
    """
    Create a Timestream write client.

    Returns:
        boto3.client: Configured Timestream write client
    """
    config = Config(
        read_timeout=20,  # Timestream recommends a 20-second timeout
        max_pool_connections=5000,
        retries={"max_attempts": 10}
    )

    return boto3.client('timestream-write', config=config)


def create_database_if_nonexistent(client, database_name):
    """
    Create a Timestream database if it doesn't already exist.

    Args:
        client (boto3.client): Timestream write client
        database_name (str): Name of the Timestream database
    """
    try:
        client.create_database(DatabaseName=database_name)
        print(f"Created database: {database_name}")
    except client.exceptions.ConflictException:
        print(f"Database {database_name} already exists")
    except Exception as e:
        print(f"Error creating database: {e}")
        raise


def create_table_if_nonexistent(client, database_name, table_name):
    """
    Create a Timestream table if it doesn't already exist.

    Args:
        client (boto3.client): Timestream write client
        database_name (str): Name of the Timestream database
        table_name (str): Name of the Timestream table
    """
    try:
        client.create_table(
            DatabaseName=database_name,
            TableName=table_name,
            RetentionProperties={
                'MemoryStoreRetentionPeriodInHours': 8766,
                'MagneticStoreRetentionPeriodInDays': 1068
            }
        )
        print(f"Created table: {table_name}")
    except client.exceptions.ConflictException:
        print(f"Table {table_name} already exists")
    except Exception as e:
        print(f"Error creating table: {e}")
        raise


def create_record():
    """
    Create an empty Timestream record structure.

    Returns:
        dict: An empty record structure
    """
    return {
        'Dimensions': [],
        'MeasureName': '',
        'MeasureValues': [],
        'MeasureValueType': 'MULTI',  # Using multi-measure records
        'Time': '',
        'TimeUnit': ''
    }


def set_record_dimensions(record, dimensions):
    """
    Set the dimensions for a Timestream record.

    Args:
        record (dict): The record to modify
        dimensions (dict): Dictionary of dimension names and values

    Returns:
        dict: The modified record
    """
    record['Dimensions'] = [
        {'Name': name, 'Value': value}
        for name, value in dimensions.items()
    ]
    return record


def set_record_measures(record, measures):
    """
    Set the measures for a Timestream record.

    Args:
        record (dict): The record to modify
        measures (dict): Dictionary of measure names, values, and types

    Returns:
        dict: The modified record
    """
    record['MeasureValues'] = [
        {
            'Name': name,
            'Value': str(details['value']),
            'Type': details['type']
        }
        for name, details in measures.items()
    ]
    return record


def set_record_timestamp(record, timestamp):
    """
    Set the timestamp for a Timestream record.

    Args:
        record (dict): The record to modify
        timestamp (str): The timestamp string

    Returns:
        dict: The modified record
    """

    # Parse the timestamp string with nanosecond precision
    # First, split the string into datetime part and nanosecond part
    datetime_part = timestamp[:26]  # Up to microseconds
    nanosecond_part = timestamp[26:] if len(timestamp) > 26 else "000"

    # Parse the datetime part
    dt = datetime.strptime(datetime_part, "%Y-%m-%d %H:%M:%S.%f")

    # Convert to nanosecond precision timestamp
    # First convert to seconds since epoch
    epoch_seconds = dt.timestamp()
    # Convert to nanoseconds and add the nanosecond part
    epoch_nanoseconds = int(epoch_seconds * 1_000_000_000) + int(nanosecond_part)

    record['Time'] = str(epoch_nanoseconds)

    return record


def set_record_time_unit(record, time_unit):
    """
    Set the time unit for a Timestream record.

    Args:
        record (dict): The record to modify
        time_unit (str): The time unit (e.g., 'MILLISECONDS', 'NANOSECONDS')

    Returns:
        dict: The modified record
    """
    record['TimeUnit'] = time_unit
    return record


def set_record_measure_name(record, measure_name):
    """
    Set the measure name for a Timestream record.

    Args:
        record (dict): The record to modify
        measure_name (str): The measure name

    Returns:
        dict: The modified record
    """
    record['MeasureName'] = measure_name
    return record


def load_json_data(file_path):
    """
    Load JSON data from a file.

    Args:
        file_path (str): Path to the JSON file

    Returns:
        list: List of records from the JSON file
    """
    with open(file_path, 'r') as f:
        return json.load(f)


def build_liveanalytics_records(json_records):
    """
    Convert JSON records to Timestream record format.

    Args:
        json_records (list): List of records from the JSON file

    Returns:
        list: List of records in Timestream format
    """
    timestream_records = []

    for json_record in json_records:
        record = create_record()

        record = set_record_dimensions(record, json_record['dimensions'])

        record = set_record_measures(record, json_record['measures'])

        record = set_record_timestamp(record, json_record['timestamp'])

        record = set_record_time_unit(record, json_record['time_unit'])

        record = set_record_measure_name(record, 'system_metrics')
 
        timestream_records.append(record)

    return timestream_records


def write_records(client, database_name, table_name, records):
    """
    Write records to Timestream in batches of max 100 records.

    Args:
        client (boto3.client): Timestream write client
        database_name (str): Name of the Timestream database
        table_name (str): Name of the Timestream table
        records (list): List of records to write

    Returns:
        int: Number of records written
    """
    total_records = len(records)
    records_written = 0

    # Process records in batches of MAX_BATCH_SIZE (100 records is Timestream maximum)
    for i in range(0, total_records, MAX_BATCH_SIZE):
        batch = records[i:i + MAX_BATCH_SIZE]
        try:
            client.write_records(
                DatabaseName=database_name,
                TableName=table_name,
                Records=batch
            )
            records_written += len(batch)
            print(f"Successfully wrote {len(batch)} records. Total: {records_written}/{total_records}")
        except client.exceptions.RejectedRecordsException as e:
            print(f"Some records were rejected: {e}")
            for rejected in e.response["RejectedRecords"]:
                print(f"Rejected record at index {rejected['RecordIndex']}: {rejected['Reason']}")
        except Exception as e:
            print(f"Error writing records: {e}")

    return records_written


def main():
    """
    Main function to run the application.
    """
    database_name = "timestream_iot_sample"
    table_name = "iot_data"
    client = create_timestream_client()

    # Create database and table if they don't exist
    create_database_if_nonexistent(client, database_name)
    create_table_if_nonexistent(client, database_name, table_name)

    json_data_path = "data/sample-multi.json"
    print(f"Loading data from {json_data_path}")
    json_records = load_json_data(json_data_path)
    print(f"Loaded {len(json_records)} records from JSON file")

    # Convert JSON records to Timestream format
    timestream_records = build_liveanalytics_records(json_records)

    # Write records to Timestream
    records_written = write_records(client, database_name, table_name, timestream_records)

    # Print summary
    print("\nSummary:")
    print(f"Total records processed: {len(json_records)}")
    print(f"Records successfully written: {records_written}")


if __name__ == "__main__":
    main()

