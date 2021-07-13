# lambda_function.py
import json
import boto3
from botocore.config import Config
import time
import base64

DATABASE_NAME = "DemoDatabase"
TABLE_NAME = "WeatherMetrics"

def create_record(measurement, location, sensor):
    dimensions = [{"Name":"Location","Value":location},
                  {"Name":"Sensor","Value":sensor}]
    measurement['Dimensions'] = dimensions
    measurements = []
    measurements.append(measurement)
    return measurements

def create_measurement(status, timestamp):
    result = {}
    result["MeasureName"] = "Alarm"
    result["MeasureValue"] = status
    result["MeasureValueType"] = "VARCHAR"
    result["Time"] = timestamp
    result["TimeUnit"] = "MILLISECONDS"
    return result

def lambda_handler(event, context):
    session = boto3.Session()

    write_client = session.client('timestream-write',
                                  config=Config(max_pool_connections=5000,
                                                retries={'max_attempts': 10}))
    records = event.get("Records")
    for record in records:
        try:
            # Process your record
            data = record["kinesis"]["data"]
            print(data)
            data = str(base64.b64decode(base64.b64decode(data)))[2:-1]
            data_split = data.split("||")
            new_dict = {}
            for entry in data_split:
                key, value = entry.split("=")
                new_dict[key] = value
            measurement = create_measurement(new_dict['status'], new_dict['time'])
            records = create_record(measurement, new_dict['location'], new_dict['sensor'])
            result = write_client.write_records(DatabaseName=DATABASE_NAME,
                                                TableName=TABLE_NAME,
                                                Records=records,
                                                CommonAttributes={})
            print(result)

        except Exception as e:
            # Return failed record's sequence number
            print("error: {}".format(e))
            return {"batchItemFailures":[{"itemIdentifier":record["kinesis"]["sequenceNumber"]}]}

    return {"batchItemFailures":[]}

