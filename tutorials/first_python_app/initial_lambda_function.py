# lambda_function.py
import json
import boto3
from botocore.config import Config
import time
import base64

DATABASE_NAME = "DemoDatabase"
TABLE_NAME = "WeatherMetrics"

def lambda_handler(event, context):
    session = boto3.Session()

    write_client = session.client('timestream-write', 
                                  config=Config(read_timeout=20, 
                                                max_pool_connections=5000,
                                                retries={'max_attempts': 10}))
    records = event.get("Records")
    for record in records:
        try:
            # Process your record
            data = base64.b64decode(record["kinesis"]["data"])
            print(data)
        except Exception as e:
            # Return failed record's sequence number
            print("error: {}".format(e))

