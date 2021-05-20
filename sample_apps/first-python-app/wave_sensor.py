#wave_sensor.py
import time
import boto3
from botocore.config import Config

DATABASE_NAME = "DemoDatabase"
TABLE_NAME = "WeatherMetrics"

def create_record(measurement):
    dimensions = [{"Name":"Location","Value":"Blog"},
		 {"Name":"Sensor","Value":"1"}]
    measurement['Dimensions'] = dimensions
    measurements = []
    measurements.append(measurement)
    return measurements

def create_measurement(metric):
    result = {}
    result["MeasureName"] = "Temperature"
    result["MeasureValue"] = metric
    result["MeasureValueType"] = "DOUBLE"
    result["Time"] = str(int(time.time()*1000))
    result["TimeUnit"] = "MILLISECONDS"
    return result


def generate_wave(high, low, start, client):
    x = start
    y = 1
    while True:
        if y > 0 and x < high - 2:
            y = 1
        elif y > 0 and x > high -2:
            y = -1
        elif y < 0 and x < low + 2:
            y = 1
        x = x + y
        measurement = create_measurement(str(x))
        records = create_record(measurement)
        print(records)
        result = client.write_records(DatabaseName=DATABASE_NAME,
                                      TableName=TABLE_NAME,
                                      Records=records,
                                      CommonAttributes={})
        print(result)
        time.sleep(1)


def authenticate():
    session = boto3.Session()
    client = session.client('timestream-write', config=Config(read_timeout=20,
                                                              max_pool_connections=5000,
                                                              retries={'max_attempts': 10}))
    return client

if __name__ == "__main__":
    client = authenticate()
    generate_wave(100, 32, 60, client)

