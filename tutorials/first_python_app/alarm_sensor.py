#alarm_sensor.py
import time
import random
import uuid
import boto3
from botocore.config import Config
import base64

STREAM_NAME = "BLOG"

def generate_reading(client):
    while True:
        x = int(random.uniform(0,25))
        if x <= 20:
            status = "Normal"
        if x > 20 < 24:
            status = "Elevated"
        if x >= 24:
            status = "Alarm"

        uid = repr(uuid.uuid1().bytes)

        tm = str(int(time.time()*1000))

        data = "id={}||status={}||time={}||location={}||sensor={}".format(uid, 
                                                                          status, 
                                                                          tm, 
                                                                          "BLOG", 
                                                                          "Alarm-1")
        byte_payload = base64.b64encode((data).encode('ascii'))
        result = client.put_record(StreamName=STREAM_NAME,
                                       Data=byte_payload,
                                       PartitionKey="0")
        print(result)
        time.sleep(1)


def authenticate():
    session = boto3.Session()
    client = session.client('kinesis', config=Config(read_timeout=20,
                                                     max_pool_connections=5000,
                                                     retries={'max_attempts': 10}))
    return client

if __name__ == "__main__":
        client = authenticate()
        generate_reading(client)

