#setup_kinesis.py
import boto3
from botocore.config import Config
import json

STREAM_NAME = "BLOG"
SHARD_COUNT = 1

def create_stream(client):
    print("Creating Stream")
    try:
        client.create_stream(StreamName=STREAM_NAME,
                             ShardCount=SHARD_COUNT)
        print("Stream {} created successfully.".format(STREAM_NAME))
    except client.exceptions.ResourceInUseException:
        print("Stream {} exists. Skipping Stream creation".format(STREAM_NAME))
    except Exception as err:
        print("Create stream failed:", err)

def add_tags(client):
    print("Adding Tags")
    try:
        client.add_tags_to_stream(StreamName=STREAM_NAME,
                                  Tags=build_tags())
        print("Tags added")
    except exeption as e:
        print("Could not add tags: {}".format(e))

def build_tags():
    return {"ENVIRONMENT":"BLOG"}

def authenticate():
    session = boto3.Session()
    client = session.client('kinesis', config=Config(read_timeout=20, 
						        max_pool_connections=5000,
                                                     retries={'max_attempts': 10}))
    return client

if __name__ == "__main__":
        client = authenticate()
        create_stream(client)
        add_tags(client)

