#cleanup.py
import boto3
import argparse
from botocore.config import Config
import json

STREAM_NAME = "BLOG"
DATABASE_NAME = "DemoDatabase"
TABLE_NAME1 = "WeatherMetrics"
FUNCTION_NAME = "kinesis-log-consumer"

def delete_stream(client):
	print("Deleting stream")
	try:
	    result = client.delete_stream(StreamName=STREAM_NAME)
	except client.exceptions.ResourceNotFoundException:
	    print("Stream doesn't exist")
	except Exception as err:
		print("Delete stream failed:", err)

def delete_database(client):
	print("Deleting database")
	try:
	    result = client.delete_database(DatabaseName=DATABASE_NAME)
	except client.exceptions.ResourceNotFoundException:
	    print("Database doesn't exist")
	except Exception as err:
	    print("Describe database failed:", err)

def delete_table(client):
	print("Deleting table")
	try:
		client.delete_table(DatabaseName=DATABASE_NAME, TableName=TABLE_NAME1)
		print("Table {} successfully deleted.".format(TABLE_NAME1))
	except Exception as err:
		print("Delete table failed:", err)

def delete_lambda(client):
	print("Deleting function")
	try:
		client.delete_function(FunctionName=FUNCTION_NAME)
		print("Function {} successfully deleted.".format(FUNCTION_NAME))
	except Exception as err:
		print("Delete function failed:", err)

def authenticate_kinesis():
	session = boto3.Session()
	client = session.client('kinesis')
	return client

def authenticate_timestream():
	session = boto3.Session()
	global client
	client = session.client('timestream-write', config=Config())
	return client

def authenticate_lambda():
	session = boto3.Session()
	global client
	client = session.client('lambda', config=Config())
	return client


if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	args = parser.parse_args()
	kinesis_client = authenticate_kinesis()
	delete_stream(kinesis_client)
	timestream_client = authenticate_timestream()
	delete_table(timestream_client)
	delete_database(timestream_client)
	lambda_client = authenticate_lambda()
	delete_lambda(lambda_client)

