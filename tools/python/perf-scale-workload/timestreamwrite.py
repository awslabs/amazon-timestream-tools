import boto3
import json
from botocore.config import Config
import time
import sys, traceback
import pprint

'''
## Create a timestream write client.
'''
def createWriteClient(region, profile = None, credStr = None, endpoint = None):
    if profile == None and credStr == None:
        print("Using credentials from the environment")

    print("Connecting to timestream ingest in region: ", region)
    config = Config(read_timeout = 335, connect_timeout = 20, retries = {'max_attempts': 10})
    if profile != None:
        session = boto3.Session(profile_name = profile)
        if endpoint != None:
            client = session.client(service_name = 'timestream-write', endpoint_url=endpoint,
                region_name = region, config = config)
        else:
            client = session.client(service_name = 'timestream-write',
                region_name = region, config = config)
    else:
        session = boto3.Session()
        if endpoint != None:
            client = session.client(service_name = 'timestream-write', endpoint_url=endpoint,
                region_name = region, config = config)
        else:
            client = session.client(service_name = 'timestream-write',
                region_name = region, config = config)
    return client

def describeTable(client, databaseName, tableName):
    return client.describe_table(DatabaseName = databaseName, TableName = tableName)

def writeRecords(client, databaseName, tableName, commonAttributes, records):
    return client.write_records(DatabaseName = databaseName, TableName = tableName,
        CommonAttributes = (commonAttributes), Records = (records))

def writeRecords(client, databaseName, tableName, records):
    return client.write_records(DatabaseName = databaseName, TableName = tableName, Records = (records))

def createDatabase(client, databaseName):
    return client.create_database(DatabaseName = databaseName)

def createTable(client, databaseName, tableName, retentionProperties):
    return client.create_table(DatabaseName=databaseName, TableName=tableName, RetentionProperties=retentionProperties)

def describeDatabase(client, databaseName):
    return client.describe_database(DatabaseName = databaseName)

def deleteTable(client, databaseName, tableName):
    return client.delete_table(DatabaseName=databaseName, TableName=tableName)

def deleteDatabase(client, databaseName):
    return client.delete_database(DatabaseName=databaseName)

def getTableList(client, databaseName):
    tables = list()
    nextToken = None
    while True:
        if nextToken == None:
            result = client.list_tables(DatabaseName = databaseName)
        else:
            result = client.list_tables(DatabaseName = databaseName, NextToken = nextToken)
        for item in result['Tables']:
            tables.append(item['TableName'])
        nextToken = result.get('NextToken')
        if nextToken == None:
            break

    return tables