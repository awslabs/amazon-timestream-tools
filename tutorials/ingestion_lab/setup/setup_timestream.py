import boto3
import argparse
from botocore.config import Config

DATABASE_NAME = "demo"
TABLE_1 = {"name":"Ingestion_Demo_1", "retention_properties":{"MemoryStoreRetentionPeriodInHours":168, "MagneticStoreRetentionPeriodInDays": 365}}
TABLE_2 = {"name":"Ingestion_Demo_100", "retention_properties":{"MemoryStoreRetentionPeriodInHours":168, "MagneticStoreRetentionPeriodInDays": 365}}
TABLE_3 = {"name":"Ingestion_Demo_multi", "retention_properties":{"MemoryStoreRetentionPeriodInHours":168, "MagneticStoreRetentionPeriodInDays": 365}}

def create_database():
	print("Creating Database")
	try:
	    client.create_database(DatabaseName=DATABASE_NAME)
	    print("Database {} created successfully.".format(DATABASE_NAME))
	except client.exceptions.ConflictException:
	    print("Database {} exists. Skipping database creation".format(DATABASE_NAME))
	except Exception as err:
	    print("Create database failed:", err)

def list_databases():
        print("Listing databases")
        try:
            result = client.list_databases(MaxResults=5)
            for database in result['Databases']:
            	print(database['DatabaseName'])
            next_token = result.get('NextToken', None)
            while next_token:
                result = client.list_databases(NextToken=next_token, MaxResults=5)
               	for database in result['Databases']:
               		print(database['DatabaseName'])
                next_token = result.get('NextToken', None)
        except Exception as err:
            print("List databases failed:", err)

def describe_database():
	print("Describing database")
	try:
	    result = client.describe_database(DatabaseName=DATABASE_NAME)
	    print("Database {} has id {}".format(DATABASE_NAME, result['Database']['Arn']))
	except client.exceptions.ResourceNotFoundException:
	    print("Database doesn't exist")
	except Exception as err:
	    print("Describe database failed:", err)

def delete_database():
	print("Deleting database")
	try:
	    result = client.delete_database(DatabaseName=DATABASE_NAME)
	except client.exceptions.ResourceNotFoundException:
	    print("Database doesn't exist")
	except Exception as err:
	    print("Describe database failed:", err)

def create_table():
        print("Creating table")
        try:
            client.create_table(DatabaseName=DATABASE_NAME, TableName=TABLE_1['name'],
                                     RetentionProperties=TABLE_1['retention_properties'])
            print("Table {} successfully created.".format(TABLE_1['name']))
        except client.exceptions.ConflictException:
            print("Table {} exists on database {}. Skipping table creation".format(
                TABLE_1['name'], DATABASE_NAME))
        except Exception as err:
            print("Create table failed:", err)

        try:
            client.create_table(DatabaseName=DATABASE_NAME, TableName=TABLE_2['name'],
                                     RetentionProperties=TABLE_2['retention_properties'])
            print("Table {} successfully created.".format(TABLE_2['name']))
        except client.exceptions.ConflictException:
            print("Table {} exists on database {}. Skipping table creation".format(
                TABLE_2['name'], DATABASE_NAME))
        except Exception as err:
            print("Create table failed:", err)

        try:
            client.create_table(DatabaseName=DATABASE_NAME, TableName=TABLE_3['name'],
                                     RetentionProperties=TABLE_3['retention_properties'])
            print("Table {} successfully created.".format(TABLE_3['name']))
        except client.exceptions.ConflictException:
            print("Table {} exists on database {}. Skipping table creation".format(
                TABLE_3['name'], DATABASE_NAME))
        except Exception as err:
            print("Create table failed:", err)

def delete_table():
	print("Deleting table")
	try:
		client.delete_table(DatabaseName=DATABASE_NAME, TableName=TABLE_1['name'])
		print("Table {} successfully deleted.".format(TABLE_1['name']))
	except Exception as err:
		print("Delete table failed:", err)
	
	try:
		client.delete_table(DatabaseName=DATABASE_NAME, TableName=TABLE_2['name'])
		print("Table {} successfully deleted.".format(TABLE_2['name']))
	except Exception as err:
		print("Delete table failed:", err)

	try:
		client.delete_table(DatabaseName=DATABASE_NAME, TableName=TABLE_3['name'])
		print("Table {} successfully deleted.".format(TABLE_3['name']))
	except Exception as err:
		print("Delete table failed:", err)

def authenticate():
	session = boto3.Session()
	global client
	client = session.client('timestream-write', config=Config(read_timeout=20, max_pool_connections=5000,
                                                                    retries={'max_attempts': 10}))
	return client

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('command', help='Enter either setup or destroy to create or destroy your Timestream Database')
	args = parser.parse_args()
	if args.command == "setup":
		authenticate()
		create_database()
		describe_database()
		list_databases()
		create_table()
	elif args.command == "destroy":
		authenticate()
		delete_table()
		delete_database()