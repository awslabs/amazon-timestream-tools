#setup_timestream.py
import boto3
from botocore.config import Config

DATABASE_NAME = "DemoDatabase"
TABLE_NAME1 = "WeatherMetrics"
MEMORY_STORE_HOURS=1
MAGNETIC_STORE_PERIOD_DAYS=7

def create_database(client):
    print("Creating Database")
    try:
        client.create_database(DatabaseName=DATABASE_NAME)
        print("Database {} created successfully.".format(DATABASE_NAME))
    except client.exceptions.ConflictException:
        print("Database {} exists. Skipping database creation".format(DATABASE_NAME))
    except Exception as err:
        print("Create database failed:", err)

def create_table(client):
    print("Creating table")
    retention_properties = {
        'MemoryStoreRetentionPeriodInHours': MEMORY_STORE_HOURS,
        'MagneticStoreRetentionPeriodInDays': MAGNETIC_STORE_PERIOD_DAYS
        }
    try:
        client.create_table(DatabaseName=DATABASE_NAME, TableName=TABLE_NAME1,
                            RetentionProperties=retention_properties)
        print("Table {} successfully created.".format(TABLE_NAME1))
    except client.exceptions.ConflictException:
        print("Table {} exists on database {}. Skipping table creation".format(
        TABLE_NAME1, DATABASE_NAME))
    except Exception as err:
        print("Create table failed:", err)

def authenticate():
    session = boto3.Session()
    client = session.client('timestream-write', config=Config(read_timeout=20, 
                                                              max_pool_connections=5000,
                                                              retries={'max_attempts': 10}))
    return client

if __name__ == "__main__":
    client = authenticate()
    create_database(client)
    create_table(client)

