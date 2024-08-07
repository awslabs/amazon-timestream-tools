import boto3

database = "devops"
table = "sample_devops"
region = "us-east-1"
memory_store_retenion_in_hours = 24
magnetic_store_retention_in_days = 365
partition_key = "hostname"

timestream_client = boto3.client('timestream-write', region_name=region)

# create database
try:
    timestream_client.create_database(DatabaseName=database)
    print(f"Database {database} created successfully")
except timestream_client.exceptions.ConflictException:
    print(f"Database {database} exists. Skipping database creation")
except Exception as err:
    print(f"Create database failed with error : {err}")
    raise

# create table
print("Creating table")
retention_properties = {
    'MemoryStoreRetentionPeriodInHours': memory_store_retenion_in_hours,
    'MagneticStoreRetentionPeriodInDays': magnetic_store_retention_in_days
}
magnetic_store_write_properties = {
    'EnableMagneticStoreWrites': True
}

schema = {
    "CompositePartitionKey": [
        {
            "EnforcementInRecord": "REQUIRED",
            "Name": partition_key,
            "Type": "DIMENSION"
        }
    ]
}

try:
    timestream_client.create_table(DatabaseName=database, TableName=table,
                                   RetentionProperties=retention_properties,
                                   MagneticStoreWriteProperties=magnetic_store_write_properties,
                                   Schema=schema
                                   )
    print(f"Table {table} successfully created")
except timestream_client.exceptions.ConflictException:
    print(
        f"Table {table} exists on database {database}. Skipping table creation")
except Exception as err:
    print(f"Create table failed: {err}")
    raise


