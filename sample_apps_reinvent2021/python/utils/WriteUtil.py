#!/usr/bin/python

import time
from utils.Constant import HT_TTL_HOURS, CT_TTL_DAYS


class WriteUtil:
    def __init__(self, client):
        self.client = client

    def create_database(self, database_name):
        print("Creating Database")
        try:
            self.client.create_database(DatabaseName=database_name)
            print("Database [%s] created successfully." % database_name)
        except self.client.exceptions.ConflictException:
            print("Database [%s] exists. Skipping database creation" % database_name)
        except Exception as err:
            print("Create database failed:", err)
            raise err

    def create_table(
        self,
        database_name,
        table_name,
        enable_magnetic_store_writes=True,
        magnetic_store_s3_bucket=None,
        memory_retention=HT_TTL_HOURS,
        magnetic_retention=CT_TTL_DAYS
    ):
        print("Creating table")
        retention_properties = {
            'MemoryStoreRetentionPeriodInHours': memory_retention,
            'MagneticStoreRetentionPeriodInDays': magnetic_retention
        }
        magnetic_store_write_properties = {
            'EnableMagneticStoreWrites': enable_magnetic_store_writes
        }
        if magnetic_store_s3_bucket:
            magnetic_store_write_properties['MagneticStoreRejectedDataLocation'] = {
                'S3Configuration': {
                    'BucketName': magnetic_store_s3_bucket,
                    'EncryptionOption': "SSE_S3"
                }
            }

        try:
            self.client.create_table(DatabaseName=database_name, TableName=table_name,
                                     RetentionProperties=retention_properties,
                                     MagneticStoreWriteProperties=magnetic_store_write_properties)
            print("Table [%s] successfully created." % table_name)
        except self.client.exceptions.ConflictException:
            print("Table [%s] exists on database [%s]. Skipping table creation" % (
                table_name, database_name))
        except Exception as err:
            print("Create table failed:", err)
            raise err

    def update_table(
        self,
        database_name,
        table_name,
        memory_retention=HT_TTL_HOURS,
        magnetic_retention=CT_TTL_DAYS,
    ):
        retention_properties = {
            'MemoryStoreRetentionPeriodInHours': memory_retention,
            'MagneticStoreRetentionPeriodInDays': magnetic_retention
        }

        try:
            self.client.update_table(DatabaseName=database_name, TableName=table_name,
                                     RetentionProperties=retention_properties)
            print("Table updated.")
        except Exception as err:
            print("Update table failed:", err)

    def delete_table(self, database_name, table_name):
        print(f"Deleting Table: {table_name}")
        try:
            result = self.client.delete_table(DatabaseName=database_name, TableName=table_name)
            if result and result['ResponseMetadata']:
                print("Delete table status [%s]" % result['ResponseMetadata']['HTTPStatusCode'])
        except self.client.exceptions.ResourceNotFoundException:
            print("Table [%s] doesn't exist" % table_name)
        except Exception as err:
            # Not raising an exception here as we want other cleanup to continue
            print("Delete table failed:", err)

    def delete_database(self, database_name):
        print(f"Deleting Database: {database_name}")
        try:
            result = self.client.delete_database(DatabaseName=database_name)
            if result and result['ResponseMetadata']:
                print("Delete database status [%s]" % result['ResponseMetadata']['HTTPStatusCode'])
        except self.client.exceptions.ResourceNotFoundException:
            print("database [%s] doesn't exist" % database_name)
        except Exception as err:
            # Not raising an exception here as we want other cleanup to continue
            print("Delete database failed:", err)

    @staticmethod
    def print_rejected_records_exceptions(err):
        print("RejectedRecords: ", err)
        for rr in err.response["RejectedRecords"]:
            print("Rejected Index " + str(rr["RecordIndex"]) + ": " + rr["Reason"])
            if "ExistingVersion" in rr:
                print("Rejected record existing version: ", rr["ExistingVersion"])

    @staticmethod
    def current_milli_time():
        return str(int(round(time.time() * 1000)))

    def describe_database(self, database_name):
        print("Describing database")
        try:
            result = self.client.describe_database(DatabaseName=database_name)
            print("Database [%s] has id [%s]" % (database_name, result['Database']['Arn']))
        except self.client.exceptions.ResourceNotFoundException:
            print("Database doesn't exist")
        except Exception as err:
            print("Describe database failed:", err)

    def update_database(self, database_name, kms_id):
        print("Updating database")
        try:
            result = self.client.update_database(DatabaseName=database_name, KmsKeyId=kms_id)
            print("Database [%s] was updated to use kms [%s] successfully" % (database_name,
                                                                              result['Database']['KmsKeyId']))
        except self.client.exceptions.ResourceNotFoundException:
            print("Database doesn't exist")
        except Exception as err:
            print("Update database failed:", err)

    def list_databases(self):
        print("Listing databases")
        try:
            result = self.client.list_databases(MaxResults=5)
            self.print_databases(result['Databases'])
            next_token = result.get('NextToken', None)
            while next_token:
                result = self.client.list_databases(NextToken=next_token, MaxResults=5)
                self.print_databases(result['Databases'])
                next_token = result.get('NextToken', None)
        except Exception as err:
            print("List databases failed:", err)

    def describe_table(self, database_name, table_name):
        print("Describing table")
        try:
            result = self.client.describe_table(DatabaseName=database_name, TableName=table_name)
            print("Table [%s] has id [%s]" % (table_name, result['Table']['Arn']))
        except self.client.exceptions.ResourceNotFoundException:
            print("Table doesn't exist")
        except Exception as err:
            print("Describe table failed:", err)

    def list_tables(self, database_name):
        print("Listing tables")
        try:
            result = self.client.list_tables(DatabaseName=database_name, MaxResults=5)
            self.print_tables(result['Tables'])
            next_token = result.get('NextToken', None)
            while next_token:
                result = self.client.list_tables(DatabaseName=database_name,
                                                 NextToken=next_token, MaxResults=5)
                self.print_tables(result['Tables'])
                next_token = result.get('NextToken', None)
        except Exception as err:
            print("List tables failed:", err)

    @staticmethod
    def print_tables(tables):
        for table in tables:
            print(table['TableName'])

    @staticmethod
    def print_databases(databases):
        for database in databases:
            print(database['DatabaseName'])

