import Constant
import time


class CrudAndSimpleIngestionExample:
    def __init__(self, client):
        self.client = client

    def create_database(self):
        print("Creating Database")
        try:
            self.client.create_database(DatabaseName=Constant.DATABASE_NAME)
            print("Database [%s] created successfully." % Constant.DATABASE_NAME)
        except self.client.exceptions.ConflictException:
            print("Database [%s] exists. Skipping database creation" % Constant.DATABASE_NAME)
        except Exception as err:
            print("Create database failed:", err)

    def describe_database(self):
        print("Describing database")
        try:
            result = self.client.describe_database(DatabaseName=Constant.DATABASE_NAME)
            print("Database [%s] has id [%s]" % (Constant.DATABASE_NAME, result['Database']['Arn']))
        except self.client.exceptions.ResourceNotFoundException:
            print("Database doesn't exist")
        except Exception as err:
            print("Describe database failed:", err)

    def update_database(self, kms_id):
        print("Updating database")
        try:
            result = self.client.update_database(DatabaseName=Constant.DATABASE_NAME, KmsKeyId=kms_id)
            print("Database [%s] was updated to use kms [%s] successfully" % (Constant.DATABASE_NAME,
                                                                              result['Database']['KmsKeyId']))
        except self.client.exceptions.ResourceNotFoundException:
            print("Database doesn't exist")
        except Exception as err:
            print("Update database failed:", err)

    def list_databases(self):
        print("Listing databases")
        try:
            result = self.client.list_databases(MaxResults=5)
            self._print_databases(result['Databases'])
            next_token = result.get('NextToken', None)
            while next_token:
                result = self.client.list_databases(NextToken=next_token, MaxResults=5)
                self._print_databases(result['Databases'])
                next_token = result.get('NextToken', None)
        except Exception as err:
            print("List databases failed:", err)

    def create_table(self):
        print("Creating table")
        retention_properties = {
            'MemoryStoreRetentionPeriodInHours': Constant.HT_TTL_HOURS,
            'MagneticStoreRetentionPeriodInDays': Constant.CT_TTL_DAYS
        }
        try:
            self.client.create_table(DatabaseName=Constant.DATABASE_NAME, TableName=Constant.TABLE_NAME,
                                     RetentionProperties=retention_properties)
            print("Table [%s] successfully created." % Constant.TABLE_NAME)
        except self.client.exceptions.ConflictException:
            print("Table [%s] exists on database [%s]. Skipping table creation" % (
                Constant.TABLE_NAME, Constant.DATABASE_NAME))
        except Exception as err:
            print("Create table failed:", err)

    def update_table(self):
        print("Updating table")
        retention_properties = {
            'MemoryStoreRetentionPeriodInHours': Constant.HT_TTL_HOURS,
            'MagneticStoreRetentionPeriodInDays': Constant.CT_TTL_DAYS
        }
        try:
            self.client.update_table(DatabaseName=Constant.DATABASE_NAME, TableName=Constant.TABLE_NAME,
                                     RetentionProperties=retention_properties)
            print("Table updated.")
        except Exception as err:
            print("Update table failed:", err)

    def describe_table(self):
        print("Describing table")
        try:
            result = self.client.describe_table(DatabaseName=Constant.DATABASE_NAME, TableName=Constant.TABLE_NAME)
            print("Table [%s] has id [%s]" % (Constant.TABLE_NAME, result['Table']['Arn']))
        except self.client.exceptions.ResourceNotFoundException:
            print("Table doesn't exist")
        except Exception as err:
            print("Describe table failed:", err)

    def list_tables(self):
        print("Listing tables")
        try:
            result = self.client.list_tables(DatabaseName=Constant.DATABASE_NAME, MaxResults=5)
            self.__print_tables(result['Tables'])
            next_token = result.get('NextToken', None)
            while next_token:
                result = self.client.list_tables(DatabaseName=Constant.DATABASE_NAME,
                                                 NextToken=next_token, MaxResults=5)
                self.__print_tables(result['Tables'])
                next_token = result.get('NextToken', None)
        except Exception as err:
            print("List tables failed:", err)

    def write_records(self):
        print("Writing records")
        current_time = self._current_milli_time()

        dimensions = [
            {'Name': 'region', 'Value': 'us-east-1'},
            {'Name': 'az', 'Value': 'az1'},
            {'Name': 'hostname', 'Value': 'host1'}
        ]

        cpu_utilization = {
            'Dimensions': dimensions,
            'MeasureName': 'cpu_utilization',
            'MeasureValue': '13.5',
            'MeasureValueType': 'DOUBLE',
            'Time': current_time
        }

        memory_utilization = {
            'Dimensions': dimensions,
            'MeasureName': 'memory_utilization',
            'MeasureValue': '40',
            'MeasureValueType': 'DOUBLE',
            'Time': current_time
        }

        records = [cpu_utilization, memory_utilization]

        try:
            result = self.client.write_records(DatabaseName=Constant.DATABASE_NAME, TableName=Constant.TABLE_NAME,
                                               Records=records, CommonAttributes={})
            print("WriteRecords Status: [%s]" % result['ResponseMetadata']['HTTPStatusCode'])
        except self.client.exceptions.RejectedRecordsException as err:
            self._print_rejected_records_exceptions(err)
        except Exception as err:
            print("Error:", err)

    def write_records_with_common_attributes(self):
        print("Writing records extracting common attributes")
        current_time = self._current_milli_time()

        dimensions = [
            {'Name': 'region', 'Value': 'us-east-1'},
            {'Name': 'az', 'Value': 'az1'},
            {'Name': 'hostname', 'Value': 'host1'}
        ]

        common_attributes = {
            'Dimensions': dimensions,
            'MeasureValueType': 'DOUBLE',
            'Time': current_time
        }

        cpu_utilization = {
            'MeasureName': 'cpu_utilization',
            'MeasureValue': '13.5'
        }

        memory_utilization = {
            'MeasureName': 'memory_utilization',
            'MeasureValue': '40'
        }

        records = [cpu_utilization, memory_utilization]

        try:
            result = self.client.write_records(DatabaseName=Constant.DATABASE_NAME, TableName=Constant.TABLE_NAME,
                                               Records=records, CommonAttributes=common_attributes)
            print("WriteRecords Status: [%s]" % result['ResponseMetadata']['HTTPStatusCode'])
        except self.client.exceptions.RejectedRecordsException as err:
            self._print_rejected_records_exceptions(err)
        except Exception as err:
            print("Error:", err)

    def write_records_with_upsert(self):
        print("Writing records with upsert")
        current_time = self._current_milli_time()
        # To achieve upsert (last writer wins) semantic, one example is to use current time as the version if you are writing directly from the data source
        version = int(self._current_milli_time())

        dimensions = [
                    {'Name': 'region', 'Value': 'us-east-1'},
                    {'Name': 'az', 'Value': 'az1'},
                    {'Name': 'hostname', 'Value': 'host1'}
                ]

        common_attributes = {
            'Dimensions': dimensions,
            'MeasureValueType': 'DOUBLE',
            'Time': current_time,
            'Version': version
        }

        cpu_utilization = {
            'MeasureName': 'cpu_utilization',
            'MeasureValue': '13.5'
        }

        memory_utilization = {
            'MeasureName': 'memory_utilization',
            'MeasureValue': '40'
        }

        records = [cpu_utilization, memory_utilization]

        # write records for first time
        try:
            result = self.client.write_records(DatabaseName=Constant.DATABASE_NAME, TableName=Constant.TABLE_NAME,
                                               Records=records, CommonAttributes=common_attributes)
            print("WriteRecords Status for first time: [%s]" % result['ResponseMetadata']['HTTPStatusCode'])
        except self.client.exceptions.RejectedRecordsException as err:
            self._print_rejected_records_exceptions(err)
        except Exception as err:
            print("Error:", err)

        # Successfully retry same writeRecordsRequest with same records and versions, because writeRecords API is idempotent.
        try:
            result = self.client.write_records(DatabaseName=Constant.DATABASE_NAME, TableName=Constant.TABLE_NAME,
                                               Records=records, CommonAttributes=common_attributes)
            print("WriteRecords Status for retry: [%s]" % result['ResponseMetadata']['HTTPStatusCode'])
        except self.client.exceptions.RejectedRecordsException as err:
            self._print_rejected_records_exceptions(err)
        except Exception as err:
            print("Error:", err)

        # upsert with lower version, this would fail because a higher version is required to update the measure value.
        version -= 1
        common_attributes["Version"] = version

        cpu_utilization["MeasureValue"] = '14.5'
        memory_utilization["MeasureValue"] = '50'

        upsertedRecords = [cpu_utilization, memory_utilization]

        try:
            upsertedResult = self.client.write_records(DatabaseName=Constant.DATABASE_NAME, TableName=Constant.TABLE_NAME,
                                                      Records=upsertedRecords, CommonAttributes=common_attributes)
            print("WriteRecords Status for upsert with lower version: [%s]" % upsertedResult['ResponseMetadata']['HTTPStatusCode'])
        except self.client.exceptions.RejectedRecordsException as err:
            self._print_rejected_records_exceptions(err)
        except Exception as err:
            print("Error:", err)


        # upsert with higher version as new data is generated
        version = int(self._current_milli_time())
        common_attributes["Version"] = version

        try:
            upsertedResult = self.client.write_records(DatabaseName=Constant.DATABASE_NAME, TableName=Constant.TABLE_NAME,
                                                      Records=upsertedRecords, CommonAttributes=common_attributes)
            print("WriteRecords Upsert Status: [%s]" % upsertedResult['ResponseMetadata']['HTTPStatusCode'])
        except self.client.exceptions.RejectedRecordsException as err:
            self._print_rejected_records_exceptions(err)
        except Exception as err:
            print("Error:", err)

    def delete_table(self):
        print("Deleting Table")
        try:
            result = self.client.delete_table(DatabaseName=Constant.DATABASE_NAME, TableName=Constant.TABLE_NAME)
            print("Delete table status [%s]" % result['ResponseMetadata']['HTTPStatusCode'])
        except self.client.exceptions.ResourceNotFoundException:
            print("Table [%s] doesn't exist" % Constant.TABLE_NAME)
        except Exception as err:
            print("Delete table failed:", err)

    def delete_database(self):
        print("Deleting Database")
        try:
            result = self.client.delete_database(DatabaseName=Constant.DATABASE_NAME)
            print("Delete database status [%s]" % result['ResponseMetadata']['HTTPStatusCode'])
        except self.client.exceptions.ResourceNotFoundException:
            print("database [%s] doesn't exist" % Constant.DATABASE_NAME)
        except Exception as err:
            print("Delete database failed:", err)

    @staticmethod
    def _print_rejected_records_exceptions(err):
        print("RejectedRecords: ", err)
        for rr in err.response["RejectedRecords"]:
            print("Rejected Index " + str(rr["RecordIndex"]) + ": " + rr["Reason"])
            if "ExistingVersion" in rr:
                print("Rejected record existing version: ", rr["ExistingVersion"])

    @staticmethod
    def _current_milli_time():
        return str(int(round(time.time() * 1000)))

    @staticmethod
    def __print_tables(tables):
        for table in tables:
            print(table['TableName'])

    @staticmethod
    def _print_databases(databases):
        for database in databases:
            print(database['DatabaseName'])
