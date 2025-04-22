import Constant
import time
from utils.WriteUtil import WriteUtil
from Constant import *


class CrudAndSimpleIngestionExample:
    def __init__(self, client):
        self.client = client
        self.write_util = WriteUtil(client)

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


        # upsert with higher version as new data in generated
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

    def run(self, kmsId):
        self.write_util.create_database(DATABASE_NAME)
        self.write_util.describe_database(DATABASE_NAME)
        self.write_util.list_databases()

        if kmsId != None:
            self.write_util.update_database(DATABASE_NAME, kmsId)
            self.write_util.describe_database(DATABASE_NAME)

        self.write_util.create_table(DATABASE_NAME, TABLE_NAME)
        self.write_util.describe_table(DATABASE_NAME, TABLE_NAME)
        self.write_util.list_tables(DATABASE_NAME)
        self.write_util.update_table(DATABASE_NAME, TABLE_NAME)

        self.write_records()
        self.write_records_with_common_attributes()

        self.write_records_with_upsert()

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
