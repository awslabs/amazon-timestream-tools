#!/usr/bin/python

from utils.WriteUtil import WriteUtil
from utils.QueryUtil import QueryUtil


class BasicExample:
    def __init__(self, database_name, table_name, write_client, query_client, skip_deletion):
        self.database_name = database_name
        self.table_name = table_name
        self.write_client = write_client
        self.query_client = query_client
        self.skip_deletion = skip_deletion

    def write_records_with_multi_measure_value_single_record(self):
        print("Writing records for multi measure value single record")
        current_time = WriteUtil.current_milli_time()

        dimensions = [
            {'Name': 'region', 'Value': 'us-east-1'},
            {'Name': 'az', 'Value': 'az1'},
            {'Name': 'hostname', 'Value': 'host1'}
        ]

        common_attributes = {
            'Dimensions': dimensions,
            'Time': current_time
        }

        cpu_utilization = {
            'Name': 'cpu_utilization',
            'Value': '13.6',
            'Type': 'DOUBLE',
        }

        memory_utilization = {
            'Name': 'memory_utilization',
            'Value': '40',
            'Type': 'DOUBLE',
        }

        computational_record = {
            'MeasureName': 'cpu_memory',
            'MeasureValues': [cpu_utilization, memory_utilization],
            'MeasureValueType': 'MULTI',
        }

        records = [computational_record]

        try:
            result = self.write_client.write_records(DatabaseName=self.database_name, TableName=self.table_name,
                                                     Records=records, CommonAttributes=common_attributes)
            if result and result['ResponseMetadata']:
                print("WriteRecords Status: [%s]" % result['ResponseMetadata']['HTTPStatusCode'])
        except self.write_client.exceptions.RejectedRecordsException as err:
            WriteUtil.print_rejected_records_exceptions(err)
        except Exception as err:
            print("Error:", err)

    def write_records_with_multi_measure_value_multiple_records(self):
        print("Writing records for multi measure value mixture type")
        current_time = WriteUtil.current_milli_time()

        dimensions = [
            {'Name': 'region', 'Value': 'us-east-1'},
            {'Name': 'az', 'Value': 'az1'},
            {'Name': 'hostname', 'Value': 'host1'}
        ]

        common_attributes = {
            'Dimensions': dimensions,
            'Time': current_time
        }

        cpu_utilization = {
            'Name': 'cpu_utilization',
            'Value': '13.6',
            'Type': 'DOUBLE',
        }

        memory_utilization = {
            'Name': 'memory_utilization',
            'Value': '40',
            'Type': 'DOUBLE',
        }

        active_cores = {
            'Name': 'active_cores',
            'Value': '4',
            'Type': 'BIGINT'
        }

        computational_record = {
            'MeasureName': 'computational_record',
            'MeasureValues': [cpu_utilization, memory_utilization, active_cores],
            'MeasureValueType': 'MULTI'
        }

        records = [computational_record]

        try:
            result = self.write_client.write_records(DatabaseName=self.database_name, TableName=self.table_name,
                                                     Records=records, CommonAttributes=common_attributes)
            if result and result['ResponseMetadata']:
                print("WriteRecords Status: [%s]" % result['ResponseMetadata']['HTTPStatusCode'])
        except self.write_client.exceptions.RejectedRecordsException as err:
            WriteUtil.print_rejected_records_exceptions(err)
        except Exception as err:
            print("Error:", err)

    def run(self, kms_id):
        write_util = WriteUtil(self.write_client)
        query_util = QueryUtil(self.query_client, self.database_name, self.table_name)

        try:
            write_util.create_database(self.database_name)
            write_util.describe_database(self.database_name)
            write_util.list_databases()

            if kms_id:
                write_util.update_database(kms_id)
                write_util.describe_database(self.database_name)

            write_util.create_table(self.database_name, self.table_name)
            write_util.describe_table(self.database_name, self.table_name)
            write_util.list_tables(self.database_name)
            write_util.update_table(self.database_name, self.table_name)

            self.write_records_with_multi_measure_value_single_record()
            self.write_records_with_multi_measure_value_multiple_records()
            query_util.run_query_with_multiple_pages(10)

        finally:
            if not self.skip_deletion:
                write_util.delete_table(self.database_name, self.table_name)
                write_util.delete_database(self.database_name)
