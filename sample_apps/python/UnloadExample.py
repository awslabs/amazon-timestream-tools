#!/usr/bin/python

import csv
import Constant
from utils.UnloadUtil import UnloadUtil
from utils.WriteUtil import WriteUtil
from utils.TimestreamDependencyHelper import TimestreamDependencyHelper


class UnloadExample:
    def __init__(self, region, write_client, query_client):
        self.region = region

        self.timestream_dependency_helper = TimestreamDependencyHelper(self.region)
        self.exported_bucket_name = Constant.S3BUCKETPREFIX +  self.region + self.timestream_dependency_helper.get_account()

        self.database_name = Constant.DATABASE_NAME
        self.table_name = Constant.TABLE_NAME
        self.write_client = write_client
        self.write_util = WriteUtil(self.write_client)
        self.query_client = query_client
        self.unload_util = UnloadUtil(self.query_client, self.timestream_dependency_helper, self.database_name, self.table_name, self.exported_bucket_name)

    def bulk_write_shopping_records(self, filepath):

        print("Ingesting to %s.%s" % (self.database_name, self.table_name))

        with open(filepath, 'r') as csvfile:
            csvreader = csv.reader(csvfile)

            records = []
            current_time = WriteUtil.current_milli_time()

            # 0 - 'channel',
            # 1 - 'ip_address',
            # 2 - 'session_id',
            # 3 - 'user_id',
            # 4 - 'event',
            # 5 - 'user_group',
            # 6 - 'current_time',
            # 7 - 'query',  - measure
            # 8 - 'product_id',  - measure
            # 9 - 'product',   -measure
            # 10 - 'quantity'  - measure

            #['Referral',
            # '63.181.186.170',
            # 'c8e5d3a8b61ba4963245eaf5e9b66e3cf0d53103d868b35b6be8955e576135f0',
            # '316223',
            # 'Search',
            # 'grp-060',
            # '1677613824427',
            # 'Blade A5 2020 2/32GB Black',
            # '',
            # '',
            # ''

            header_row = []
            # extracting each data row one by one
            row_counter = 0
            for i, row in enumerate(csvreader, 1):

                if(len(row) == 0 ):
                    continue;

                if(i == 1 ):
                    header_row = row
                    continue

                row_counter = row_counter + 1

                record = {
                    'Dimensions': [
                        {'Name': header_row[0], 'Value': row[0]},
                        {'Name': header_row[1], 'Value': row[1]},
                        {'Name': header_row[2], 'Value': row[2]},
                        {'Name': header_row[3], 'Value': row[3]},
                        {'Name': header_row[4], 'Value': row[4]},
                        {'Name': header_row[5], 'Value': row[5]},
                    ],
                    'Time': str(int(current_time) - (i * 50))
                }

                measure_values = []

                if (row[7] != ""):
                    measure_values.append( {
                        "Name": header_row[7],
                        "Value": row[7],
                        "Type": 'VARCHAR',
                    })

                if (row[8] != ""):
                    measure_values.append( {
                        "Name": header_row[8],
                        "Value": row[8],
                        "Type": 'VARCHAR',
                    })

                if (row[9] != ""):
                    measure_values.append( {
                        "Name": header_row[9],
                        "Value": row[9],
                        "Type": 'VARCHAR',
                    })

                if (row[10] != ""):
                    measure_values.append( {
                        "Name": header_row[10],
                        "Value": row[10],
                        "Type": 'DOUBLE',
                    })

                record.update(
                    {
                        'MeasureName': "metrics",
                        'MeasureValueType': "MULTI",
                        'MeasureValues': measure_values
                    }
                )
                records.append(record)

                if len(records) == 100:
                    self.__submit_batch(records, row_counter)
                    records = []

            if records:
                self.__submit_batch(records, row_counter)

            print("Ingested %d records" % row_counter)



    def __submit_batch(self, records, n):
        try:
            result = self.write_client.write_records(DatabaseName=self.database_name, TableName=self.table_name,
                                                     Records=records, CommonAttributes={})
            if result and result['ResponseMetadata']:
                print("Processed [%d] records. WriteRecords Status: [%s]" % (n, result['ResponseMetadata']['HTTPStatusCode']))
        except Exception as err:
            print("Error:", err)

    def run_sample_queries(self):
        self.unload_util.run_all_queries()

    def run(self, csv_file_path, skip_deletion):
        if not csv_file_path:
            print('csv_file_path is required')
            return


        try:
            # Create base table and ingest records
            self.write_util.create_database(self.database_name)
            self.write_util.create_table(self.database_name, self.table_name)


            self.s3_error_bucket = self.timestream_dependency_helper.create_s3_bucket(self.exported_bucket_name)

            self.bulk_write_shopping_records(csv_file_path)
            self.unload_util.run_single_query()

            # Commenting the code to run all the queries
            # self.run_sample_queries()

        finally:
            if not skip_deletion:
                self.write_util.delete_table(self.database_name, self.table_name)
                self.write_util.delete_database(self.database_name)
                self.timestream_dependency_helper.delete_s3_bucket(self.exported_bucket_name)
