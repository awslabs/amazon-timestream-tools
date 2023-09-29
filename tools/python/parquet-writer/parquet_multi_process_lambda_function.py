import random
import time
import boto3
import datetime
import glob
import numpy as np

import pandas as pd

data_frame = None

# from multiprocessing.sharedctypes import Value, Array
from multiprocessing import Process, Lock

from botocore.config import Config

epoch = datetime.datetime.utcfromtimestamp(0)

def unix_time_millis(dt):
    # epoch = datetime.datetime.utcfromtimestamp(0)
    return int((dt - pd.Timestamp("1970-01-01")).total_seconds() * 1000.0)

def load_parquet(folder_name):
    rows = []

    for file_name in glob.glob(folder_name + '/value.parquet'):

        df = pd.read_parquet(file_name)
        print(df)
        df_records = df.to_records()

        for df_record in df_records:
            time = unix_time_millis(df_record['time'])
            signal = df_record['signal']
            value = df_record['value']
            source = df_record['source']
            # print(df_record)
            row = {
                'time': time,
                'signal': signal,
                'value': value,
                'source': source
            }
            rows.append(row)

    return rows

class Generator:
    INTERVAL = 0.001 # Seconds
    INTERVAL_MILLI = 1
    BATCH_SIZE = 100

    table_name = ''
    database_name = ''

    def prepare_common_attributes(self):
        common_attributes = {
            #'Dimensions': [
    #            {'Name': 'country', 'Value': COUNTRY.replace('${', '$ {')},
            #    {'Name': 'country', 'Value': COUNTRY} #,
                #{'Name': 'city', 'Value': CITY},
                #{'Name': 'hostname', 'Value': HOSTNAME}
            #],
            'MeasureName': self.measure_name,
            'MeasureValueType': 'MULTI'
        }
        print(common_attributes)
        #print(COUNTRY)
        #self.variable_test(COUNTRY)
        return common_attributes

    def prepare_record(self, current_time):
        record = {
            'Time': str(current_time),
            'MeasureValues': [],
            'Dimensions':[]
        }
        return record

    def prepare_measure(self, measure_name, measure_value):
        measure = {
            'Name': measure_name,
            'Value': str(measure_value),
            'Type': 'DOUBLE'
        }
        return measure

    def prepare_dimension(self, name, value):
        dimension = {
            'Name': name,
            'Value': str(value)  #,
             #'DimensionValueType': 'VARCHAR'
        }
        return dimension

    def write_records(self, records, common_attributes):
        try:
            result = self.write_client.write_records(DatabaseName=self.database_name,
                                                TableName=self.table_name,
                                                CommonAttributes=common_attributes,
                                                Records=records)
            status = result['ResponseMetadata']['HTTPStatusCode']
            #print("Processed %d records. WriteRecords HTTPStatusCode: %s" %
            #      (len(records), status))
        except Exception as err:
            print("Error:", err)
            print(records)

    def unix_time_millis(self, dt):
        epoch = datetime.datetime.utcfromtimestamp(0)
        return (dt - epoch).total_seconds() * 1000.0

    def write_buffer(self, buffer, common_attributes):
        start_time = time.time()
        total_records = 0
        for records in buffer:
            elapsed_time = time.time() - start_time
            self.write_records(records, common_attributes)
            total_records += len(records)
            if elapsed_time == 0.0:
                elapsed_time = 0.00001
            rps = total_records/elapsed_time
        print(f'{total_records} written rps = {rps}')


    # User can change this based on there record dimension/measure value
    def create_record(self, item):
        current_time = str(item['time'])
        source = item['source']
        value = item['value']
        signal = item['signal']

        record = self.prepare_record(current_time)

        record['Dimensions'].append(self.prepare_dimension('source', source))
        record['Dimensions'].append(self.prepare_dimension('signal', signal))
        # add more Dimensions from item as needed

        record['MeasureValues'].append(self.prepare_measure('value', value))
        # append more MeasureValues as measure columns as needed

        return record

    def generate_data(self, pid, region, database_name, table_name, data):
        self.data = data

        self.database_name = database_name
        self.table_name = table_name
        print(f'writing data to database {self.database_name} table {self.table_name}')

        session = boto3.Session(region_name=region)
        self.write_client = session.client('timestream-write', config=Config(
            read_timeout=20, max_pool_connections=5000, retries={'max_attempts': 10}))

        self.measure_name = 'metric-' + str(pid % 8192)
        common_attributes = self.prepare_common_attributes()

        records = []

        total_records = 0

        launch_time = time.time()

        for item in data:
            print(item)
            record = self.create_record(item)

            records.append(record)

            if len(records) == self.BATCH_SIZE:
                total_records += len(records)

                self.write_records(records, common_attributes)

                records = []

            if self.INTERVAL > 0.0:
                time.sleep(self.INTERVAL)

        if len(records) > 0:
            total_records += len(records)

            self.write_records(records, common_attributes)

        total_time = time.time() - launch_time

        if total_time == 0:
            total_time = 0.00001
        rps = total_records / total_time
        print(f'Total Records in thread: {total_records:,} in {rps} rps')

        return total_records


def lambda_handler(event, context):

    folder_name = event['folder']
    records = load_parquet(folder_name)

    lambda_time = time.time()

    pid = 1

    max_threads = event['threads']

    processes = []
    record_counts = []

    for i in range(0, max_threads):
        id = i
        process = Process(target=thread_handler, args=(id, event, context, records))
        process.start()
        print(
            f'[{pid}] process_record: Started process #{i} with pid={process.pid} to dump data chunk to timestream')
        processes.append(process)

    # Wait for all processes to complete
    for process in processes:
        process.join()

    end_time = time.time()
    total_time = end_time - lambda_time

    s_lambda_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(lambda_time))
    s_end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))
    print(f'{s_lambda_time} - {s_end_time}')

    return

def thread_handler(pid, event, context, records):
    generator = Generator()
    region = event['region']
    database = event['database']
    table = event['table']
    threads = int(event['threads'])
    generator.generate_data(pid, region, database, table, records)

    return

if __name__ == '__main__':
    event = {
        'threads': 1,
        'region': 'us-east-1',
        'database': 'sandbox-w-642',
        'table': 'ingestion-parquet2',
        'folder': './'
    }
    context = {}
    lambda_handler(event, context)


