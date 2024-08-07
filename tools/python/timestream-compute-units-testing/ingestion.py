import random
import time
import boto3
import datetime
import threading
from botocore.config import Config

class Generator:
    INTERVAL = 1 # Seconds
    INTERVAL_MILLI = 100
    BATCH_SIZE = 100


    def __init__(self):
        self.time_lock = threading.Lock()


    def prepare_common_attributes(self):
        common_attributes = {
            'MeasureName': self.measure_name,
            'MeasureValueType': 'MULTI'
        }
        print(common_attributes)
        return common_attributes

    def prepare_record(self, current_time):
        record = {
            'Time': str(current_time),
            'TimeUnit': 'SECONDS',
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
            'Value': str(value),
        }
        return dimension

    def write_records(self, records, common_attributes):
        try:
            result = self.write_client.write_records(DatabaseName=self.database_name,
                                                TableName=self.table_name,
                                                CommonAttributes=common_attributes,
                                                Records=records)
            status = result['ResponseMetadata']['HTTPStatusCode']
            print("Processed %d records. WriteRecords HTTPStatusCode: %s" %
                  (len(records), status))
            print(result)
        except Exception as err:
            print("Error:", err)
            print(f'Error ingesting data for : {str(err.response["RejectedRecords"])}')

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

    def generate_data(self, pid, region, database_name, table_name, max_records):

        self.database_name = database_name
        self.table_name = table_name
        print(f'writing data to database {self.database_name} table {self.table_name}')

        session = boto3.Session(region_name=region)
        self.write_client = session.client('timestream-write', config=Config(
            read_timeout=20, max_pool_connections=5000, retries={'max_attempts': 10}))

        self.measure_name = f"metric{pid % 8192}"
        common_attributes = self.prepare_common_attributes()

        records = []

        total_records = 0

        launch_time = time.time()
        current_time_seconds = int(datetime.datetime.now().timestamp())
        current_time_nanoseconds = current_time_seconds * 10**9
        time_delta_seconds = 365 * 24 * 60 * 60  # 365 days in seconds
        host_number = 0


        for i in range(0, int(max_records)):
            current_time = int(time.time())
            record = self.prepare_record(current_time)
            host_number += 1

            record['Dimensions'].append(self.prepare_dimension('hostname', f"host{host_number}"))
            record['Dimensions'].append(self.prepare_dimension('region', "us-east-1"))
            record['Dimensions'].append(self.prepare_dimension('az', f"az{int(random.randint(1,6))}"))
            record['MeasureValues'].append(self.prepare_measure('cpu_utilization', float(random.randint(0, 1000)) / 10.0))
            record['MeasureValues'].append(self.prepare_measure('memory', float(random.randint(0, 1000)) / 10.0))
            record['MeasureValues'].append(self.prepare_measure('network_in', float(random.randint(0, 1000)) / 10.0))
            record['MeasureValues'].append(self.prepare_measure('network_out', float(random.randint(0, 1000)) / 10.0))
            record['MeasureValues'].append(self.prepare_measure('disk_read_ops', float(random.randint(0, 1000)) / 10.0))
            record['MeasureValues'].append(self.prepare_measure('dicsk_write_iops', float(random.randint(0, 1000)) / 10.0))
            records.append(record)


            if len(records) == self.BATCH_SIZE:
                total_records += len(records)

                self.write_records(records, common_attributes)

                if self.INTERVAL > 0.0:
                    time.sleep(self.INTERVAL)
                    host_number = 0
                records = []

        if len(records) > 0:
            total_records += len(records)
            self.write_records(records, common_attributes)

        total_time = time.time() - launch_time
        rps = total_records / total_time
        print(f'Total Records in thread: {total_records:,} in {rps} rps')

        return total_records


def lambda_handler(event, context):
    lambda_time = time.time()
    pid = 1
    max_threads = event['threads']
    threads = []
    record_counts = []

    for i in range(0, max_threads):
        id = i
        thread = threading.Thread(target=thread_handler, args=(id, event, context))
        thread.start()
        print(
            f'[{pid}] process_record: Started process #{i} with pid={thread} to dump data chunk to timestream')
        time.sleep(0.1)
        threads.append(thread)

    for thread in threads:
        thread.join()

    end_time = time.time()
    total_time = end_time - lambda_time
    total_records = int(event['records'])
    rps = total_records / total_time
    s_lambda_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(lambda_time))
    s_end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))
    print(f'{s_lambda_time} - {s_end_time}')
    print(f'Total Records in lambda: {total_records:,} in {rps} rps')

    return

def thread_handler(pid, event, context):
    generator = Generator()
    region = event['region']
    database = event['database']
    table = event['table']
    threads = int(event['threads'])
    max_records_per_thread = int(event['records']) / threads
    generator.generate_data(pid, region, database, table, max_records_per_thread)
    return

if __name__ == '__main__':
    event = {
        'threads': 1,
        'records': 1000000000,
        'region': 'us-east-2',
        'database': 'devops',
        'table': 'sample_devops'
    }
    context = {}
    lambda_handler(event, context)


