import random
import string
from Constant import DATABASE_NAME
from Constant import HT_TTL_HOURS
from Constant import CT_TTL_DAYS
from utils.WriteUtil import WriteUtil
from CrudAndSimpleIngestionExample import CrudAndSimpleIngestionExample
from QueryExample import QueryExample
from utils.TimestreamDependencyHelper import TimestreamDependencyHelper

'''
This code example is to demonstrate two composite partition key samples, one with dimension and one with measure name
            
            Dimension sample
            1. Create table with dimension type partition key, with optional enforcement.
            2. Ingest records with missing partition key. Records will be accepted as enforcement level is optional
            3. Update table with required enforcement.
            4. Ingest records with same partition key.
            5. Ingest records with missing partition key. This will return rejected records as they do not
            contain required partition key.
            6. Query records with same partition key.
            
            Measure name sample
            1. Create table with measure name type partition key
            2. Ingest records and query
'''

class CompositePartitionKeyExample:
    partition_key_dimension_table_name = "host_metrics_dim_pk";
    partition_key_measure_table_name = 'host_metrics_measure_pk';

    # Composite Partition Keys are most effective when dimension has high cardinality
    # and are frequently accessed in queries.
    # Using dimension name with high cardinality, 'hostId'
    composite_partition_key_dim_name = 'hostId';
    composite_partition_key_dim_value = 'host1';
    # Different dimension name to demonstrate enforcement level and record rejection
    composite_partition_key_diff_name = 'hostIdDiff';


    def __init__(self, write_client, query_client, skip_deletion, region):
        self.skip_deletion = skip_deletion
        self.crud_and_simple_ingestion_example = CrudAndSimpleIngestionExample(write_client)
        self.query_example = QueryExample(query_client)
        self.write_util = WriteUtil(write_client)
        self.client = write_client
        self.timestream_dependency_helper = TimestreamDependencyHelper(region)
        self.s3_bucket_name = "error-configuration-sample-s3-bucket-" + \
                              ''.join(random.choices(string.ascii_lowercase + string.digits, k=5))

    def create_table(self, database_name, table_name, composite_partition_key):
        print('Creating table with table name: ' + table_name)
        # Set RetentionProperties for the table
        retention_properties = {
            'MemoryStoreRetentionPeriodInHours': HT_TTL_HOURS,
            'MagneticStoreRetentionPeriodInDays': CT_TTL_DAYS
        }
        # Set MagneticStoreWriteProperties for the table
        magnetic_store_write_properties = {
            'EnableMagneticStoreWrites': True
        }
        magnetic_store_write_properties['MagneticStoreRejectedDataLocation'] = {
            'S3Configuration': {
                'BucketName': self.s3_bucket_name,
                'EncryptionOption': "SSE_S3"
            }
        }
        # Set CompositePartitionKey for the table
        schema = {
            'CompositePartitionKey': composite_partition_key
        }
        try:
            self.client.create_table(DatabaseName=database_name, TableName=table_name,
                                     RetentionProperties=retention_properties,
                                     MagneticStoreWriteProperties=magnetic_store_write_properties,
                                     Schema=schema)
            print('Table [%s] successfully created.' % table_name)
        except self.client.exceptions.ConflictException:
            print('Table [%s] exists on database [%s]. Skipping table creation' % (
                table_name, database_name))
        except Exception as err:
            print('Create table failed:', err)
            raise err

    def describe_table(self, database_name, table_name):
        print('Describing table with table name:' + table_name)
        try:
            result = self.client.describe_table(DatabaseName=database_name, TableName=table_name)
            print("Table [%s] has id [%s]" % (table_name, result['Table']['Arn']))
            print("Table [%s] has composite partition key [%s]" % (table_name, result['Table']['Schema']))
        except self.client.exceptions.ResourceNotFoundException:
            print("Table doesn't exist")
        except Exception as err:
            print("Describe table failed:", err)

    def update_table(self, database_name, table_name, composite_partition_key):
        print('Updating table with table name: ' + table_name)
        try:
            schema = {
                'CompositePartitionKey': composite_partition_key
            }
            self.client.update_table(DatabaseName=database_name, TableName=table_name,
                                     Schema=schema)
            print('Table updated.')
        except Exception as err:
            print('Update table failed:', err)

    def write_records(self, database_name, table_name, composite_partition_key):
        current_time = self.write_util.current_milli_time()

        dimensions = [
            {'Name': 'region', 'Value': 'us-east-1'},
            {'Name': 'az', 'Value': 'az1'},
            {'Name': composite_partition_key, 'Value': 'host1'}
        ]

        common_attributes = {
            'Dimensions': dimensions,
            'Time': current_time
        }

        cpu_utilization = {
            'MeasureName': 'cpu_utilization',
            'MeasureValue': '13.5',
            'MeasureValueType': 'DOUBLE'
        }

        memory_utilization = {
            'MeasureName': 'memory_utilization',
            'MeasureValue': '40',
            'MeasureValueType': 'DOUBLE'
        }

        cpu_memory = {
            'MeasureName': 'cpu_memory',
            'MeasureValueType': "MULTI",
            'MeasureValues': [
                {
                    'Name': 'cpu_utilization',
                    'Value': '13.5',
                    'Type': 'DOUBLE'
                },
                {
                    'Name': 'memory_utilization',
                    'Value': '40',
                    'Type': 'DOUBLE'
                }
            ]
        }

        records = [cpu_utilization, memory_utilization, cpu_memory]

        try:
            result = self.client.write_records(DatabaseName=database_name, TableName=table_name,
                                               Records=records, CommonAttributes=common_attributes)
            print('WriteRecords Status: [%s]' % result['ResponseMetadata']['HTTPStatusCode'])
        except self.client.exceptions.RejectedRecordsException as err:
            self.crud_and_simple_ingestion_example._print_rejected_records_exceptions(err)
        except Exception as err:
            print('Error:', err)

    def run_single_query(self, database_name, table_name, partition_key_name, partition_key_value):
        query_string = "SELECT * FROM \"{}\".\"{}\" WHERE \"{}\"={}" \
                .format(database_name, table_name, partition_key_name, partition_key_value)
        print("Running query: " + query_string)
        self.query_example.run_query(query_string)

    def run_sample_with_dimension_partition_key(self, database_name, table_name):
        print("Starting example for dimension type partition key")

        # Create table with partition key type dimension and OPTIONAL enforcement
        partition_key_with_dimension_and_optional_enforcement = [
            {
                'Type': 'DIMENSION',
                'Name': self.composite_partition_key_dim_name,
                'EnforcementInRecord': 'OPTIONAL'
            }
        ]
        self.create_table(database_name, table_name, partition_key_with_dimension_and_optional_enforcement)
        self.describe_table(database_name, table_name)

        print("Writing records without partition key dimension into table with optional enforcement." +
              "Since the enforcement level is OPTIONAL the records will be ingested")
        self.write_records(database_name, table_name, self.composite_partition_key_diff_name)

        # Update partition key enforcement level to REQUIRED
        partition_key_with_dimension_and_required_enforcement = [
            {
                'Type': 'DIMENSION',
                'Name': self.composite_partition_key_dim_name,
                'EnforcementInRecord': 'REQUIRED'
            }
        ]
        self.update_table(database_name, table_name, partition_key_with_dimension_and_required_enforcement)
        self.describe_table(database_name, table_name)

        print("Writing records with partition key dimension")
        self.write_records(database_name, table_name, self.composite_partition_key_dim_name)
        print("Writing records without partition key dimension into table with required enforcement." +
              "Since the enforcement level is REQUIRED the records will be rejected");
        self.write_records(database_name, table_name, self.composite_partition_key_diff_name)
        # Run query with partition key
        self.run_single_query(database_name, table_name, self.composite_partition_key_dim_name,
                              "'" + self.composite_partition_key_dim_value + "'")

    def run_sample_with_measure_name_partition_key(self, database_name, table_name):
        print("Starting example for measure name type partition key")

        # Create table with measure name type partition key
        partition_key_with_measure_name = [
            {'Type': 'MEASURE'}
        ]
        self.create_table(database_name, table_name, partition_key_with_measure_name)
        self.describe_table(database_name, table_name)
        print("Writing records");
        self.write_records(database_name, table_name, self.composite_partition_key_dim_name)
        self.run_single_query(database_name, table_name, 'cpu_utilization', '13.5')

    def run(self):
        try:
            # S3 bucket creation to store rejected records for MagneticStoreWrite Upsert
            self.s3_error_bucket = self.timestream_dependency_helper.create_s3_bucket(self.s3_bucket_name)
            self.write_util.create_database(DATABASE_NAME)

            '''
            This sample demonstrates workflow using dimension type partition key
            1. Create table with dimension type partition key, with optional enforcement.
            2. Ingest records with missing partition key. Records will be accepted as enforcement level is optional
            3. Update table with required enforcement.
            4. Ingest records with same partition key.
            5. Ingest records with missing partition key. This will return rejected records as they do not
            contain required partition key.
            6. Query records with same partition key.
            '''
            self.run_sample_with_dimension_partition_key(DATABASE_NAME, self.partition_key_dimension_table_name)

            '''
            This sample demonstrates workflow using measure name type partition key
	 	    1. Create table with measure name type partition key
	 	    2. Ingest records and query
            '''
            self.run_sample_with_measure_name_partition_key(DATABASE_NAME, self.partition_key_measure_table_name)

        finally:
            if not self.skip_deletion:
                self.write_util.delete_table(DATABASE_NAME, self.partition_key_dimension_table_name)
                self.write_util.delete_table(DATABASE_NAME, self.partition_key_measure_table_name)
                self.write_util.delete_database(DATABASE_NAME)
                self.timestream_dependency_helper.delete_s3_bucket(self.s3_bucket_name)

