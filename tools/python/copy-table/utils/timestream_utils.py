import boto3
import backoff
from utils.logger_utils import create_logger
import threading
import queue
import botocore


class timestreamUtility:
    def __init__(self, region, database, table, sns_topic_arn):
        botocore_config = botocore.config.Config(
            max_pool_connections=5000, retries={'max_attempts': 10})
        self.timestream_client = boto3.client(
            'timestream-write', region_name=region)
        self.sns_client = boto3.client(
            'sns', region_name=region, config=botocore_config)
        self.logger = create_logger("timestream_logger")
        self.database = database
        self.table = table
        self.sns_topic_arn = sns_topic_arn
        self.partition_queue = queue.Queue()

    def backoff_handler(details):
        print(
            f"Retrying batch load task status in {details['wait']:.2f} seconds...")

    def sns_publish_message(self, message, subject, message_structure='email'):
        response = self.sns_client.publish(
            TopicArn=self.sns_topic_arn, Message=message, Subject=subject, MessageStructure=message_structure)
        print(response)

    @backoff.on_predicate(backoff.constant, predicate=lambda r: r['BatchLoadTaskDescription']['TaskStatus'] in ['CREATED', 'IN_PROGRESS'], interval=10, jitter=None, max_tries=720, on_backoff=backoff_handler)
    def check_task_status(self, batch_load_task_id, s3_target_bucket_location, partition):
        response = self.timestream_client.describe_batch_load_task(
            TaskId=batch_load_task_id
        )
        task_status = response['BatchLoadTaskDescription']['TaskStatus']

        self.logger.info(f"batch load for partition : {partition} taskid : {batch_load_task_id} status ----> {task_status} \n Progress report : {response['BatchLoadTaskDescription']['ProgressReport']}")


        if task_status in ['PROGRESS_STOPPED', 'FAILED', 'PENDING_RESUME']:
            self.logger.error(
                response['BatchLoadTaskDescription']['ProgressReport'])
            error_message = f"Location of job logging reports {response['BatchLoadTaskDescription']['ReportConfiguration']}"
            # sns notification
            exception_message = f'Timestream batchload job for source bucket {s3_target_bucket_location} and partition {partition} associated to timestream batchload task_id {batch_load_task_id} is {task_status}. \n{error_message}'
            if self.sns_topic_arn is not None:
                self.sns_publish_message(
                    exception_message, f"timestream batchload status")
            raise Exception(exception_message)
        return response

    def timestream_batch_load(self, folder, data_model, s3_target_bucket_location, s3_error_bucket_location):
        try:
            result = self.timestream_client.create_batch_load_task(TargetDatabaseName=self.database, TargetTableName=self.table,
                                                                   DataModelConfiguration={"DataModel": data_model
                                                                                           },
                                                                   DataSourceConfiguration={
                                                                       "DataSourceS3Configuration": {
                                                                           "BucketName": s3_target_bucket_location,
                                                                           "ObjectKeyPrefix": folder
                                                                       },
                                                                       "DataFormat": "CSV"
                                                                   },
                                                                   ReportConfiguration={
                                                                       "ReportS3Configuration": {
                                                                           "BucketName":  s3_error_bucket_location,
                                                                           "ObjectKeyPrefix": "batch_load_errors",
                                                                           "EncryptionOption": "SSE_S3",
                                                                       }
                                                                   }
                                                                   )
            task_id = result["TaskId"]
            self.logger.info(
                f" Successfully created batch load task for partition {folder}:{task_id} \n")
            self.check_task_status(task_id, s3_target_bucket_location, folder)
        except Exception as err:
            self.logger.error(f"Create batch load task job failed:{err}")
            raise

    def create_timestream_res(self, partition_key, memory_store_retenion_in_hours, magnetic_store_retention_in_days):

        # create database if not exists
        self.logger.info("Creating Database")
        try:
            self.timestream_client.create_database(DatabaseName=self.database)
            self.logger.info(f"Database {self.database} created successfully")
        except self.timestream_client.exceptions.ConflictException:
            self.logger.info(
                f"Database {self.database} exists. Skipping database creation")
        except Exception as err:
            self.logger.info(f"Create database failed with error : {err}")
            raise

        # create table
        self.logger.info("Creating table")
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
            self.timestream_client.create_table(DatabaseName=self.database, TableName=self.table,
                                                RetentionProperties=retention_properties,
                                                MagneticStoreWriteProperties=magnetic_store_write_properties,
                                                Schema=schema
                                                )
            self.logger.info(f"Table {self.table} successfully created")
        except self.timestream_client.exceptions.ConflictException:
            self.logger.info(
                f"Table {self.table} exists on database {self.database}. Skipping table creation")
        except Exception as err:
            self.logger.error(f"Create table failed: {err}")
            raise

    def thread_handler(self, data_model, s3_target_bucket_location, s3_error_bucket_location):
        while True:
            try:
                # Get the next partition from the queue
                partition = self.partition_queue.get_nowait()
            except queue.Empty:
                # If the queue is empty, break the loop
                break
            self.timestream_batch_load(
                partition, data_model, s3_target_bucket_location, s3_error_bucket_location)
            # can add wait time logic sleep(2 hours) time.sleep(sleep_interval_in_seconds) here based on the volume skipping here as this is just a sample.
            self.partition_queue.task_done()

    def multi_thread_handler(self, threads_count, partition_list, data_model, s3_target_bucket_location, s3_error_bucket_location):

        # Fill the queue with partitions
        for partition in partition_list:
            self.partition_queue.put(partition)

        threads = []

        for i in range(threads_count):
            thread = threading.Thread(target=self.thread_handler, args=(
                data_model, s3_target_bucket_location, s3_error_bucket_location))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()
